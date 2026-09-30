// Package wal is a segmented write-ahead log with group commit.
//
// Concurrent Write calls are merged by a single writer goroutine into one
// write(2) and at most one fsync, so durability costs one disk flush per batch
// of requests instead of one per request. Records are len|crc32c|payload;
// a torn tail left by a crash is truncated on replay.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	SegmentSize = 64 << 20
	maxRecord   = 32 << 20
	maxBatch    = 512
)

var (
	table     = crc32.MakeTable(crc32.Castagnoli)
	ErrClosed = errors.New("wal: closed")
)

type req struct {
	data []byte
	done chan error
}

type WAL struct {
	dir   string
	fsync bool
	seg   *os.File
	size  int64
	reqs  chan req
	quit  chan struct{}
	done  chan struct{}
	buf   []byte

	Batches, Records, Syncs int64 // read after Close, or approximately while running
}

func segName(start time.Time) string { return fmt.Sprintf("%020d.wal", start.UnixMilli()) }

func Open(dir string, fsync bool) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, fsync: fsync, reqs: make(chan req, 4096), quit: make(chan struct{}), done: make(chan struct{})}
	if err := w.rotate(); err != nil {
		return nil, err
	}
	go w.loop()
	return w, nil
}

func (w *WAL) rotate() error {
	if w.seg != nil {
		if err := w.seg.Sync(); err != nil {
			return err
		}
		w.seg.Close()
	}
	f, err := os.OpenFile(filepath.Join(w.dir, segName(time.Now())), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.seg, w.size = f, 0
	return nil
}

// Write blocks until data is in the OS page cache (fsync=false) or on disk (fsync=true).
func (w *WAL) Write(data []byte) error {
	if len(data) > maxRecord {
		return fmt.Errorf("wal: record of %d bytes exceeds %d", len(data), maxRecord)
	}
	done := make(chan error, 1)
	select {
	case w.reqs <- req{data, done}:
	case <-w.quit:
		return ErrClosed
	}
	return <-done
}

func (w *WAL) loop() {
	batch := make([]req, 0, maxBatch)
	for {
		select {
		case r := <-w.reqs:
			batch = append(batch[:0], r)
		drain:
			for len(batch) < maxBatch {
				select {
				case r := <-w.reqs:
					batch = append(batch, r)
				default:
					break drain
				}
			}
			err := w.commit(batch)
			for _, r := range batch {
				r.done <- err
			}
		case <-w.quit:
			for {
				select {
				case r := <-w.reqs:
					r.done <- ErrClosed
				default:
					close(w.done)
					return
				}
			}
		}
	}
}

func (w *WAL) commit(batch []req) error {
	w.buf = w.buf[:0]
	for _, r := range batch {
		w.buf = binary.LittleEndian.AppendUint32(w.buf, uint32(len(r.data)))
		w.buf = binary.LittleEndian.AppendUint32(w.buf, crc32.Checksum(r.data, table))
		w.buf = append(w.buf, r.data...)
	}
	if _, err := w.seg.Write(w.buf); err != nil {
		return err
	}
	w.size += int64(len(w.buf))
	w.Batches++
	w.Records += int64(len(batch))
	if w.fsync {
		if err := w.seg.Sync(); err != nil {
			return err
		}
		w.Syncs++
	}
	if w.size >= SegmentSize {
		return w.rotate()
	}
	return nil
}

func (w *WAL) Close() error {
	close(w.quit)
	<-w.done
	if err := w.seg.Sync(); err != nil {
		return err
	}
	return w.seg.Close()
}

func segments(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".wal") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out) // zero-padded start time sorts chronologically
	return out, nil
}

// Replay feeds every intact record to fn in write order. A damaged tail is truncated.
func Replay(dir string, fn func([]byte) error) (records int, err error) {
	segs, err := segments(dir)
	if err != nil {
		return 0, err
	}
	for _, p := range segs {
		n, err := replaySegment(p, fn)
		records += n
		if err != nil {
			return records, err
		}
	}
	return records, nil
}

func replaySegment(path string, fn func([]byte) error) (int, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var off int64
	hdr := make([]byte, 8)
	n := 0
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			if err == io.EOF {
				return n, nil
			}
			return n, f.Truncate(off)
		}
		size := binary.LittleEndian.Uint32(hdr)
		sum := binary.LittleEndian.Uint32(hdr[4:])
		if size > maxRecord {
			return n, f.Truncate(off)
		}
		body := make([]byte, size)
		if _, err := io.ReadFull(r, body); err != nil || crc32.Checksum(body, table) != sum {
			return n, f.Truncate(off)
		}
		if err := fn(body); err != nil {
			return n, err
		}
		n++
		off += int64(8 + size)
	}
}

// Prune deletes segments whose successor started before cutoff: all their
// records are older than the retention window. The newest segment is never removed.
func Prune(dir string, cutoff time.Time) (removed int, err error) {
	segs, err := segments(dir)
	if err != nil {
		return 0, err
	}
	for i := 0; i+1 < len(segs); i++ {
		next, err := strconv.ParseInt(strings.TrimSuffix(filepath.Base(segs[i+1]), ".wal"), 10, 64)
		if err != nil || next >= cutoff.UnixMilli() {
			break
		}
		if err := os.Remove(segs[i]); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}
