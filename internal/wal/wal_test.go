package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestConcurrentWritesReplayInFull(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	const writers, each = 16, 200
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if err := w.Write([]byte(fmt.Sprintf("g%d-%d", g, i))); err != nil {
					t.Error(err)
				}
			}
		}(g)
	}
	wg.Wait()
	w.Close()
	if w.Syncs >= writers*each {
		t.Errorf("group commit did not merge: %d fsyncs for %d records", w.Syncs, writers*each)
	}
	t.Logf("%d records in %d batches (%.1f per fsync)", w.Records, w.Batches, float64(w.Records)/float64(w.Syncs))

	seen := map[string]bool{}
	n, err := Replay(dir, func(b []byte) error { seen[string(b)] = true; return nil })
	if err != nil || n != writers*each || len(seen) != writers*each {
		t.Fatalf("replayed %d records (%d unique), err %v", n, len(seen), err)
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir, false)
	for i := 0; i < 10; i++ {
		w.Write(bytes.Repeat([]byte{byte(i)}, 100))
	}
	w.Close()
	segs, _ := segments(dir)
	f, _ := os.OpenFile(segs[0], os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{50, 0, 0, 0, 1, 2, 3, 4, 9, 9}) // header promises 50 bytes, only 2 arrive
	f.Close()

	n, err := Replay(dir, func([]byte) error { return nil })
	if err != nil || n != 10 {
		t.Fatalf("replay: %d records, err %v", n, err)
	}
	st, _ := os.Stat(segs[0])
	if st.Size() != 10*(8+100) {
		t.Fatalf("tail not truncated: size %d", st.Size())
	}
}

func TestPruneKeepsNewest(t *testing.T) {
	dir := t.TempDir()
	base := time.UnixMilli(1_000_000)
	for i := 0; i < 4; i++ {
		os.WriteFile(filepath.Join(dir, segName(base.Add(time.Duration(i)*time.Hour))), nil, 0o644)
	}
	removed, err := Prune(dir, base.Add(150*time.Minute))
	if err != nil || removed != 2 {
		t.Fatalf("removed %d, err %v", removed, err)
	}
	left, _ := segments(dir)
	if len(left) != 2 {
		t.Fatalf("left %d segments", len(left))
	}
}
