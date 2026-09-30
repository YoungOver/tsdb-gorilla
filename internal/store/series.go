package store

import (
	"sync"

	"github.com/YoungOver/tsdb-gorilla/internal/gorilla"
)

// ChunkSize is the number of samples per sealed chunk (same as Prometheus).
const ChunkSize = 120

type chunk struct {
	minT, maxT int64
	n          int
	data       []byte
}

// Series is one time series: sealed immutable chunks plus a mutable head.
type Series struct {
	mu     sync.Mutex
	sealed []chunk
	head   *gorilla.Encoder
	headT0 int64
}

func newSeries() *Series { return &Series{head: gorilla.NewEncoder()} }

// Append returns false for out-of-order or duplicate timestamps.
func (s *Series) Append(t int64, v float64) bool {
	if s.head.Len() > 0 && t <= s.head.LastT() {
		return false
	}
	if s.head.Len() == 0 && len(s.sealed) > 0 && t <= s.sealed[len(s.sealed)-1].maxT {
		return false
	}
	if s.head.Len() == 0 {
		s.headT0 = t
	}
	s.head.Append(t, v)
	if s.head.Len() == ChunkSize {
		s.seal()
	}
	return true
}

func (s *Series) seal() {
	b := s.head.Bytes()
	data := make([]byte, len(b)) // shrink-to-fit: the encoder over-allocates
	copy(data, b)
	s.sealed = append(s.sealed, chunk{minT: s.headT0, maxT: s.head.LastT(), n: s.head.Len(), data: data})
	s.head = gorilla.NewEncoder()
}

// Bytes reports compressed size (sealed + head).
func (s *Series) Bytes() (bytes, points int) {
	for _, c := range s.sealed {
		bytes += len(c.data)
		points += c.n
	}
	return bytes + len(s.head.Bytes()), points + s.head.Len()
}

// Scan calls fn for every sample in [from, to]. The head is copied under the lock,
// sealed chunks are immutable, so decoding happens without blocking writers.
func (s *Series) Scan(from, to int64, fn func(t int64, v float64)) error {
	s.mu.Lock()
	sealed := s.sealed
	var head chunk
	if n := s.head.Len(); n > 0 {
		b := s.head.Bytes()
		head = chunk{minT: s.headT0, maxT: s.head.LastT(), n: n, data: append([]byte(nil), b...)}
	}
	s.mu.Unlock()

	for _, c := range append(sealed[:len(sealed):len(sealed)], head) {
		if c.n == 0 || c.maxT < from || c.minT > to {
			continue
		}
		it := gorilla.NewIterator(c.data, c.n)
		for it.Next() {
			t, v := it.At()
			if t > to {
				break
			}
			if t >= from {
				fn(t, v)
			}
		}
		if err := it.Err(); err != nil {
			return err
		}
	}
	return nil
}

// dropBefore removes sealed chunks that end before cutoff; reports if the series is now empty.
func (s *Series) dropBefore(cutoff int64) (dropped int, empty bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := 0
	for i < len(s.sealed) && s.sealed[i].maxT < cutoff {
		dropped += s.sealed[i].n
		i++
	}
	if i > 0 {
		s.sealed = append([]chunk(nil), s.sealed[i:]...)
	}
	return dropped, len(s.sealed) == 0 && (s.head.Len() == 0 || s.head.LastT() < cutoff)
}
