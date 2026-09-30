// Package store keeps compressed series in memory, sharded by key hash so that
// concurrent writers touching different series never contend on one lock.
package store

import (
	"context"
	"errors"
	"hash/maphash"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const shardCount = 256

type shard struct {
	mu sync.RWMutex
	m  map[string]*Series
}

type Store struct {
	seed   maphash.Seed
	shards [shardCount]shard

	Points    atomic.Int64
	Rejected  atomic.Int64
	Dropped   atomic.Int64
	SeriesNum atomic.Int64
}

func New() *Store {
	s := &Store{seed: maphash.MakeSeed()}
	for i := range s.shards {
		s.shards[i].m = make(map[string]*Series)
	}
	return s
}

func (s *Store) shardFor(key []byte) *shard {
	return &s.shards[maphash.Bytes(s.seed, key)%shardCount]
}

// get finds or creates a series. The map lookup with string(key) does not allocate.
func (s *Store) get(key []byte) *Series {
	sh := s.shardFor(key)
	sh.mu.RLock()
	se := sh.m[string(key)]
	sh.mu.RUnlock()
	if se != nil {
		return se
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if se = sh.m[string(key)]; se == nil {
		se = newSeries()
		sh.m[string(key)] = se
		s.SeriesNum.Add(1)
	}
	return se
}

// Append writes one sample. Key must be canonical (see lineproto).
func (s *Store) Append(key []byte, t int64, v float64) bool {
	se := s.get(key)
	se.mu.Lock()
	ok := se.Append(t, v)
	se.mu.Unlock()
	if ok {
		s.Points.Add(1)
	} else {
		s.Rejected.Add(1)
	}
	return ok
}

func (s *Store) lookup(key string) *Series {
	sh := s.shardFor([]byte(key))
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.m[key]
}

type Point struct {
	T int64   `json:"t"`
	V float64 `json:"v"`
}

var (
	ErrNoSeries = errors.New("series not found")
	ErrBadAgg   = errors.New("agg must be one of raw, avg, min, max, sum, count, last, rate")
)

// Query returns raw samples, or one aggregated point per step-aligned window.
// rate is per-second increase within the window, with counter resets handled.
func (s *Store) Query(key string, from, to, step int64, agg string) ([]Point, error) {
	se := s.lookup(key)
	if se == nil {
		return nil, ErrNoSeries
	}
	if agg == "" || agg == "raw" || step <= 0 {
		var out []Point
		err := se.Scan(from, to, func(t int64, v float64) { out = append(out, Point{t, v}) })
		return out, err
	}
	type acc struct {
		start, n           int64
		sum, min, max      float64
		first, last, reset float64
		firstT, lastT      int64
	}
	var out []Point
	var cur *acc
	flush := func() {
		if cur == nil || cur.n == 0 {
			return
		}
		var v float64
		switch agg {
		case "avg":
			v = cur.sum / float64(cur.n)
		case "min":
			v = cur.min
		case "max":
			v = cur.max
		case "sum":
			v = cur.sum
		case "count":
			v = float64(cur.n)
		case "last":
			v = cur.last
		case "rate":
			if cur.lastT == cur.firstT {
				return
			}
			v = (cur.last - cur.first + cur.reset) / (float64(cur.lastT-cur.firstT) / 1000)
		}
		out = append(out, Point{cur.start, v})
	}
	switch agg {
	case "avg", "min", "max", "sum", "count", "last", "rate":
	default:
		return nil, ErrBadAgg
	}
	err := se.Scan(from, to, func(t int64, v float64) {
		w := t - ((t%step)+step)%step
		if cur == nil || cur.start != w {
			flush()
			cur = &acc{start: w, min: math.Inf(1), max: math.Inf(-1), first: v, firstT: t}
		} else if v < cur.last {
			cur.reset += cur.last // counter reset: count the value reached before it
		}
		cur.n++
		cur.sum += v
		cur.min = min(cur.min, v)
		cur.max = max(cur.max, v)
		cur.last, cur.lastT = v, t
	})
	flush()
	return out, err
}

// Series lists keys starting with prefix, sorted, capped at limit.
func (s *Store) Series(prefix string, limit int) []string {
	var out []string
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for k := range sh.m {
			if strings.HasPrefix(k, prefix) {
				out = append(out, k)
			}
		}
		sh.mu.RUnlock()
	}
	sort.Strings(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// Footprint walks every series; use for /stats, not on the hot path.
func (s *Store) Footprint() (bytes, points int64) {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for _, se := range sh.m {
			se.mu.Lock()
			b, p := se.Bytes()
			se.mu.Unlock()
			bytes += int64(b)
			points += int64(p)
		}
		sh.mu.RUnlock()
	}
	return
}

// Retain drops chunks older than keep and removes empty series.
func (s *Store) Retain(now time.Time, keep time.Duration) {
	cutoff := now.Add(-keep).UnixMilli()
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for k, se := range sh.m {
			d, empty := se.dropBefore(cutoff)
			s.Dropped.Add(int64(d))
			if empty {
				delete(sh.m, k)
				s.SeriesNum.Add(-1)
			}
		}
		sh.mu.Unlock()
	}
}

func (s *Store) Janitor(ctx context.Context, every, keep time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			s.Retain(now, keep)
		}
	}
}
