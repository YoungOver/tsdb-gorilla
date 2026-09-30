package store

import (
	"math"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

// Aggregates must equal a naive computation over the raw samples.
func TestQueryMatchesNaive(t *testing.T) {
	s := New()
	r := rand.New(rand.NewPCG(7, 8))
	key := []byte("load{host=a}")
	var raw []Point
	ts := int64(0)
	for i := 0; i < 5000; i++ {
		ts += 1 + r.Int64N(20_000)
		v := math.Round(r.NormFloat64()*1000) / 10
		if !s.Append(key, ts, v) {
			t.Fatal("rejected in-order sample")
		}
		raw = append(raw, Point{ts, v})
	}
	from, to, step := raw[100].T, raw[4000].T, int64(60_000)
	for _, agg := range []string{"avg", "min", "max", "sum", "count", "last"} {
		got, err := s.Query(string(key), from, to, step, agg)
		if err != nil {
			t.Fatal(err)
		}
		want := naive(raw, from, to, step, agg)
		if len(got) != len(want) {
			t.Fatalf("%s: %d windows, want %d", agg, len(got), len(want))
		}
		for i := range want {
			if got[i].T != want[i].T || math.Abs(got[i].V-want[i].V) > 1e-9*math.Max(1, math.Abs(want[i].V)) {
				t.Fatalf("%s window %d: got %+v want %+v", agg, i, got[i], want[i])
			}
		}
	}
}

func naive(raw []Point, from, to, step int64, agg string) []Point {
	var out []Point
	var w int64 = -1
	var vals []float64
	emit := func() {
		if len(vals) == 0 {
			return
		}
		var v float64
		switch agg {
		case "avg", "sum":
			for _, x := range vals {
				v += x
			}
			if agg == "avg" {
				v /= float64(len(vals))
			}
		case "min":
			v = math.Inf(1)
			for _, x := range vals {
				v = math.Min(v, x)
			}
		case "max":
			v = math.Inf(-1)
			for _, x := range vals {
				v = math.Max(v, x)
			}
		case "count":
			v = float64(len(vals))
		case "last":
			v = vals[len(vals)-1]
		}
		out = append(out, Point{w, v})
	}
	for _, p := range raw {
		if p.T < from || p.T > to {
			continue
		}
		pw := p.T - p.T%step
		if pw != w {
			emit()
			w, vals = pw, vals[:0]
		}
		vals = append(vals, p.V)
	}
	emit()
	return out
}

func TestRateHandlesCounterReset(t *testing.T) {
	s := New()
	k := []byte("req_total")
	// 0..300 over 30s, reset to 0 at 40s, then up to 200: total increase 500 over 60s
	for i, v := range []float64{0, 100, 200, 300, 0, 100, 200} {
		s.Append(k, int64(i)*10_000, v)
	}
	got, _ := s.Query("req_total", 0, 60_000, 120_000, "rate")
	if len(got) != 1 || math.Abs(got[0].V-500.0/60) > 1e-9 {
		t.Fatalf("rate = %+v, want 8.33/s", got)
	}
}

func TestOutOfOrderRejected(t *testing.T) {
	s := New()
	k := []byte("x")
	s.Append(k, 10, 1)
	if s.Append(k, 10, 2) || s.Append(k, 5, 3) {
		t.Fatal("accepted non-increasing timestamp")
	}
	for i := 0; i < ChunkSize; i++ {
		s.Append(k, int64(11+i), 0)
	}
	if s.Append(k, 12, 0) {
		t.Fatal("accepted timestamp older than a sealed chunk")
	}
}

func TestRetention(t *testing.T) {
	s := New()
	now := time.UnixMilli(10_000_000)
	for i := 0; i < ChunkSize*3; i++ {
		s.Append([]byte("old"), int64(i), 1)
	}
	s.Append([]byte("fresh"), now.UnixMilli(), 1)
	s.Retain(now, time.Hour)
	if s.lookup("old") != nil || s.lookup("fresh") == nil {
		t.Fatal("retention removed the wrong series")
	}
}

func TestConcurrentWritersAndReaders(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := []byte{'s', byte('a' + w)}
			for i := 0; i < 20_000; i++ {
				s.Append(key, int64(i), float64(i))
			}
		}(w)
	}
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				s.Query("sa", 0, 1<<40, 1000, "avg")
			}
		}
	}()
	wg.Wait()
	close(stop)
	if got := s.Points.Load(); got != 8*20_000 {
		t.Fatalf("points = %d", got)
	}
	pts, _ := s.Query("sh", 0, 1<<40, 0, "raw")
	if len(pts) != 20_000 || pts[19_999].V != 19_999 {
		t.Fatalf("readback: %d points", len(pts))
	}
}

func BenchmarkAppendParallel(b *testing.B) {
	s := New()
	keys := make([][]byte, 10_000)
	for i := range keys {
		keys[i] = []byte("m{host=h" + string(rune('a'+i%26)) + string(rune('a'+i/26%26)) + string(rune('a'+i/676)) + "}")
	}
	var ctr int64
	var mu sync.Mutex
	b.RunParallel(func(pb *testing.PB) {
		mu.Lock()
		ctr++
		base := ctr * 1_000_000_000
		mu.Unlock()
		i := int64(0)
		for pb.Next() {
			i++
			s.Append(keys[(base+i)%int64(len(keys))], base+i, float64(i&1023))
		}
	})
}
