package gorilla

import (
	"math"
	"math/rand/v2"
	"testing"
)

type pt struct {
	t int64
	v float64
}

func roundTrip(t *testing.T, pts []pt) *Encoder {
	t.Helper()
	e := NewEncoder()
	for _, p := range pts {
		e.Append(p.t, p.v)
	}
	it := NewIterator(e.Bytes(), e.Len())
	for i := 0; it.Next(); i++ {
		gt, gv := it.At()
		if gt != pts[i].t || math.Float64bits(gv) != math.Float64bits(pts[i].v) {
			t.Fatalf("point %d: got (%d, %v) want (%d, %v)", i, gt, gv, pts[i].t, pts[i].v)
		}
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}
	return e
}

func TestRoundTripRandom(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for trial := 0; trial < 300; trial++ {
		n := 1 + r.IntN(500)
		ts := int64(1_700_000_000_000 + r.Int64N(1e9))
		v := r.NormFloat64() * 100
		pts := make([]pt, n)
		for i := range pts {
			switch r.IntN(6) {
			case 0:
				ts += 1 + r.Int64N(1<<40) // huge jump: 64-bit dod bucket
			case 1:
				ts += 1 + r.Int64N(5000)
			default:
				ts += 15_000 // regular scrape interval
			}
			switch r.IntN(5) {
			case 0:
				v = r.NormFloat64() * 1e6
			case 1:
				v = math.Float64frombits(r.Uint64()) // any bit pattern, including NaN and Inf
			case 2:
				// unchanged value
			default:
				v += r.NormFloat64()
			}
			pts[i] = pt{ts, v}
		}
		roundTrip(t, pts)
	}
}

func TestDoDBucketEdges(t *testing.T) {
	var pts []pt
	ts := int64(0)
	delta := int64(1000)
	for _, n := range []int64{7, 9, 12} {
		for _, dod := range []int64{1<<(n-1) - 1, 1 << (n - 1), -(1 << (n - 1)) + 1, -(1 << (n - 1)), 1<<(n-1) + 1} {
			delta += dod
			ts += delta
			pts = append(pts, pt{ts, float64(len(pts))})
		}
	}
	// delta may go negative above; keep the stream valid by rebasing
	for i := 1; i < len(pts); i++ {
		if pts[i].t <= pts[i-1].t {
			pts[i].t = pts[i-1].t + 1
		}
	}
	roundTrip(t, pts)
}

// Compression depends on the data shape, so each shape gets its own budget.
func TestCompressionByShape(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	shapes := []struct {
		name   string
		budget float64
		next   func(prev float64) float64
	}{
		{"constant gauge", 0.45, func(p float64) float64 { return p }},
		{"counter +0..3", 2.5, func(p float64) float64 { return p + float64(r.IntN(4)) }},
		{"sparse changes", 1.5, func(p float64) float64 {
			if r.IntN(10) == 0 {
				return float64(r.IntN(100))
			}
			return p
		}},
		{"noisy gauge", 8, func(p float64) float64 { return p + math.Round(r.NormFloat64()*10)/10 }},
	}
	for _, s := range shapes {
		e := NewEncoder()
		ts, v := int64(1_700_000_000_000), 50.0
		const n = 120
		for i := 0; i < n; i++ {
			ts += 15_000
			v = s.next(v)
			e.Append(ts, v)
		}
		bpp := float64(len(e.Bytes())) / n
		t.Logf("%-15s %.2f bytes/point (raw 16)", s.name, bpp)
		if bpp > s.budget {
			t.Errorf("%s: %.2f bytes/point, budget %.2f", s.name, bpp, s.budget)
		}
	}
}

func BenchmarkAppend(b *testing.B) {
	e := NewEncoder()
	ts := int64(0)
	v := 0.0
	for i := 0; i < b.N; i++ {
		if e.Len() == 120 {
			e = NewEncoder()
		}
		ts += 15_000
		v += 0.5
		e.Append(ts, v)
	}
}

func BenchmarkIterate(b *testing.B) {
	e := NewEncoder()
	for i := 0; i < 120; i++ {
		e.Append(int64(i)*15_000, float64(i%17)*1.25)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i += 120 {
		it := NewIterator(e.Bytes(), e.Len())
		for it.Next() {
		}
	}
}
