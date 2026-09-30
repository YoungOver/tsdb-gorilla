package lineproto

import (
	"strings"
	"testing"
)

type sample struct {
	key string
	t   int64
	v   float64
}

func parse(t *testing.T, in string) ([]sample, int) {
	t.Helper()
	var p Parser
	var out []sample
	_, bad := p.Parse([]byte(in), 42, func(k []byte, ts int64, v float64) {
		out = append(out, sample{string(k), ts, v})
	})
	return out, bad
}

func TestParse(t *testing.T) {
	got, bad := parse(t, strings.Join([]string{
		"cpu,region=eu,host=a usage=0.5,idle=99.5 1000",
		"mem value=12i 2000",
		"# comment",
		"",
		"temp,room=kitchen value=21.5",
		"broken",
		"x,tag bad=1 5",
		"y f=notanumber 5",
	}, "\n"))
	want := []sample{
		{"cpu.usage{host=a,region=eu}", 1000, 0.5},
		{"cpu.idle{host=a,region=eu}", 1000, 99.5},
		{"mem", 2000, 12},
		{"temp{room=kitchen}", 42, 21.5},
	}
	if bad != 3 {
		t.Fatalf("bad = %d, want 3", bad)
	}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sample %d: got %+v want %+v", i, got[i], want[i])
		}
	}
}

func BenchmarkParse(b *testing.B) {
	line := []byte("http_requests,service=api,method=GET,code=200 count=1827,latency_ms=12.5 1700000000000\n")
	buf := []byte(strings.Repeat(string(line), 1000))
	var p Parser
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.Parse(buf, 0, func([]byte, int64, float64) {})
	}
}
