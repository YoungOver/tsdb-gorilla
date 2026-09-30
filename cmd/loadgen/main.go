// loadgen simulates a fleet of hosts pushing metrics every interval and reports
// ingest throughput and write latency, then hammers the query endpoint.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var fields = []string{"cpu_user", "cpu_system", "mem_used", "disk_read", "disk_write",
	"net_rx", "net_tx", "http_requests", "http_errors", "latency_ms"}

func main() {
	target := flag.String("url", "http://127.0.0.1:8428", "server base URL")
	hosts := flag.Int("hosts", 10_000, "simulated hosts (x10 series each)")
	workers := flag.Int("c", 16, "concurrent writers")
	perReq := flag.Int("batch", 200, "hosts per request (x10 samples each)")
	dur := flag.Duration("d", 20*time.Second, "write phase duration")
	queries := flag.Int("q", 20_000, "queries in the read phase")
	flag.Parse()

	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: *workers * 2}}
	var samples, reqs, errs atomic.Int64
	lat := make([][]time.Duration, *workers)
	deadline := time.Now().Add(*dur)
	base := time.Now().UnixMilli() - 3_600_000
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 99))
			lo, hi := w**hosts / *workers, (w+1)**hosts / *workers
			state := make([]float64, (hi-lo)*len(fields))
			var buf bytes.Buffer
			scratch := make([]byte, 0, 32)
			for round := int64(0); time.Now().Before(deadline); round++ {
				ts := base + round*1000 // each round is one scrape, 1s apart in data time
				for h := lo; h < hi; h += *perReq {
					buf.Reset()
					end := min(h+*perReq, hi)
					for host := h; host < end; host++ {
						fmt.Fprintf(&buf, "host,dc=dc%d,id=h%05d ", host%4, host)
						for m := range fields {
							st := &state[(host-lo)*len(fields)+m]
							*st = next(r, m, *st)
							if m > 0 {
								buf.WriteByte(',')
							}
							buf.WriteString(fieldName(m))
							buf.WriteByte('=')
							buf.Write(strconv.AppendFloat(scratch[:0], *st, 'f', -1, 64))
						}
						fmt.Fprintf(&buf, " %d\n", ts)
					}
					start := time.Now()
					resp, err := client.Post(*target+"/write", "text/plain", bytes.NewReader(buf.Bytes()))
					if err != nil || resp.StatusCode != http.StatusNoContent {
						errs.Add(1)
						if resp != nil {
							io.Copy(io.Discard, resp.Body)
							resp.Body.Close()
						}
						continue
					}
					n, _ := strconv.Atoi(resp.Header.Get("X-Samples"))
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					lat[w] = append(lat[w], time.Since(start))
					samples.Add(int64(n))
					reqs.Add(1)
				}
			}
		}(w)
	}
	t0 := time.Now()
	wg.Wait()
	el := time.Since(t0).Seconds()
	all := slices.Concat(lat...)
	slices.Sort(all)
	fmt.Printf("write: %d samples in %.1fs = %.0f samples/s, %d requests (%.0f req/s), errors %d\n",
		samples.Load(), el, float64(samples.Load())/el, reqs.Load(), float64(reqs.Load())/el, errs.Load())
	if len(all) > 0 {
		fmt.Printf("write latency p50 %v  p99 %v  max %v\n", pct(all, 50), pct(all, 99), all[len(all)-1])
	}

	// read phase: 1h window, 1-minute avg buckets over random series
	var qlat [][]time.Duration = make([][]time.Duration, *workers)
	var qerr atomic.Int64
	var next64 atomic.Int64
	t1 := time.Now()
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 7))
			for next64.Add(1) <= int64(*queries) {
				host := r.IntN(*hosts)
				key := fmt.Sprintf("host.%s{dc=dc%d,id=h%05d}", fieldName(r.IntN(len(fields))), host%4, host)
				u := *target + "/query?series=" + url.QueryEscape(key) + "&from=" + strconv.FormatInt(base, 10) +
					"&to=" + strconv.FormatInt(base+3_600_000, 10) + "&step=60000&agg=avg"
				start := time.Now()
				resp, err := client.Get(u)
				if err != nil {
					qerr.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					qerr.Add(1)
					continue
				}
				qlat[w] = append(qlat[w], time.Since(start))
			}
		}(w)
	}
	wg.Wait()
	qel := time.Since(t1).Seconds()
	qall := slices.Concat(qlat...)
	slices.Sort(qall)
	if len(qall) > 0 {
		fmt.Printf("query: %d in %.1fs = %.0f q/s, p50 %v p99 %v, errors %d\n",
			len(qall), qel, float64(len(qall))/qel, pct(qall, 50), pct(qall, 99), qerr.Load())
	}
	if resp, err := client.Get(*target + "/stats"); err == nil {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		fmt.Printf("server stats: %s", b)
	}
	if errs.Load() > 0 || qerr.Load() > 0 {
		os.Exit(1)
	}
}

func fieldName(m int) string { return fields[m] }

// next produces realistic shapes: bounded gauges, monotonic counters, rare errors.
func next(r *rand.Rand, m int, prev float64) float64 {
	switch m {
	case 0, 1: // cpu %, one decimal
		return math.Round(math.Max(0, math.Min(100, prev+r.NormFloat64()*2))*10) / 10
	case 2: // memory in MiB, moves in pages
		return math.Max(512, prev+float64(r.IntN(9)-4)*4)
	case 3, 4, 5, 6, 7: // counters
		return prev + float64(r.IntN(2048))
	case 8: // errors: mostly flat
		if r.IntN(50) == 0 {
			return prev + 1
		}
		return prev
	default: // latency ms, integer
		return float64(20 + r.IntN(30))
	}
}

func pct(s []time.Duration, p int) time.Duration { return s[(len(s)-1)*p/100] }
