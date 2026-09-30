package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/YoungOver/tsdb-gorilla/internal/lineproto"
	"github.com/YoungOver/tsdb-gorilla/internal/store"
	"github.com/YoungOver/tsdb-gorilla/internal/wal"
)

var (
	ingested = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tsdb_samples_ingested_total", Help: "Samples accepted, by transport.",
	}, []string{"via"})
	badLines = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tsdb_bad_lines_total", Help: "Unparseable lines skipped.",
	})
	writeDur = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "tsdb_write_seconds", Help: "HTTP write latency including WAL commit.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
	})
	queryDur = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "tsdb_query_seconds", Help: "Query latency.",
		Buckets: prometheus.ExponentialBuckets(0.00005, 2, 16),
	})
)

type server struct {
	st      *store.Store
	wal     *wal.WAL
	parsers sync.Pool
	maxBody int64
}

// ingest makes a batch durable first, then applies it to memory.
func (s *server) ingest(body []byte, via string) (ok, bad int, err error) {
	if s.wal != nil {
		if err := s.wal.Write(body); err != nil {
			return 0, 0, err
		}
	}
	ok, bad = s.apply(body)
	ingested.WithLabelValues(via).Add(float64(ok))
	badLines.Add(float64(bad))
	return ok, bad, nil
}

func (s *server) apply(body []byte) (ok, bad int) {
	p := s.parsers.Get().(*lineproto.Parser)
	defer s.parsers.Put(p)
	now := time.Now().UnixMilli()
	return p.Parse(body, now, func(k []byte, t int64, v float64) { s.st.Append(k, t, v) })
}

func (s *server) write(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	ok, bad, err := s.ingest(body, "http")
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeDur.Observe(time.Since(start).Seconds())
	w.Header().Set("X-Samples", strconv.Itoa(ok))
	w.Header().Set("X-Bad-Lines", strconv.Itoa(bad))
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) query(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	q := r.URL.Query()
	now := time.Now().UnixMilli()
	from := intParam(q.Get("from"), now-3_600_000)
	to := intParam(q.Get("to"), now)
	step := intParam(q.Get("step"), 0)
	pts, err := s.st.Query(q.Get("series"), from, to, step, q.Get("agg"))
	switch {
	case errors.Is(err, store.ErrNoSeries):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case errors.Is(err, store.ErrBadAgg):
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	queryDur.Observe(time.Since(start).Seconds())
	writeJSON(w, map[string]any{"series": q.Get("series"), "points": pts})
}

func (s *server) stats(w http.ResponseWriter, _ *http.Request) {
	bytes, points := s.st.Footprint()
	bpp := 0.0
	if points > 0 {
		bpp = float64(bytes) / float64(points)
	}
	writeJSON(w, map[string]any{
		"series": s.st.SeriesNum.Load(), "points_in_memory": points, "compressed_bytes": bytes,
		"bytes_per_point": bpp, "accepted_total": s.st.Points.Load(),
		"rejected_out_of_order": s.st.Rejected.Load(), "dropped_by_retention": s.st.Dropped.Load(),
	})
}

func intParam(v string, def int64) int64 {
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n
	}
	return def
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func (s *server) serveUDP(ctx context.Context, addr string) error {
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}
	go func() { <-ctx.Done(); pc.Close() }()
	buf := make([]byte, 64<<10)
	for {
		n, _, err := pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		pkt := append([]byte(nil), buf[:n]...) // the WAL keeps a reference until commit
		s.ingest(pkt, "udp")
	}
}

func main() {
	addr := flag.String("addr", ":8428", "HTTP listen address")
	udp := flag.String("udp", ":8089", "UDP line-protocol address, empty to disable")
	dir := flag.String("wal", "data/wal", "WAL directory, empty to run in-memory only")
	fsync := flag.Bool("fsync", false, "fsync every group commit")
	keep := flag.Duration("retention", 24*time.Hour, "how long samples are kept")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	prometheus.MustRegister(ingested, badLines, writeDur, queryDur)

	s := &server{st: store.New(), maxBody: 32 << 20}
	s.parsers.New = func() any { return new(lineproto.Parser) }

	if *dir != "" {
		start := time.Now()
		n, err := wal.Replay(*dir, func(b []byte) error { s.apply(b); return nil })
		if err != nil {
			log.Error("wal replay", "err", err)
			os.Exit(1)
		}
		log.Info("wal replayed", "records", n, "samples", s.st.Points.Load(), "took", time.Since(start).String())
		if s.wal, err = wal.Open(*dir, *fsync); err != nil {
			log.Error("wal open", "err", err)
			os.Exit(1)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go s.st.Janitor(ctx, time.Minute, *keep)
	if *dir != "" {
		go func() {
			t := time.NewTicker(10 * time.Minute)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case now := <-t.C:
					wal.Prune(*dir, now.Add(-*keep))
				}
			}
		}()
	}
	if *udp != "" {
		go func() {
			if err := s.serveUDP(ctx, *udp); err != nil {
				log.Error("udp", "err", err)
			}
		}()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /write", s.write)
	mux.HandleFunc("GET /query", s.query)
	mux.HandleFunc("GET /series", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, s.st.Series(r.URL.Query().Get("prefix"), int(intParam(r.URL.Query().Get("limit"), 100))))
	})
	mux.HandleFunc("GET /stats", s.stats)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("GET /metrics", promhttp.Handler())

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Info("listening", "http", *addr, "udp", *udp, "wal", *dir, "fsync", *fsync)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http", "err", err)
	}
	if s.wal != nil {
		s.wal.Close()
	}
}
