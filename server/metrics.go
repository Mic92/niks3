package server

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Mic92/niks3/server/pg"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// inventoryRefreshInterval bounds how stale the inventory gauges can be; no
// point refreshing faster than the scrape interval.
const inventoryRefreshInterval = time.Minute

// Metrics holds the Prometheus registry and instruments exposed at /metrics.
type Metrics struct {
	registry          *prometheus.Registry
	cacheObjects      prometheus.Gauge
	cacheLogicalBytes prometheus.Gauge
	pendingClosures   prometheus.Gauge
	dbConnsInUse      prometheus.Gauge
	dbConnsMax        prometheus.Gauge
	httpRequests      *prometheus.CounterVec
	httpDuration      *prometheus.HistogramVec
	httpInFlight      prometheus.Gauge
}

// NewMetrics builds a registry with the Go/process collectors and the cache
// inventory gauges.
func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	factory := promauto.With(registry)

	return &Metrics{
		registry: registry,
		cacheObjects: factory.NewGauge(prometheus.GaugeOpts{
			Name: "niks3_cache_objects",
			Help: "Number of live objects in the cache.",
		}),
		cacheLogicalBytes: factory.NewGauge(prometheus.GaugeOpts{
			Name: "niks3_cache_logical_bytes",
			Help: "Logical (uncompressed) size of live objects in bytes; excludes objects with an unknown size.",
		}),
		pendingClosures: factory.NewGauge(prometheus.GaugeOpts{
			Name: "niks3_pending_closures",
			Help: "Number of in-flight uploads (pending closures) not yet committed.",
		}),
		dbConnsInUse: factory.NewGauge(prometheus.GaugeOpts{
			Name: "niks3_db_connections_in_use",
			Help: "Database connections currently acquired from the pool.",
		}),
		dbConnsMax: factory.NewGauge(prometheus.GaugeOpts{
			Name: "niks3_db_connections_max",
			Help: "Maximum size of the database connection pool.",
		}),
		httpRequests: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "niks3_http_requests_total",
			Help: "HTTP requests by method, matched route and status code.",
		}, []string{"method", "route", "status"}),
		httpDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "niks3_http_request_duration_seconds",
			Help:    "HTTP request duration by method and matched route.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
		httpInFlight: factory.NewGauge(prometheus.GaugeOpts{
			Name: "niks3_http_requests_in_flight",
			Help: "HTTP requests currently being served.",
		}),
	}
}

// statusRecorder captures the response status for instrumentation. Unwrap lets
// http.ResponseController reach the underlying writer (e.g. for flushing).
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Instrument records request count, duration and in-flight gauge for next. The
// route label is the matched ServeMux pattern (templated), bounding cardinality.
func (m *Metrics) Instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.httpInFlight.Inc()
		defer m.httpInFlight.Dec()

		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}

		m.httpRequests.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Inc()
		m.httpDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
	})
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (s *Service) refreshInventory(ctx context.Context) error {
	poolStat := s.Pool.Stat()
	s.Metrics.dbConnsInUse.Set(float64(poolStat.AcquiredConns()))
	s.Metrics.dbConnsMax.Set(float64(poolStat.MaxConns()))

	queries := pg.New(s.Pool)

	stats, err := queries.GetObjectStats(ctx)
	if err != nil {
		return err
	}

	s.Metrics.cacheObjects.Set(float64(stats.ObjectCount))
	s.Metrics.cacheLogicalBytes.Set(float64(stats.TotalBytes))

	pending, err := queries.CountPendingClosures(ctx)
	if err != nil {
		return err
	}

	s.Metrics.pendingClosures.Set(float64(pending))

	return nil
}

// StartInventoryRefresh seeds the inventory gauges and refreshes them on a
// ticker until ctx is cancelled. Independent of GC so the gauges are correct
// shortly after startup.
func (s *Service) StartInventoryRefresh(ctx context.Context) {
	refresh := func() {
		if err := s.refreshInventory(ctx); err != nil {
			slog.Warn("failed to refresh inventory metrics", "error", err)
		}
	}

	refresh()

	go func() {
		ticker := time.NewTicker(inventoryRefreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refresh()
			}
		}
	}()
}
