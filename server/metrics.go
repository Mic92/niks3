package server

import (
	"context"
	"log/slog"
	"net/http"
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
	}
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (s *Service) refreshInventory(ctx context.Context) error {
	stats, err := pg.New(s.Pool).GetObjectStats(ctx)
	if err != nil {
		return err
	}

	s.Metrics.cacheObjects.Set(float64(stats.ObjectCount))
	s.Metrics.cacheLogicalBytes.Set(float64(stats.TotalBytes))

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
