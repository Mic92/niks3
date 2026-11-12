package server_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

// queryHook runs test code next to one of the service's SQL statements.
type queryHook struct {
	name   string
	before bool
	f      func()
}

type queryHookKey struct{}

func (h *queryHook) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !strings.Contains(data.SQL, "-- name: "+h.name+" :") {
		return ctx
	}

	if h.before {
		h.f()

		return ctx
	}

	return context.WithValue(ctx, queryHookKey{}, true)
}

func (h *queryHook) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if ctx.Value(queryHookKey{}) != nil {
		h.f()
	}
}

// beforeQuery makes the service run f right before each query of that name.
func beforeQuery(t *testing.T, service *server.Service, name string, f func()) {
	t.Helper()
	traceQueries(t, service, &queryHook{name: name, before: true, f: f})
}

// afterQuery makes the service run f once each query of that name has returned its rows.
func afterQuery(t *testing.T, service *server.Service, name string, f func()) {
	t.Helper()
	traceQueries(t, service, &queryHook{name: name, f: f})
}

func traceQueries(t *testing.T, service *server.Service, h *queryHook) {
	t.Helper()

	cfg := service.Pool.Config()
	cfg.ConnConfig.Tracer = h

	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	ok(t, err)

	service.Pool.Close()
	service.Pool = pool
}

// s3Hook sees every request the service sends to S3 before S3 does.
type s3Hook func(r *http.Request)

func (h s3Hook) RoundTrip(r *http.Request) (*http.Response, error) {
	h(r)

	return http.DefaultTransport.RoundTrip(r) //nolint:wrapcheck // test transport
}

func beforeS3Request(t *testing.T, service *server.Service, f func(r *http.Request)) {
	t.Helper()

	var err error

	service.MinioClient, err = minio.New(service.MinioClient.EndpointURL().Host,
		&minio.Options{Creds: testRustfsServer.Creds(), Transport: s3Hook(f)})
	ok(t, err)
}
