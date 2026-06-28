package server

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/Mic92/niks3/api"
)

// ServeForTest exposes the graceful-shutdown serve loop to tests, driven by a
// caller-supplied context instead of OS signals.
func ServeForTest(shutdownCtx context.Context, server *http.Server, addr string) error {
	return serve(shutdownCtx, server, &options{HTTPAddr: addr}, false)
}

// GCAdvisoryLockKey exposes the GC advisory lock key to tests.
const GCAdvisoryLockKey = gcAdvisoryLockKey

// RunGCForTest runs a full garbage collection synchronously and returns the
// final task status.
func (s *Service) RunGCForTest(age, pendingAge time.Duration, force bool) api.GCTaskStatus {
	result := s.GCTasks.Start(api.GCTaskParams{})
	s.runGarbageCollection(result.Task, age, pendingAge, force)

	snap, _ := s.GCTasks.Get()

	return snap
}

// Test-only exports for gcTask methods, callable from server_test package.

func (t *gcTask) TestSucceed(stats api.GCStats)             { t.succeed(stats) }
func (t *gcTask) TestFail(stats api.GCStats, errMsg string) { t.fail(stats, errMsg) }
func (t *gcTask) TestSetPhase(phase api.GCTaskPhase)        { t.setPhase(phase) }
func (t *gcTask) TestUpdateStats(stats api.GCStats)         { t.updateStats(stats) }

// Test-only re-exports for proxy range parsing.

type ByteRange = byteRange

var ErrUnsatisfiableRange = errUnsatisfiableRange

func ParseSingleRange(spec string, size int64) (*ByteRange, error) {
	return parseSingleRange(spec, size)
}

func (br ByteRange) Start() int64 { return br.start }
func (br ByteRange) End() int64   { return br.end }

// ServerTLSConfig is an export of serverTLSConfig for tests.
func ServerTLSConfig(clientCA string) (*tls.Config, error) {
	return serverTLSConfig(clientCA)
}
