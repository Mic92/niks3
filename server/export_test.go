package server

import (
	"context"
	"crypto/tls"
	"flag"
	"net"
	"net/http"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server/oidc"
)

// SystemdListenerForTest exposes systemdListener to tests.
func SystemdListenerForTest() (net.Listener, error) { return systemdListener() }

// ServeForTest exposes the graceful-shutdown serve loop to tests, driven by a
// caller-supplied context and listener instead of OS signals.
func ServeForTest(shutdownCtx context.Context, server *http.Server, ln net.Listener) error {
	return serve(shutdownCtx, []endpoint{{server: server, ln: ln}}, nil)
}

// RunWatchdogForTest exposes runWatchdog to tests.
func RunWatchdogForTest(ctx context.Context, interval time.Duration, check func(context.Context) error) {
	runWatchdog(ctx, interval, check)
}

// DeleteClosuresBefore exposes deleteClosuresBefore to tests.
func DeleteClosuresBefore(ctx context.Context, service *Service, cutoff time.Time) (int, error) {
	return deleteClosuresBefore(ctx, service.Pool, cutoff)
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

// MaxNarinfoSize exposes the read proxy's narinfo bound to tests.
const MaxNarinfoSize = maxNarinfoSize

// Test-only re-exports for proxy range parsing.

type ByteRange = byteRange

var ErrUnsatisfiableRange = errUnsatisfiableRange

func ParseSingleRange(spec string, size int64) (*ByteRange, error) {
	return parseSingleRange(spec, size)
}

func (br ByteRange) Start() int64 { return br.start }
func (br ByteRange) End() int64   { return br.end }

// ResolveDBConnectionString is an export of resolveDBConnectionString for tests.
func ResolveDBConnectionString(flagValue, file string, lookupEnv func(string) (string, bool)) (string, error) {
	return resolveDBConnectionString(flagValue, file, lookupEnv)
}

// GlobToLike exports globToLike for tests.
func GlobToLike(pattern string) string {
	return globToLike(pattern)
}

// ServerTLSConfig is an export of serverTLSConfig for tests.
func ServerTLSConfig(clientCA string) (*tls.Config, error) {
	return serverTLSConfig(clientCA)
}

func init() { //nolint:gochecknoinits // fast lead tests
	leadHeartbeat = 50 * time.Millisecond
	incumbentGrace = 0
}

func LeadHeartbeat() time.Duration { return leadHeartbeat }

const FarmLeadLockKey = farmLeadLockKey

func LeadPingTimeout() time.Duration { return leadPingTimeout() }

// SetLeadHeartbeat changes the heartbeat until the returned func restores it.
// Only for tests that do not run in parallel.
func SetLeadHeartbeat(d time.Duration) func() {
	old := leadHeartbeat
	leadHeartbeat = d

	return func() { leadHeartbeat = old }
}

// RestartedNow makes the lead handler behave as if the server just started.
func RestartedNow(grace time.Duration) {
	startedAt = time.Now()
	incumbentGrace = grace
}

// ServeProxySocketForTest serves handler like Run does with
// --mtls-proxy-socket: headers are stripped on the TCP listener and trusted
// on the unix one.
func ServeProxySocketForTest(shutdownCtx context.Context, handler http.Handler, tcp, unix net.Listener, headers ...string) error {
	return serve(shutdownCtx, []endpoint{
		{server: &http.Server{Handler: stripHeaders(handler, headers...), ReadHeaderTimeout: time.Second}, ln: tcp},
		{server: &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}, ln: unix},
	}, nil)
}

// AbortSucceeded exposes abortSucceeded for tests.
func AbortSucceeded(err error) bool {
	return abortSucceeded(err)
}

// EnsureAbortUploadsRule exposes ensureAbortUploadsRule for tests.
func (s *Service) EnsureAbortUploadsRule(ctx context.Context) {
	s.ensureAbortUploadsRule(ctx)
}

// Options is an export of options for tests.
type Options = options

// ValidateTLSOptions exposes validateTLSOptions for tests.
func ValidateTLSOptions(opts *Options) error {
	return validateTLSOptions(opts)
}

// CommonName exposes commonName for tests.
func CommonName(dn string) (string, bool) {
	return commonName(dn)
}

// PrincipalFromContext extracts the principal from a request context for tests.
func PrincipalFromContext(r *http.Request) ([]string, []oidc.Scope, bool) {
	p, ok := r.Context().Value(principalKey{}).(principal)
	if !ok {
		return nil, nil, false
	}

	return p.readPins, p.scopes, true
}

// ParseArgsFrom exports parseArgsFrom for testing.
func ParseArgsFrom(fs *flag.FlagSet, args []string) (*Options, error) {
	return parseArgsFrom(fs, args)
}
