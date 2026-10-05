package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/jackc/pgx/v5/pgxpool"
)

const farmLeadLockKey int64 = 0x6e696b73336c64 // "niks3ld"

//nolint:gochecknoglobals // tests shorten them
var (
	leadHeartbeat = 5 * time.Second
	// How long after start a non-incumbent holds back from taking the lock.
	incumbentGrace = 10 * time.Second
	startedAt      = time.Now()
)

// leadPingTimeoutFloor keeps a busy database from failing a healthy ping.
const leadPingTimeoutFloor = time.Second

// leadPingTimeout bounds the leader's ping. A silently dead connection would
// otherwise hang it for about fifteen minutes, with the stream still open.
func leadPingTimeout() time.Duration { return max(leadHeartbeat/2, leadPingTimeoutFloor) }

// tryLead takes the session advisory lock on a dedicated connection, so the
// lock lives exactly as long as that connection. A nil conn means someone
// else leads.
func tryLead(ctx context.Context, pool *pgxpool.Pool) (*pgxpool.Conn, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	var got bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", farmLeadLockKey).Scan(&got); err != nil || !got {
		conn.Release()

		return nil, err //nolint:wrapcheck
	}

	return conn, nil
}

// announceDelay is how long a new lock holder stays quiet. An incumbent comes
// back after its own stream broke, so no older stream of its own is left to
// wait for.
func announceDelay(incumbent bool) time.Duration {
	if incumbent {
		return 0
	}

	return leadHeartbeat + leadPingTimeout()
}

// pingLead checks that the lock connection answers within leadPingTimeout.
func pingLead(ctx context.Context, conn *pgxpool.Conn) error {
	pingCtx, cancel := context.WithTimeout(ctx, leadPingTimeout())
	defer cancel()

	err := conn.Ping(pingCtx)
	if err != nil && ctx.Err() == nil {
		slog.Warn("lead: lock connection lost", "error", err)
	}

	return err //nolint:wrapcheck
}

// LeadHandler elects the build farm scheduler. Each candidate keeps one
// NDJSON stream open and is told every heartbeat whether it leads.
// Leadership ends when the stream, this process or Postgres goes away.
//
// A leader whose connection died keeps leading until its ping fails, but the
// lock is already free. A new holder waits a heartbeat plus the ping timeout
// before announcing, so the two leaders never overlap. The incumbent is the
// exception: it only reconnects after its own stream broke.
func (s *Service) LeadHandler(w http.ResponseWriter, r *http.Request) {
	defer closeRequestBody(r)

	var req api.LeadRequest
	if r.Body != nil && r.ContentLength != 0 {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	holdBack := time.Time{}
	if !req.Incumbent {
		holdBack = startedAt.Add(incumbentGrace)
	}

	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})

	w.Header().Set("Content-Type", "application/x-ndjson")

	enc := json.NewEncoder(w)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	if s.Streams != nil {
		defer context.AfterFunc(s.Streams, cancel)() //nolint:contextcheck
	}

	tick := time.NewTicker(leadHeartbeat)
	defer tick.Stop()

	var (
		conn *pgxpool.Conn
		// announceAt is the earliest time a new lock holder may say true.
		announceAt time.Time
	)

	defer func() { //nolint:contextcheck // must close even though ctx is done
		if conn != nil {
			_ = conn.Hijack().Close(context.Background())

			slog.Info("lead: released", "remote", r.RemoteAddr)
		}
	}()

	for {
		if conn == nil && !time.Now().Before(holdBack) {
			var err error
			if conn, err = tryLead(ctx, s.Pool); err != nil {
				slog.Error("lead", "error", err)

				return
			}

			if conn != nil {
				announceAt = time.Now().Add(announceDelay(req.Incumbent))

				slog.Info("lead: acquired", "remote", r.RemoteAddr)
			}
		} else if conn != nil && pingLead(ctx, conn) != nil {
			return
		}

		if err := enc.Encode(api.LeadStatus{Lead: conn != nil && !time.Now().Before(announceAt)}); err != nil {
			return
		}

		if rc.Flush() != nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
