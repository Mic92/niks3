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

// leadPingTimeoutFloor keeps the leader's ping bound above what a busy
// database may take to answer, however short the heartbeat.
const leadPingTimeoutFloor = time.Second

// leadPingTimeout bounds the leader's ping on its lock connection. A
// connection that died without a reset, as when the database host crashes
// or fails over or the network partitions, would otherwise hang the ping
// for the kernel's retransmission timeout, around fifteen minutes, with the
// stream open and the lock gone.
func leadPingTimeout() time.Duration { return max(leadHeartbeat/2, leadPingTimeoutFloor) }

// leadQuietBeats is how many heartbeats a new holder of the lock reports
// Lead: false for, the one that took the lock included: enough for the old
// leader's next heartbeat and its ping to have run out. See LeadHandler.
func leadQuietBeats() int {
	return 1 + int((leadPingTimeout()+leadHeartbeat-1)/leadHeartbeat)
}

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

// pingLead checks that the lock connection still answers within
// leadPingTimeout.
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
// A leader whose database connection died still believes it leads until
// its next heartbeat finds the ping failing and its stream closes. The lock
// is free from the moment the connection died, so a candidate that took it
// and announced at once would overlap with the old leader for up to one
// heartbeat plus the ping timeout. A new holder therefore stays quiet for
// leadQuietBeats heartbeats after taking the lock: by then the old leader's
// ping has run and, on a dead connection, timed out.
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
		// quiet counts the heartbeats on which a new holder of the lock
		// still reports Lead: false.
		quiet int
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
				quiet = leadQuietBeats()

				slog.Info("lead: acquired", "remote", r.RemoteAddr)
			}
		} else if conn != nil && pingLead(ctx, conn) != nil {
			return
		}

		if err := enc.Encode(api.LeadStatus{Lead: conn != nil && quiet == 0}); err != nil {
			return
		}

		if quiet > 0 {
			quiet--
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
