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

// LeadHandler elects the build farm scheduler. Each candidate keeps one
// NDJSON stream open and is told every heartbeat whether it leads.
// Leadership ends when the stream, this process or Postgres goes away.
//
// A leader whose database connection died still believes it leads until
// its next heartbeat finds the ping failing and its stream closes. The lock
// is free from the moment the connection died, so a candidate that took it
// and announced at once would overlap with the old leader for up to one
// heartbeat. A new holder therefore stays quiet for one heartbeat after
// taking the lock: by then the old leader's ping has run.
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
		// fresh is set for the heartbeat on which the lock was taken; that
		// beat still reports Lead: false.
		fresh bool
	)

	defer func() { //nolint:contextcheck // must close even though ctx is done
		if conn != nil {
			_ = conn.Hijack().Close(context.Background())

			slog.Info("lead: released", "remote", r.RemoteAddr)
		}
	}()

	for {
		fresh = false

		if conn == nil && !time.Now().Before(holdBack) {
			var err error
			if conn, err = tryLead(ctx, s.Pool); err != nil {
				slog.Error("lead", "error", err)

				return
			}

			if conn != nil {
				fresh = true

				slog.Info("lead: acquired", "remote", r.RemoteAddr)
			}
		} else if conn != nil && conn.Ping(ctx) != nil {
			return
		}

		if err := enc.Encode(api.LeadStatus{Lead: conn != nil && !fresh}); err != nil {
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
