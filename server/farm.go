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

// lead is the farm lead as one stream holds it.
type lead struct {
	// conn holds the session advisory lock, so the lock lives exactly as
	// long as this connection.
	conn *pgxpool.Conn
	// generation is drawn with the lock: whoever takes the lock later draws
	// a higher one.
	generation uint64
}

// tryLead takes the lock on a dedicated connection. Nil means someone else
// leads.
func tryLead(ctx context.Context, pool *pgxpool.Pool) (*lead, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	var generation *uint64

	err = conn.QueryRow(ctx,
		"SELECT CASE WHEN pg_try_advisory_lock($1) THEN nextval('farm_lead_generation') END",
		farmLeadLockKey).Scan(&generation)
	if err != nil {
		// The lock may have been taken before the query failed.
		_ = conn.Hijack().Close(context.Background()) //nolint:contextcheck // must close even though ctx is done

		return nil, err //nolint:wrapcheck
	}

	if generation == nil {
		conn.Release()

		return nil, nil //nolint:nilnil // not leading is no error
	}

	return &lead{conn: conn, generation: *generation}, nil
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
		held *lead
		// announceAt is the earliest time a new lock holder may say true.
		announceAt time.Time
	)

	defer func() { //nolint:contextcheck // must close even though ctx is done
		if held != nil {
			_ = held.conn.Hijack().Close(context.Background())

			slog.Info("lead: released", "remote", r.RemoteAddr, "generation", held.generation)
		}
	}()

	for {
		if held == nil && !time.Now().Before(holdBack) {
			var err error
			if held, err = tryLead(ctx, s.Pool); err != nil {
				slog.Error("lead", "error", err)

				return
			}

			if held != nil {
				announceAt = time.Now().Add(announceDelay(req.Incumbent))

				slog.Info("lead: acquired", "remote", r.RemoteAddr, "generation", held.generation)
			}
		} else if held != nil && pingLead(ctx, held.conn) != nil {
			return
		}

		var status api.LeadStatus
		if held != nil && !time.Now().Before(announceAt) {
			status = api.LeadStatus{Lead: true, Generation: held.generation}
		}

		if err := enc.Encode(status); err != nil {
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
