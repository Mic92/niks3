package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	farmLeadLockKey int64 = 0x6e696b73336c64 // "niks3ld"
	// Held by a returning incumbent while it waits for the lead (claimLead).
	farmClaimLockKey int64 = 0x6e696b7333636c // "niks3cl"
)

// leadClaimed only looks at the claim lock, so it cannot get in the way of an
// incumbent taking it.
func leadClaimed() string {
	return fmt.Sprintf(`EXISTS (
		SELECT FROM pg_locks
		WHERE locktype = 'advisory' AND granted AND objsubid = 1
		  AND classid = %d AND objid = %d
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database()))`,
		farmClaimLockKey>>32, farmClaimLockKey&0xffffffff) //nolint:mnd // halves of the key
}

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

type lead struct {
	// The session advisory lock lives exactly as long as this connection.
	conn *pgxpool.Conn
	// Drawn with the lock, so a later holder has a higher one.
	generation uint64
}

// tryLead takes the lock on a dedicated connection. Nil means someone else
// leads, or, for a stream that is no incumbent, that one is on its way back.
func tryLead(ctx context.Context, pool *pgxpool.Pool, incumbent bool) (*lead, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	var generation *uint64

	err = conn.QueryRow(ctx, `
		SELECT CASE WHEN NOT $2 AND `+leadClaimed()+` THEN NULL
		            WHEN pg_try_advisory_lock($1) THEN nextval('farm_lead_generation') END`,
		farmLeadLockKey, incumbent).Scan(&generation)
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

func (l *lead) release() {
	_ = l.conn.Hijack().Close(context.Background())
}

// yields tells whether an incumbent wants the lock back.
func (l *lead) yields(ctx context.Context) (bool, error) {
	var claimed bool

	err := l.conn.QueryRow(ctx, "SELECT "+leadClaimed()).Scan(&claimed)

	return claimed, err //nolint:wrapcheck
}

// claimLead asks an unannounced holder to give the lead back and everybody
// else not to take it. Only the lead lock decides who leads.
func claimLead(ctx context.Context, pool *pgxpool.Pool) func() {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return func() {}
	}

	// Two incumbents cannot both be right; the second goes without.
	_, _ = conn.Exec(ctx, "SELECT pg_try_advisory_lock($1)", farmClaimLockKey)

	return sync.OnceFunc(func() { //nolint:contextcheck // must close even though ctx is done
		_ = conn.Hijack().Close(context.Background())
	})
}

// claimFor: the holder sees the claim within a heartbeat, the incumbent finds
// the lock free within another.
func claimFor() time.Duration { return 2*leadHeartbeat + leadPingTimeout() }

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
// exception: it only reconnects after its own stream broke, and a holder
// still waiting then gives the lock back.
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
			held.release()

			slog.Info("lead: released", "remote", r.RemoteAddr, "generation", held.generation)
		}
	}()

	dropClaim := func() {}
	claimUntil := time.Now().Add(claimFor())

	if req.Incumbent {
		dropClaim = claimLead(ctx, s.Pool)
	}

	defer dropClaim()

	for {
		now := time.Now()
		announced := held != nil && !now.Before(announceAt)

		if held != nil || !now.Before(claimUntil) {
			dropClaim()
		}

		switch {
		case held == nil && !now.Before(holdBack):
			var err error
			if held, err = tryLead(ctx, s.Pool, req.Incumbent); err != nil {
				slog.Error("lead", "error", err)

				return
			}

			if held != nil {
				announceAt = time.Now().Add(announceDelay(req.Incumbent))
				announced = !time.Now().Before(announceAt)

				slog.Info("lead: acquired", "remote", r.RemoteAddr, "generation", held.generation)
			}
		case held == nil:
		case pingLead(ctx, held.conn) != nil:
			return
		case !announced:
			yield, err := held.yields(ctx)
			if err != nil {
				return
			}

			if yield {
				held.release() //nolint:contextcheck // closes with its own context

				slog.Info("lead: given back to the incumbent", "remote", r.RemoteAddr, "generation", held.generation)

				held = nil
			}
		}

		var status api.LeadStatus
		if held != nil && announced {
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
