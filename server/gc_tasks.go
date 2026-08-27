package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// gcAdvisoryLockKey serializes GC across replicas sharing one database.
const gcAdvisoryLockKey int64 = 0x6e696b73336763 // "niks3gc"

var errGCAlreadyRunning = errors.New("another garbage collection is already running on this database")

// gcTask is a gc_runs row plus the advisory lock. Progress is written
// through so any replica can answer status polls.
type gcTask struct {
	id      int64
	pool    *pgxpool.Pool
	release func()
	phase   api.GCTaskPhase
	stats   api.GCStats
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	return b
}

// write uses context.Background: progress must land even if the request is gone.
func (t *gcTask) write() {
	err := pg.New(t.pool).UpdateGCRunProgress(context.Background(), pg.UpdateGCRunProgressParams{
		ID: t.id, Phase: string(t.phase), Stats: mustJSON(t.stats),
	})
	if err != nil {
		slog.Warn("failed to record GC progress", "error", err)
	}
}

func (t *gcTask) setPhase(phase api.GCTaskPhase) {
	t.phase = phase
	t.write()
}

func (t *gcTask) updateStats(stats api.GCStats) {
	t.stats = stats
	t.write()
}

func (t *gcTask) finish(state api.GCTaskState, stats api.GCStats, errMsg string) {
	t.stats = stats

	err := pg.New(t.pool).FinishGCRun(context.Background(), pg.FinishGCRunParams{
		ID: t.id, State: string(state), Stats: mustJSON(stats), Error: errMsg,
	})
	if err != nil {
		slog.Error("failed to record GC result", "error", err)
	}

	t.release()
}

func (t *gcTask) succeed(stats api.GCStats) { t.finish(api.GCTaskStateSucceeded, stats, "") }

func (t *gcTask) fail(stats api.GCStats, errMsg string) {
	t.finish(api.GCTaskStateFailed, stats, errMsg)
}

// GCTaskStore starts GC runs and reports on the latest one.
type GCTaskStore struct {
	pool *pgxpool.Pool
}

func NewGCTaskStore(pool *pgxpool.Pool) *GCTaskStore {
	return &GCTaskStore{pool: pool}
}

// StartResult describes the outcome of a Start call.
type StartResult struct {
	Task     *gcTask
	Status   api.GCTaskStatus
	IsNew    bool
	Conflict bool
}

func gcRunToStatus(r pg.GcRun) api.GCTaskStatus {
	st := api.GCTaskStatus{
		State:     api.GCTaskState(r.State),
		Phase:     api.GCTaskPhase(r.Phase),
		Error:     r.Error,
		StartedAt: r.StartedAt.Time.UTC(),
		UpdatedAt: r.UpdatedAt.Time.UTC(),
	}

	_ = json.Unmarshal(r.Params, &st.Params)
	_ = json.Unmarshal(r.Stats, &st.Stats)

	if r.FinishedAt.Valid {
		t := r.FinishedAt.Time.UTC()
		st.FinishedAt = &t
	}

	return st
}

// acquireGCLock takes the GC advisory lock on a dedicated connection. The
// returned func unlocks and releases the connection.
func acquireGCLock(ctx context.Context, pool *pgxpool.Pool) (func(), error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection for GC lock: %w", err)
	}

	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", gcAdvisoryLockKey).Scan(&acquired); err != nil {
		conn.Release()

		return nil, fmt.Errorf("failed to query GC advisory lock: %w", err)
	}

	if !acquired {
		conn.Release()

		return nil, errGCAlreadyRunning
	}

	//nolint:contextcheck // unlock must run even if the GC context is done
	release := func() {
		if _, err := conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", gcAdvisoryLockKey); err != nil {
			slog.Error("failed to release GC advisory lock", "error", err)
		}

		conn.Release()
	}

	return release, nil
}

// Start becomes the running GC (lock held until the task finishes), joins
// an identical running one, or reports a conflict.
func (s *GCTaskStore) Start(ctx context.Context, params api.GCTaskParams) (StartResult, error) {
	queries := pg.New(s.pool)

	release, err := acquireGCLock(ctx, s.pool)
	if errors.Is(err, errGCAlreadyRunning) {
		run, err := queries.GetLatestGCRun(ctx)
		if err != nil {
			return StartResult{}, fmt.Errorf("reading running GC: %w", err)
		}

		st := gcRunToStatus(run)
		if st.State == api.GCTaskStateRunning && st.Params == params {
			return StartResult{Status: st}, nil
		}

		return StartResult{Status: st, Conflict: true}, nil
	}

	if err != nil {
		return StartResult{}, err
	}

	if err := queries.FailInterruptedGCRuns(ctx); err != nil {
		release()

		return StartResult{}, fmt.Errorf("clearing interrupted GC runs: %w", err)
	}

	run, err := queries.InsertGCRun(ctx, mustJSON(params))
	if err != nil {
		release()

		return StartResult{}, fmt.Errorf("recording GC run: %w", err)
	}

	task := &gcTask{id: run.ID, pool: s.pool, release: release}

	return StartResult{Task: task, Status: gcRunToStatus(run), IsNew: true}, nil
}

// Get returns the latest GC run, if any.
func (s *GCTaskStore) Get(ctx context.Context) (api.GCTaskStatus, bool, error) {
	run, err := pg.New(s.pool).GetLatestGCRun(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return api.GCTaskStatus{}, false, nil
	}

	if err != nil {
		return api.GCTaskStatus{}, false, fmt.Errorf("reading GC run: %w", err)
	}

	return gcRunToStatus(run), true, nil
}
