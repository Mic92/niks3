package server

import (
	"sync"
	"time"

	"github.com/Mic92/niks3/api"
)

// gcTask is a mutable, concurrency-safe wrapper around api.GCTaskStatus.
type gcTask struct {
	mu     sync.RWMutex
	status api.GCTaskStatus
}

func (t *gcTask) snapshot() api.GCTaskStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.status
}

func (t *gcTask) setPhase(phase api.GCTaskPhase) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.status.Phase = phase
	t.status.UpdatedAt = time.Now().UTC()
}

func (t *gcTask) updateStats(stats api.GCStats) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.status.Stats = stats
	t.status.UpdatedAt = time.Now().UTC()
}

func (t *gcTask) succeed(stats api.GCStats) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now().UTC()
	t.status.State = api.GCTaskStateSucceeded
	t.status.Stats = stats
	t.status.Phase = ""
	t.status.UpdatedAt = now
	t.status.FinishedAt = &now
}

func (t *gcTask) fail(stats api.GCStats, errMsg string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now().UTC()
	t.status.State = api.GCTaskStateFailed
	t.status.Stats = stats
	t.status.Error = errMsg
	t.status.UpdatedAt = now
	t.status.FinishedAt = &now
}

// GCTaskStore manages the singleton GC task. At most one GC runs at a time;
// the most recent task (active or completed) is always available for status
// polling via GET /api/gc/status.
type GCTaskStore struct {
	mu   sync.RWMutex
	task *gcTask
}

func NewGCTaskStore() *GCTaskStore {
	return &GCTaskStore{}
}

// StartResult describes the outcome of a Start call.
type StartResult struct {
	Task     *gcTask
	Status   api.GCTaskStatus
	IsNew    bool
	Conflict bool
}

// Start creates a new GC task, deduplicates to the active one if params match,
// or reports a conflict.
func (s *GCTaskStore) Start(params api.GCTaskParams) StartResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.task != nil {
		snap := s.task.snapshot()
		if snap.State == api.GCTaskStateRunning {
			if snap.Params == params {
				return StartResult{Task: s.task, Status: snap}
			}

			return StartResult{Status: snap, Conflict: true}
		}
	}

	now := time.Now().UTC()

	task := &gcTask{
		status: api.GCTaskStatus{
			State:     api.GCTaskStateRunning,
			Params:    params,
			StartedAt: now,
			UpdatedAt: now,
		},
	}
	s.task = task

	return StartResult{Task: task, Status: task.status, IsNew: true}
}

// Get returns a snapshot of the current (or most recent) GC task.
func (s *GCTaskStore) Get() (api.GCTaskStatus, bool) {
	s.mu.RLock()
	t := s.task
	s.mu.RUnlock()

	if t == nil {
		return api.GCTaskStatus{}, false
	}

	return t.snapshot(), true
}
