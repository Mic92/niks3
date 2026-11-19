package server_test

import (
	"testing"
	"time"
)

func TestGC_KeepsWhatAPushCommittedDuringTheMark(t *testing.T) {
	t.Parallel()

	service := claimService(t)
	defer service.Close()

	ctx := t.Context()

	other, dep, top := newBuild(1, 1, 1), newBuild(2, 1, 1), newBuild(3, 1, 1)
	completePush(t, service, other.push(t, service))
	completePush(t, service, dep.push(t, service))

	_, err := service.Pool.Exec(ctx, `DELETE FROM closures`)
	ok(t, err)

	// The mark locks the rows it changes, so it stops here after it has
	// decided what is unreachable.
	hold, err := service.Pool.Begin(ctx)
	ok(t, err)

	defer func() { _ = hold.Rollback(ctx) }()

	_, err = hold.Exec(ctx, `SELECT FROM objects WHERE key = $1 FOR UPDATE`, other.keys()[0])
	ok(t, err)

	collected := make(chan struct{})

	go func() {
		defer close(collected)

		collectGarbage(t, service)
	}()

	for countRows(t, service, `SELECT count(*) FROM pg_stat_activity
		WHERE wait_event_type = 'Lock' AND datname = current_database()`) == 0 {
		time.Sleep(10 * time.Millisecond)
	}

	depKeys := []string{dep.keys()[0], dep.nars[0]}
	push := createPush(t, service, top.keys(), []map[string]any{
		{"key": top.keys()[0], "type": "narinfo", "refs": []string{top.nars[0], depKeys[0]}},
		{"key": top.nars[0], "type": "nar", "refs": []string{}},
		{"key": depKeys[0], "type": "narinfo", "refs": []string{depKeys[1]}},
		{"key": depKeys[1], "type": "nar", "refs": []string{}},
	})
	completePush(t, service, push.ID)

	ok(t, hold.Rollback(ctx))
	<-collected
	collectGarbage(t, service)

	if kept := countRows(t, service, `SELECT count(*) FROM objects WHERE key = any($1)`, depKeys); kept != 2 {
		t.Fatalf("the cache holds %d of the 2 objects the pushed path depends on", kept)
	}
}
