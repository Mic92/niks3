package server_test

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"

	"github.com/Mic92/niks3/server/pg"
)

// Concurrent commits of closures that share objects must not deadlock.
// commit_pending_closure upserts every object of the closure in one statement;
// without a fixed order two commits lock the same rows in opposite orders and
// Postgres aborts one with "deadlock detected".
func TestConcurrentCommitsSharingObjectsDoNotDeadlock(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	q := pg.New(service.Pool)

	const (
		sharedObjects = 3000
		closures      = 4
	)

	keys := make([]string, sharedObjects)
	for i := range keys {
		keys[i] = fmt.Sprintf("nar/%052d.nar.zst", i)
	}

	ids := make([]int64, 0, closures)

	for i := range closures {
		hash := strings.Repeat(string(rune('a'+i)), 32)

		pc, err := q.InsertPendingClosure(ctx, hash+".narinfo")
		ok(t, err)

		// Insert the shared objects in a different heap order per closure, as
		// the map iteration in createPendingClosureInner does.
		rows := make([]pg.InsertPendingObjectsParams, 0, sharedObjects+1)
		rows = append(rows, pg.InsertPendingObjectsParams{PendingClosureID: pc.ID, Key: hash + ".narinfo", Refs: keys})

		for _, j := range rand.Perm(sharedObjects) { //nolint:gosec // shuffling test data
			rows = append(rows, pg.InsertPendingObjectsParams{PendingClosureID: pc.ID, Key: keys[j], Refs: []string{}})
		}

		_, err = q.InsertPendingObjects(ctx, rows)
		ok(t, err)

		ids = append(ids, pc.ID)
	}

	var wg sync.WaitGroup

	errs := make(chan error, closures)

	for _, id := range ids {
		wg.Go(func() { errs <- q.CommitPendingClosure(ctx, id) })
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("concurrent commit failed: %v", err)
		}
	}
}
