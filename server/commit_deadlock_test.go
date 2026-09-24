package server_test

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgtype"
)

// Concurrent commits of closures that share objects must not deadlock.
// commit_pending_closure upserts every object of the closure in one statement;
// without a fixed order two commits lock the same rows in opposite orders and
// Postgres aborts one with "deadlock detected".
//
// The pending cleanup writes the objects of the closures it drops as
// tombstones in one INSERT too, so it must lock in the same order when it
// races commits that insert the same new objects.
func TestConcurrentCommitsSharingObjectsDoNotDeadlock(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()
	databaseOnlyTest(t)

	ctx := t.Context()
	q := pg.New(service.Pool)

	const (
		sharedObjects = 3000
		closures      = 4
	)

	keyRange := func(from int) []string {
		keys := make([]string, sharedObjects)
		for i := range keys {
			keys[i] = fmt.Sprintf("nar/%052d.nar.zst", from+i)
		}

		return keys
	}

	// pendingClosure inserts a closure over keys, in a different heap order
	// per closure, as the map iteration in createPendingClosureInner does.
	pendingClosure := func(hash string, keys []string) int64 {
		t.Helper()

		pc, err := q.InsertPendingClosure(ctx, hash+".narinfo")
		ok(t, err)

		rows := make([]pg.InsertPendingObjectsParams, 0, len(keys)+1)
		rows = append(rows, pg.InsertPendingObjectsParams{PendingClosureID: pc.ID, Key: hash + ".narinfo", Refs: keys})

		for _, j := range rand.Perm(len(keys)) { //nolint:gosec // shuffling test data
			rows = append(rows, pg.InsertPendingObjectsParams{PendingClosureID: pc.ID, Key: keys[j], Refs: []string{}})
		}

		_, err = q.InsertPendingObjects(ctx, rows)
		ok(t, err)

		return pc.ID
	}

	concurrently := func(what string, ops ...func() error) {
		t.Helper()

		var wg sync.WaitGroup

		errs := make(chan error, len(ops))

		for _, op := range ops {
			wg.Go(func() { errs <- op() })
		}

		wg.Wait()
		close(errs)

		for err := range errs {
			if err != nil {
				t.Errorf("%s failed: %v", what, err)
			}
		}
	}

	commit := func(id int64) func() error {
		return func() error { return q.CommitPendingClosure(ctx, id) }
	}

	shared := keyRange(0)
	commits := make([]func() error, 0, closures)

	for i := range closures {
		commits = append(commits, commit(pendingClosure(strings.Repeat(string(rune('a'+i)), 32), shared)))
	}

	concurrently("concurrent commit", commits...)

	// An aged closure over objects that do not exist yet, dropped by the
	// cleanup while three fresh closures over the same objects commit.
	fresh := keyRange(sharedObjects)
	aged := pendingClosure(strings.Repeat("w", 32), fresh)

	_, err := service.Pool.Exec(ctx, "UPDATE pending_closures SET started_at = started_at - interval '2 hours' WHERE id = $1", aged)
	ok(t, err)

	const racingCommits = 3

	ops := make([]func() error, 0, racingCommits+1)
	ops = append(ops, func() error {
		_, err := q.CleanupPendingClosures(ctx, pg.CleanupPendingClosuresParams{
			Cutoff: pgtype.Timestamp{Time: time.Now().UTC().Add(-time.Hour), Valid: true},
			Keep:   []int64{},
		})

		return err //nolint:wrapcheck // reported as is
	})

	for i := range racingCommits {
		ops = append(ops, commit(pendingClosure(strings.Repeat(string(rune('x'+i)), 32), fresh)))
	}

	concurrently("pending cleanup racing commits", ops...)
}
