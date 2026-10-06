package server_test

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/pg"
)

// A pin that commits while old closures are deleted must not fail the delete.
// The window is inside one statement, so pinners hammer a large table.
func TestDeleteClosuresRacingPins(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	const closures = 20000

	for round := range 3 {
		_, err := service.Pool.Exec(ctx, `INSERT INTO closures (key, updated_at)
			SELECT lpad(i::text, 32, '0') || '.narinfo', timezone('UTC', now()) - interval '30 days'
			FROM generate_series(1, $1::int) AS i`, closures)
		ok(t, err)

		var (
			stop atomic.Bool
			wg   sync.WaitGroup
		)

		for w := range 8 {
			wg.Go(func() {
				rng := rand.New(rand.NewPCG(uint64(round), uint64(w))) //nolint:gosec // test data

				for !stop.Load() {
					key := fmt.Sprintf("%032d.narinfo", 1+rng.IntN(closures))

					tx, err := service.Pool.Begin(ctx)
					if err != nil {
						return
					}

					q := pg.New(tx)
					if _, err := q.GetClosureForShare(ctx, key); err == nil {
						_ = q.UpsertPin(ctx, pg.UpsertPinParams{Name: "pin" + key[:8], NarinfoKey: key, StorePath: "/nix/store/x"})
					}

					_ = tx.Commit(ctx)
				}
			})
		}

		time.Sleep(20 * time.Millisecond)

		_, err = server.DeleteClosuresBefore(ctx, service, time.Now().UTC())

		stop.Store(true)
		wg.Wait()

		if err != nil {
			t.Fatalf("round %d: deleting old closures failed: %v", round, err)
		}

		_, err = service.Pool.Exec(ctx, `TRUNCATE pins, closures CASCADE`)
		ok(t, err)
	}
}
