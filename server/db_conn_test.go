package server_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3/lock"
)

func TestResolveDBConnectionString(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	file := filepath.Join(dir, "uri")

	if err := os.WriteFile(file, []byte("postgres://from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	lookup := func(env map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) {
			v, ok := env[k]

			return v, ok
		}
	}

	tests := []struct {
		name    string
		flag    string
		file    string
		env     map[string]string
		want    string
		wantErr bool
	}{
		{name: "flag wins", flag: "postgres://flag", file: file, want: "postgres://flag"},
		{name: "file when flag empty", file: file, want: "postgres://from-file"},
		{name: "missing file is an error", file: filepath.Join(dir, "nope"), wantErr: true},
		{name: "PGHOST allows empty", env: map[string]string{"PGHOST": "db"}, want: ""},
		{name: "nothing configured", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := server.ResolveDBConnectionString(tc.flag, tc.file, lookup(tc.env))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}

			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// A migrating peer holds goose's session lock. Connect must wait for it
// instead of failing startup when its budget runs out.
func TestConnectWaitsForAPeerMigration(t *testing.T) {
	t.Parallel()

	connString := createTestDatabase(t)

	peer, err := pgx.Connect(t.Context(), connString)
	ok(t, err)

	defer func() { _ = peer.Close(context.Background()) }()

	_, err = peer.Exec(t.Context(), "SELECT pg_advisory_lock($1)", lock.DefaultLockID)
	ok(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var (
		pool    *pgxpool.Pool
		connErr error
	)

	done := make(chan struct{})

	go func() {
		defer close(done)

		pool, connErr = pg.Connect(ctx, connString)
	}()

	// Run out the budget only once Connect waits, so a slow connection on a
	// loaded builder cannot use it up first.
	waitForLockAttempt(t, peer, done)
	cancel()

	_ = peer.Close(context.Background())

	<-done

	if connErr != nil {
		t.Fatalf("Connect gave up while a peer held the migration lock: %v", connErr)
	}

	defer pool.Close()

	var tables int
	ok(t, pool.QueryRow(t.Context(), "SELECT count(*) FROM pg_tables WHERE tablename = 'objects'").Scan(&tables))

	if tables != 1 {
		t.Fatalf("schema not migrated after Connect returned")
	}
}

// waitForLockAttempt waits until another session has tried goose's lock,
// which peer holds.
func waitForLockAttempt(t *testing.T, peer *pgx.Conn, done <-chan struct{}) {
	t.Helper()

	for {
		var attempts int
		ok(t, peer.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid()
			AND query LIKE 'SELECT pg_try_advisory_lock%'`).Scan(&attempts))

		if attempts > 0 {
			return
		}

		select {
		case <-done:
			t.Fatal("Connect returned while a peer held the migration lock")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
