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

// A peer replica migrating the same database holds goose's session lock, and
// a migration rewriting a large table can run for minutes. Either outlasts
// the budget for reaching the database; Connect must wait for the schema
// instead of failing startup, which would fail every restart the same way.
func TestConnectWaitsForAPeerMigration(t *testing.T) {
	t.Parallel()

	connString := createTestDatabase(t)

	peer, err := pgx.Connect(t.Context(), connString)
	ok(t, err)

	defer func() { _ = peer.Close(context.Background()) }()

	_, err = peer.Exec(t.Context(), "SELECT pg_advisory_lock($1)", lock.DefaultLockID)
	ok(t, err)

	const connectBudget = time.Second

	// The peer finishes after the budget has run out.
	time.AfterFunc(2*connectBudget, func() { _ = peer.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(t.Context(), connectBudget)
	defer cancel()

	pool, err := pg.Connect(ctx, connString)
	if err != nil {
		t.Fatalf("Connect gave up while a peer held the migration lock: %v", err)
	}

	defer pool.Close()

	var tables int
	ok(t, pool.QueryRow(t.Context(), "SELECT count(*) FROM pg_tables WHERE tablename = 'objects'").Scan(&tables))

	if tables != 1 {
		t.Fatalf("schema not migrated after Connect returned")
	}
}
