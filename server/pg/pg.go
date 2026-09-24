package pg

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed migrations/*.sql functions/*.sql
var embedMigrations embed.FS

// Connect opens a pool on connString and brings the schema up to date.
//
// Migrations run through a goose Provider rather than the package-level
// goose functions: those keep their base FS and dialect in globals, so two
// Connect calls in one process (the test suite) race on them, and they take
// no lock, so two replicas starting against one database could run the same
// migration at once. The provider holds a Postgres session-level advisory
// lock for the duration.
//
// ctx bounds reaching the database, not migrating it: a migration that
// rewrites a large table, or waiting for a peer's run to release the lock,
// can take longer than any connect timeout, and failing it would fail every
// restart the same way. The lock wait is bounded by goose's own retry limit.
func Connect(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	slog.Debug("connecting to database", "connection_string", connString)

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()

		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	if err := migrate(context.WithoutCancel(ctx), pool); err != nil {
		pool.Close()

		return nil, err
	}

	return pool, nil
}

func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	slog.Debug("migrating database")

	db := stdlib.OpenDBFromPool(pool)

	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("failed to create migration locker: %w", err)
	}

	migrations, err := fs.Sub(embedMigrations, "migrations")
	if err != nil {
		return fmt.Errorf("failed to open embedded migrations: %w", err)
	}

	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations, goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("failed to create migration provider: %w", err)
	}

	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("failed to migrate db: %w", err)
	}

	// Stored procedures are re-applied on every start (CREATE OR REPLACE),
	// so they are not versioned.
	functions, err := fs.Sub(embedMigrations, "functions")
	if err != nil {
		return fmt.Errorf("failed to open embedded functions: %w", err)
	}

	provider, err = goose.NewProvider(goose.DialectPostgres, db, functions,
		goose.WithSessionLocker(locker), goose.WithDisableVersioning(true))
	if err != nil {
		return fmt.Errorf("failed to create stored procedure provider: %w", err)
	}

	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("failed to migrate stored procedures: %w", err)
	}

	return nil
}
