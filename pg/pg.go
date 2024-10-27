package pg

import (
	"context"
	"embed"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql functions/*.sql
var embedMigrations embed.FS

func Connect(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	slog.Debug("connecting to database", "connection_string", connString)

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	// migrate the database
	slog.Debug("migrating database")
	goose.SetBaseFS(embedMigrations)

	db := stdlib.OpenDBFromPool(pool)

	if err = goose.SetDialect("postgres"); err != nil {
		return nil, fmt.Errorf("failed to set dialect: %w", err)
	} else if err = goose.Up(db, "migrations"); err != nil {
		return nil, fmt.Errorf("failed to migrate db: %w", err)
	} else if err = goose.Up(db, "functions", goose.WithNoVersioning()); err != nil {
		return nil, fmt.Errorf("failed to migrate stored procedures: %w", err)
	}

	return pool, err
}
