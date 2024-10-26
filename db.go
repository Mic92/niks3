package main

import (
	"embed"
	"fmt"
	"log/slog"

	"github.com/pressly/goose/v3"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

//go:embed migrations/*.sql
var embedMigrations embed.FS

type DB struct {
	db                       *sqlx.DB
	upsertClosureStmt        *sqlx.NamedStmt
	insertUploadStmt         *sqlx.NamedStmt
	upsertObjectsStmt        *sqlx.NamedStmt
	insertClosureObjectsStmt *sqlx.NamedStmt
	deleteUploadStmt         *sqlx.NamedStmt
}

func ConnectDB(connectionString string) (*DB, error) {
	d := &DB{}
	var err error
	defer func() {
		if err == nil {
			return
		}
		d.Close()
	}()
	slog.Debug("connecting to database", "connection_string", connectionString)

	if d.db, err = sqlx.Connect("postgres", connectionString); err != nil {
		return nil, fmt.Errorf("failed to connect to db: %w", err)
	}

	// migrate the database
	slog.Debug("migrating database")
	goose.SetBaseFS(embedMigrations)

	if err := goose.SetDialect("postgres"); err != nil {
		return nil, fmt.Errorf("failed to set dialect: %w", err)
	} else if err = goose.Up(d.db.DB, "migrations"); err != nil {
		return nil, fmt.Errorf("failed to migrate db: %w", err)
	}

	if d.upsertClosureStmt, err = d.db.PrepareNamed(upsertClosureQuery); err != nil {
		return nil, fmt.Errorf("failed to prepare upsert closure statement: %w", err)
	}

	if d.insertUploadStmt, err = d.db.PrepareNamed(insertUploadQuery); err != nil {
		return nil, fmt.Errorf("failed to prepare insert upload statement: %w", err)
	}

	if d.upsertObjectsStmt, err = d.db.PrepareNamed(upsertObjectQuery); err != nil {
		return nil, fmt.Errorf("failed to prepare upsert objects statement: %w", err)
	}

	if d.insertClosureObjectsStmt, err = d.db.PrepareNamed(insertClosureObjectsQuery); err != nil {
		return nil, fmt.Errorf("failed to prepare insert closure objects statement: %w", err)
	}

	if d.deleteUploadStmt, err = d.db.PrepareNamed(deleteUploadQuery); err != nil {
		return nil, fmt.Errorf("failed to prepare delete upload statement: %w", err)
	}
	return d, nil
}

func (d *DB) Close() {
	if d.db != nil {
		if err := d.db.Close(); err != nil {
			slog.Error("failed to close db", "error", err)
		}
	}
	for _, obj := range []*sqlx.NamedStmt{
		d.upsertClosureStmt,
		d.insertUploadStmt,
		d.upsertObjectsStmt,
		d.insertClosureObjectsStmt,
	} {
		if obj == nil {
			continue
		}

		if err := obj.Close(); err != nil {
			slog.Error("failed to close statement", "error", err)
		}
	}
}
