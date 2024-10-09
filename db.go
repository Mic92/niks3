package main

import (
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var schema = `
CREATE TABLE IF NOT EXISTS closures (
	nar_hash char(32) primary key,
  updated_at timestamp not null
);

CREATE TABLE IF NOT EXISTS uploads (
  id int generated always as identity primary key,
  started_at timestamp not null,
  closure_nar_hash char(32) not null references closures(nar_hash)
);

CREATE TABLE IF NOT EXISTS objects (
	nar_hash char(32) primary key,
	reference_count integer not null
);
-- partial index to find objects with reference_count == 0
CREATE INDEX IF NOT EXISTS objects_reference_count_zero_idx ON objects(nar_hash) WHERE reference_count = 0;

CREATE TABLE IF NOT EXISTS closure_objects (
	closure_nar_hash char(32) not null references closures(nar_hash),
	nar_hash char(32) not null references objects(nar_hash)
);
CREATE INDEX IF NOT EXISTS closure_objects_closure_nar_hash_idx ON closure_objects(closure_nar_hash);
`

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
	if _, err = d.db.Exec(schema); err != nil {
		return nil, fmt.Errorf("failed to create schema: %w", err)
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
