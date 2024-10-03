package main

import (
	"fmt"
	"io"
	"log"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var schema = `
CREATE TABLE IF NOT EXISTS closures (
  id int generated always as identity primary key,
  upload_started_at timestamp not null
);

CREATE TABLE IF NOT EXISTS uploads (
  id int generated always as identity primary key,
  upload_started_at timestamp not null,
  closure_id int not null references closures(id)
);

CREATE TABLE IF NOT EXISTS objects (
	nar_hash char(32) primary key,
	reference_count integer not null
);
-- partial index to find objects with reference_count == 0
CREATE INDEX IF NOT EXISTS objects_reference_count_zero_idx ON objects(nar_hash) WHERE reference_count = 0;

CREATE TABLE IF NOT EXISTS closure_objects (
	closure_id int not null references closures(id),
	nar_hash char(32) not null references objects(nar_hash)
);
CREATE INDEX IF NOT EXISTS closure_objects_closure_id_idx ON closure_objects(closure_id);
`

type DB struct {
	db                       *sqlx.DB
	insertClosureStmt        *sqlx.Stmt
	insertUploadStmt         *sqlx.Stmt
	upsertObjects            *sqlx.NamedStmt
	insertClosureObjectsStmt *sqlx.NamedStmt
}

func cleanupDB(db *DB) {
		if db == nil {
			return
		}
		for _, obj := range []io.Closer{
			db.insertClosureStmt,
			db.insertUploadStmt,
			db.upsertObjects,
			db.insertClosureObjectsStmt,
		} {
			if obj == nil {
				return
			}
			if err := obj.Close(); err != nil {
					slog.Error("failed to close statement", "error", err)
			}
		}
	if db.db != nil {
		if err := db.db.Close(); err != nil {
			slog.Error("failed to close db", "error", err)
		}
	}
}

func ConnectDB(connectionString string) (*DB, error) {
	tmpDB := &DB{}
	defer cleanupDB(tmpDB)
	db, err := sqlx.Connect("postgres", connectionString)
	if err != nil {
		log.Fatalln(err)
	}
	tmpDB.db = db
	if _, err = db.Exec(schema); err != nil {
		return nil, fmt.Errorf("failed to create schema: %w", err)
	}

	insertClosureStmt, err := db.Preparex("INSERT INTO closures (upload_started_at) VALUES ($1) RETURNING id")
	if err != nil {
		return nil, err
	}
	tmpDB.insertClosureStmt = insertClosureStmt

	insertUploadStmt, err := db.Preparex("INSERT INTO uploads (upload_started_at, closure_id) VALUES ($1, $2) RETURNING id")
	if err != nil {
		return nil, err
	}
	tmpDB.insertUploadStmt = insertUploadStmt

	upsertObjects, err := db.PrepareNamed(`
		MERGE INTO objects USING (SELECT :nar_hash AS nar_hash) AS input
		ON objects.nar_hash = input.nar_hash
		WHEN MATCHED THEN
		  UPDATE SET reference_count = reference_count + 1
		WHEN NOT MATCHED THEN
		  INSERT (nar_hash, reference_count)
	    VALUES (:nar_hash, :reference_count)
	`)
	if err != nil {
		return nil, err
	}
	tmpDB.upsertObjects = upsertObjects

	insertClosureObjectsStmt, err := db.PrepareNamed(`INSERT INTO closure_objects (closure_id, nar_hash) VALUES (:closure_id, :nar_hash)`)
	if err != nil {
		return nil, err
	}
	tmpDB.insertClosureObjectsStmt = insertClosureObjectsStmt
	res := tmpDB
	tmpDB = nil
	return res, nil
}

type Upload struct {
	ID              int64     `json:"id"`
	UploadStartedAt time.Time `json:"upload_started_at"`
}

type ClosureObjectRow struct {
	ClosureID int64  `db:"closure_id"`
	NarHash   string `db:"nar_hash"`
}

type ObjectRow struct {
	NarHash        string `db:"nar_hash"`
	ReferenceCount int    `db:"reference_count"`
}

func (d *DB) StartUpload(storePaths []string) (*Upload, error) {
	startUpload := time.Now().UTC()
	tx, err := d.db.Beginx()
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	commitSuccess := false
	defer func() {
		if commitSuccess {
			return
		}
		if err = tx.Rollback(); err != nil {
			slog.Error("failed to rollback transaction", "error", err)
		}
	}()
	var closureID, uploadID int64

	row := tx.Stmtx(d.insertClosureStmt).QueryRowx(startUpload)
	if err = row.Scan(&closureID); err != nil {
		return nil, fmt.Errorf("failed to get closure id: %w", err)
	}
	row = tx.Stmtx(d.insertUploadStmt).QueryRowx(startUpload, closureID)
	if err = row.Scan(&uploadID); err != nil {
		return nil, fmt.Errorf("failed to insert upload: %w", err)
	}

	objects := make([]ObjectRow, 0, len(storePaths))
	// upsert objects
	for _, storePath := range storePaths {
		objects = append(objects, ObjectRow{
			NarHash:        storePath,
			ReferenceCount: 1,
		})
	}
	if _, err := tx.NamedStmt(d.upsertObjects).Exec(objects); err != nil {
		return nil, fmt.Errorf("failed to upsert objects: %w", err)
	}

	closureObjects := make([]ClosureObjectRow, 0, len(storePaths))
	for _, storePath := range storePaths {
		closureObjects = append(closureObjects, ClosureObjectRow{
			ClosureID: closureID,
			NarHash:   storePath,
		})
	}
	if _, err := tx.NamedStmt(d.insertClosureObjectsStmt).Exec(closureObjects); err != nil {
		return nil, fmt.Errorf("failed to insert closure objects: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	commitSuccess = true
	return &Upload{
		ID:              uploadID,
		UploadStartedAt: startUpload,
	}, nil
}

func (d *DB) Close() error {
	cleanupDB(d)
}
