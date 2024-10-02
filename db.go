package main

import (
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var schema = `
CREATE TABLE IF NOT EXISTS closures (
  id int generated always as identity primary key,
  created_at timestamp not null
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

type Db struct {
	db *sqlx.DB
}

func ConnectDB(connectionString string) (*Db, error) {
	db, err := sqlx.Connect("postgres", connectionString)
	if err != nil {
		log.Fatalln(err)
	}
	db.MustExec(schema)
	return &Db{db}, nil
}

type Upload struct {
	ID              int64     `json:"id"`
	UploadStartedAt time.Time `json:"upload_started_at"`
}

type ClosureObject struct {
	ClosureID int64 `db:"closure_id"`
	// maybe a fixed size byte array would be better?
	NarHash string `db:"nar_hash"`
}

type Objects struct {
	NarHash        string `db:"nar_hash"`
	ReferenceCount int    `db:"reference_count"`
}

func (d *Db) GarbageCollect() error {
	_, err := d.db.Exec(`
		DELETE FROM objects
		WHERE nar_hash IN (
			SELECT nar_hash
			FROM objects
			WHERE reference_count = 0
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to garbage collect objects: %w", err)
	}
	return nil
}

func (d *Db) StartUpload(storePaths []string) (*Upload, error) {
	startUpload := time.Now()
	tx, err := d.db.Beginx()
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	commitSuccess := false
	defer func() {
		if !commitSuccess {
			err = tx.Rollback()
			if err != nil {
				slog.Error("failed to rollback transaction", "error", err)
			}
		}
	}()
	var closureID, uploadID int64

	row := tx.QueryRow("INSERT INTO closures (created_at) VALUES ($1) RETURNING id", startUpload)
	if err = row.Scan(&closureID); err != nil {
		return nil, fmt.Errorf("failed to get closure id: %w", err)
	}
	row = tx.QueryRow("INSERT INTO uploads (upload_started_at, closure_id) VALUES ($1, $2) RETURNING id", startUpload, closureID)
	if err = row.Scan(&uploadID); err != nil {
		return nil, fmt.Errorf("failed to insert upload: %w", err)
	}

	objects := make([]Objects, 0, len(storePaths))
	// upsert objects
	for _, storePath := range storePaths {
		objects = append(objects, Objects{
			NarHash:        storePath,
			ReferenceCount: 1,
		})
	}
	_, err = tx.NamedExec(`
		MERGE INTO objects USING (SELECT :nar_hash AS nar_hash) AS input
		ON objects.nar_hash = input.nar_hash
		WHEN MATCHED THEN
		  UPDATE SET reference_count = reference_count + 1
		WHEN NOT MATCHED THEN
		  INSERT (nar_hash, reference_count)
	    VALUES (:nar_hash, :reference_count)
	`, objects)

	if err != nil {
		return nil, fmt.Errorf("failed to upsert objects: %w", err)
	}

	closureObjects := make([]ClosureObject, 0, len(storePaths))
	slog.Info("Inserting closure objects", "closure_id", closureID, "store_paths", storePaths)
	for _, storePath := range storePaths {
		closureObjects = append(closureObjects, ClosureObject{
			ClosureID: closureID,
			NarHash:   storePath,
		})
	}
	slog.Info("Inserting closure objects", "closure_objects", closureObjects)
	_, err = tx.NamedExec(`INSERT INTO closure_objects (closure_id, nar_hash) VALUES (:closure_id, :nar_hash)`, closureObjects)
	if err != nil {
		return nil, fmt.Errorf("failed to insert closure objects: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	commitSuccess = true
	return &Upload{
		ID:              uploadID,
		UploadStartedAt: startUpload,
	}, nil
}

func (d *Db) Close() error {
	return d.db.Close()
}
