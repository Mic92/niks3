package main

import (
	"fmt"
	"log/slog"
	"time"
)

type UploadResponse struct {
	ID        string    `json:"id"`
	StartedAt time.Time `json:"started_at"`
}

const upsertClosureQuery = `
MERGE INTO closures USING (SELECT :key AS key) AS input
ON closures.key = input.key
WHEN MATCHED THEN
  UPDATE SET updated_at = :updated_at
WHEN NOT MATCHED THEN
  INSERT (key, updated_at)
  VALUES (:key, :updated_at)
`

type upsertClosureParams struct {
	Key       string    `db:"key"`
	UpdatedAt time.Time `db:"updated_at"`
}

const upsertObjectQuery = `
	INSERT INTO objects (key, reference_count)
	VALUES (:key, 1)
	ON CONFLICT (key) DO UPDATE SET reference_count = objects.reference_count + 1
`

type objectParams struct {
	Key string `db:"key"`
}

const insertUploadQuery = "INSERT INTO uploads (started_at, closure_key) VALUES (:started_at, :closure_key) RETURNING id"

type uploadArgsParams struct {
	ClosureKey string    `db:"closure_key"`
	StartedAt  time.Time `db:"started_at"`
}

const insertClosureObjectsQuery = `INSERT INTO closure_objects (closure_key, object_key) VALUES (:closure_key, :object_key)`

type closureObjectParams struct {
	ClosureKey string `db:"closure_key"`
	ObjectKey  string `db:"object_key"`
}

func (d *DB) StartUpload(closureKey string, storePathSet map[string]bool) (*UploadResponse, error) {
	uploadStartedAt := time.Now().UTC()
	tx, err := d.db.Beginx()
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer func() {
		if err == nil {
			return
		}
		if err = tx.Rollback(); err != nil {
			slog.Error("failed to rollback transaction", "error", err)
		}
	}()
	var uploadID int64

	// upsert closure
	if _, err = tx.NamedStmt(d.upsertClosureStmt).Exec(upsertClosureParams{
		Key:       closureKey,
		UpdatedAt: uploadStartedAt,
	}); err != nil {
		return nil, fmt.Errorf("failed to upsert closure: %w", err)
	}

	if err = tx.NamedStmt(d.insertUploadStmt).QueryRowx(uploadArgsParams{
		StartedAt:  uploadStartedAt,
		ClosureKey: closureKey,
	}).Scan(&uploadID); err != nil {
		return nil, fmt.Errorf("failed to insert upload: %w", err)
	}

	objects := make([]objectParams, 0, len(storePathSet))
	// upsert objects
	for path := range storePathSet {
		objects = append(objects, objectParams{Key: path})
	}
	if _, err = tx.NamedExec(upsertObjectQuery, objects); err != nil {
		return nil, fmt.Errorf("failed to upsert objects: %w", err)
	}

	closureObjectRows := make([]closureObjectParams, 0, len(storePathSet))
	for storePath := range storePathSet {
		closureObjectRows = append(closureObjectRows, closureObjectParams{
			ClosureKey: closureKey,
			ObjectKey:  storePath,
		})
	}

	if _, err = tx.NamedExec(insertClosureObjectsQuery, closureObjectRows); err != nil {
		return nil, fmt.Errorf("failed to insert closure objects: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return &UploadResponse{
		// use string to avoid json marshalling issues
		ID:        fmt.Sprintf("%d", uploadID),
		StartedAt: uploadStartedAt,
	}, nil
}
