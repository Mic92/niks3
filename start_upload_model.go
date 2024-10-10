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
MERGE INTO closures USING (SELECT :nar_hash AS nar_hash) AS input
ON closures.nar_hash = input.nar_hash
WHEN MATCHED THEN
  UPDATE SET updated_at = :updated_at
WHEN NOT MATCHED THEN
  INSERT (nar_hash, updated_at)
  VALUES (:nar_hash, :updated_at)
`

type upsertClosureParams struct {
	NarHash   string    `db:"nar_hash"`
	UpdatedAt time.Time `db:"updated_at"`
}

//const upsertObjectQuery = `
//MERGE INTO objects USING (SELECT :nar_hash AS nar_hash) AS input
//ON objects.nar_hash = input.nar_hash
//WHEN MATCHED THEN
//  UPDATE SET reference_count = reference_count + 1
//WHEN NOT MATCHED THEN
//  INSERT (nar_hash, reference_count)
//  VALUES (:nar_hash, 1)
//`

const upsertObjectQuery = `
	INSERT INTO objects (nar_hash, reference_count)
	VALUES (:nar_hash, 1)
	ON CONFLICT (nar_hash) DO UPDATE SET reference_count = objects.reference_count + 1
`

type objectParams struct {
	NarHash string `db:"nar_hash"`
}

const insertUploadQuery = "INSERT INTO uploads (started_at, closure_nar_hash) VALUES (:started_at, :closure_nar_hash) RETURNING id"

type uploadArgsParams struct {
	StartedAt      time.Time `db:"started_at"`
	ClosureNarHash string    `db:"closure_nar_hash"`
}

const insertClosureObjectsQuery = `INSERT INTO closure_objects (closure_nar_hash, nar_hash) VALUES (:closure_nar_hash, :nar_hash)`

type closureObjectParams struct {
	ClosureNarHash string `db:"closure_nar_hash"`
	NarHash        string `db:"nar_hash"`
}

func (d *DB) StartUpload(closureNarHash string, storePaths []string) (*UploadResponse, error) {
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
		NarHash:   closureNarHash,
		UpdatedAt: uploadStartedAt,
	}); err != nil {
		return nil, fmt.Errorf("failed to upsert closure: %w", err)
	}

	if err = tx.NamedStmt(d.insertUploadStmt).QueryRowx(uploadArgsParams{
		StartedAt:      uploadStartedAt,
		ClosureNarHash: closureNarHash,
	}).Scan(&uploadID); err != nil {
		return nil, fmt.Errorf("failed to insert upload: %w", err)
	}

	objects := make([]objectParams, 0, len(storePaths))
	// upsert objects
	for _, storePath := range storePaths {
		objects = append(objects, objectParams{NarHash: storePath})
	}
	if _, err = tx.NamedExec(upsertObjectQuery, objects); err != nil {
		return nil, fmt.Errorf("failed to upsert objects: %w", err)
	}

	closureObjectRows := make([]closureObjectParams, 0, len(storePaths))
	for _, storePath := range storePaths {
		closureObjectRows = append(closureObjectRows, closureObjectParams{
			ClosureNarHash: closureNarHash,
			NarHash:        storePath,
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
