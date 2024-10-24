package main

import (
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// Basic structure of a binary cache:
//   ././
//  ├──  26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo
//  ├──  4hcdxyjf9yiq7qf3i4548drb6sjmwa1v.narinfo
//  ├──  jwsdpq2yxw43ixalh93z726czz7bay2j.narinfo
//  ├──  log/
//  ├──  nar/
//  │   ├──  08242al70hn299yh1vk6il2cyahh6p86qvm72rmqz1z07q36vsk2.nar.xz
//  │   ├──  1767a9kz9xjpy5nh94d1prn3wv8rlcw7k9xhcsm0qcnx4l5qhq2n.nar.xz
//  │   ├──  17fm917985vcvrkrsckjb3i7q6rsxc4xlw8m1d6i5hdmxf9rxhh2.nar.xz
//  │   ├──  1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.xz
//  │   └──  1qva1j5l6gwjlj2xw69r3w8ldcgs14vp33hl7rm124r6q3fw13il.nar.xz
//  ├──  nix-cache-info
//  ├──  realisations/
//  │   └──  sha256:9d7d12c511042dac015ce38181f045b86da5a8d83a6d0364fa3b3fc48d28c203!out.doi
//  ├──  sl141d1g77wvhr050ah87lcyz2czdxa3.narinfo
//  └──  w19cxz37j5nrkg8w80y91bga89310jgi.narinfo

var schema = `
CREATE TABLE IF NOT EXISTS closures (
	key char(32) primary key,
  updated_at timestamp not null
);

CREATE TABLE IF NOT EXISTS uploads (
  id int generated always as identity primary key,
  started_at timestamp not null,
  closure_key char(32) not null references closures(key)
);
CREATE TABLE IF NOT EXISTS objects (
  -- do we need longer names than this?
  -- how long can be output names?
	key char(200) primary key,
	reference_count integer not null
);
-- partial index to find objects with reference_count == 0
CREATE INDEX IF NOT EXISTS objects_reference_count_zero_idx ON objects(key) WHERE reference_count = 0;

CREATE TABLE IF NOT EXISTS closure_objects (
	closure_key char(32) not null references closures(key),
	object_key char(200) not null references objects(key)
);
CREATE INDEX IF NOT EXISTS closure_objects_closure_key_idx ON closure_objects(closure_key);
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
