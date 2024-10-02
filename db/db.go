package db

import (
	bolt "go.etcd.io/bbolt/cmd/bbolt"
)

type Db struct {
	db *bolt.DB
}

func New(dbPath string) (*Db, error) {
	db, err := bolt.Open(dbPath, 0o600, nil)
	if err != nil {
		return nil, err
	}
	return &Db{db}, nil
}

func (d *Db) Close() error {
	return d.db.Close()
}
