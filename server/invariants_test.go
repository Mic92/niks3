package server_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	pgx "github.com/jackc/pgx/v5"
	minio "github.com/minio/minio-go/v7"
)

// storeExemptions lists, per test, keys whose database row the test wrote
// without an object behind it on purpose (seeded state, not an outcome).
//
//nolint:gochecknoglobals // keyed by test, read by the cleanup of the same test
var storeExemptions sync.Map

// seededWithoutS3 exempts keys a test made live in the objects table without
// uploading them from the live-row-has-an-object invariant. Only fixtures
// belong here; anything the code under test decided must still hold.
func seededWithoutS3(tb testing.TB, keys ...string) {
	tb.Helper()

	exemptions(tb).add(keys...)
}

// databaseOnlyTest exempts every live row of a test that exercises the
// database alone and never uploads what it commits.
func databaseOnlyTest(tb testing.TB) {
	tb.Helper()

	set := exemptions(tb)
	set.mu.Lock()
	set.all = true
	set.mu.Unlock()
}

func exemptions(tb testing.TB) *keySet {
	tb.Helper()

	v, _ := storeExemptions.LoadOrStore(tb, &keySet{})
	set, _ := v.(*keySet)

	return set
}

type keySet struct {
	mu   sync.Mutex
	all  bool
	keys map[string]bool
}

func (s *keySet) add(keys ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.keys == nil {
		s.keys = make(map[string]bool, len(keys))
	}

	for _, k := range keys {
		s.keys[k] = true
	}
}

func (s *keySet) has(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.all || s.keys[key]
}

// checkStoreInvariants asserts, once a test is done with its service, that
// the database and the bucket agree, without any hypothesis about what the
// test did:
//
//   - every live objects row has an object in S3, or a client that was told
//     the object is present would be served a miss;
//   - every multipart upload still open in the bucket has a multipart_uploads
//     row, the only handle anything can reap it by;
//   - object_stats equals a recount of the live rows.
//
// It opens its own connection and S3 client because the test may have
// closed the service's pool or swapped its S3 client for a broken one.
func checkStoreInvariants(tb testing.TB, connString, bucket string) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		tb.Errorf("store invariants: connect: %v", err)

		return
	}

	defer func() { _ = conn.Close(ctx) }()

	s3 := testRustfsServer.Client(tb)

	inBucket := map[string]bool{}

	for obj := range s3.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
		if obj.Err != nil {
			tb.Errorf("store invariants: list bucket: %v", obj.Err)

			return
		}

		inBucket[obj.Key] = true
	}

	exempt := &keySet{}
	if v, found := storeExemptions.LoadAndDelete(tb); found {
		exempt, _ = v.(*keySet)
	}

	live, err := queryStrings(ctx, conn, "SELECT key FROM objects WHERE deleted_at IS NULL ORDER BY key")
	if err != nil {
		tb.Errorf("store invariants: %v", err)

		return
	}

	var missing []string

	for _, key := range live {
		if !inBucket[key] && !exempt.has(key) {
			missing = append(missing, key)
		}
	}

	if len(missing) > 0 {
		tb.Errorf("store invariant: %d live objects row(s) with no object in S3, e.g. %v",
			len(missing), missing[:min(3, len(missing))])
	}

	tracked, err := queryStrings(ctx, conn, "SELECT upload_id FROM multipart_uploads")
	if err != nil {
		tb.Errorf("store invariants: %v", err)

		return
	}

	for upload := range s3.ListIncompleteUploads(ctx, bucket, "", true) {
		if upload.Err != nil {
			tb.Errorf("store invariants: list multipart uploads: %v", upload.Err)

			return
		}

		if !slices.Contains(tracked, upload.UploadID) {
			tb.Errorf("store invariant: multipart upload %s of %s is open in S3 with no multipart_uploads row",
				upload.UploadID, upload.Key)
		}
	}

	var statCount, statBytes, liveCount, liveBytes int64

	err = conn.QueryRow(ctx, `SELECT s.object_count, s.total_bytes, l.n, l.bytes
		FROM object_stats AS s,
		     (SELECT count(*) AS n, coalesce(sum(size), 0)::bigint AS bytes FROM objects WHERE deleted_at IS NULL) AS l
		WHERE s.id`).Scan(&statCount, &statBytes, &liveCount, &liveBytes)
	if err != nil {
		tb.Errorf("store invariants: object_stats: %v", err)

		return
	}

	if statCount != liveCount || statBytes != liveBytes {
		tb.Errorf("store invariant: object_stats says %d objects / %d bytes, recount says %d / %d",
			statCount, statBytes, liveCount, liveBytes)
	}
}

func queryStrings(ctx context.Context, conn *pgx.Conn, sql string) ([]string, error) {
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query %q: %w", sql, err)
	}

	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("query %q: %w", sql, err)
	}

	return out, nil
}
