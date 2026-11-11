package server_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/minio/minio-go/v7"
)

// A random program of pushes through both APIs, commits, abandoned pushes,
// expiring closures and garbage collections, some of which lose S3's answer,
// runs against a real server. After every step:
//
//   - an object's stored refs include every ref a committed push declared;
//   - whatever a closure reaches is live and in S3;
//   - whatever a pending push was told not to upload stays live and in S3;
//   - every live row has its object, every open multipart upload its row,
//     and object_stats equals a recount.
//
// The seeds run as a test; `go test -fuzz=FuzzPushGC -run '^$' ./server`
// searches further. A failure prints the steps it ran.

type modelPath struct {
	narinfo, nar, ls, realisation string
	references                    []int
}

// Two chains of references that join, so closures overlap in part.
func modelPaths() []modelPath {
	const nixBase32 = "0123456789abcdfghijklmnpqrsvwxyz"

	references := [][]int{{}, {0}, {0}, {1}, {2, 3}, {}, {5}, {4, 6}}
	paths := make([]modelPath, 0, len(references))

	for i, refs := range references {
		c := string(nixBase32[i+10])
		paths = append(paths, modelPath{
			narinfo:     strings.Repeat(c, 32) + ".narinfo",
			nar:         "nar/" + strings.Repeat(c, 52) + ".nar.zst",
			ls:          strings.Repeat(c, 32) + ".ls",
			realisation: "realisations/sha256:" + strings.Repeat(c, 8) + "!out.doi",
			references:  refs,
		})
	}

	return paths
}

type modelObject struct {
	Key     string   `json:"key"`
	Type    string   `json:"type"`
	Refs    []string `json:"refs"`
	NarSize *uint64  `json:"nar_size,omitempty"`
}

// closure lists root's closure the way a client declares it. Bit i adds the
// realisation of path i, or leaves out its listing, so that two pushes can
// declare a narinfo with refs neither of which includes the other.
func closure(paths []modelPath, root int, realised, unlisted int) []modelObject {
	var (
		objects []modelObject
		seen    = map[int]bool{}
		visit   func(i int)
		size    = uint64(1)
	)

	visit = func(i int) {
		if seen[i] {
			return
		}

		seen[i] = true
		p := paths[i]
		refs := []string{p.nar}
		objects = append(objects, modelObject{Key: p.nar, Type: "nar", Refs: []string{}, NarSize: &size})

		if unlisted&(1<<i) == 0 {
			refs = append(refs, p.ls)
			objects = append(objects, modelObject{Key: p.ls, Type: "listing", Refs: []string{}})
		}

		if realised&(1<<i) != 0 {
			refs = append(refs, p.realisation)
			objects = append(objects, modelObject{Key: p.realisation, Type: "realisation", Refs: []string{}})
		}

		for _, j := range p.references {
			refs = append(refs, paths[j].narinfo)
			visit(j)
		}

		objects = append(objects, modelObject{Key: p.narinfo, Type: "narinfo", Refs: refs})
	}
	visit(root)

	return objects
}

// lossyS3 lets S3 carry out a batch delete and then loses the answer.
type lossyS3 struct {
	lose atomic.Bool
}

func (l *lossyS3) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(r)
	if err != nil || !r.URL.Query().Has("delete") || !l.lose.Load() {
		return resp, err //nolint:wrapcheck
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	// Anything minio-go would retry only slows the test down.
	return &http.Response{
		StatusCode: http.StatusForbidden,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader("")),
		Request:    r,
	}, nil
}

type modelPush struct {
	id      string
	viaPush bool
	root    string
	objects []modelObject
	upload  map[string]server.PendingObject
}

type pushGCModel struct {
	t       *testing.T
	service *server.Service
	s3      *lossyS3
	paths   []modelPath
	// Refs that committed pushes declared for an object since it was last
	// unreachable.
	declared map[string]map[string]bool
	roots    map[string]bool
	pending  []*modelPush
	steps    []string
}

func (m *pushGCModel) stepf(format string, args ...any) {
	m.steps = append(m.steps, fmt.Sprintf(format, args...))
}

func (m *pushGCModel) fatalf(format string, args ...any) {
	m.t.Helper()
	m.t.Fatalf("%s\nsteps:\n  %s", fmt.Sprintf(format, args...), strings.Join(m.steps, "\n  "))
}

func (m *pushGCModel) exec(sql string, args ...any) {
	m.t.Helper()

	_, err := m.service.Pool.Exec(m.t.Context(), sql, args...)
	ok(m.t, err)
}

// call sends body to a handler as the request of push id and returns the
// answer, which must be a success.
func (m *pushGCModel) call(handler http.HandlerFunc, id string, body any) []byte {
	m.t.Helper()

	encoded, err := json.Marshal(body)
	ok(m.t, err)

	req := httptest.NewRequestWithContext(m.t.Context(), http.MethodPost, "/", bytes.NewReader(encoded))
	req.SetPathValue("id", id)

	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code/100 != 2 {
		m.fatalf("status %d: %s", w.Code, w.Body.String())
	}

	return w.Body.Bytes()
}

func (m *pushGCModel) begin(viaPush bool, root, realised, unlisted int) {
	push := &modelPush{viaPush: viaPush, root: m.paths[root].narinfo, objects: closure(m.paths, root, realised, unlisted)}
	m.stepf("begin push=%v root=%s realised=%08b unlisted=%08b", viaPush, push.root, realised, unlisted)

	var answer []byte
	if viaPush {
		answer = m.call(m.service.CreatePushHandler, "",
			map[string]any{"roots": []string{push.root}, "objects": push.objects})
	} else {
		answer = m.call(m.service.CreatePendingClosureHandler, "",
			map[string]any{"closure": push.root, "objects": push.objects})
	}

	var resp server.PendingClosureResponse
	ok(m.t, json.Unmarshal(answer, &resp))

	push.id, push.upload = resp.ID, resp.PendingObjects
	m.pending = append(m.pending, push)
}

func (m *pushGCModel) put(url string) string {
	m.t.Helper()

	req, err := http.NewRequestWithContext(m.t.Context(), http.MethodPut, url, strings.NewReader("x"))
	ok(m.t, err)

	resp, err := http.DefaultClient.Do(req)
	ok(m.t, err)

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		m.fatalf("PUT %s: %s", url, resp.Status)
	}

	return resp.Header.Get("ETag")
}

func (m *pushGCModel) upload(key string, obj server.PendingObject) {
	m.t.Helper()

	if obj.MultipartInfo == nil {
		m.put(obj.PresignedURL)

		return
	}

	m.call(m.service.CompleteMultipartUploadHandler, "", map[string]any{
		"object_key": key,
		"upload_id":  obj.MultipartInfo.UploadID,
		"parts":      []map[string]any{{"part_number": 1, "etag": m.put(obj.MultipartInfo.PartURLs[0])}},
	})
}

func (m *pushGCModel) finish(i int) {
	push := m.pending[i]
	m.pending = slices.Delete(m.pending, i, i+1)
	m.stepf("finish id=%s uploads=%d", push.id, len(push.upload))

	for key, obj := range push.upload {
		m.upload(key, obj)
	}

	if push.viaPush {
		m.call(m.service.CompletePushHandler, push.id, nil)
	} else {
		m.call(m.service.CommitPendingClosureHandler, push.id, nil)
	}

	m.roots[push.root] = true

	for _, obj := range push.objects {
		if m.declared[obj.Key] == nil {
			m.declared[obj.Key] = map[string]bool{}
		}

		for _, ref := range obj.Refs {
			m.declared[obj.Key][ref] = true
		}
	}
}

// abandon makes a push old enough for the next collection to drop it.
func (m *pushGCModel) abandon(i int) {
	push := m.pending[i]
	m.pending = slices.Delete(m.pending, i, i+1)
	m.stepf("abandon id=%s", push.id)

	m.exec("UPDATE pending_closures SET started_at = started_at - interval '2 hours' WHERE id = $1::bigint", push.id)
}

// expire makes a closure old enough to be collected. A later push of the
// same root renews it.
func (m *pushGCModel) expire(root int) {
	key := m.paths[root].narinfo
	m.stepf("expire root=%s", key)

	m.exec("UPDATE closures SET updated_at = updated_at - interval '48 hours' WHERE key = $1", key)
	delete(m.roots, key)
}

// collect drops pushes older than an hour and closures older than a day.
// Unless forced, it only marks unreachable objects and deletes those marked
// an hour ago; aged makes the existing marks that old.
func (m *pushGCModel) collect(force, aged, loseAnswer bool) {
	m.stepf("gc force=%v aged=%v loseAnswer=%v", force, aged, loseAnswer)

	// Once nothing reaches an object, a later push may declare it anew.
	reachable := m.reachable()
	for key := range m.declared {
		if !reachable[key] {
			delete(m.declared, key)
		}
	}

	if aged {
		m.exec("UPDATE objects SET first_deleted_at = first_deleted_at - interval '2 hours' WHERE first_deleted_at IS NOT NULL")
	}

	m.s3.lose.Store(loseAnswer)
	st := m.service.RunGCForTest(24*time.Hour, time.Hour, force)
	m.s3.lose.Store(false)

	if st.State != "succeeded" && !loseAnswer {
		m.fatalf("GC failed: %s", st.Error)
	}
}

func (m *pushGCModel) reachable() map[string]bool {
	seen := map[string]bool{}

	var visit func(key string)

	visit = func(key string) {
		if seen[key] {
			return
		}

		seen[key] = true

		for ref := range m.declared[key] {
			visit(ref)
		}
	}

	for root := range m.roots {
		visit(root)
	}

	return seen
}

type storedObject struct {
	refs []string
	live bool
}

func (m *pushGCModel) stored() (map[string]storedObject, map[string]bool) {
	m.t.Helper()

	ctx := m.t.Context()
	objects := map[string]storedObject{}

	rows, err := m.service.Pool.Query(ctx, "SELECT key, refs, deleted_at IS NULL FROM objects")
	ok(m.t, err)

	for rows.Next() {
		var (
			key string
			obj storedObject
		)

		ok(m.t, rows.Scan(&key, &obj.refs, &obj.live))
		objects[key] = obj
	}

	ok(m.t, rows.Err())

	inS3 := map[string]bool{}

	for info := range m.service.MinioClient.ListObjects(ctx, m.service.Bucket, minio.ListObjectsOptions{Recursive: true}) {
		ok(m.t, info.Err)
		inS3[info.Key] = true
	}

	return objects, inS3
}

func (m *pushGCModel) check() {
	m.t.Helper()

	objects, inS3 := m.stored()
	held := func(key string) bool { return objects[key].live && inS3[key] }

	for key, refs := range m.declared {
		for ref := range refs {
			if obj := objects[key]; obj.live && !slices.Contains(obj.refs, ref) {
				m.fatalf("%s: stored refs %v lack the declared %s", key, obj.refs, ref)
			}
		}
	}

	for key := range m.reachable() {
		if !held(key) {
			m.fatalf("a closure reaches %s, but live=%v inS3=%v", key, objects[key].live, inS3[key])
		}
	}

	for _, push := range m.pending {
		for _, obj := range push.objects {
			if _, asked := push.upload[obj.Key]; !asked && !held(obj.Key) {
				m.fatalf("push %s was told %s is there, but live=%v inS3=%v",
					push.id, obj.Key, objects[obj.Key].live, inS3[obj.Key])
			}
		}
	}

	for key, obj := range objects {
		if obj.live && !inS3[key] {
			m.fatalf("%s is live without an object in S3", key)
		}
	}

	m.checkBookkeeping()
}

func (m *pushGCModel) checkBookkeeping() {
	m.t.Helper()

	ctx := m.t.Context()

	for upload := range m.service.MinioClient.ListIncompleteUploads(ctx, m.service.Bucket, "", true) {
		ok(m.t, upload.Err)

		var tracked bool
		ok(m.t, m.service.Pool.QueryRow(ctx,
			"SELECT EXISTS (SELECT FROM multipart_uploads WHERE upload_id = $1)", upload.UploadID).Scan(&tracked))

		if !tracked {
			m.fatalf("multipart upload of %s is open in S3 without a row", upload.Key)
		}
	}

	var stats, recount [2]int64
	ok(m.t, m.service.Pool.QueryRow(ctx, `
		SELECT s.object_count, s.total_bytes, l.n, l.bytes
		FROM object_stats AS s,
		     (SELECT count(*) AS n, coalesce(sum(size), 0)::bigint AS bytes
		      FROM objects WHERE deleted_at IS NULL) AS l
		WHERE s.id`).Scan(&stats[0], &stats[1], &recount[0], &recount[1]))

	if stats != recount {
		m.fatalf("object_stats says %v objects and bytes, a recount %v", stats, recount)
	}
}

func (m *pushGCModel) run(program []byte) {
	const maxSteps = 128

	next := func(n int) int {
		if len(program) == 0 {
			return 0
		}

		b := int(program[0])
		program = program[1:]

		return b % n
	}

	for range maxSteps {
		if len(program) == 0 {
			break
		}

		switch op := next(8); {
		case op <= 2:
			m.begin(next(2) == 1, next(len(m.paths)), next(256), next(256))
		case op <= 4 && len(m.pending) > 0:
			m.finish(next(len(m.pending)))
		case op == 5 && len(m.pending) > 0:
			m.abandon(next(len(m.pending)))
		case op == 6:
			mode := next(8)
			m.collect(mode&1 != 0, mode&2 != 0, mode&4 != 0)
		case op == 7:
			m.expire(next(len(m.paths)))
		}

		m.check()
	}

	for len(m.pending) > 0 {
		m.finish(0)
		m.check()
	}

	m.collect(true, false, false)
	m.check()
}

func FuzzPushGC(f *testing.F) {
	const (
		seeds  = 16
		length = 256
	)

	for seed := range uint64(seeds) {
		rng := rand.New(rand.NewPCG(seed, seed)) //nolint:gosec // reproducible on purpose
		program := make([]byte, length)

		for i := range program {
			program[i] = byte(rng.UintN(256)) //nolint:gosec // fits
		}

		f.Add(program)
	}

	f.Fuzz(func(t *testing.T, program []byte) {
		t.Parallel()

		service := createTestService(t)
		defer service.Close()

		s3 := &lossyS3{}

		var err error

		service.MinioClient, err = minio.New(service.MinioClient.EndpointURL().Host,
			&minio.Options{Creds: testRustfsServer.Creds(), Transport: s3})
		ok(t, err)

		m := &pushGCModel{
			t:        t,
			service:  service,
			s3:       s3,
			paths:    modelPaths(),
			declared: map[string]map[string]bool{},
			roots:    map[string]bool{},
		}
		m.run(program)
	})
}
