package client_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Mic92/niks3/client"
	"github.com/Mic92/niks3/internal/testwait"
)

type result struct {
	ID      uint64 `json:"id"`
	Path    string `json:"path"`
	Status  string `json:"status"`
	Message string `json:"message"`
	Skipped string `json:"skipped"`

	Signatures []string `json:"signatures"`
}

func runStream(t *testing.T, push client.StreamPushFunc, parallel, batch int, feed func(w io.Writer)) []result {
	t.Helper()

	return runPusher(t, client.NewStreamPusher(push, parallel, batch), feed)
}

func runPusher(t *testing.T, s *client.StreamPusher, feed func(w io.Writer)) []result {
	t.Helper()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()

	done := make(chan error, 1)

	go func() {
		done <- s.Run(context.Background(), inR, outW)

		_ = outW.Close()
	}()

	go func() {
		feed(inW)

		_ = inW.Close()
	}()

	var results []result

	sc := bufio.NewScanner(outR)
	for sc.Scan() {
		var r result
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("bad output line %q: %v", sc.Text(), err)
		}

		results = append(results, r)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testwait.Hung):
		t.Fatal("Run did not return after stdin EOF")
	}

	return results
}

func TestStreamPushReportsEveryPath(t *testing.T) {
	t.Parallel()

	var (
		mu     sync.Mutex
		pushed []string
	)

	push := func(_ context.Context, paths []string) (map[string]client.PathReport, error) {
		mu.Lock()
		defer mu.Unlock()

		pushed = append(pushed, paths...)

		return map[string]client.PathReport{}, nil
	}

	results := runStream(t, push, 4, 10, func(w io.Writer) {
		_, _ = io.WriteString(w, "/nix/store/a\n\n/nix/store/b\n/nix/store/c\n")
	})

	got := make([]string, 0, len(results))

	for _, r := range results {
		if r.Status != "ok" {
			t.Errorf("path %s: status %s (%s)", r.Path, r.Status, r.Message)
		}

		got = append(got, r.Path)
	}

	slices.Sort(got)
	slices.Sort(pushed)

	want := []string{"/nix/store/a", "/nix/store/b", "/nix/store/c"}
	if !slices.Equal(got, want) {
		t.Errorf("results = %v, want %v", got, want)
	}

	if !slices.Equal(pushed, want) {
		t.Errorf("pushed = %v, want %v", pushed, want)
	}
}

// While the only push is busy, lines accumulate into one batch, also when
// they arrive one at a time with the input empty in between.
func TestStreamPushBatchesUnderLoad(t *testing.T) {
	t.Parallel()

	for _, oneAtATime := range []bool{false, true} {
		t.Run(fmt.Sprintf("oneAtATime=%v", oneAtATime), func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) { testBatchesUnderLoad(t, oneAtATime) })
		})
	}
}

func testBatchesUnderLoad(t *testing.T, oneAtATime bool) {
	t.Helper()

	release := make(chan struct{})

	var batches [][]string

	push := func(ctx context.Context, paths []string) (map[string]client.PathReport, error) {
		batches = append(batches, slices.Clone(paths))
		if len(batches) == 1 {
			<-release
		}

		return map[string]client.PathReport{}, ctx.Err()
	}

	inR, inW := io.Pipe()
	done := make(chan error, 1)

	var out bytes.Buffer

	go func() { done <- client.NewStreamPusher(push, 1, 10).Run(t.Context(), inR, &out) }()

	write := func(s string) {
		_, _ = io.WriteString(inW, s)

		synctest.Wait()
	}

	write("/nix/store/first\n")

	lines := []string{"/nix/store/x\n", "/nix/store/y\n", "/nix/store/z\n"}
	if !oneAtATime {
		lines = []string{strings.Join(lines, "")}
	}

	for _, l := range lines {
		write(l)
	}

	close(release)

	_ = inW.Close()

	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if n := strings.Count(out.String(), "\n"); n != 4 {
		t.Errorf("got %d results, want 4", n)
	}

	if len(batches) != 2 || len(batches[1]) != 3 {
		t.Errorf("batches = %v, want [first] then [x y z]", batches)
	}
}

func TestStreamPushIsolatesFailures(t *testing.T) {
	t.Parallel()

	errBad := errors.New("bad path")

	push := func(_ context.Context, paths []string) (map[string]client.PathReport, error) {
		if slices.Contains(paths, "/nix/store/bad") {
			return nil, errBad
		}

		return map[string]client.PathReport{}, nil
	}

	results := runStream(t, push, 1, 10, func(w io.Writer) {
		_, _ = io.WriteString(w, "/nix/store/good1\n/nix/store/bad\n/nix/store/good2\n")
	})

	status := map[string]result{}
	for _, r := range results {
		status[r.Path] = r
	}

	for _, p := range []string{"/nix/store/good1", "/nix/store/good2"} {
		if status[p].Status != "ok" {
			t.Errorf("%s: %+v, want ok", p, status[p])
		}
	}

	if bad := status["/nix/store/bad"]; bad.Status != "error" || !strings.Contains(bad.Message, "bad path") {
		t.Errorf("bad: %+v, want error with message", bad)
	}
}

// With the server down, each failed batch costs the batch call plus at most
// streamIsolationProbes single-path probes. The number of batches depends on
// scheduling, so the test rebuilds them from the recorded calls.
func TestStreamPushGivesUpOnDeadServer(t *testing.T) {
	t.Parallel()

	errDown := errors.New("connection refused")

	var (
		mu    sync.Mutex
		calls [][]string
	)

	push := func(_ context.Context, paths []string) (map[string]client.PathReport, error) {
		mu.Lock()
		defer mu.Unlock()

		calls = append(calls, slices.Clone(paths))

		return nil, errDown
	}

	var sb strings.Builder
	for i := range 20 {
		sb.WriteString("/nix/store/p" + string(rune('a'+i)) + "\n")
	}

	results := runStream(t, push, 1, 50, func(w io.Writer) {
		_, _ = io.WriteString(w, sb.String())
	})

	if len(results) != 20 {
		t.Fatalf("got %d results, want 20", len(results))
	}

	for _, r := range results {
		if r.Status != "error" {
			t.Errorf("%s: status %s, want error", r.Path, r.Status)
		}
	}

	// A multi-path call is a batch. The single-path calls after it that name
	// one of its paths are its probes.
	for i := 0; i < len(calls); i++ {
		batch := calls[i]
		if len(batch) == 1 {
			continue
		}

		probes := 0

		for i+1 < len(calls) && len(calls[i+1]) == 1 && slices.Contains(batch, calls[i+1][0]) {
			probes++
			i++
		}

		if want := min(len(batch), 3); probes != want {
			t.Errorf("batch of %d paths was probed %d times, want %d", len(batch), probes, want)
		}
	}
}

func TestStreamPushRequestLine(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		calls []string
	)

	push := func(_ context.Context, paths []string) (map[string]client.PathReport, error) {
		mu.Lock()

		calls = append(calls, fmt.Sprintf("%v", paths))

		mu.Unlock()

		if slices.Contains(paths, "/nix/store/d") {
			return nil, errors.New("boom")
		}

		return map[string]client.PathReport{}, nil
	}

	results := runStream(t, push, 1, 10, func(w io.Writer) {
		_, _ = io.WriteString(w, "/nix/store/a\n"+
			`{"paths":["/nix/store/b","/nix/store/c"]}`+"\n"+
			`{"paths":["/nix/store/d"]}`+"\n"+
			"{bad\n")
	})

	status := map[string]string{}
	for _, r := range results {
		status[r.Path] = r.Status
	}

	want := map[string]string{"/nix/store/a": "ok", "/nix/store/b": "ok", "/nix/store/c": "ok", "/nix/store/d": "error", "{bad": "error"}
	if !maps.Equal(status, want) {
		t.Fatalf("got %v, want %v", status, want)
	}

	// Request lines may exceed bufio's default token size.
	long := make([]string, 0, 2000)
	for i := range 2000 {
		long = append(long, fmt.Sprintf("/nix/store/%080d", i))
	}

	longLine, err := json.Marshal(client.StreamRequest{ID: 9, Paths: long})
	if err != nil {
		t.Fatal(err)
	}

	if got := runStream(t, push, 1, 10, func(w io.Writer) { _, _ = w.Write(append(longLine, '\n')) }); len(got) != len(long) {
		t.Fatalf("long request: %d results", len(got))
	}

	// Concurrent requests may share a path; the id tells their acks apart.
	shared := runStream(t, push, 2, 10, func(w io.Writer) {
		_, _ = io.WriteString(w, `{"id":1,"paths":["/nix/store/a"]}`+"\n"+`{"id":2,"paths":["/nix/store/a"]}`+"\n")
	})

	ids := make([]uint64, 0, len(shared))
	for _, r := range shared {
		ids = append(ids, r.ID)
	}

	slices.Sort(ids)

	if !slices.Equal(ids, []uint64{1, 2}) {
		t.Fatalf("ids %v", ids)
	}

	slices.Sort(calls)
	calls = slices.DeleteFunc(slices.Compact(calls), func(c string) bool { return len(c) > 100 })

	// The plain path is never merged into a request batch.
	wantCalls := []string{"[/nix/store/a]", "[/nix/store/b /nix/store/c]", "[/nix/store/d]"}
	if !slices.Equal(calls, wantCalls) {
		t.Fatalf("calls %v, want %v", calls, wantCalls)
	}
}

// The answer of a path carries what the push reported for it.
func TestStreamPushAnswersWithTheReport(t *testing.T) {
	t.Parallel()

	push := func(_ context.Context, paths []string) (map[string]client.PathReport, error) {
		if slices.Contains(paths, "/nix/store/bad") {
			return nil, errors.New("boom")
		}

		return map[string]client.PathReport{
			"/nix/store/signed": {Signatures: []string{"key-1:sig"}},
			"/nix/store/lost":   {OtherBuild: "sha256:abc"},
			"/nix/store/big":    {Skipped: "too big"},
		}, nil
	}

	var out strings.Builder

	in := `{"id":7,"paths":["/nix/store/signed","/nix/store/lost","/nix/store/big"]}
/nix/store/signed
/nix/store/bad
`
	if err := client.NewStreamPusher(push, 1, 1).Run(t.Context(), strings.NewReader(in), &out); err != nil {
		t.Fatal(err)
	}

	want := `{"id":7,"path":"/nix/store/signed","status":"ok","signatures":["key-1:sig"]}
{"id":7,"path":"/nix/store/lost","status":"ok","other_build":"sha256:abc"}
{"id":7,"path":"/nix/store/big","status":"ok","message":"too big","skipped":"max-nar-size"}
{"path":"/nix/store/signed","status":"ok","signatures":["key-1:sig"]}
{"path":"/nix/store/bad","status":"error","message":"boom"}
`
	if out.String() != want {
		t.Errorf("got %s", out.String())
	}
}

// Dependencies get no answer, so nothing is reported for them.
func TestClientReportsOnlyAskedPaths(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_, _ = io.WriteString(w, `{"signatures":{"a.narinfo":["k:a"],"dep.narinfo":["k:dep"]}}`)
		}
	}))
	t.Cleanup(ts.Close)

	c, err := client.NewTestClientForServer(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	narinfos := map[string]client.NarinfoMetadata{
		"a.narinfo":   {StorePath: "/nix/store/a"},
		"dep.narinfo": {StorePath: "/nix/store/dep"},
	}
	pending := map[string]client.PendingObject{
		"a.narinfo":   {PresignedURL: ts.URL + "/a"},
		"dep.narinfo": {PresignedURL: ts.URL + "/dep"},
	}
	reports := map[string]client.PathReport{"/nix/store/a": {}}

	err = c.SignAndUploadNarinfos(t.Context(), "pushes", map[string]map[string]client.NarinfoMetadata{"1": narinfos}, pending, reports)
	if err != nil {
		t.Fatal(err)
	}

	if len(reports) != 1 || !slices.Equal(reports["/nix/store/a"].Signatures, []string{"k:a"}) {
		t.Errorf("reports: %v", reports)
	}
}

// A driver waits for one answer per line, also after a signal.
func TestStreamPushAnswersEveryLineWhenCanceled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())

		var pushes, late atomic.Int32

		push := func(ctx context.Context, _ []string) (map[string]client.PathReport, error) {
			pushes.Add(1)

			if ctx.Err() != nil {
				late.Add(1)
			}

			<-ctx.Done()

			return map[string]client.PathReport{}, ctx.Err()
		}

		inR, inW := io.Pipe()
		defer inW.Close()

		var out bytes.Buffer

		done := make(chan error, 1)

		go func() { done <- client.NewStreamPusher(push, 1, 2).Run(ctx, inR, &out) }()

		// a and b are being pushed, the request waits, e is read but not taken.
		go func() {
			_, _ = io.WriteString(inW, "/nix/store/a\n/nix/store/b\n"+
				`{"id":7,"paths":["/nix/store/c","/nix/store/d"]}`+"\n/nix/store/e\n")
		}()

		synctest.Wait()

		if n := pushes.Load(); n != 1 {
			t.Fatalf("%d pushes in flight, want 1", n)
		}

		cancel()

		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run: %v, want context.Canceled", err)
		}

		if n := late.Load(); n != 0 {
			t.Errorf("%d pushes started after the cancellation", n)
		}

		got := map[string]uint64{}

		for line := range strings.Lines(out.String()) {
			var r result

			if err := json.Unmarshal([]byte(line), &r); err != nil {
				t.Fatalf("bad output line %q: %v", line, err)
			}

			if _, dup := got[r.Path]; dup || r.Status != "error" {
				t.Errorf("%s: status %s, answered twice: %v", r.Path, r.Status, dup)
			}

			got[r.Path] = r.ID
		}

		want := map[string]uint64{"/nix/store/a": 0, "/nix/store/b": 0, "/nix/store/c": 7, "/nix/store/d": 7, "/nix/store/e": 0}
		if !maps.Equal(got, want) {
			t.Errorf("answers by request ID = %v, want %v", got, want)
		}
	})
}

// The caller keeps stdin open for the life of the process, so a signal has to end Run.
func TestStreamPushStopsWhenCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	in, _ := io.Pipe()
	done := make(chan error, 1)

	go func() { done <- client.NewStreamPusher(nil, 1, 1).Run(ctx, in, io.Discard) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run: %v, want context.Canceled", err)
		}
	case <-time.After(testwait.Hung):
		t.Fatal("Run still waits for input")
	}
}

func TestStreamPushLogLine(t *testing.T) {
	t.Parallel()

	pusher := client.NewStreamPusher(func(context.Context, []string) (map[string]client.PathReport, error) {
		return nil, errors.New("a log line must not push paths")
	}, 1, 10)
	pusher.PushLog = func(_ context.Context, drvPath string) error {
		if drvPath == "/nix/store/lost.drv" {
			return errors.New("no build log")
		}

		return nil
	}

	results := runPusher(t, pusher, func(w io.Writer) {
		_, _ = io.WriteString(w, `{"id":3,"log":"/nix/store/failed.drv"}`+"\n"+
			`{"id":4,"log":"/nix/store/lost.drv"}`+"\n"+
			`{"log":"/nix/store/both.drv","paths":["/nix/store/a"]}`+"\n")
	})

	status := map[string]string{}
	for _, r := range results {
		status[fmt.Sprintf("%d %s", r.ID, r.Path)] = r.Status
	}

	want := map[string]string{
		"3 /nix/store/failed.drv":                                  "ok",
		"4 /nix/store/lost.drv":                                    "error",
		`0 {"log":"/nix/store/both.drv","paths":["/nix/store/a"]}`: "error",
	}
	if !maps.Equal(status, want) {
		t.Fatalf("got %v, want %v", status, want)
	}
}
