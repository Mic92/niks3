package client_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Mic92/niks3/client"
)

type result struct {
	ID      uint64 `json:"id"`
	Path    string `json:"path"`
	Status  string `json:"status"`
	Message string `json:"message"`

	Signatures []string `json:"signatures"`
}

func runStream(t *testing.T, push client.StreamPushFunc, parallel, batch int, feed func(w io.Writer)) []result {
	t.Helper()

	return runStreamSigned(t, push, nil, parallel, batch, feed)
}

func runStreamSigned(t *testing.T, push client.StreamPushFunc, sigs func(string) []string, parallel, batch int, feed func(w io.Writer)) []result {
	t.Helper()

	s := client.NewStreamPusher(push, parallel, batch)
	s.Signatures = sigs

	return runPusher(t, s, feed)
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
	case <-time.After(5 * time.Second):
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

	push := func(_ context.Context, paths []string) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()

		pushed = append(pushed, paths...)

		return paths, nil
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

// While the only push is busy, lines accumulate into one batch however they
// arrive: all at once, or one at a time with the input momentarily empty in
// between. Fixing the batch whenever the input was empty split lines that
// trickle in during a long push into one push each.
func TestStreamPushBatchesUnderLoad(t *testing.T) {
	t.Parallel()

	// waitTaken reports whether Run took want next. The pusher's test hook
	// makes the schedule explicit: sleeping between lines left it to the
	// scheduler, and a loaded builder delivered the last line only after the
	// first push had returned.
	waitTaken := func(t *testing.T, taken <-chan string, want string) bool {
		t.Helper()

		select {
		case got := <-taken:
			if got != want {
				t.Errorf("took %s, want %s", got, want)

				return false
			}

			return true
		case <-time.After(5 * time.Second):
			t.Errorf("%s was never taken: the batch was fixed while the push was busy", want)

			return false
		}
	}

	for _, tc := range []struct {
		name string
		feed func(t *testing.T, w io.Writer, taken <-chan string)
	}{
		{
			name: "together",
			feed: func(t *testing.T, w io.Writer, taken <-chan string) {
				t.Helper()

				_, _ = io.WriteString(w, "/nix/store/x\n/nix/store/y\n/nix/store/z\n")

				for _, p := range []string{"/nix/store/x", "/nix/store/y", "/nix/store/z"} {
					if !waitTaken(t, taken, p) {
						return
					}
				}
			},
		},
		{
			name: "one at a time",
			feed: func(t *testing.T, w io.Writer, taken <-chan string) {
				t.Helper()

				// Each line is written only once the previous one was
				// taken, so Run finds the input empty in between.
				for _, p := range []string{"/nix/store/x", "/nix/store/y", "/nix/store/z"} {
					_, _ = io.WriteString(w, p+"\n")

					if !waitTaken(t, taken, p) {
						return
					}
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			started := make(chan struct{})
			release := make(chan struct{})

			var (
				mu      sync.Mutex
				batches [][]string
			)

			push := func(_ context.Context, paths []string) ([]string, error) {
				mu.Lock()

				batches = append(batches, slices.Clone(paths))
				first := len(batches) == 1

				mu.Unlock()

				if first {
					close(started)
					<-release
				}

				return paths, nil
			}

			taken := make(chan string, 4)

			s := client.NewStreamPusher(push, 1, 10)
			s.SetTestHookTaken(func(line string) { taken <- line })

			results := runPusher(t, s, func(w io.Writer) {
				defer close(release)

				_, _ = io.WriteString(w, "/nix/store/first\n")

				if !waitTaken(t, taken, "/nix/store/first") {
					return
				}

				// The only push is busy from here on.
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Error("the first push never started")

					return
				}

				tc.feed(t, w, taken)
			})

			if len(results) != 4 {
				t.Fatalf("got %d results, want 4", len(results))
			}

			mu.Lock()
			defer mu.Unlock()

			if len(batches) != 2 || len(batches[1]) != 3 {
				t.Errorf("batches = %v, want [first] then [x y z]", batches)
			}
		})
	}
}

func TestStreamPushIsolatesFailures(t *testing.T) {
	t.Parallel()

	errBad := errors.New("bad path")

	push := func(_ context.Context, paths []string) ([]string, error) {
		if slices.Contains(paths, "/nix/store/bad") {
			return nil, errBad
		}

		return paths, nil
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
// streamIsolationProbes single-path probes before the rest is given up.
//
// How many batches the 20 lines form depends on scheduling: Run flushes
// whenever stdin has nothing more ready, so under load the input may split
// into several batches, or even singletons that are never probed. The
// assertion therefore reconstructs the batches from the recorded calls
// instead of assuming one.
func TestStreamPushGivesUpOnDeadServer(t *testing.T) {
	t.Parallel()

	errDown := errors.New("connection refused")

	var (
		mu    sync.Mutex
		calls [][]string
	)

	push := func(_ context.Context, paths []string) ([]string, error) {
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

	// A call with several paths is a batch; the single-path calls that follow
	// it and name one of its paths are its isolation probes. Paths are unique,
	// so a singleton batch cannot be mistaken for a probe of another batch.
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

	push := func(_ context.Context, paths []string) ([]string, error) {
		mu.Lock()

		calls = append(calls, fmt.Sprintf("%v", paths))

		mu.Unlock()

		if slices.Contains(paths, "/nix/store/d") {
			return nil, errors.New("boom")
		}

		return paths, nil
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

func TestStreamPushReportsSignatures(t *testing.T) {
	t.Parallel()

	push := func(_ context.Context, paths []string) ([]string, error) {
		if slices.Contains(paths, "/nix/store/bad") {
			return nil, errors.New("boom")
		}

		return paths, nil
	}
	sigs := func(p string) []string { return []string{"key-1:" + p} }

	results := runStreamSigned(t, push, sigs, 1, 1, func(w io.Writer) {
		fmt.Fprintln(w, "/nix/store/a")
		fmt.Fprintln(w, `{"id":7,"paths":["/nix/store/b"]}`)
		fmt.Fprintln(w, "/nix/store/bad")
	})

	got := map[string]result{}
	for _, r := range results {
		got[r.Path] = r
	}

	for _, p := range []string{"/nix/store/a", "/nix/store/b"} {
		if got[p].Status != "ok" || !slices.Equal(got[p].Signatures, []string{"key-1:" + p}) {
			t.Errorf("%s: %+v", p, got[p])
		}
	}

	if got["/nix/store/bad"].Status != "error" || got["/nix/store/bad"].Signatures != nil {
		t.Errorf("failed path reports signatures: %+v", got["/nix/store/bad"])
	}
}

func TestClientSignaturesByStorePath(t *testing.T) {
	t.Parallel()

	c := &client.Client{}
	c.RecordSignatures(
		map[string]client.NarinfoMetadata{"h1.narinfo": {StorePath: "/nix/store/h1-a"}, "h2.narinfo": {StorePath: "/nix/store/h2-b"}},
		map[string][]string{"h1.narinfo": {"k:1"}},
	)

	if got := c.Signatures("/nix/store/h1-a"); !slices.Equal(got, []string{"k:1"}) {
		t.Errorf("signed path: %v", got)
	}

	if got := c.Signatures("/nix/store/h2-b"); got != nil {
		t.Errorf("unsigned path: %v", got)
	}

	// The stream pusher reads each path's signatures exactly once, so it
	// takes them: a driver pushing paths for days must not accumulate an
	// entry per path.
	if got := c.TakeSignatures("/nix/store/h1-a"); !slices.Equal(got, []string{"k:1"}) {
		t.Errorf("take: %v", got)
	}

	if got := c.Signatures("/nix/store/h1-a"); got != nil {
		t.Errorf("signatures kept after being taken: %v", got)
	}

	if got := c.TakeSignatures("/nix/store/never-signed"); got != nil {
		t.Errorf("take of unknown path: %v", got)
	}
}

// Cancelling the context must end Run even while the input stays open: a CI
// driver that keeps the pipe up would otherwise make Ctrl-C a no-op, and with
// the signal context still installed the second Ctrl-C too. Paths that were
// read but not pushed are reported as failed so nothing is left dangling.
//
// Each case parks Run at one of its blocking points, with the only push slot
// held by a push of /nix/store/a, and then cancels. The push in flight is
// released only once the case's unsent lines have been reported, so it
// cannot free the slot for a push that would start after the cancellation.
func TestStreamPushStopsOnCancel(t *testing.T) {
	t.Parallel()

	const busy = "/nix/store/a"

	for _, tc := range []struct {
		name string
		// Blocking point Run is parked at when the context is cancelled.
		batchSize int
		lines     string
		// The input ends after lines, so the last batch waits for a slot in
		// the final flush rather than in the read loop.
		closeInput bool
		// Paths reported as failed without being pushed, and under which ID.
		unsent map[string]uint64
	}{
		{
			name:      "waiting for input",
			batchSize: 10,
		},
		{
			name:      "partial batch waiting for a slot",
			batchSize: 10,
			lines:     "/nix/store/b\n",
			unsent:    map[string]uint64{"/nix/store/b": 0},
		},
		{
			name:       "input closed, last batch waiting for a slot",
			batchSize:  10,
			lines:      "/nix/store/b\n/nix/store/c\n",
			closeInput: true,
			unsent:     map[string]uint64{"/nix/store/b": 0, "/nix/store/c": 0},
		},
		{
			name:      "full batch waiting for a slot",
			batchSize: 2,
			lines:     "/nix/store/b\n/nix/store/c\n",
			unsent:    map[string]uint64{"/nix/store/b": 0, "/nix/store/c": 0},
		},
		{
			name:      "request line waiting for a slot",
			batchSize: 10,
			lines:     `{"id":7,"paths":["/nix/store/d","/nix/store/e"]}` + "\n",
			unsent:    map[string]uint64{"/nix/store/d": 7, "/nix/store/e": 7},
		},
		{
			// A full batch waits for the slot and four more lines sit in
			// the input channel behind it, read but not yet taken.
			name:      "lines read but not taken",
			batchSize: 4,
			lines:     "/nix/store/b\n/nix/store/c\n/nix/store/d\n/nix/store/e\n/nix/store/f\n/nix/store/g\n/nix/store/h\n/nix/store/i\n",
			unsent: map[string]uint64{
				"/nix/store/b": 0, "/nix/store/c": 0, "/nix/store/d": 0, "/nix/store/e": 0,
				"/nix/store/f": 0, "/nix/store/g": 0, "/nix/store/h": 0, "/nix/store/i": 0,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inR, inW := io.Pipe()
			outR, outW := io.Pipe()

			var (
				mu          sync.Mutex
				pushed      [][]string
				pushStarted = make(chan struct{})
				release     = make(chan struct{})
				releaseOnce sync.Once
			)

			releaseBusy := func() { releaseOnce.Do(func() { close(release) }) }

			push := func(ctx context.Context, paths []string) ([]string, error) {
				mu.Lock()
				pushed = append(pushed, slices.Clone(paths))
				first := len(pushed) == 1
				mu.Unlock()

				if !first {
					return nil, ctx.Err()
				}

				close(pushStarted)
				<-release

				return nil, errors.New("interrupted")
			}

			s := client.NewStreamPusher(push, 1, tc.batchSize)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			done := make(chan error, 1)

			go func() {
				done <- s.Run(ctx, inR, outW)

				_ = outW.Close()
			}()

			// If Run hangs, let everything unwind once the test has failed.
			t.Cleanup(func() {
				releaseBusy()
				_ = inW.Close()
			})

			results := make(chan result, 64)

			go func() {
				defer close(results)

				sc := bufio.NewScanner(outR)
				for sc.Scan() {
					var r result
					if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
						t.Errorf("bad output line %q: %v", sc.Text(), err)

						continue
					}

					results <- r
				}
			}()

			got := make(map[string][]result)
			await := func(paths []string) {
				t.Helper()

				deadline := time.After(5 * time.Second)

				for _, p := range paths {
					for len(got[p]) == 0 {
						select {
						case r, ok := <-results:
							if !ok {
								t.Fatalf("output closed before %s was reported", p)
							}

							got[r.Path] = append(got[r.Path], r)
						case <-deadline:
							t.Fatalf("%s not reported within 5s of the cancellation", p)
						}
					}
				}
			}

			_, _ = io.WriteString(inW, busy+"\n")
			<-pushStarted

			if tc.lines != "" {
				_, _ = io.WriteString(inW, tc.lines)
			}

			// Otherwise the pipe stays open: the driver is still running.
			if tc.closeInput {
				_ = inW.Close()
			}

			// Give the lines time to reach the batch.
			time.Sleep(50 * time.Millisecond)
			cancel()

			await(slices.Sorted(maps.Keys(tc.unsent)))
			releaseBusy()
			await([]string{busy})

			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("Run returned %v, want context.Canceled", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return after cancellation while stdin stayed open")
			}

			for r := range results {
				got[r.Path] = append(got[r.Path], r)
			}

			want := maps.Clone(tc.unsent)
			if want == nil {
				want = map[string]uint64{}
			}

			want[busy] = 0

			for p, rs := range got {
				id, ok := want[p]

				switch {
				case !ok:
					t.Errorf("%s: reported but never sent", p)
				case len(rs) != 1:
					t.Errorf("%s: reported %d times, want once", p, len(rs))
				case rs[0].Status != "error" || rs[0].ID != id:
					t.Errorf("%s: %+v, want an error under id %d", p, rs[0], id)
				}
			}

			// Nothing is pushed after the cancellation: only the push that
			// was already in flight ran.
			mu.Lock()
			defer mu.Unlock()

			if len(pushed) != 1 {
				t.Errorf("pushes %v, want only [%s]: nothing may be pushed after cancellation", pushed, busy)
			}
		})
	}
}
