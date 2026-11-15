package client_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Mic92/niks3/client"
)

// `nix realisation info --json <drv>^out` output.
const (
	realisationJSONNix234 = `[{"dependentRealisations":{},"id":"sha256:512885212ca5cbb73870c2f224efd06d13de313d2e7c15056b53521f788764a6!out","outPath":"zvi3467pwnaq46bmjw4lxkfdd415xc0g-ca-fixture","signatures":["niks3-test-1:KreZVIap7uvMmo04LS66ZchZ85wNqkrbaIFzPkErnKu+LQNnSVnZvNw+H58fuv9rsnIYV0O2LAkjdj9C7ANwBw=="]}]`
	realisationJSONNix235 = `[{"key":{"drvPath":"3ny5n3w5lva8f9saqkcjrdgpx5zvpgv7-ca-fixture.drv","outputName":"out"},"value":{"outPath":"l7cppcmmzf8ikklbkwx0dsnxsqsndlr7-ca-fixture","signatures":[{"keyName":"niks3-test-1","sig":"sp+I8xi2JdVFeLI2axOffkMMQtWFV9kUZV076O10UTMcQG2yBdgr6zpJ2oxNz3zDB5cTaV0GB35P/wGogvtsAw=="}]}},{"opaquePath":"/nix/store/l7cppcmmzf8ikklbkwx0dsnxsqsndlr7-ca-fixture"}]`
)

func TestParseRealisationsJSON(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		input   string
		want    []client.RealisationInfo
		keys    []string
		content []string
	}{
		{
			name:  "nix 2.34",
			input: realisationJSONNix234,
			want: []client.RealisationInfo{{
				ID:                    "sha256:512885212ca5cbb73870c2f224efd06d13de313d2e7c15056b53521f788764a6!out",
				OutPath:               "zvi3467pwnaq46bmjw4lxkfdd415xc0g-ca-fixture",
				Signatures:            []string{"niks3-test-1:KreZVIap7uvMmo04LS66ZchZ85wNqkrbaIFzPkErnKu+LQNnSVnZvNw+H58fuv9rsnIYV0O2LAkjdj9C7ANwBw=="},
				DependentRealisations: map[string]string{},
			}},
			keys:    []string{"realisations/sha256:512885212ca5cbb73870c2f224efd06d13de313d2e7c15056b53521f788764a6!out.doi"},
			content: []string{`{"dependentRealisations":{},"id":"sha256:512885212ca5cbb73870c2f224efd06d13de313d2e7c15056b53521f788764a6!out","outPath":"zvi3467pwnaq46bmjw4lxkfdd415xc0g-ca-fixture","signatures":["niks3-test-1:KreZVIap7uvMmo04LS66ZchZ85wNqkrbaIFzPkErnKu+LQNnSVnZvNw+H58fuv9rsnIYV0O2LAkjdj9C7ANwBw==","srv:c2ln"]}`},
		},
		{
			name:  "nix 2.35",
			input: realisationJSONNix235,
			want: []client.RealisationInfo{{
				DrvPath:    "3ny5n3w5lva8f9saqkcjrdgpx5zvpgv7-ca-fixture.drv",
				OutputName: "out",
				OutPath:    "l7cppcmmzf8ikklbkwx0dsnxsqsndlr7-ca-fixture",
				Signatures: []string{"niks3-test-1:sp+I8xi2JdVFeLI2axOffkMMQtWFV9kUZV076O10UTMcQG2yBdgr6zpJ2oxNz3zDB5cTaV0GB35P/wGogvtsAw=="},
			}},
			keys:    []string{"build-trace-v2/3ny5n3w5lva8f9saqkcjrdgpx5zvpgv7-ca-fixture.drv/out.doi"},
			content: []string{`{"outPath":"l7cppcmmzf8ikklbkwx0dsnxsqsndlr7-ca-fixture","signatures":[{"keyName":"niks3-test-1","sig":"sp+I8xi2JdVFeLI2axOffkMMQtWFV9kUZV076O10UTMcQG2yBdgr6zpJ2oxNz3zDB5cTaV0GB35P/wGogvtsAw=="},{"keyName":"srv","sig":"c2ln"}]}`},
		},
		{
			name:  "only opaque paths",
			input: `[{"opaquePath":"/nix/store/l7cppcmmzf8ikklbkwx0dsnxsqsndlr7-ca-fixture"}]`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := client.ParseRealisationsJSON([]byte(tc.input))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %d realisations, want %d: %+v", len(got), len(tc.want), got)
			}
			for i := range got {
				if !reflect.DeepEqual(got[i], tc.want[i]) {
					t.Errorf("realisation %d = %+v, want %+v", i, got[i], tc.want[i])
				}
				if key := got[i].Key(); key != tc.keys[i] {
					t.Errorf("Key() = %q, want %q", key, tc.keys[i])
				}
				content, err := got[i].FileContent(append(got[i].Signatures, "srv:c2ln"))
				if err != nil {
					t.Fatal(err)
				}
				if string(content) != tc.content[i] {
					t.Errorf("FileContent() = %s, want %s", content, tc.content[i])
				}
				if !json.Valid(content) {
					t.Errorf("FileContent() is not JSON: %s", content)
				}
			}
		})
	}
}

func TestParseRealisationsJSONRejectsGarbage(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		`{}`,
		`[{"key":{"drvPath":"x.drv","outputName":"out"},"value":{"outPath":"y","signatures":[42]}}]`,
	} {
		if _, err := client.ParseRealisationsJSON([]byte(input)); err == nil {
			t.Errorf("ParseRealisationsJSON(%s): expected an error", input)
		}
	}
}

func TestRealisationInstallable(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		path, deriver, want string
	}{
		{"/nix/store/l7cppcmmzf8ikklbkwx0dsnxsqsndlr7-hello", "/nix/store/3ny5n3w5lva8f9saqkcjrdgpx5zvpgv7-hello.drv", "/nix/store/3ny5n3w5lva8f9saqkcjrdgpx5zvpgv7-hello.drv^out"},
		{"/nix/store/l7cppcmmzf8ikklbkwx0dsnxsqsndlr7-hello-dev", "/nix/store/3ny5n3w5lva8f9saqkcjrdgpx5zvpgv7-hello.drv", "/nix/store/3ny5n3w5lva8f9saqkcjrdgpx5zvpgv7-hello.drv^dev"},
		{"/nix/store/l7cppcmmzf8ikklbkwx0dsnxsqsndlr7-hello-man-de", "/nix/store/3ny5n3w5lva8f9saqkcjrdgpx5zvpgv7-hello.drv", "/nix/store/3ny5n3w5lva8f9saqkcjrdgpx5zvpgv7-hello.drv^man-de"},
		// The deriver did not make a path of this name.
		{"/nix/store/l7cppcmmzf8ikklbkwx0dsnxsqsndlr7-other", "/nix/store/3ny5n3w5lva8f9saqkcjrdgpx5zvpgv7-hello.drv", ""},
		{"/nix/store/l7cppcmmzf8ikklbkwx0dsnxsqsndlr7-hello", "/nix/store/3ny5n3w5lva8f9saqkcjrdgpx5zvpgv7-hello", ""},
	} {
		if got := client.RealisationInstallable(tc.path, tc.deriver); got != tc.want {
			t.Errorf("RealisationInstallable(%q, %q) = %q, want %q", tc.path, tc.deriver, got, tc.want)
		}
	}
}

// Nix < 2.35 rejects "signatures": null.
func TestRealisationFileContentWithoutSignatures(t *testing.T) {
	t.Parallel()

	for _, r := range []client.RealisationInfo{
		{ID: "sha256:00!out", OutPath: "x-a"},
		{DrvPath: "y-a.drv", OutputName: "out", OutPath: "x-a"},
	} {
		content, err := r.FileContent(nil)
		if err != nil {
			t.Fatal(err)
		}

		var parsed struct {
			Signatures []any `json:"signatures"`
		}
		if err := json.Unmarshal(content, &parsed); err != nil || parsed.Signatures == nil {
			t.Errorf("FileContent() = %s, want a signatures array", content)
		}
	}
}
