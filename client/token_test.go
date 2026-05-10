package client_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mic92/niks3/client"
)

const (
	tok1 = "tok-1"
	tok2 = "tok-2"
)

func TestStaticToken(t *testing.T) {
	t.Parallel()

	const tok = "static-abc"

	if got := mustToken(t, client.StaticToken(tok)); got != tok {
		t.Fatalf("got %q, want %s", got, tok)
	}
}

func TestFileTokenReadsAndCaches(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "tok")
	writeFile(t, path, "first\n")

	ts := client.FileToken(path)

	if got := mustToken(t, ts); got != "first" {
		t.Fatalf("got %q, want first", got)
	}

	// Change the file. Within the reread interval the cached value sticks.
	writeFile(t, path, "second\n")

	if got := mustToken(t, ts); got != "first" {
		t.Fatalf("cached read changed: got %q, want first", got)
	}
}

func TestFileTokenMissing(t *testing.T) {
	t.Parallel()

	ts := client.FileToken(filepath.Join(t.TempDir(), "nope"))

	if _, err := ts(t.Context()); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestFileTokenEmpty(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "tok")
	writeFile(t, path, "  \n")

	if _, err := client.FileToken(path)(t.Context()); err == nil {
		t.Fatal("expected error for empty token file")
	}
}

func TestScriptTokenNoExpiryRerunsEveryCall(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	counter := filepath.Join(dir, "n")
	writeFile(t, counter, "0")

	// Script bumps a counter and emits it as the token, so we can observe
	// reruns. No expires_at ⇒ no caching.
	ts := client.ScriptToken(counterScript(t, dir, counter, ""))

	if got := mustToken(t, ts); got != tok1 {
		t.Fatalf("got %q, want %s", got, tok1)
	}

	if got := mustToken(t, ts); got != tok2 {
		t.Fatalf("got %q, want %s (script should rerun)", got, tok2)
	}
}

func TestScriptTokenCachesUntilRefresh(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	counter := filepath.Join(dir, "n")
	writeFile(t, counter, "0")

	// Token expires 100s from "now"; refresh at 75% ⇒ 75s.
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	expires := now.Add(100 * time.Second).Format(time.RFC3339)

	ts := client.ScriptTokenWithClock(counterScript(t, dir, counter, expires), func() time.Time { return now })

	if got := mustToken(t, ts); got != tok1 {
		t.Fatalf("got %q, want %s", got, tok1)
	}

	// Before the 75% mark: cached.
	now = now.Add(74 * time.Second)

	if got := mustToken(t, ts); got != tok1 {
		t.Fatalf("got %q, want %s (still cached)", got, tok1)
	}

	// After the 75% mark: refreshed.
	now = now.Add(2 * time.Second)

	if got := mustToken(t, ts); got != tok2 {
		t.Fatalf("got %q, want %s (should refresh)", got, tok2)
	}
}

func TestScriptTokenEmptyToken(t *testing.T) {
	t.Parallel()

	cmd := echoScript(t, t.TempDir(), `{"token":""}`)
	if _, err := client.ScriptToken(cmd)(t.Context()); err == nil {
		t.Fatal("expected error for empty token")
	}
}

func TestScriptTokenBadJSON(t *testing.T) {
	t.Parallel()

	cmd := echoScript(t, t.TempDir(), "not-json")
	if _, err := client.ScriptToken(cmd)(t.Context()); err == nil {
		t.Fatal("expected error for non-JSON output")
	}
}

func TestScriptTokenScriptFails(t *testing.T) {
	t.Parallel()

	if _, err := client.ScriptToken("false")(t.Context()); err == nil {
		t.Fatal("expected error for failing script")
	}
}

func TestScriptTokenEmptyCommand(t *testing.T) {
	t.Parallel()

	if _, err := client.ScriptToken("   ")(t.Context()); err == nil {
		t.Fatal("expected error for empty command")
	}
}

// counterScript writes a shell helper that bumps a counter file and prints
// scriptOutput JSON with the count baked into the token. expires is an RFC
// 3339 timestamp or "" to omit the field.
func counterScript(t *testing.T, dir, counter, expires string) string {
	t.Helper()

	var exp string
	if expires != "" {
		exp = fmt.Sprintf(`,"expires_at":"%s"`, expires)
	}

	body := fmt.Sprintf(
		"#!/bin/sh\nn=$(($(cat %q)+1)); echo \"$n\" > %q; printf '{\"token\":\"tok-%%s\"%s}' \"$n\"\n",
		counter, counter, exp,
	)

	return writeScript(t, dir, "counter.sh", body)
}

func echoScript(t *testing.T, dir, out string) string {
	t.Helper()

	return writeScript(t, dir, "echo.sh", "#!/bin/sh\nprintf '%s' "+fmt.Sprintf("%q", out)+"\n")
}

func writeScript(t *testing.T, dir, name, body string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	// Invoke via sh rather than exec'ing the file directly. Avoids ETXTBSY
	// when another parallel test forks while this WriteFile fd is briefly
	// open (fds are inherited until close-on-exec).
	return "sh " + path
}

func mustToken(t *testing.T, ts client.TokenSource) string {
	t.Helper()

	tok, err := ts(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	return tok
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
