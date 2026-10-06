package signing_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Mic92/niks3/server/signing"
)

const (
	externalPublicKey  = "test-key:O2onvM62pC1io6jQKm8Nc2UyFXcd4kOmOsBIoYtZ2ik="
	externalSignature  = "test-key:Q7JG4oOWwCyqpWakWIfE8hXGcD92NPKNAsFKrjhVgkHVHLUnI8FQiwusGsypMTw9a06ZGg3i/OAl8KsVI3XlAQ=="
	externalSignature2 = "test-key:YVnA37ruTaVFRiLRfqMrdfSM8yU9OStWcP2o8rNtaQ94BEEp0wij6wvmGUQdit4UTyAS/3QI9ATGpZ00NhJUBA=="
)

// loopProgram answers each request line with the given shell snippet's output.
// $id holds the request id; the raw line is in $line.
func loopProgram(reply string) string {
	return `echo started >> "$0.starts"
while IFS= read -r line; do
  id=${line#*\"id\":}; id=${id%%,*}
  printf '%s\n' "$line" >> "$0.input"
  ` + reply + `
done
`
}

func testInfos() map[string]*signing.NarInfo {
	return map[string]*signing.NarInfo{
		"first.narinfo": {
			StorePath:  "/nix/store/first",
			NarHash:    "sha256:0000000000000000000000000000000000000000000000000000",
			NarSize:    123,
			References: []string{"/nix/store/zzz", "/nix/store/aaa"},
		},
		"second.narinfo": {
			StorePath: "/nix/store/second",
			NarHash:   "sha256:0000000000000000000000000000000000000000000000000000",
			NarSize:   456,
		},
	}
}

func newTestSigner(t *testing.T, body string) (*signing.ExternalSigner, string) {
	t.Helper()

	program := writeSigningProgram(t, body)
	signer, err := signing.NewExternalSigner(program, externalPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(signer.Close)

	return signer, program
}

func starts(t *testing.T, program string) int {
	t.Helper()

	data, err := os.ReadFile(program + ".starts")
	if err != nil {
		return 0
	}

	return strings.Count(string(data), "started")
}

func TestExternalSigner(t *testing.T) {
	t.Parallel()

	signer, program := newTestSigner(t, loopProgram(
		`printf '{"id":%s,"signatures":{"first.narinfo":"`+externalSignature+`","second.narinfo":"`+externalSignature2+`"},"extra":true}\n' "$id"`))

	publicKey, err := signer.PublicKey()
	if err != nil || publicKey != externalPublicKey {
		t.Fatalf("PublicKey = %q, %v", publicKey, err)
	}

	infos := testInfos()
	want := map[string]string{"first.narinfo": externalSignature, "second.narinfo": externalSignature2}
	// The same process serves consecutive requests.
	for range 2 {
		signatures, err := signer.Sign(t.Context(), infos)
		if err != nil {
			t.Fatal(err)
		}
		if !maps.Equal(signatures, want) {
			t.Fatalf("signatures = %v, want %v", signatures, want)
		}
	}
	if n := starts(t, program); n != 1 {
		t.Fatalf("program started %d times, want 1", n)
	}

	input, err := os.ReadFile(program + ".input")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(input)), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d request lines, want 2", len(lines))
	}
	var request struct {
		ID           int               `json:"id"`
		Fingerprints map[string]string `json:"fingerprints"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &request); err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{
		"first.narinfo":  "1;/nix/store/first;sha256:0000000000000000000000000000000000000000000000000000;123;/nix/store/aaa,/nix/store/zzz",
		"second.narinfo": "1;/nix/store/second;sha256:0000000000000000000000000000000000000000000000000000;456;",
	}
	if len(request.Fingerprints) != len(expected) {
		t.Fatalf("received %d fingerprints, want %d", len(request.Fingerprints), len(expected))
	}
	for key, fingerprint := range expected {
		decoded, err := base64.StdEncoding.DecodeString(request.Fingerprints[key])
		if err != nil || string(decoded) != fingerprint {
			t.Errorf("fingerprint %q = %q, %v; want %q", key, decoded, err, fingerprint)
		}
	}

	infos["first.narinfo"].NarSize++
	if signatures, err := signer.Sign(t.Context(), infos); err == nil || len(signatures) != 0 {
		t.Fatalf("modified narinfo: got %v, %v; want no signatures and an error", signatures, err)
	}
}

func TestExternalSignerWrongPublicKey(t *testing.T) {
	t.Parallel()

	program := writeSigningProgram(t, loopProgram(
		`printf '{"id":%s,"signatures":{"first.narinfo":"`+externalSignature+`","second.narinfo":"`+externalSignature2+`"}}\n' "$id"`))
	signer, err := signing.NewExternalSigner(program, "test-key:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(signer.Close)

	if signatures, err := signer.Sign(t.Context(), testInfos()); err == nil || len(signatures) != 0 {
		t.Fatalf("got %v, %v; want no signatures and an error", signatures, err)
	}
}

func TestExternalSignerErrors(t *testing.T) {
	t.Parallel()

	const zeroSig = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="
	for name, body := range map[string]string{
		"exit at startup":          "exit 42\n",
		"exit on request":          `read -r line; exit 42` + "\n",
		"invalid JSON":             loopProgram(`echo 'not json'`),
		"wrong shape":              loopProgram(`echo '[]'`),
		"null":                     loopProgram(`echo null`),
		"wrong id":                 loopProgram(`echo '{"id":999,"signatures":{}}'`),
		"missing signatures":       loopProgram(`printf '{"id":%s}\n' "$id"`),
		"null signatures":          loopProgram(`printf '{"id":%s,"signatures":null}\n' "$id"`),
		"wrong signatures shape":   loopProgram(`printf '{"id":%s,"signatures":[]}\n' "$id"`),
		"too few signatures":       loopProgram(`printf '{"id":%s,"signatures":{}}\n' "$id"`),
		"unrequested key":          loopProgram(`printf '{"id":%s,"signatures":{"x.narinfo":"test-key:` + zeroSig + `"}}\n' "$id"`),
		"missing signature name":   loopProgram(`printf '{"id":%s,"signatures":{"first.narinfo":"signature"}}\n' "$id"`),
		"wrong signature name":     loopProgram(`printf '{"id":%s,"signatures":{"first.narinfo":"other-key:` + zeroSig + `"}}\n' "$id"`),
		"invalid signature base64": loopProgram(`printf '{"id":%s,"signatures":{"first.narinfo":"test-key:invalid!"}}\n' "$id"`),
		"wrong signature length":   loopProgram(`printf '{"id":%s,"signatures":{"first.narinfo":"test-key:YQ=="}}\n' "$id"`),
		"program error":            loopProgram(`printf '{"id":%s,"error":"hsm busy"}\n' "$id"`),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			signer, _ := newTestSigner(t, body)
			infos := map[string]*signing.NarInfo{"first.narinfo": testInfos()["first.narinfo"]}
			if _, err := signer.Sign(t.Context(), infos); err == nil {
				t.Fatal("expected signing to fail")
			}
		})
	}
}

func TestExternalSignerRestartsAfterCrash(t *testing.T) {
	t.Parallel()

	// Answers the first request, then dies after reading the second one.
	signer, program := newTestSigner(t, `echo started >> "$0.starts"
read -r line
id=${line#*\"id\":}; id=${id%%,*}
printf '{"id":%s,"signatures":{"first.narinfo":"`+externalSignature+`"}}\n' "$id"
if [ "$(wc -l < "$0.starts")" -eq 1 ]; then read -r line; exit 1; fi
read -r line
id=${line#*\"id\":}; id=${id%%,*}
printf '{"id":%s,"signatures":{"first.narinfo":"`+externalSignature+`"}}\n' "$id"
`)
	infos := map[string]*signing.NarInfo{"first.narinfo": testInfos()["first.narinfo"]}

	if _, err := signer.Sign(t.Context(), infos); err != nil {
		t.Fatal(err)
	}
	// The second request hits the dying process, which is replaced transparently.
	if _, err := signer.Sign(t.Context(), infos); err != nil {
		t.Fatalf("expected restart to recover: %v", err)
	}
	if n := starts(t, program); n != 2 {
		t.Fatalf("program started %d times, want 2", n)
	}
}

func TestExternalSignerProgramErrorKeepsProcess(t *testing.T) {
	t.Parallel()

	signer, program := newTestSigner(t, loopProgram(`printf '{"id":%s,"error":"nope"}\n' "$id"`))
	infos := map[string]*signing.NarInfo{"first.narinfo": testInfos()["first.narinfo"]}

	for range 2 {
		if _, err := signer.Sign(t.Context(), infos); err == nil || !strings.Contains(err.Error(), "nope") {
			t.Fatalf("got %v, want program error", err)
		}
	}
	if n := starts(t, program); n != 1 {
		t.Fatalf("program started %d times, want 1", n)
	}
}

func TestExternalSignerClose(t *testing.T) {
	t.Parallel()

	signer, _ := newTestSigner(t, loopProgram(`printf '{"id":%s,"signatures":{"first.narinfo":"`+externalSignature+`"}}\n' "$id"`))
	infos := map[string]*signing.NarInfo{"first.narinfo": testInfos()["first.narinfo"]}
	if _, err := signer.Sign(t.Context(), infos); err != nil {
		t.Fatal(err)
	}
	signer.Close()
	if _, err := signer.Sign(t.Context(), infos); err == nil {
		t.Fatal("expected Sign after Close to fail")
	}
}

func TestExternalSignerRejectsInvalidPublicKey(t *testing.T) {
	t.Parallel()

	for _, publicKey := range []string{"", "no-colon", ":AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "test-key:invalid!", "test-key:YQ=="} {
		t.Run(publicKey, func(t *testing.T) {
			t.Parallel()

			if _, err := signing.NewExternalSigner("sh", publicKey); err == nil {
				t.Fatal("expected invalid public key to fail")
			}
		})
	}
}

func TestExternalSignerCancellation(t *testing.T) {
	t.Parallel()

	signer, program := newTestSigner(t, `sleep 30 &
child=$!
printf '%s\n' "$child" > "$0.pid"
wait "$child"
`)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := signer.Sign(ctx, map[string]*signing.NarInfo{"first.narinfo": testInfos()["first.narinfo"]})
		done <- err
	}()

	deadline := time.After(5 * time.Second)
	var pid int
	for pid == 0 {
		data, err := os.ReadFile(program + ".pid")
		if err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
		}
		select {
		case <-deadline:
			t.Fatal("signing program did not start its child")
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Sign returned %v, want context.Canceled", err)
		}
	case <-deadline:
		t.Fatal("cancellation did not stop the signing program and its child")
	}
}

func writeSigningProgram(t *testing.T, body string) string {
	t.Helper()

	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Fatal(err)
	}
	program := filepath.Join(t.TempDir(), "sign program")
	//nolint:gosec // test script must be executable
	if err := os.WriteFile(program, []byte("#!"+shell+"\nset -eu\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}

	return program
}
