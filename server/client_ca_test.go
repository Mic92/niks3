package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server"
	"github.com/klauspost/compress/zstd"
	minio "github.com/minio/minio-go/v7"
)

// A floating content-addressed derivation.
const caTestExpr = `
	derivation {
		name = "ca-test";
		system = builtins.currentSystem;
		builder = "/bin/sh";
		args = [ "-c" "echo 'Hello from CA derivation' > $out" ];
		__contentAddressed = true;
		outputHashMode = "recursive";
		outputHashAlgo = "sha256";
	}
`

func nixOutput(ctx context.Context, t *testing.T, nixEnv []string, args ...string) string {
	t.Helper()

	cmd := exec.CommandContext(ctx, args[0], args[1:]...) //nolint:gosec // test commands
	cmd.Env = nixEnv

	output, err := cmd.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			stderr = exitErr.Stderr
		}

		t.Fatalf("%s failed: %v\nstderr: %s", strings.Join(args, " "), err, stderr)
	}

	return strings.TrimSpace(string(output))
}

// buildCADerivation returns the drv path, output path and expression file.
func buildCADerivation(ctx context.Context, t *testing.T, nixEnv []string) (string, string, string) {
	t.Helper()

	nixExpr := filepath.Join(t.TempDir(), "ca-test.nix")
	ok(t, os.WriteFile(nixExpr, []byte(caTestExpr), 0o600))

	drvPath := nixOutput(ctx, t, nixEnv, "nix-instantiate", "--extra-experimental-features", "ca-derivations", nixExpr)

	// Build locally only, without binary caches.
	output := nixOutput(ctx, t, nixEnv, "nix-build", nixExpr, "--no-out-link",
		"--extra-experimental-features", "ca-derivations",
		"--option", "substitute", "false")

	lines := strings.Split(output, "\n")
	storePath := lines[len(lines)-1]
	t.Logf("Built CA derivation %s: %s", drvPath, storePath)

	return drvPath, storePath, nixExpr
}

// expectedRealisationKey returns the cache key the local Nix version uses.
func expectedRealisationKey(ctx context.Context, t *testing.T, nixEnv []string, drvPath, outPath string) string {
	t.Helper()

	output := nixOutput(ctx, t, nixEnv, "nix", "--extra-experimental-features", "nix-command ca-derivations",
		"realisation", "info", "--json", drvPath+"^out")
	t.Logf("nix realisation info: %s", output)

	var entries []struct {
		Key *struct {
			DrvPath    string `json:"drvPath"`    //nolint:tagliatelle // Nix's JSON format
			OutputName string `json:"outputName"` //nolint:tagliatelle // Nix's JSON format
		} `json:"key"`
		Value *struct {
			OutPath string `json:"outPath"` //nolint:tagliatelle // Nix's JSON format
		} `json:"value"`
		ID      string `json:"id"`
		OutPath string `json:"outPath"` //nolint:tagliatelle // Nix's JSON format
	}
	ok(t, json.Unmarshal([]byte(output), &entries))

	for _, e := range entries {
		switch {
		case e.Key != nil && e.Value != nil && e.Value.OutPath == filepath.Base(outPath):
			return "build-trace-v2/" + e.Key.DrvPath + "/" + e.Key.OutputName + ".doi"
		case e.ID != "" && e.OutPath == filepath.Base(outPath):
			return "realisations/" + e.ID + ".doi"
		}
	}

	t.Fatalf("no realisation for %s^out in %s", drvPath, output)

	return ""
}

func readZstdObject(ctx context.Context, t *testing.T, testService *server.Service, key string) []byte {
	t.Helper()

	obj, err := testService.MinioClient.GetObject(ctx, testService.Bucket, key, minio.GetObjectOptions{})
	ok(t, err)

	defer func() { _ = obj.Close() }()

	compressed, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("reading %s: %v", key, err)
	}

	decoder, err := zstd.NewReader(bytes.NewReader(compressed))
	ok(t, err)

	defer decoder.Close()

	content, err := io.ReadAll(decoder)
	ok(t, err)

	return content
}

func TestClientCADerivations(t *testing.T) {
	t.Parallel()

	// Start test service
	testService := createTestServiceWithAuth(t, testAuthToken)
	t.Cleanup(func() { testService.Close() })

	// Initialize the bucket
	err := testService.InitializeBucket(t.Context())
	ok(t, err)

	mux := http.NewServeMux()
	registerTestHandlers(mux, testService)

	ts := httptest.NewServer(mux)

	t.Cleanup(func() { ts.Close() })

	ctx := t.Context()
	nixEnv := setupIsolatedNixStore(t)

	drvPath, caPath, nixExpr := buildCADerivation(ctx, t, nixEnv)

	// Upload to server
	uploadedCount := runClientAndVerifyUpload(ctx, t, testService, caPath, ts.URL, testAuthToken, nixEnv)
	if uploadedCount < 1 {
		t.Error("Expected at least one CA derivation upload")
	}

	hash, _, _ := strings.Cut(filepath.Base(caPath), "-")

	narinfo := string(readZstdObject(ctx, t, testService, hash+".narinfo"))
	if !strings.Contains(narinfo, "CA:") {
		t.Errorf("Narinfo missing CA field:\n%s", narinfo)
	}

	realisationKey := expectedRealisationKey(ctx, t, nixEnv, drvPath, caPath)
	content := readZstdObject(ctx, t, testService, realisationKey)
	t.Logf("Realisation %s: %s", realisationKey, content)

	var realisation struct {
		OutPath string `json:"outPath"` //nolint:tagliatelle // Nix's JSON format
	}
	ok(t, json.Unmarshal(content, &realisation))

	if realisation.OutPath != filepath.Base(caPath) {
		t.Errorf("realisation outPath = %q, want %q", realisation.OutPath, filepath.Base(caPath))
	}

	// A fresh store must substitute drv^out. Unsigned: the isolated store
	// dir can't be signed; the NixOS test checks signatures.
	var storeDir string

	for _, envVar := range nixEnv {
		if after, ok0 := strings.CutPrefix(envVar, "NIX_STORE_DIR="); ok0 {
			storeDir = after
		}
	}

	if storeDir == "" {
		storeDir = defaultNixStoreDir
	}

	// Nix ignores a cache for another store dir.
	cacheInfo := "StoreDir: " + storeDir + "\n"
	_, err = testService.MinioClient.PutObject(ctx, testService.Bucket, "nix-cache-info",
		strings.NewReader(cacheInfo), int64(len(cacheInfo)), minio.PutObjectOptions{ContentType: "text/plain"})
	ok(t, err)

	endpoint := testService.MinioClient.EndpointURL().Host
	binaryCacheURL := fmt.Sprintf("s3://%s?endpoint=http://%s&region=eu-west-1", testService.Bucket, endpoint)

	testEnv := slices.Concat(nixEnv, []string{
		"AWS_ACCESS_KEY_ID=rustfsadmin",
		"AWS_SECRET_ACCESS_KEY=" + testRustfsServer.secret,
	})
	otherStore := "local?root=" + filepath.Join(t.TempDir(), "other")

	otherDrv := nixOutput(ctx, t, testEnv, "nix-instantiate", "--store", otherStore,
		"--extra-experimental-features", "ca-derivations", nixExpr)
	if otherDrv != drvPath {
		t.Fatalf("derivation in the other store = %s, want %s", otherDrv, drvPath)
	}

	// Nix disables substitution without Internet unless it is set.
	substituted := nixOutput(ctx, t, testEnv, "nix", "--extra-experimental-features", "nix-command ca-derivations",
		"build", "--store", otherStore, "--no-link", "--print-out-paths", "--max-jobs", "0",
		"--option", "substitute", "true", "--option", "substituters", binaryCacheURL, "--option", "require-sigs", "false",
		drvPath+"^out")
	if substituted != caPath {
		t.Errorf("substituted %q, want %q", substituted, caPath)
	}
}
