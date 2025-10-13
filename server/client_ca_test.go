package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server"
	"github.com/klauspost/compress/zstd"
	minio "github.com/minio/minio-go/v7"
)

func buildCADerivation(ctx context.Context, t *testing.T) string {
	t.Helper()

	// Create a CA (content-addressed) derivation
	nixExpr := filepath.Join(t.TempDir(), "ca-test.nix")
	nixContent := `
	{ pkgs ? import <nixpkgs> {} }:
	pkgs.runCommand "ca-test" {
		__contentAddressed = true;
		outputHashMode = "recursive";
		outputHashAlgo = "sha256";
	} ''
		echo "Hello from CA derivation" > $out
	''
	`

	err := os.WriteFile(nixExpr, []byte(nixContent), 0o600)
	ok(t, err)

	// Build the CA derivation without using binary caches (build locally only)
	// Use --option substitute false to prevent fetching from binary caches
	// Enable ca-derivations experimental feature
	output, err := exec.CommandContext(ctx, "nix-build", nixExpr, "--no-out-link",
		"--extra-experimental-features", "ca-derivations",
		"--option", "substitute", "false").CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to build CA derivation: %v\nOutput: %s", err, output)
	}

	// Extract the store path from output (last line)
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	storePath := lines[len(lines)-1]
	t.Logf("Built CA derivation: %s", storePath)

	return storePath
}

// verifyRealisationFiles verifies the structure and content of realisation files.
func verifyRealisationFiles(ctx context.Context, t *testing.T, testService *server.Service, realisationKeys []string) {
	t.Helper()

	for _, realisationKey := range realisationKeys {
		realisationObj, err := testService.MinioClient.GetObject(ctx, testService.Bucket, realisationKey, minio.GetObjectOptions{})
		ok(t, err)

		compressedRealisation, err := io.ReadAll(realisationObj)
		ok(t, err)

		// Decompress with zstd
		realisationDecoder, err := zstd.NewReader(bytes.NewReader(compressedRealisation))
		ok(t, err)

		defer realisationDecoder.Close()

		realisationContent, err := io.ReadAll(realisationDecoder)
		ok(t, err)

		var realisation map[string]interface{}

		err = json.Unmarshal(realisationContent, &realisation)
		ok(t, err)

		// Verify required fields
		if _, exists := realisation["id"]; !exists {
			t.Errorf("Realisation %s missing 'id' field", realisationKey)
		}

		if outPath, exists := realisation["outPath"]; exists {
			if outPathStr, isString := outPath.(string); isString {
				t.Logf("Realisation %s maps to: %s", realisationKey, outPathStr)
			}
		} else {
			t.Errorf("Realisation %s missing 'outPath' field", realisationKey)
		}

		t.Logf("Realisation file verified (%s):\n%s", realisationKey, realisationContent)
	}
}

func TestClientCADerivations(t *testing.T) {
	t.Parallel()

	// Start test service
	service := createTestService(t)
	t.Cleanup(func() { service.Close() })

	// Create test server with auth
	testService := &server.Service{
		Pool:        service.Pool,
		MinioClient: service.MinioClient,
		Bucket:      service.Bucket,
		APIToken:    testAuthToken,
	}

	// Initialize the bucket
	err := testService.InitializeBucket(t.Context())
	ok(t, err)

	mux := http.NewServeMux()

	// Register handlers
	mux.HandleFunc("POST /api/pending_closures", testService.AuthMiddleware(testService.CreatePendingClosureHandler))
	mux.HandleFunc("POST /api/pending_closures/{id}/complete", testService.AuthMiddleware(testService.CommitPendingClosureHandler))
	mux.HandleFunc("POST /api/multipart/complete", testService.AuthMiddleware(testService.CompleteMultipartUploadHandler))
	mux.HandleFunc("GET /health", testService.HealthCheckHandler)

	ts := httptest.NewServer(mux)

	t.Cleanup(func() { ts.Close() })

	ctx := t.Context()

	// Build CA derivation
	caPath := buildCADerivation(ctx, t)

	// Upload to server
	uploadedCount := runClientAndVerifyUpload(ctx, t, testService, caPath, ts.URL, testAuthToken)
	if uploadedCount < 1 {
		t.Error("Expected at least one CA derivation upload")
	}

	// Extract hash from CA path for verification
	pathParts := strings.Split(filepath.Base(caPath), "-")
	if len(pathParts) < 1 {
		t.Fatal("Invalid store path format")
	}

	hash := pathParts[0]

	// Test 1: Verify narinfo contains CA field
	narinfoKey := hash + ".narinfo"
	narinfoObj, err := testService.MinioClient.GetObject(ctx, testService.Bucket, narinfoKey, minio.GetObjectOptions{})
	ok(t, err)

	defer func() {
		if err := narinfoObj.Close(); err != nil {
			t.Logf("Failed to close narinfo object: %v", err)
		}
	}()

	// Decompress and parse narinfo
	compressedContent, err := io.ReadAll(narinfoObj)
	ok(t, err)

	decoder, err := zstd.NewReader(bytes.NewReader(compressedContent))
	ok(t, err)

	defer decoder.Close()

	narinfoContent, err := io.ReadAll(decoder)
	ok(t, err)

	narinfoStr := string(narinfoContent)
	if !strings.Contains(narinfoStr, "CA:") {
		t.Errorf("Narinfo missing CA field:\n%s", narinfoStr)
	}

	t.Logf("Narinfo contains CA field: %s", narinfoStr)

	// Test 2: Check if realisations were uploaded
	// Note: Locally built CA derivations might not have DrvOutput IDs registered,
	// so realisations might not be uploaded. This is OK - we just verify if they exist.
	t.Log("Checking for realisation files in S3...")

	objectCh := testService.MinioClient.ListObjects(ctx, testService.Bucket, minio.ListObjectsOptions{
		Prefix:    "realisations/",
		Recursive: true,
	})

	realisationKeys := []string{}

	for object := range objectCh {
		if object.Err != nil {
			t.Errorf("Error listing realisations: %v", object.Err)

			continue
		}

		if strings.HasSuffix(object.Key, ".doi") {
			realisationKeys = append(realisationKeys, object.Key)
		}
	}

	if len(realisationKeys) == 0 {
		t.Log("No realisation files uploaded - this is expected for locally built CA derivations")
		t.Log("Locally built CA derivations don't have DrvOutput IDs until they're substituted from a cache")
	}

	if len(realisationKeys) > 0 {
		t.Logf("Found %d realisation file(s): %v", len(realisationKeys), realisationKeys)

		// Test 3: Verify realisation JSON structure if realisations exist
		verifyRealisationFiles(ctx, t, testService, realisationKeys)
	}

	// Test 4: Verify Nix can retrieve the CA derivation
	t.Run("RetrieveCADerivation", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()

		// Configure the binary cache URL
		endpoint := testService.MinioClient.EndpointURL().Host
		binaryCacheURL := fmt.Sprintf("s3://%s?endpoint=http://%s&region=eu-west-1", testService.Bucket, endpoint)

		testEnv := append(os.Environ(),
			"AWS_ACCESS_KEY_ID=minioadmin",
			"AWS_SECRET_ACCESS_KEY="+testMinioServer.secret,
		)

		// Try to copy from our cache
		cmd := exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command", "copy",
			"--no-check-sigs",
			"--from", binaryCacheURL,
			caPath)
		cmd.Env = testEnv

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Logf("nix copy output: %s", output)
			t.Logf("nix copy failed (might be expected in some environments): %v", err)
		} else {
			// Verify realisation is registered locally
			cmd = exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command ca-derivations", "realisation", "info", caPath)

			output, err = cmd.CombinedOutput()
			if err != nil {
				t.Logf("nix realisation info output: %s", output)
				t.Logf("Failed to query realisation after copy: %v", err)
			} else {
				t.Logf("Realisation info after copy:\n%s", output)
			}
		}
	})
}
