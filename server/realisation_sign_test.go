package server_test

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server/signing"
)

// The server signs realisations in both Nix formats.
func TestPush_SignsRealisations(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	t.Cleanup(service.Close)

	key, err := signing.ParseKey(testSigningSecret)
	ok(t, err)

	service.SigningKeys = []signing.Signer{key}

	hash := strings.Repeat("a", 32)
	drv := strings.Repeat("d", 32) + "-test.drv"
	otherDrv := strings.Repeat("f", 32) + "-x.drv"
	buildTrace := "build-trace-v2/" + drv + "/out.doi"
	oldID := "sha256:" + strings.Repeat("0", 64) + "!out"
	oldKey := "realisations/" + oldID + ".doi"
	outPath := hash + "-test"

	objects := pkgObjects(hash)
	objects[0]["refs"] = []string{narKeyFor(hash), buildTrace, oldKey}
	objects = append(objects,
		map[string]any{"key": buildTrace, "type": "realisation", "refs": []string{}},
		map[string]any{"key": oldKey, "type": "realisation", "refs": []string{}},
	)
	resp := createPush(t, service, []string{hash + ".narinfo"}, objects)

	sign := func(t *testing.T, realisations map[string]any, status int) []byte {
		t.Helper()

		body, err := json.Marshal(map[string]any{"narinfos": map[string]any{}, "realisations": realisations})
		ok(t, err)

		check := checkStatusCode(status)
		rr := testRequest(t, &TestRequest{
			method:        "POST",
			path:          "/api/pushes/" + resp.ID + "/sign",
			body:          body,
			handler:       service.SignNarinfosHandler,
			pathValues:    map[string]string{"id": resp.ID},
			checkResponse: &check,
		})

		return rr.Body.Bytes()
	}

	var signed struct {
		Signatures map[string][]string `json:"signatures"`
	}

	ok(t, json.Unmarshal(sign(t, map[string]any{
		buildTrace: map[string]any{"drv_path": drv, "output_name": "out", "out_path": outPath},
		oldKey:     map[string]any{"id": oldID, "out_path": outPath},
	}, http.StatusOK), &signed))

	_, encoded, _ := strings.Cut(testSigningPublic, ":")
	publicKey, err := base64.StdEncoding.DecodeString(encoded)
	ok(t, err)

	for objectKey, r := range map[string]signing.Realisation{
		buildTrace: {DrvPath: drv, OutputName: "out", OutPath: outPath},
		oldKey:     {ID: oldID, OutPath: outPath},
	} {
		sigs := signed.Signatures[objectKey]
		if len(sigs) != 1 {
			t.Fatalf("signatures for %s = %v, want one", objectKey, sigs)
		}

		fingerprint, err := signing.GenerateRealisationFingerprint(&r)
		ok(t, err)

		name, sig, _ := strings.Cut(sigs[0], ":")
		raw, err := base64.StdEncoding.DecodeString(sig)
		ok(t, err)

		if name != "niks3-test-1" || !ed25519.Verify(publicKey, fingerprint, raw) {
			t.Errorf("signature %q for %s does not verify", sigs[0], objectKey)
		}
	}

	t.Run("not pending", func(t *testing.T) {
		t.Parallel()

		sign(t, map[string]any{
			"build-trace-v2/" + otherDrv + "/out.doi": map[string]any{"drv_path": otherDrv, "output_name": "out", "out_path": outPath},
		}, http.StatusForbidden)
	})

	t.Run("other key", func(t *testing.T) {
		t.Parallel()

		sign(t, map[string]any{
			buildTrace: map[string]any{"drv_path": otherDrv, "output_name": "out", "out_path": outPath},
		}, http.StatusBadRequest)
	})

	t.Run("invalid", func(t *testing.T) {
		t.Parallel()

		sign(t, map[string]any{
			buildTrace: map[string]any{"drv_path": drv, "output_name": "out", "out_path": "/nix/store/" + outPath},
		}, http.StatusBadRequest)
	})
}
