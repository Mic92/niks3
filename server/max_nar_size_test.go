package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server"
)

func TestCacheConfigHandlerMaxNarSize(t *testing.T) {
	t.Parallel()

	s := &server.Service{MaxNarSize: 2 << 30}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/cache-config", nil)
	rr := httptest.NewRecorder()
	s.CacheConfigHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}

	var got api.CacheConfig
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.MaxNarSize != 2<<30 {
		t.Errorf("MaxNarSize = %d, want %d", got.MaxNarSize, 2<<30)
	}
}

// Oversized NAR objects are rejected before any DB or S3 work, so a bare
// Service with only MaxNarSize set is enough for this test.
func TestCreatePendingClosureRejectsOversizedNAR(t *testing.T) {
	t.Parallel()

	s := &server.Service{MaxNarSize: 1024}

	closureHash := strings.Repeat("a", 32)
	narKey := narKeyFor(closureHash)

	body, err := json.Marshal(map[string]any{
		"closure": closureHash + ".narinfo",
		"objects": []map[string]any{
			{"key": closureHash + ".narinfo", "type": "narinfo", "refs": []string{narKey}},
			{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": 4096},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/pending_closures", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	s.CreatePendingClosureHandler(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}

	if !strings.Contains(rr.Body.String(), "exceeds server max NAR size") {
		t.Errorf("unexpected error message: %s", rr.Body.String())
	}
}
