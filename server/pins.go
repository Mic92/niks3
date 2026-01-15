package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"

	"github.com/Mic92/niks3/server/pg"
	minio "github.com/minio/minio-go/v7"
)

// pinNameRegex validates pin names: alphanumeric, dash, underscore, dot only.
var pinNameRegex = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

type createPinRequest struct {
	StorePath string `json:"store_path"`
}

// CreatePinHandler handles POST /api/pins/{name} endpoint.
// Creates or updates a pin that maps a name to a store path.
// The pin protects the associated closure from garbage collection.
func (s *Service) CreatePinHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received create pin request", "method", r.Method, "path", r.URL.Path)

	defer func() {
		if err := r.Body.Close(); err != nil {
			slog.Error("Failed to close request body", "error", err)
		}
	}()

	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "missing pin name", http.StatusBadRequest)

		return
	}

	// Validate pin name
	if !pinNameRegex.MatchString(name) {
		http.Error(w, "invalid pin name: must contain only alphanumeric characters, dashes, underscores, and dots", http.StatusBadRequest)

		return
	}

	if len(name) > 256 {
		http.Error(w, "pin name too long: maximum 256 characters", http.StatusBadRequest)

		return
	}

	// Parse request body
	req := &createPinRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		http.Error(w, "failed to decode request: "+err.Error(), http.StatusBadRequest)

		return
	}

	if req.StorePath == "" {
		http.Error(w, "missing store_path", http.StatusBadRequest)

		return
	}

	// Convert store path to narinfo key
	narinfoKey, err := storePathToNarinfoKey(req.StorePath)
	if err != nil {
		http.Error(w, "invalid store path: "+err.Error(), http.StatusBadRequest)

		return
	}

	// Verify the closure exists in the database
	queries := pg.New(s.Pool)

	_, err = queries.GetClosure(r.Context(), narinfoKey)
	if err != nil {
		slog.Error("Failed to get closure for pin", "narinfo_key", narinfoKey, "error", err)
		http.Error(w, "closure not found: store path must be pushed before pinning", http.StatusNotFound)

		return
	}

	// Upsert the pin in the database
	err = queries.UpsertPin(r.Context(), pg.UpsertPinParams{
		Name:       name,
		NarinfoKey: narinfoKey,
		StorePath:  req.StorePath,
	})
	if err != nil {
		slog.Error("Failed to upsert pin", "name", name, "narinfo_key", narinfoKey, "error", err)
		http.Error(w, "failed to create pin: "+err.Error(), http.StatusInternalServerError)

		return
	}

	// Write the pin to S3 for easy retrieval via curl
	pinKey := "pins/" + name

	_, err = s.MinioClient.PutObject(r.Context(), s.Bucket, pinKey,
		bytes.NewReader([]byte(req.StorePath)), int64(len(req.StorePath)),
		minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		slog.Error("Failed to write pin to S3", "key", pinKey, "error", err)
		http.Error(w, "failed to write pin to S3: "+err.Error(), http.StatusInternalServerError)

		return
	}

	slog.Info("Created/updated pin", "name", name, "store_path", req.StorePath, "narinfo_key", narinfoKey)

	w.WriteHeader(http.StatusNoContent)
}

// PinInfo represents a pin's information for API responses.
type PinInfo struct {
	Name      string `json:"name"`
	StorePath string `json:"store_path"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// ListPinsHandler handles GET /api/pins endpoint.
// Returns a list of all pins.
func (s *Service) ListPinsHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received list pins request", "method", r.Method, "path", r.URL.Path)

	queries := pg.New(s.Pool)

	pins, err := queries.ListPins(r.Context())
	if err != nil {
		slog.Error("Failed to list pins", "error", err)
		http.Error(w, "failed to list pins: "+err.Error(), http.StatusInternalServerError)

		return
	}

	// Convert to API response format
	result := make([]PinInfo, 0, len(pins))
	for _, p := range pins {
		result = append(result, PinInfo{
			Name:      p.Name,
			StorePath: p.StorePath,
			CreatedAt: p.CreatedAt.Time.Format("2006-01-02T15:04:05Z"),
			UpdatedAt: p.UpdatedAt.Time.Format("2006-01-02T15:04:05Z"),
		})
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(result); err != nil {
		slog.Error("Failed to encode response", "error", err)
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)

		return
	}
}

// DeletePinHandler handles DELETE /api/pins/{name} endpoint.
// Deletes a pin by name.
func (s *Service) DeletePinHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received delete pin request", "method", r.Method, "path", r.URL.Path)

	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "missing pin name", http.StatusBadRequest)

		return
	}

	queries := pg.New(s.Pool)

	// Check if pin exists
	_, err := queries.GetPin(r.Context(), name)
	if err != nil {
		slog.Error("Pin not found", "name", name, "error", err)
		http.Error(w, "pin not found", http.StatusNotFound)

		return
	}

	// Delete from database
	err = queries.DeletePin(r.Context(), name)
	if err != nil {
		slog.Error("Failed to delete pin from database", "name", name, "error", err)
		http.Error(w, "failed to delete pin: "+err.Error(), http.StatusInternalServerError)

		return
	}

	// Delete from S3
	pinKey := "pins/" + name

	err = s.MinioClient.RemoveObject(r.Context(), s.Bucket, pinKey, minio.RemoveObjectOptions{})
	if err != nil {
		// Log but don't fail - the database is the source of truth
		slog.Warn("Failed to delete pin from S3", "key", pinKey, "error", err)
	}

	slog.Info("Deleted pin", "name", name)
	w.WriteHeader(http.StatusNoContent)
}

// storePathToNarinfoKey converts a store path like /nix/store/abc123-name to abc123.narinfo.
func storePathToNarinfoKey(storePath string) (string, error) {
	// Extract the hash from the store path
	// Store paths are like: /nix/store/abc123def456-package-name-1.0
	// We need to extract: abc123def456

	// Find the last path component
	lastSlash := -1

	for i := len(storePath) - 1; i >= 0; i-- {
		if storePath[i] == '/' {
			lastSlash = i

			break
		}
	}

	base := storePath
	if lastSlash >= 0 {
		base = storePath[lastSlash+1:]
	}

	// Find the first dash which separates hash from name
	dashIdx := -1

	for i := range len(base) {
		if base[i] == '-' {
			dashIdx = i

			break
		}
	}

	if dashIdx <= 0 {
		return "", &invalidStorePathError{storePath}
	}

	hash := base[:dashIdx]

	return hash + ".narinfo", nil
}

type invalidStorePathError struct {
	path string
}

func (e *invalidStorePathError) Error() string {
	return "invalid store path format: " + e.path
}
