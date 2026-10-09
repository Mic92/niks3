package server

import (
	"bytes"
	"log/slog"
	"net/http"
	"path"
	"regexp"
	"time"

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

	defer closeRequestBody(r)

	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "missing pin name", http.StatusBadRequest)

		return
	}

	if !pinNameRegex.MatchString(name) {
		http.Error(w, "invalid pin name: must contain only alphanumeric characters, dashes, underscores, and dots", http.StatusBadRequest)

		return
	}

	if len(name) > 256 {
		http.Error(w, "pin name too long: maximum 256 characters", http.StatusBadRequest)

		return
	}

	if !s.mayWritePin(r, name) {
		slog.Warn("Refused reserved pin", "name", name)
		http.Error(w, "Forbidden: pin "+name+" is reserved for another rule", http.StatusForbidden)

		return
	}

	req := &createPinRequest{}
	if !decodeJSONBody(w, r, maxAPIRequestBody, req) {
		return
	}

	if req.StorePath == "" {
		http.Error(w, "missing store_path", http.StatusBadRequest)

		return
	}

	narinfoKey, err := storePathToNarinfoKey(req.StorePath)
	if err != nil {
		http.Error(w, "invalid store path: "+err.Error(), http.StatusBadRequest)

		return
	}

	// Run the existence check, the upsert and the S3 write in one
	// transaction. The closure row is locked FOR SHARE, so concurrent GC
	// cannot delete the closure in between (which would surface as an FK
	// violation / 500). The pin row is written before the S3 object and
	// committed after it: the upsert's row lock makes a second writer of the
	// same name wait until this one has committed, so the last writer to
	// commit is also the last to write S3, and a failed S3 write rolls the
	// row back. Written the other way round, two writers could leave S3
	// serving one closure while the database protects the other from GC.
	tx, err := s.Pool.Begin(r.Context())
	if err != nil {
		slog.Error("Failed to begin transaction", "error", err)
		http.Error(w, "failed to create pin: "+err.Error(), http.StatusInternalServerError)

		return
	}

	committed := false

	defer rollbackOnError(r.Context(), &tx, &err, &committed)

	queries := pg.New(tx)

	_, err = queries.GetClosureForShare(r.Context(), narinfoKey)
	if err != nil {
		slog.Error("Failed to get closure for pin", "narinfo_key", narinfoKey, "error", err)
		http.Error(w, "closure not found: store path must be pushed before pinning", http.StatusNotFound)

		return
	}

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

	// If the commit below fails after this write, S3 is ahead of the
	// database until the pin is written again.
	pinKey := "pins/" + name

	_, err = s.MinioClient.PutObject(r.Context(), s.Bucket, pinKey,
		bytes.NewReader([]byte(req.StorePath)), int64(len(req.StorePath)),
		minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		slog.Error("Failed to write pin to S3", "key", pinKey, "error", err)
		http.Error(w, "failed to write pin to S3: "+err.Error(), http.StatusInternalServerError)

		return
	}

	if err = tx.Commit(r.Context()); err != nil {
		slog.Error("Failed to commit pin transaction", "name", name, "error", err)
		http.Error(w, "failed to create pin: "+err.Error(), http.StatusInternalServerError)

		return
	}

	committed = true

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

	result := make([]PinInfo, 0, len(pins))
	for _, p := range pins {
		result = append(result, PinInfo{
			Name:      p.Name,
			StorePath: p.StorePath,
			CreatedAt: formatPinTime(p.CreatedAt.Time),
			UpdatedAt: formatPinTime(p.UpdatedAt.Time),
		})
	}

	writeJSONResponse(w, result)
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

	// Delete the row and the S3 object in one transaction, and commit only
	// once S3 is done. Consumers read the S3 object, and a public pin whose
	// row is gone names a closure GC is free to collect; a later delete would
	// find no row and never remove the object. The row delete comes first so
	// its lock holds off a concurrent create of the same name until then.
	tx, err := s.Pool.Begin(r.Context())
	if err != nil {
		slog.Error("Failed to begin transaction", "error", err)
		http.Error(w, "failed to delete pin: "+err.Error(), http.StatusInternalServerError)

		return
	}

	committed := false

	defer rollbackOnError(r.Context(), &tx, &err, &committed)

	queries := pg.New(tx)

	if _, err = queries.GetPin(r.Context(), name); err != nil {
		slog.Error("Pin not found", "name", name, "error", err)
		http.Error(w, "pin not found", http.StatusNotFound)

		return
	}

	if err = queries.DeletePin(r.Context(), name); err != nil {
		slog.Error("Failed to delete pin from database", "name", name, "error", err)
		http.Error(w, "failed to delete pin: "+err.Error(), http.StatusInternalServerError)

		return
	}

	pinKey := "pins/" + name

	if err = s.MinioClient.RemoveObject(r.Context(), s.Bucket, pinKey, minio.RemoveObjectOptions{}); err != nil {
		slog.Error("Failed to delete pin from S3", "key", pinKey, "error", err)
		http.Error(w, "failed to delete pin from S3: "+err.Error(), http.StatusInternalServerError)

		return
	}

	if err = tx.Commit(r.Context()); err != nil {
		slog.Error("Failed to commit pin deletion", "name", name, "error", err)
		http.Error(w, "failed to delete pin: "+err.Error(), http.StatusInternalServerError)

		return
	}

	committed = true

	slog.Info("Deleted pin", "name", name)
	w.WriteHeader(http.StatusNoContent)
}

// formatPinTime renders a timestamp as UTC RFC 3339. pgx decodes timestamptz
// into the local zone, so converting to UTC first is required for the "Z"
// suffix to be truthful.
func formatPinTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// storePathRe matches an absolute store path: directory components free of
// whitespace and control characters, then a nix32 hash and a name from Nix's
// store path alphabet. The pin's S3 object is this string verbatim and
// consumers substitute it into a command line, so it must be one path.
var storePathRe = regexp.MustCompile(`^(?:/[^/\x00-\x20\x7f]+)+/([0-9a-df-np-sv-z]{32})-[a-zA-Z0-9+\-._?=]+$`)

// storePathToNarinfoKey converts a store path like /nix/store/abc123-name to abc123.narinfo.
func storePathToNarinfoKey(storePath string) (string, error) {
	m := storePathRe.FindStringSubmatch(storePath)
	if m == nil || path.Clean(storePath) != storePath {
		return "", &invalidStorePathError{storePath}
	}

	return m[1] + ".narinfo", nil
}

type invalidStorePathError struct {
	path string
}

func (e *invalidStorePathError) Error() string {
	return "invalid store path format: " + e.path
}
