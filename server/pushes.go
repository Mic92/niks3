package server

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgconn"
)

type createPushRequest struct {
	Roots    []string         `json:"roots"`
	Objects  []objectWithRefs `json:"objects"`
	VerifyS3 bool             `json:"verify_s3,omitempty"`
}

// CreatePushHandler handles POST /api/pushes.
//
// A push names the narinfo keys it wants to publish (roots) and lists every
// object under them once. The server keeps one pending row per object and
// answers with the objects the client has to upload. Completing the push
// publishes a closure row for each root.
func (s *Service) CreatePushHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received push request", "method", r.Method, "path", r.URL.Path)

	defer closeRequestBody(r)

	req := &createPushRequest{}
	if !decodeJSONBody(w, r, MaxClosureRequestBody, req) {
		return
	}

	if len(req.Roots) == 0 {
		http.Error(w, "missing roots", http.StatusBadRequest)

		return
	}

	if len(req.Objects) == 0 {
		http.Error(w, "missing objects key", http.StatusBadRequest)

		return
	}

	objectsMap, ok := s.validateObjects(w, req.Objects)
	if !ok {
		return
	}

	for _, root := range req.Roots {
		if !strings.HasSuffix(root, ".narinfo") {
			http.Error(w, "root key must end with .narinfo: "+root, http.StatusBadRequest)

			return
		}

		if _, found := objectsMap[root]; !found {
			http.Error(w, "root is not among the objects: "+root, http.StatusBadRequest)

			return
		}
	}

	push, err := s.createPendingClosure(r.Context(), s.Pool, req.Roots[0], req.Roots, objectsMap, req.VerifyS3)
	if err != nil {
		if s.handleS3Error(w, err, "create push") {
			return
		}

		http.Error(w, "failed to start push: "+err.Error(), http.StatusInternalServerError)

		return
	}

	writeJSONResponse(w, push)
}

// CompletePushHandler handles POST /api/pushes/{id}/complete.
// It fails with 409 if an object the push relied on has been collected since
// the push started; the client then has to start a new push.
func (s *Service) CompletePushHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received complete push request", "method", r.Method, "path", r.URL.Path)

	defer closeRequestBody(r)

	id, ok := parsePendingClosureID(w, r)
	if !ok {
		return
	}

	if err := pg.New(s.Pool).CommitPush(r.Context(), id); err != nil {
		var pgError *pgconn.PgError

		switch {
		case errors.As(err, &pgError) && strings.HasPrefix(pgError.Message, "Push does not exist"):
			http.Error(w, "push not found", http.StatusNotFound)
		case errors.As(err, &pgError) && strings.HasPrefix(pgError.Message, "Push object missing"):
			http.Error(w, pgError.Message, http.StatusConflict)
		default:
			slog.Error("Failed to complete push", "id", id, "error", err)
			http.Error(w, "failed to complete push: "+err.Error(), http.StatusInternalServerError)
		}

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
