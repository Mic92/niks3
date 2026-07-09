package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
)

// closeRequestBody closes the request body and logs failures. Meant for use
// with defer in handlers that read the body.
func closeRequestBody(r *http.Request) {
	if err := r.Body.Close(); err != nil {
		slog.Error("Failed to close request body", "error", err)
	}
}

// writeJSONResponse writes v as a JSON response with the appropriate content
// type. Encoding failures are reported as a 500 to the client.
func writeJSONResponse(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("Failed to encode response", "error", err)
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)
	}
}

// parsePendingClosureID extracts and parses the {id} path value. On error it
// writes a 400 response and returns false.
func parsePendingClosureID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	idValue := r.PathValue("id")
	if idValue == "" {
		http.Error(w, "missing id", http.StatusBadRequest)

		return 0, false
	}

	id, err := strconv.ParseInt(idValue, 10, 32)
	if err != nil {
		http.Error(w, "invalid id: "+err.Error(), http.StatusBadRequest)

		return 0, false
	}

	return id, true
}
