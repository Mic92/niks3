package server

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/Mic92/niks3/api"
)

// CacheConfigHandler handles GET /api/cache-config.
//
// Unauthenticated — everything returned is already public (the landing page
// shows the same keys). This lets CI integrations auto-configure substituters
// and OIDC audience without users hardcoding them in workflow YAML.
//
// Query param ?issuer=<url> selects which OIDC provider's audience to return.
// An unknown issuer is not an error: oidc_audience is simply omitted and the
// caller must supply the audience itself (or the server has no OIDC for that CI).
func (s *Service) CacheConfigHandler(w http.ResponseWriter, r *http.Request) {
	cfg := api.CacheConfig{
		SubstituterURL: s.CacheURL,
		PublicKeys:     make([]string, 0, len(s.SigningKeys)),
	}

	for _, key := range s.SigningKeys {
		pub, err := key.PublicKey()
		if err != nil {
			slog.Error("failed to derive public key", "error", err)
			http.Error(w, "failed to derive public key", http.StatusInternalServerError)

			return
		}

		cfg.PublicKeys = append(cfg.PublicKeys, pub)
	}

	if issuer := r.URL.Query().Get("issuer"); issuer != "" && s.OIDCValidator != nil {
		if aud, ok := s.OIDCValidator.AudienceForIssuer(issuer); ok {
			cfg.OIDCAudience = aud
		}
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(cfg); err != nil {
		slog.Error("failed to encode cache-config response", "error", err)
	}
}
