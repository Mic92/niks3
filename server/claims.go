package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultClaimHeartbeat  = 10 * time.Second
	defaultMaxClaimStreams = 4096
	staleHeartbeats        = 3
	claimNotifyChannel     = "claims"
)

// claimHub wakes local waiters when any instance releases a claim. The NOTIFY
// payload is "<hexkey>" for a release or "<hexkey> <kind>" for a failure,
// which is handed to current waiters and not remembered.
type claimHub struct {
	ctx     context.Context //nolint:containedctx // bounds all claim streams to the service lifetime
	streams atomic.Int64
	mu      sync.Mutex
	waiters map[string]map[chan string]struct{}
}

func (h *claimHub) subscribe(key string) (chan string, func()) {
	ch := make(chan string, 1)

	h.mu.Lock()
	if h.waiters[key] == nil {
		h.waiters[key] = map[chan string]struct{}{}
	}

	h.waiters[key][ch] = struct{}{}
	h.mu.Unlock()

	return ch, func() {
		h.mu.Lock()
		delete(h.waiters[key], ch)
		h.mu.Unlock()
	}
}

func (h *claimHub) wake(payload string) {
	key, kind, _ := strings.Cut(payload, " ")

	h.mu.Lock()
	defer h.mu.Unlock()

	for ch := range h.waiters[key] {
		select {
		case ch <- kind:
		default:
		}
	}
}

// StartClaims runs the LISTEN loop that wakes waiters across instances.
func (s *Service) StartClaims(ctx context.Context) {
	ctx, s.stopClaims = context.WithCancel(ctx)
	s.claims = &claimHub{waiters: map[string]map[chan string]struct{}{}, ctx: ctx}

	go func() {
		for ctx.Err() == nil {
			if err := s.listenClaims(ctx); err != nil && ctx.Err() == nil {
				slog.Error("claims listener", "error", err)
				time.Sleep(time.Second)
			}
		}
	}()
}

func (s *Service) listenClaims(ctx context.Context) error {
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return err //nolint:wrapcheck
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN "+claimNotifyChannel); err != nil {
		return err //nolint:wrapcheck
	}

	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err //nolint:wrapcheck
		}

		s.claims.wake(n.Payload)
	}
}

func (s *Service) maxClaimStreams() int64 {
	if s.MaxClaimStreams > 0 {
		return int64(s.MaxClaimStreams)
	}

	return defaultMaxClaimStreams
}

func (s *Service) claimHeartbeat() time.Duration {
	if s.ClaimHeartbeat > 0 {
		return s.ClaimHeartbeat
	}

	return defaultClaimHeartbeat
}

func claimKey(outputs []string) []byte {
	sorted := slices.Clone(outputs)
	slices.Sort(sorted)
	h := sha256.Sum256([]byte(strings.Join(sorted, "\n")))

	return h[:]
}

func (s *Service) ClaimHandler(w http.ResponseWriter, r *http.Request) {
	defer closeRequestBody(r)
	defer s.claims.streams.Add(-1)

	if s.claims.streams.Add(1) > s.maxClaimStreams() {
		w.Header().Set("Retry-After", strconv.Itoa(int(s.claimHeartbeat().Seconds())+1))
		http.Error(w, "too many claim streams", http.StatusServiceUnavailable)

		return
	}

	var req api.ClaimRequest
	if !decodeJSONBody(w, r, MaxClosureRequestBody, &req) {
		return
	}

	if len(req.Outputs) == 0 {
		http.Error(w, "outputs required", http.StatusBadRequest)

		return
	}

	// The stream outlives the server's WriteTimeout and sits behind the
	// metrics wrapper, which hides http.Flusher.
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		slog.Warn("claim: cannot clear write deadline", "error", err)
	}

	enc := json.NewEncoder(w)
	send := func(st api.ClaimStatus) error {
		if err := enc.Encode(st); err != nil {
			return err //nolint:wrapcheck
		}

		return rc.Flush()
	}

	w.Header().Set("Content-Type", "application/x-ndjson")

	key := claimKey(req.Outputs)
	hexKey := hex.EncodeToString(key)

	wake, unsubscribe := s.claims.subscribe(hexKey)
	defer unsubscribe()

	tick := time.NewTicker(s.claimHeartbeat())
	defer tick.Stop()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	stop := context.AfterFunc(s.claims.ctx, cancel) //nolint:contextcheck // also end the stream on service shutdown
	defer stop()

	waiting := false

	for {
		st, err := s.tryClaim(ctx, key, req)
		if err != nil {
			slog.Error("claim", "output", req.Outputs[0], "error", err)

			return
		}

		switch st.Status {
		case api.ClaimBuilt:
			_ = send(st)

			return
		case api.ClaimBuild:
			if send(st) != nil {
				return
			}

			s.holdClaim(ctx, st.Token, tick, send)

			return
		}

		if !waiting {
			if send(st) != nil {
				return
			}

			waiting = true
		}

		select {
		case <-ctx.Done():
			return
		case kind := <-wake:
			if kind != "" {
				_ = send(api.ClaimStatus{Status: api.ClaimFailed, Kind: kind})

				return
			}
		case <-tick.C:
			if send(api.ClaimStatus{Status: api.ClaimHeartbeat}) != nil {
				return
			}
		}
	}
}

func (s *Service) tryClaim(ctx context.Context, key []byte, req api.ClaimRequest) (api.ClaimStatus, error) {
	q := pg.New(s.Pool)

	present, err := q.GetPresentObjects(ctx, req.Outputs)
	if err != nil {
		return api.ClaimStatus{}, err //nolint:wrapcheck
	}

	if len(present) == len(req.Outputs) {
		_ = q.TouchClosures(ctx, req.Outputs)

		return api.ClaimStatus{Status: api.ClaimBuilt}, nil
	}

	token, err := q.TryClaim(ctx, pg.TryClaimParams{
		Key: key, Token: req.Token,
		StaleSecs: (staleHeartbeats * s.claimHeartbeat()).Seconds(),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return api.ClaimStatus{Status: api.ClaimWait}, nil
	} else if err != nil {
		return api.ClaimStatus{}, err //nolint:wrapcheck
	}

	if len(req.Inputs) > 0 {
		_ = q.TouchClosures(ctx, req.Inputs)
	}

	return api.ClaimStatus{Status: api.ClaimBuild, Token: token}, nil
}

// holdClaim keeps the row fresh while the holder's stream is open. A broken
// stream leaves the row: the worker is still building and re-enters with its
// token, or the row goes stale. Only complete/fail release it.
func (s *Service) holdClaim(ctx context.Context, token int64, tick *time.Ticker, send func(api.ClaimStatus) error) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			n, err := pg.New(s.Pool).HeartbeatClaim(ctx, token)
			if err != nil || n == 0 {
				return
			}

			if send(api.ClaimStatus{Status: api.ClaimHeartbeat}) != nil {
				return
			}
		}
	}
}

// releaseClaimTx deletes the claim and notifies waiters in one transaction,
// optionally running extra work (the closure commit) inside it. A non-empty
// kind tells current waiters the build failed instead of promoting one.
func releaseClaimTx(ctx context.Context, pool *pgxpool.Pool, token int64, kind string, inTx func(context.Context, pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { //nolint:wrapcheck
		q := pg.New(tx)

		key, err := q.LockClaim(ctx, token)
		if err != nil {
			return err //nolint:wrapcheck
		}

		if inTx != nil {
			if err := inTx(ctx, tx); err != nil {
				return err
			}
		}

		if err := q.ReleaseClaim(ctx, token); err != nil {
			return err //nolint:wrapcheck
		}

		payload := hex.EncodeToString(key)
		if kind != "" {
			payload += " " + kind
		}

		_, err = tx.Exec(ctx, "SELECT pg_notify($1, $2)", claimNotifyChannel, payload)

		return err //nolint:wrapcheck
	})
}

func (s *Service) FailHandler(w http.ResponseWriter, r *http.Request) {
	defer closeRequestBody(r)

	var req api.FailRequest
	if !decodeJSONBody(w, r, MaxClosureRequestBody, &req) {
		return
	}

	if strings.ContainsAny(req.Kind, " \n") {
		http.Error(w, "bad kind", http.StatusBadRequest)

		return
	}

	err := releaseClaimTx(r.Context(), s.Pool, req.ClaimToken, req.Kind, nil)
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, "stale claim token", http.StatusConflict)

		return
	} else if err != nil {
		slog.Error("fail claim", "error", err)
		http.Error(w, "fail: "+err.Error(), http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
