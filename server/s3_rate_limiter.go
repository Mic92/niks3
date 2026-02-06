package server

import (
	"errors"
	"log/slog"
	"net/http"

	minio "github.com/minio/minio-go/v7"
)

// handleS3Error checks if an error is a rate limit and returns appropriate HTTP response.
// Returns true if the error was handled (caller should return), false if caller should
// handle the error itself. It checks the error message for rate limit indicators since
// errors may be wrapped as they bubble up through the call stack.
func (s *Service) handleS3Error(w http.ResponseWriter, err error, operation string) bool {
	if err == nil {
		return false
	}

	// Check if any error in the chain is a rate limit error
	if isRateLimitError(err) {
		// Record throttle to adapt the rate limiter
		s.S3RateLimiter.RecordThrottle()

		slog.Warn("S3 rate limit hit", "operation", operation, "error", err)
		w.Header().Set("Retry-After", "2")
		http.Error(w, "S3 rate limit exceeded, please retry", http.StatusTooManyRequests)

		return true
	}

	return false
}

// isRateLimitError checks if a minio error (or any error in a wrapped chain)
// is a rate limit/throttle response. S3 uses 503 with "SlowDown" error code.
// Some S3-compatible providers may use 429 or other codes.
func isRateLimitError(err error) bool {
	for err != nil {
		if isMinioRateLimitError(err) {
			return true
		}

		err = errors.Unwrap(err)
	}

	return false
}

// isMinioRateLimitError checks a single error (not unwrapped) for rate limit indicators.
func isMinioRateLimitError(err error) bool {
	errResp := minio.ToErrorResponse(err)

	// Check S3 error codes (primary detection method)
	switch errResp.Code {
	case "SlowDown", "SlowDownRead", "SlowDownWrite",
		"Throttling", "ThrottlingException",
		"RequestThrottled", "RequestLimitExceeded":
		return true
	}

	// Some S3-compatible providers use 429 instead of 503+SlowDown
	if errResp.StatusCode == http.StatusTooManyRequests {
		return true
	}

	return false
}
