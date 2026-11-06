package server

import (
	"context"
	"log/slog"
	"slices"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
)

const (
	ruleEnabled        = "Enabled"
	narPrefix          = "nar/"
	abortUploadsRuleID = "niks3-abort-incomplete-multipart-uploads"
	// Well above the GC's 6h failed-upload cutoff and any single push.
	defaultAbortIncompleteUploadsDays = 2
)

// abortsNARUploads reports whether r aborts incomplete multipart uploads of NARs.
func abortsNARUploads(r lifecycle.Rule) bool {
	if r.Status != ruleEnabled || r.AbortIncompleteMultipartUpload.DaysAfterInitiation <= 0 {
		return false
	}

	prefix := r.Prefix
	if !r.RuleFilter.IsNull() {
		prefix = r.RuleFilter.Prefix
		if prefix == "" {
			prefix = r.RuleFilter.And.Prefix
		}
	}

	// The rule covers NARs if its prefix is a prefix of "nar/".
	return strings.HasPrefix(narPrefix, prefix)
}

// ensureAbortUploadsRule makes S3 abort multipart uploads niks3 lost track of,
// as a backstop for cleanupPendingClosures. It keeps the bucket's other rules
// and leaves an operator's own abort rule alone.
func (s *Service) ensureAbortUploadsRule(ctx context.Context) {
	days := s.AbortIncompleteUploadsDays
	if days <= 0 {
		return
	}

	warn := func(msg string, err error) {
		slog.Warn(msg+"; incomplete uploads are only removed by niks3 gc. "+
			"Add an AbortIncompleteMultipartUpload lifecycle rule to the bucket, "+
			"or set --abort-incomplete-uploads-days=0 to silence this warning",
			"bucket", s.Bucket, "error", err)
	}

	cfg, err := s.MinioClient.GetBucketLifecycle(ctx, s.Bucket)
	if err != nil {
		if minio.ToErrorResponse(err).Code != "NoSuchLifecycleConfiguration" {
			warn("Cannot read the bucket lifecycle configuration", err)

			return
		}

		cfg = lifecycle.NewConfiguration()
	}

	for _, r := range cfg.Rules {
		if r.ID == abortUploadsRuleID {
			if int(r.AbortIncompleteMultipartUpload.DaysAfterInitiation) == days && r.Status == ruleEnabled {
				return
			}

			continue
		}

		if abortsNARUploads(r) {
			return
		}
	}

	cfg.Rules = slices.DeleteFunc(cfg.Rules, func(r lifecycle.Rule) bool { return r.ID == abortUploadsRuleID })
	cfg.Rules = append(cfg.Rules, lifecycle.Rule{
		ID:         abortUploadsRuleID,
		Status:     ruleEnabled,
		RuleFilter: lifecycle.Filter{Prefix: narPrefix},
		AbortIncompleteMultipartUpload: lifecycle.AbortIncompleteMultipartUpload{
			DaysAfterInitiation: lifecycle.ExpirationDays(days),
		},
	})

	if err := s.MinioClient.SetBucketLifecycle(ctx, s.Bucket, cfg); err != nil {
		warn("Cannot install the lifecycle rule that aborts incomplete uploads", err)

		return
	}

	slog.Info("Installed lifecycle rule aborting incomplete NAR uploads", "bucket", s.Bucket, "days", days)
}
