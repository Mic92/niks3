package server_test

import (
	"testing"

	"github.com/minio/minio-go/v7/pkg/lifecycle"
)

func TestEnsureAbortUploadsRule(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	t.Cleanup(func() { service.Close() })

	// An operator's unrelated rule must survive.
	operatorRule := lifecycle.Rule{
		ID:         "operator",
		Status:     "Enabled",
		RuleFilter: lifecycle.Filter{Prefix: "tmp/"},
		Expiration: lifecycle.Expiration{Days: 7},
	}
	ok(t, service.MinioClient.SetBucketLifecycle(t.Context(), service.Bucket,
		&lifecycle.Configuration{Rules: []lifecycle.Rule{operatorRule}}))

	service.AbortIncompleteUploadsDays = 2
	service.EnsureAbortUploadsRule(t.Context())

	cfg, err := service.MinioClient.GetBucketLifecycle(t.Context(), service.Bucket)
	ok(t, err)

	ids := map[string]lifecycle.Rule{}
	for _, r := range cfg.Rules {
		ids[r.ID] = r
	}

	if _, found := ids["operator"]; !found {
		t.Errorf("operator rule dropped: %+v", cfg.Rules)
	}

	ours, found := ids["niks3-abort-incomplete-multipart-uploads"]
	if !found || ours.AbortIncompleteMultipartUpload.DaysAfterInitiation != 2 || ours.RuleFilter.Prefix != "nar/" {
		t.Fatalf("abort rule not installed: %+v", cfg.Rules)
	}
}
