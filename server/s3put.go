package server

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // S3 ETags are MD5, not used for security
	"encoding/hex"
	"fmt"

	"github.com/minio/minio-go/v7"
)

// putSmallObject skips the PUT if the single-part ETag (the MD5) already matches.
func (s *Service) putSmallObject(ctx context.Context, key string, data []byte, contentType string) error {
	sum := md5.Sum(data) //nolint:gosec // see import

	info, err := s.MinioClient.StatObject(ctx, s.Bucket, key, minio.StatObjectOptions{})
	if err == nil && info.ETag == hex.EncodeToString(sum[:]) &&
		info.ContentType == contentType && info.Metadata.Get("Content-Encoding") == "" {
		return nil
	}

	_, err = s.MinioClient.PutObject(ctx, s.Bucket, key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{
			ContentType: contentType,
			// Avoids aws-chunked over plain HTTP, which RGW before ceph#64440 serves back.
			DisableContentSha256: true,
			SendContentMd5:       true,
		})
	if err != nil {
		return fmt.Errorf("putting %s: %w", key, err)
	}

	return nil
}
