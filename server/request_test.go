package server_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/pg"
	minio "github.com/minio/minio-go/v7"
)

func createTestService(t *testing.T) *server.Service {
	t.Helper()

	if testPostgresServer == nil {
		t.Fatal("postgres server not started")
	}

	if testMinioServer == nil {
		t.Fatal("minio server not started")
	}

	// create database for test
	dbName := "db" + strconv.Itoa(int(testDBCount.Add(1)))
	//nolint:gosec
	command := exec.Command("createdb", "-h", testPostgresServer.tempDir, "-U", "postgres", dbName)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	err := command.Run()
	ok(t, err)

	connectionString := fmt.Sprintf("postgres://?dbname=%s&user=postgres&host=%s", dbName, testPostgresServer.tempDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pg.Connect(ctx, connectionString)
	if err != nil {
		ok(t, err)
	}
	// create bucket for test
	bucketName := "bucket" + strconv.Itoa(int(testBucketCount.Add(1)))
	minioClient := testMinioServer.Client(t)

	err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	ok(t, err)

	return &server.Service{
		Pool:        pool,
		Bucket:  bucketName,
		MinioClient: minioClient,
	}
}

type TestRequest struct {
	method  string
	path    string
	body    []byte
	handler http.HandlerFunc
	// function to checkResponse the response
	checkResponse *func(*testing.T, *httptest.ResponseRecorder)
	header        map[string]string
	pathValues    map[string]string
}

func testRequest(t *testing.T, req *TestRequest) *httptest.ResponseRecorder {
	t.Helper()

	rr := httptest.NewRecorder()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, req.method, req.path, bytes.NewBuffer(req.body))
	for k, v := range req.pathValues {
		httpReq.SetPathValue(k, v)
	}

	for k, v := range req.header {
		httpReq.Header.Set(k, v)
	}

	ok(t, err)
	req.handler.ServeHTTP(rr, httpReq)

	if req.checkResponse == nil {
		if rr.Code < 200 || rr.Code >= 300 {
			httpOkDepth(t, rr)
		}
	} else {
		(*req.checkResponse)(t, rr)
	}

	return rr
}
