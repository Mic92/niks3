package main

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

	"github.com/Mic92/niks3/pg"
	minio "github.com/minio/minio-go/v7"
)

func createTestServer(t *testing.T) *Server {
	if testPostgresServer == nil {
		t.Fatal("postgres server not started")
	}
	if testMinioServer == nil {
		t.Fatal("minio server not started")
	}

	// create database for test
	dbName := "db" + strconv.Itoa(int(testDBCount.Add(1)))
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

	return &Server{
		pool:        pool,
		bucketName:  bucketName,
		minioClient: minioClient,
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

func testRequest(req *TestRequest, tb *testing.T) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(req.handler)
	httpReq, err := http.NewRequest(req.method, req.path, bytes.NewBuffer(req.body))
	for k, v := range req.pathValues {
		httpReq.SetPathValue(k, v)
	}
	for k, v := range req.header {
		httpReq.Header.Set(k, v)
	}
	ok(tb, err)
	handler.ServeHTTP(rr, httpReq)
	if req.checkResponse == nil {
		if rr.Code < 200 || rr.Code >= 300 {
			httpOkDepth(tb, rr, 2)
		}
	} else {
		(*req.checkResponse)(tb, rr)
	}
	return rr
}
