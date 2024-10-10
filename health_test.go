package main

import (
	"testing"
)

func TestServer_healthCheckHandler(t *testing.T) {
	server := createTestServer(t)
	defer server.Close()

	// check that health check works also with database closed
	server.db.Close()

	testRequest(&TestRequest{
		method: "GET",
	  path: "/health",
	  handler: server.healthCheckHandler,
	}, t)
}
