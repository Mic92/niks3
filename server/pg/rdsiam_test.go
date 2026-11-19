package pg_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server/pg"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRDSIAMBeforeConnect(t *testing.T) {
	t.Parallel()

	cc, err := pgx.ParseConfig("postgres://niks3@db.example.us-east-1.rds.amazonaws.com:5432/niks3?sslmode=require")
	if err != nil {
		t.Fatal(err)
	}

	creds := credentials.NewStaticCredentialsProvider("AKIDEXAMPLE", "secret", "")
	if err := pg.RDSIAMBeforeConnect("us-east-1", creds)(t.Context(), cc); err != nil {
		t.Fatal(err)
	}

	host, rawQuery, found := strings.Cut(cc.Password, "?")
	if !found || host != "db.example.us-east-1.rds.amazonaws.com:5432" {
		t.Fatalf("token does not start with the endpoint: %q", cc.Password)
	}

	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[string]string{"Action": "connect", "DBUser": "niks3"} {
		if got := query.Get(key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}

	if !strings.Contains(query.Get("X-Amz-Credential"), "/us-east-1/rds-db/") {
		t.Errorf("token is not scoped to rds-db in us-east-1: %q", query.Get("X-Amz-Credential"))
	}

	if query.Get("X-Amz-Signature") == "" {
		t.Error("token is not signed")
	}
}

func TestWithRDSIAMAuthSetsBeforeConnect(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")

	cfg, err := pgxpool.ParseConfig("postgres://niks3@localhost/niks3")
	if err != nil {
		t.Fatal(err)
	}

	if err := pg.WithRDSIAMAuth("us-east-1")(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}

	if cfg.BeforeConnect == nil {
		t.Fatal("BeforeConnect is not set")
	}
}
