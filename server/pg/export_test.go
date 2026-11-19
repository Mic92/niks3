package pg

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/jackc/pgx/v5"
)

func RDSIAMBeforeConnect(region string, creds aws.CredentialsProvider) func(context.Context, *pgx.ConnConfig) error {
	return rdsIAMBeforeConnect(region, creds)
}
