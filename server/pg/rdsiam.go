package pg

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WithRDSIAMAuth authenticates every new connection with an RDS IAM token,
// using credentials from the default AWS chain (environment, IRSA, EKS Pod
// Identity, instance profile). A token is valid for 15 minutes but only
// checked when a connection opens, so one is built for each connection and
// open connections are not affected when it expires.
func WithRDSIAMAuth(region string) Option {
	return func(ctx context.Context, cfg *pgxpool.Config) error {
		awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			return fmt.Errorf("failed to load AWS configuration for RDS IAM auth: %w", err)
		}

		cfg.BeforeConnect = rdsIAMBeforeConnect(region, awsCfg.Credentials)

		return nil
	}
}

func rdsIAMBeforeConnect(region string, creds aws.CredentialsProvider) func(context.Context, *pgx.ConnConfig) error {
	return func(ctx context.Context, cc *pgx.ConnConfig) error {
		endpoint := net.JoinHostPort(cc.Host, strconv.Itoa(int(cc.Port)))

		token, err := auth.BuildAuthToken(ctx, endpoint, region, cc.User, creds)
		if err != nil {
			return fmt.Errorf("failed to build RDS IAM auth token: %w", err)
		}

		cc.Password = token

		return nil
	}
}
