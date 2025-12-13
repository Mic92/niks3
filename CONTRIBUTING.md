# Contributing to niks3

Thank you for your interest in contributing to niks3! This document provides guidelines and information for developers.

## Development Setup

### Local Development Environment

Start the complete development environment with:

```bash
nix run .#dev
```

This launches a process-compose setup with:

- **PostgreSQL**: Database server with automatic initialization and health checks
- **RustFS**: S3-compatible storage server with health checks
- **niks3-server**: API server with automatic recompilation on code changes (via watchexec)

### Features

- **Auto-reload**: The niks3-server automatically recompiles and restarts when Go source files change
- **Health checks**: Services wait for dependencies to be healthy before starting
- **Signing keys**: Nix signing key pair is automatically generated on first run
- **Environment variables**: All configuration is in `.envrc` (see NIKS3\_\*, PGDATA, MINIO_DATA)

### Data Management

- State is stored in `.data/` directory
- For a fresh environment, delete `.data/` and restart

### Environment Variables

Key variables configured in `.envrc`:

- `DATABASE_URL`: PostgreSQL connection string
- `NIKS3_*`: Server configuration (endpoint, credentials, bucket, etc.)
- `NIKS3_SIGN_KEY_PATHS`: Path to signing key (auto-generated)

## Database

### Migrations

We use [Goose](https://github.com/pressly/goose) for database migrations.

Migrations are located in `pg/migrations`.

### SQL Querying

We use [sqlc](https://sqlc.dev/) with [pgx](https://github.com/jackc/pgx).

Config is located at `sqlc.yml`. Re-generate using:

```bash
sqlc generate
```

## Testing

### Benchmarks

A benchmark for uploading a closure to S3 is available.

To run the benchmark:

```bash
cd server
go test -bench=BenchmarkPythonClosure -benchtime=3x -v
```

## Contributing S3 Provider Testing Results

We're collecting real-world performance data for different S3 providers. If you test niks3 with any provider, please update the [S3 Provider Comparison](https://github.com/Mic92/niks3/wiki/S3-Provider-Comparison) wiki page with your findings!

Include:

- **Provider name and plan**
- **`.narinfo` lookup latency**
- **Download speeds**
- **Reliability notes**
- **Configuration requirements** (CDN, headers, etc.)

## Code Style

- Follow Go conventions and best practices
- Run `go fmt` before committing
- Ensure tests pass before submitting PRs

## Pull Request Process

1. Fork the repository
1. Create a feature branch
1. Make your changes
1. Test your changes locally
1. Submit a pull request with a clear description of the changes

## Questions?

If you have questions about contributing, feel free to open an issue or reach out to the maintainers.
