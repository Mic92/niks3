# Contributing to niks3

Thank you for your interest in contributing to niks3! This document provides guidelines and information for developers.

## Development Setup

### Local Development Environment

Enter the development shell with:

```bash
nix develop
```

Or use [direnv](https://direnv.net/) (`direnv allow`) to load it automatically.

The shell provides Go tooling, PostgreSQL, RustFS (S3-compatible storage),
goose, sqlc, s5cmd and awscli.

### Development Workflow

Develop against the test suite instead of a manually started server. The Go
tests start their own PostgreSQL and RustFS (see `server/main_test.go`):

```bash
go test ./server/...
```

When adding features or fixing bugs, cover the new behavior with tests.

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
go test -bench=BenchmarkUploadClosure -benchtime=3x -v
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
