# GC server for Nix binary caches based on S3-compatible storage

Status: beta

The idea is to have all reads be handled by the s3 cache (which itself can be high-available)
and have a gc server that tracks all uploads to the cache and runs periodic GC on s3 cache.
Since writes to a binary cache are often not as critical as reads,
we can vastly simplify the operational complexity of the GC server, i.e. only
running one instance next to the CI infrastructure.

## Features

### Binary Cache Protocol Support

niks3 implements the [Nix binary cache specification](https://nixos.org/manual/nix/stable/command-ref/new-cli/nix3-help-stores.html#s3-binary-cache-store) with the following features:

- **NAR files** (`nar/`): Compressed with zstd, stored in S3
- **Narinfo files** (`.narinfo`): Metadata with cryptographic signatures
  - StorePath, URL, Compression, NarHash, NarSize
  - FileHash, FileSize (for compressed NAR)
  - References, Deriver
  - Signatures (Sig fields)
  - CA field for content-addressed derivations
- **Build logs** (`log/`): Compressed build output storage
- **Realisation files** (`realisations/*.doi`): For content-addressed derivations
- **Cache info** (`nix-cache-info`): Automatic generation with WantMassQuery, Priority

### Advanced Features

- **Cryptographic signing**: NAR signatures using Ed25519 keys (compatible with `nix key generate-secret`)
- **Content-addressed derivations**: Full CA support with realisation info
- **Multipart uploads**: Efficient handling of large NARs (>100MB)
- **Transactional uploads**: Atomic closure uploads with rollback on failure
- **Garbage collection**: Reference-tracking GC with configurable retention
- **Parallel uploads**: Client parallelizes NAR and metadata uploads

### Operational Features

- Authentication via API tokens (Bearer auth)

## Setup

### Prerequisites

- NixOS system (or Nix with flakes enabled)
- S3-compatible storage (MinIO, AWS S3, etc.)
- PostgreSQL database (automatically configured on NixOS)
- Nix signing keys

### NixOS Module Configuration

```nix
{
  services.niks3 = {
    enable = true;
    httpAddr = "0.0.0.0:5751";

    # S3 configuration
    s3 = {
      endpoint = "s3.amazonaws.com";  # or your S3-compatible endpoint
      bucket = "my-nix-cache";
      useSSL = true;
      accessKeyFile = "/run/secrets/s3-access-key";
      secretKeyFile = "/run/secrets/s3-secret-key";
    };

    # API authentication token (minimum 36 characters)
    apiTokenFile = "/run/secrets/niks3-api-token";

    # Signing keys for NAR signing
    signKeyFiles = [ "/run/secrets/niks3-signing-key" ];
  };
}
```

### Generating Signing Keys

Generate a signing key pair:

```bash
# Generate secret key
nix key generate-secret --key-name my-cache-1 > /run/secrets/niks3-signing-key

# Extract public key
nix key convert-secret-to-public < /run/secrets/niks3-signing-key
# Output: my-cache-1:base64encodedpublickey...
```

Configure Nix clients to trust the public key:

```nix
{
  nix.settings.trusted-public-keys = [
    "my-cache-1:base64encodedpublickey..."
  ];
}
```

### Client Usage

#### Pushing Store Paths

```bash
export NIKS3_SERVER_URL=http://server:5751
export NIKS3_AUTH_TOKEN_FILE=/path/to/token-file

niks3 push /nix/store/...-package-name
```

The push operation uploads:

- NAR (compressed with zstd)
- Signed narinfo
- Build logs (if available)
- Realisation info for content-addressed derivations

#### Pulling from Cache

Use Nix's native S3 support:

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key

nix copy --from 's3://my-nix-cache?endpoint=http://localhost:9000&region=us-east-1' \
         /nix/store/...-package-name
```

Signatures are verified automatically using configured trusted public keys.

#### Viewing Build Logs

```bash
nix log --store 's3://my-nix-cache?endpoint=http://localhost:9000&region=us-east-1' \
        /nix/store/...-package-name
```

### Garbage Collection

niks3 implements reference-tracking garbage collection to clean up old closures and unreachable objects from the cache.

#### Running Garbage Collection

```bash
export NIKS3_SERVER_URL=http://server:5751
export NIKS3_AUTH_TOKEN_FILE=/path/to/token-file

# Delete closures older than 30 days
niks3 gc --older-than=720h
```

The GC process runs in three phases:

1. **Clean up failed uploads**: Removes incomplete uploads older than `--pending-older-than` (default: 6h)
1. **Delete old closures**: Removes closures older than `--older-than`
1. **Mark and delete orphaned objects**: Marks unreachable objects, then deletes them after a grace period

#### Statistics Output

The GC command logs detailed statistics:

```
INFO Starting garbage collection older-than=720h pending-older-than=6h force=false
INFO Garbage collection completed successfully failed-uploads-deleted=5 old-closures-deleted=142 objects-marked-for-deletion=1523 objects-deleted-after-grace-period=1520 objects-failed-to-delete=3
```

Statistics explained:

- **failed-uploads-deleted**: Number of incomplete/failed uploads cleaned up
- **old-closures-deleted**: Number of closures older than the threshold that were removed
- **objects-marked-for-deletion**: Number of unreachable objects marked as deleted (first phase)
- **objects-deleted-after-grace-period**: Number of objects actually removed from S3 and database after the grace period
- **objects-failed-to-delete**: Number of objects that couldn't be deleted from S3 and were marked active again

#### Grace Period

The grace period (default: same as `--pending-older-than`) prevents race conditions during concurrent uploads. Objects are marked for deletion first, then deleted only after the grace period has elapsed. This ensures that objects from in-flight uploads are not prematurely deleted.

#### Force Mode (Dangerous)

```bash
# WARNING: Immediate deletion without grace period
niks3 gc --older-than=720h --force
```

Force mode bypasses the grace period and deletes objects immediately. **Only use this when no uploads are in progress**, as it may delete objects that are currently being uploaded or referenced.

#### Automatic Garbage Collection (NixOS Module)

The NixOS module includes automatic garbage collection via a systemd timer:

```nix
{
  services.niks3 = {
    enable = true;
    # ... other configuration ...

    gc = {
      enable = true;           # Default: true
      olderThan = "720h";      # 30 days (default)
      schedule = "daily";      # Run at midnight daily (default)
      randomizedDelaySec = 1800; # Add 0-30 min random delay (default)
    };
  };
}
```

**Options:**

- `gc.enable`: Enable/disable automatic garbage collection (default: `true`)
- `gc.olderThan`: How old closures must be before deletion (default: `"720h"` = 30 days)
  - Examples: `"168h"` (7 days), `"2160h"` (90 days)
- `gc.failedUploadsOlderThan`: How old failed uploads must be before cleanup (default: `"6h"` = 6 hours)
  - Examples: `"12h"` (12 hours), `"24h"` (1 day)
- `gc.schedule`: When to run GC in systemd calendar format (default: `"daily"`)
  - Examples: `"weekly"`, `"*-*-* 02:00:00"` (daily at 2 AM), `"Sun *-*-* 03:00:00"` (Sundays at 3 AM)
- `gc.randomizedDelaySec`: Random delay in seconds before starting (default: `1800` = 30 minutes)
  - Helps distribute load across multiple instances

The automatic GC runs as a systemd service (`niks3-gc.service`) triggered by a timer (`niks3-gc.timer`). View logs with:

```bash
# View GC service logs
journalctl -u niks3-gc.service

# Check next scheduled run
systemctl list-timers niks3-gc.timer

# Run GC manually
systemctl start niks3-gc.service
```

## DB Migrations

We use [Goose].

Migrations are located in `pg/migrations`.

## SQL Querying

We use [sqlc] with [pgx].

Config is located at `sqlc.yml`. Re-generate using `sqlc generate`.

## Local Development Environment

Start the complete development environment with `nix run .#dev`.

This launches a process-compose setup with:

- **PostgreSQL**: Database server with automatic initialization and health checks
- **MinIO**: S3-compatible storage server with health checks
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

## Benchmarks

A benchmark for uploading a closure to S3 is available.

To run the benchmark:

```bash
cd server
go test -bench=BenchmarkPythonClosure -benchtime=3x -v
```

## Need commercial support or customization?

For commercial support, please contact [Mic92](https://github.com/Mic92/) at
joerg@thalheim.io or reach out to [Numtide](https://numtide.com/contact/).

[goose]: https://github.com/pressly/goose
[pgx]: https://github.com/jackc/pgx
[sqlc]: https://sqlc.dev/
