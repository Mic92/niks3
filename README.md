# GC server for Nix binary caches based on S3-compatible storage

Status: WIP, nothing works yet

The idea is to have all reads be handled by the s3 cache (which itself can be high-available)
and have a gc server that tracks all uploads to the cache and runs periodic GC on s3 cache.
Since writes to a binary cache are often not as critical as reads,
we can vastly simplify the operational complexity of the GC server, i.e. only
running one instance next to the CI infrastructure.

## DB Migrations

We use [Goose].

Migrations are located in `pg/migrations`.

## SQL Querying

We use [sqlc] with [pgx].

Config is located at `sqlc.yml`. Re-generate using `sqlc generate`.

## Local dev services

A `postgres` and `minio` service is available for local dev by running `nix run .#dev`.

It uses `process-compose`. Look in `.envrc` for some env variables that are related.

State is stored in `.data`. For a fresh local dev environment, delete `.data`.

[goose]: https://github.com/pressly/goose
[pgx]: https://github.com/jackc/pgx
[sqlc]: https://sqlc.dev/
