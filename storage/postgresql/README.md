# Tessera on PostgreSQL

This directory contains the implementation of a storage backend for Tessera using PostgreSQL. This allows Tessera to leverage PostgreSQL as its underlying database for storing checkpoint, entry hashes and data in tiles format.

## Requirements

- A running PostgreSQL server instance. This storage implementation has been tested against PostgreSQL 17.
- The [pgx](https://github.com/jackc/pgx) driver is used internally for native PostgreSQL protocol support.

## Usage

### Constructing the Storage Object

Here is an example code snippet to initialise the PostgreSQL storage in Tessera.

```go
import (
    "context"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/transparency-dev/tessera/storage/postgresql"
    "k8s.io/klog/v2"
)

func main() {
    ctx := context.Background()
    pgURI := "postgres://user:password@db:5432/tessera?sslmode=disable"

    pool, err := pgxpool.New(ctx, pgURI)
    if err != nil {
        klog.Exitf("Failed to create PostgreSQL pool: %v", err)
    }
    defer pool.Close()

    storage, err := postgresql.New(ctx, pool)
    if err != nil {
        klog.Exitf("Failed to create new PostgreSQL storage: %v", err)
    }
}
```

### Example Personality

See [PostgreSQL conformance example](/cmd/conformance/postgresql/).
