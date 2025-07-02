# Tessera on MySQL

This directory contains the implementation of a storage backend for Tessera using MySQL. This allows Tessera to leverage MySQL as its underlying database for storing checkpoint, entry hashes and data in tiles format.

## Design

See [MySQL storage design documentation](./DESIGN.md).

### Requirements

- A running MySQL server instance. This storage implementation has been tested against MySQL 8.4.

## Usage

### Constructing the Storage Object

Here is an example code snippet to initialise the MySQL storage in Tessera.

```go
import (
    "context"

    "github.com/transparency-dev/tessera"
    "github.com/transparency-dev/tessera/storage/mysql"
    "k8s.io/klog/v2"
)

func main() {
    mysqlURI := "user:password@tcp(db:3306)/tessera"
    db, err := sql.Open("mysql", mysqlURI)
    if err != nil {
        klog.Exitf("Failed to connect to DB: %v", err)
    }

    storage, err := mysql.New(ctx, db)
    if err != nil {
        klog.Exitf("Failed to create new MySQL storage: %v", err)
    }
}
```

### Example Personality

See [MySQL conformance example](/cmd/conformance/mysql/).

## Future Work

- [Separate sequencing and integration](https://github.com/transparency-dev/tessera/pull/282)
