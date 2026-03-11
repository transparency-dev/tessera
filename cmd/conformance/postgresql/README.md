# Conformance PostgreSQL log

This binary runs an HTTP web server that accepts POST HTTP requests to an `/add` endpoint.
This endpoint takes arbitrary data and adds it to a PostgreSQL based Tessera log.

> [!WARNING]
> - This is an example and is not fit for production use, but demonstrates a way of using the Tessera Log with PostgreSQL storage backend.
> - This example is built on the [tlog tiles API](https://c2sp.org/tlog-tiles) for read endpoints and exposes a /add endpoint that allows any POSTed data to be added to the log.

## Bring up a log

This will help you bring up a PostgreSQL database to store a Tessera log, and start a personality
binary that can add entries to it.

You can run this personality using Docker Compose or manually with `go run`.

Note that all the commands are executed at the root directory of this repository.

### Docker Compose

#### Prerequisites

Install [Docker Compose](https://docs.docker.com/compose/install/).

#### Start the log

```sh
docker compose -f ./cmd/conformance/postgresql/docker/compose.yaml up
```

Add `-d` if you want to run the log in detached mode.

#### Stop the log

```sh
docker compose -f ./cmd/conformance/postgresql/docker/compose.yaml down
```

### Manual

#### Prerequisites

You need to have a PostgreSQL database configured to run on port `5432`, accepting
password auth for user `tessera` with the password set to `tessera`, and a database
called `tessera`.

You can start one using [Docker](https://docs.docker.com/engine/install/).

```sh
docker run --name test-postgresql -p 5432:5432 -e POSTGRES_USER=tessera -e POSTGRES_PASSWORD=tessera -e POSTGRES_DB=tessera -d postgres:17
```

#### Start the log

```sh
go run ./cmd/conformance/postgresql --pg_uri="postgres://tessera:tessera@localhost:5432/tessera?sslmode=disable" --init_schema_path="./storage/postgresql/schema.sql" --private_key_path="./cmd/conformance/postgresql/docker/testdata/key"
```

#### Stop the log

<kbd>Ctrl</kbd> <kbd>C</kbd>

## Add entries to the log

### Manually

Head over to the [codelab](../#codelab) to manually add entries to the log, and inspect the log.

### Using the hammer

In this example, we're running 256 writers against the log to add 1024 new leaves within 1 minute.

> [!NOTE]
> If you started the log via Docker Compose, use port `2025`. If you started manually with `go run`, use port `2024`.

```shell
go run ./internal/hammer \
  --log_public_key=transparency.dev/tessera/example+ae330e15+ASf4/L1zE859VqlfQgGzKy34l91Gl8W6wfwp+vKP62DW \
  --log_url=http://localhost:2025/ \
  --max_read_ops=0 \
  --num_writers=256 \
  --max_write_ops=256 \
  --max_runtime=1m \
  --leaf_write_goal=1024 \
  --show_ui=false
```

Optionally, inspect the log using the woodpecker tool to see the contents:

```shell
go run github.com/mhutchinson/woodpecker@main --custom_log_type=tiles --custom_log_url=http://localhost:2025/ --custom_log_vkey=transparency.dev/tessera/example+ae330e15+ASf4/L1zE859VqlfQgGzKy34l91Gl8W6wfwp+vKP62DW
```
