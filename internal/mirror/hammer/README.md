# Hammer: A load testing tool for tlog-mirror servers

This hammer sets up write traffic to a mirror to test correctness and performance under load.
The write traffic is sent according to the [tlog-mirror](https://c2sp.org/tlog-mirror) spec, and thus could be used to load test any tlog-mirror server.

The target mirror server must support `POST` requests to both `/add-checkpoint` and `/add-entries` paths.

## UI

The hammer runs using a text-based UI in the terminal that shows the current status, logs, and supports increasing/decreasing read and write traffic.
The process can be killed with <kbd>Ctrl</kbd>+<kbd>C</kbd>.
This TUI allows for a level of interactivity when probing a new configuration of a log in order to find any cliffs where performance degrades.

For real load-testing applications, especially headless runs as part of a CI pipeline, it is recommended to run the tool with `show_ui=false` in order to disable the UI.

## Usage

There are two modes in which the hammer can be run, in both modes the hammer continuously tracks a source log and replicates its data to one or more target mirrors.
The difference between the two modes is where the data for the source log originates:

1. **Standalone log**: The hammer creates and continuously populates its own local POSIX log.
2. **Replication**: The hammer reads from an existing log.

In both cases the hammer uses the data from the source log as the data to send to the mirror server.

### Mode 1: Standalone Hammer Log

First, store the hammer log test private key:

```shell
go run github.com/transparency-dev/witness/cmd/generate_keys@latest \
  --mldsa \
  --origin "example.com/log/testdata" \
  --out_priv /tmp/hammer-log.key \
  --out_pub /tmp/hammer-log.pub
```

Then, add the hammer log public key to the mirror server's configuration.

Example usage to test a deployment of [`cmd/mtc/mirror/posix`](/cmd/mtc/mirror/posix) server:

```shell
go run ./internal/mirror/hammer \
  --mirror_url=http://localhost:8080 \
  --storage_dir=/tmp/hammer-log \
  --log_private_key=/tmp/hammer-log.key \
  --num_writers=256 \
  --max_write_ops=42
```

For a headless write-only example that could be used for integration tests, this command attempts to write 2500 leaves within 1 minute.
If the target number of leaves is reached then it exits successfully.
If the timeout of 1 minute is reached first, then it exits with an exit code of 1.

```shell
go run ./internal/mirror/hammer \
  --mirror_url=http://localhost:8080 \
  --storage_dir=/tmp/hammer-log \
  --log_private_key=/tmp/hammer-log.key \
  --num_writers=512 \
  --max_write_ops=512 \
  --max_runtime=1m \
  --leaf_write_goal=2500 \
  --show_ui=false
```

### Mode 2: Replication

Determine the source log's public key and URL, and ensure that the target mirror is configured to accept data from this log.

Write the source log's `vkey` to a file, e.g. `/tmp/source-log.pub`.

Then start the hammer in replication mode, e.g.:

```shell
go run ./internal/mirror/hammer \
  --mirror_url=http://localhost:8080 \
  --source_log_url=https://example.com/source-log \
  --source_log_pubkey_path=/tmp/source-log.pub \
  --show_ui=false
```