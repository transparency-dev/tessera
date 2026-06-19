# Hammer: A load testing tool for tlog-mirror servers

This hammer sets up write traffic to a mirror to test correctness and performance under load.
The write traffic is sent according to the [tlog-mirror](https://c2sp.org/tlog-mirror) spec, and thus could be used to load test any tlog-mirror server.

The target log must support `POST` requests to both `/add-checkpoint` and `add-entries` paths.

## UI

The hammer runs using a text-based UI in the terminal that shows the current status, logs, and supports increasing/decreasing read and write traffic.
The process can be killed with `<Ctrl-C>`.
This TUI allows for a level of interactivity when probing a new configuration of a log in order to find any cliffs where performance degrades.

For real load-testing applications, especially headless runs as part of a CI pipeline, it is recommended to run the tool with `show_ui=false` in order to disable the UI.

## Usage

Example usage to test a deployment of `cmd/conformance/posix`:

```shell
go run ./internal/mirror/hammer \
  --log_public_key=transparency.dev/tessera/example+ae330e15+ASf4/L1zE859VqlfQgGzKy34l91Gl8W6wfwp+vKP62DW \
  --log_url=http://localhost:2024 \
  --max_read_ops=1024 \
  --num_readers_random=128 \
  --num_readers_full=128 \
  --num_writers=256 \
  --max_write_ops=42
```

For a headless write-only example that could be used for integration tests, this command attempts to write 2500 leaves within 1 minute.
If the target number of leaves is reached then it exits successfully.
If the timeout of 1 minute is reached first, then it exits with an exit code of 1.

```shell
go run ./internal/mirror/hammer \
  --log_public_key=transparency.dev/tessera/example+ae330e15+ASf4/L1zE859VqlfQgGzKy34l91Gl8W6wfwp+vKP62DW \
  --log_url=http://localhost:2024 \
  --max_read_ops=0 \
  --num_writers=512 \
  --max_write_ops=512 \
  --max_runtime=1m \
  --leaf_write_goal=2500 \
  --show_ui=false
```
