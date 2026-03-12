# Hammer: A load testing tool for Tessera logs

This hammer sets up read and (optionally) write traffic to a log to test correctness and performance under load.
The read traffic is sent according to the [tlog-tiles](https://github.com/C2SP/C2SP/blob/main/tlog-tiles.md) spec, and thus could be used to load test any tiles-based log, not just Tessera logs.

If write traffic is enabled, then the target log must support `POST` requests to a `/add` path.
A successful request MUST return an ASCII decimal number representing the index that has been assigned to the new value.

## UI

The hammer runs using a text-based UI in the terminal that shows the current status, logs, and supports increasing/decreasing read and write traffic.
The process can be killed with `<Ctrl-C>`.
This TUI allows for a level of interactivity when probing a new configuration of a log in order to find any cliffs where performance degrades.

For real load-testing applications, especially headless runs as part of a CI pipeline, it is recommended to run the tool with `show_ui=false` in order to disable the UI.

## Usage

Example usage to test a deployment of `cmd/conformance/posix`:

```shell
go run ./internal/hammer \
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
go run ./internal/hammer \
  --log_public_key=transparency.dev/tessera/example+ae330e15+ASf4/L1zE859VqlfQgGzKy34l91Gl8W6wfwp+vKP62DW \
  --log_url=http://localhost:2024 \
  --max_read_ops=0 \
  --num_writers=512 \
  --max_write_ops=512 \
  --max_runtime=1m \
  --leaf_write_goal=2500 \
  --show_ui=false
```

# Design

## Objective

Write a tool that can send write and read requests to a Tessera log in order to check the performance of writes and reads, and ensure that these logs are behaving correctly.

## Architecture

### Components

Interactions with the log are performed by different implementations of worker, that are managed by separate pools:
 - writer: adds new leaves to the tree using a `POST` request to an `/add` endpoint
 - full reader: reads all leaves from the tree, starting at 0 and fetching them all
 - random reader: reads leaves randomly within the size of the tree

All readers verify inclusion proofs against a common checkpoint, so it is cryptographically assured that they all see consistent views of the data.

An important point here is that readers exercise the Tessera log via standard tlog-tiles endpoints so will work for any deployment of that spec.
However, the writers exercise a `/add` endpoint that is not defined in any spec, and is simply a convenient endpoint added to the example applications to allow for this kind of testing.
A production log would be very unlikely to have such an endpoint.

The number of each of these workers, and the rate at which they work is configurable (both via flags and through the UI).
The number of workers is configured by increasing the size of the pool, which increases concurrency.
The amount of work to be performed in a given duration is controlled by a pair of throttles: one for read operations, and one for write operations.

Higher level components are:
  - Hammer: orchestrates the work and owns the worker pools
  - Hammer Analyser: consumes the results of the hammer and workers to determine throughput and system health
  - UI: displays the current state of the system and allows some control over the number of workers

The TUI is only intended to be used to interactively explore the capabilities of a log; actual load testing should be done headlessly.

## Deployment

There are 2 main modes that the hammer has been designed to run in:
  1. Headless, which has 2 main sub-modes:
     1. Goal-oriented: given both a timeout and a target number of entries to write, process exits successfully if enough entries can be _written_ to the log before the time expires, or otherwise an error code is returned
     1. Infinite: keeps running until killed in order to perform long-lived performance tests
  1. TUI: runs in the console with a Text UI that allows some interactivity to tune the load characteristics and see the results

