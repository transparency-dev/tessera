# conformance-posix
This binary runs an HTTP web server that accepts POST HTTP requests to an `/add` endpoint.
This endpoint takes arbitrary data and adds it to a file-based log.

## Bring up a log
This will create a directory in your filesystem to store a log, and start a personality binary
that can add entries to this log.

First, define a few environment vaiables:

```shell
export LOG_PRIVATE_KEY="PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"
export LOG_PUBLIC_KEY="example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx"
export LOG_DIR=/tmp/mylog
```

Then, start the personality:

```shell
go run ./cmd/conformance/posix \
  --storage_dir=${LOG_DIR} \
  --listen=:2025 \
```

## Add entries to the log
### Manually
Head over to the [codelab](../#codelab) to manually add entries to the log, and inspect the log.

### Using the hammer
In another terminal, run the [hammer](./internal/hammer) against the log.
In this example, we're running 32 writers against the log to add 128 new leaves within 1 minute.

```shell
go run ./internal/hammer \
  --log_public_key=example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx \
  --log_url=http://localhost:2025 \
  --max_read_ops=0 \
  --num_writers=32 \
  --max_write_ops=64 \
  --max_runtime=1m \
  --leaf_write_goal=128 \
  --show_ui=false
```

Optionally, inspect the log on the filesystem using the woodpecker tool to see the contents.
Note that this reads only from the files on disk, so none of the commands above need to be running for this to work.

```shell
go run github.com/mhutchinson/woodpecker@main \
  --custom_log_type=tiles \
  --custom_log_url=file:///${LOG_DIR}/ \
  --custom_log_origin=example.com/log/testdata \
  --custom_log_vkey=${LOG_PUBLIC_KEY}
```
