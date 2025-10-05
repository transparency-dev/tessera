# POSIX one-shot CLI

`posix-oneshot` is a command line tool to add entries to a log stored on the local filesystem.

## Example usage

The commands below create a new log and add entries to it, and then show a few approaches to inspect the contents of the log.

```shell
# Set the keys via environment variables
export LOG_PRIVATE_KEY="PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"
export LOG_PUBLIC_KEY="example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx"

# Create files containing new leaves to add
mkdir /tmp/stuff
echo "foo" > /tmp/stuff/foo
echo "bar" > /tmp/stuff/bar
echo "baz" > /tmp/stuff/baz

# Make a blank dir where we'll store the log data
export LOG_DIR="/tmp/tlog"
mkdir "$LOG_DIR"

# Integrate all of these leaves into the tree
go run ./cmd/examples/posix-oneshot --storage_dir=${LOG_DIR} --entries="/tmp/stuff/*"

# Check that the checkpoint is of the correct size and the leaves are present
cat ${LOG_DIR}/checkpoint
cat ${LOG_DIR}/tile/entries/000.p/*

# Optionally, inspect the log using the woodpecker tool to see the contents
go run github.com/mhutchinson/woodpecker@main --custom_log_type=tiles --custom_log_url=file:///${LOG_DIR}/ --custom_log_origin=example.com/log/testdata --custom_log_vkey=${LOG_PUBLIC_KEY}

# More entries can be added to the log using the following:
go run ./cmd/examples/posix-oneshot --storage_dir=${LOG_DIR} --entries="/tmp/stuff/*"
```

## Using the log

A POSIX log can be used directly via file paths, but a more common approach to using such a log is to use static file hosting to do this.
A couple of approaches are:
 - [NGINX](https://docs.nginx.com/nginx/admin-guide/web-server/serving-static-content/)
 - [GitHub](https://docs.github.com/en/repositories/working-with-files/using-files/viewing-a-file) - the files can be accessed via the raw URLs
