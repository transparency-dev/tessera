# fsck

`fsck` is a simple tool for verifying the integrity of a [`tlog-tiles`](https://c2sp.org/tlog-tiles) log.

It is so-named as a nod towards the 'nix tools which perform a similar job for filesystems.
Note, however, that this tool is generally applicable for all tlog-tile instances accessible
via a HTTP, not just those which _happen_ to be backed by a POSIX filesystem.

## Usage

The tool is provided the URL of the log to check, and will attempt to re-derive 
the claimed root hash from the log's `checkpoint`, as well as the contents of all
tiles implied by the tree size it contains.

It can be run with the following command:

```bash
$ go run github.com/transparency-dev/tessera/cmd/fsck@main --storage_url=http://localhost:2024/ --public_key=tessera.pub
```

[!image](https://private-user-images.githubusercontent.com/7648032/490573648-7d06ffbb-acf6-4e2d-a1ef-7c848b58fb34.gif?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NTgxMjk1NjUsIm5iZiI6MTc1ODEyOTI2NSwicGF0aCI6Ii83NjQ4MDMyLzQ5MDU3MzY0OC03ZDA2ZmZiYi1hY2Y2LTRlMmQtYTFlZi03Yzg0OGI1OGZiMzQuZ2lmP1gtQW16LUFsZ29yaXRobT1BV1M0LUhNQUMtU0hBMjU2JlgtQW16LUNyZWRlbnRpYWw9QUtJQVZDT0RZTFNBNTNQUUs0WkElMkYyMDI1MDkxNyUyRnVzLWVhc3QtMSUyRnMzJTJGYXdzNF9yZXF1ZXN0JlgtQW16LURhdGU9MjAyNTA5MTdUMTcxNDI1WiZYLUFtei1FeHBpcmVzPTMwMCZYLUFtei1TaWduYXR1cmU9MjFhOGZiNDkxM2I5Zjg2ZmZmNjA0NjgzZWJmOTM5NTQ0MTEyYTQ0YzllZGFhZTAwYjc4ODcwZDYzYTUzYmRiNCZYLUFtei1TaWduZWRIZWFkZXJzPWhvc3QifQ.FqR5NAJBVQUE5ulyEi5s09GhrRerGBUMCmnN3eSjwoI)

Optional flags may be used to control the amount of parallelism used during the process, run the tool with `--help`
for more details.

