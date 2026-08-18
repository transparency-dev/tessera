# POSIX MTC Log

This directory contains an MTC (`draft-ietf-plants-merkle-tree-certs`) issuance
log server backed by Tessera's POSIX storage implementation.

> [!WARNING] This binary and the internal packages it uses are still under
> active development, and should be considered experimental and not
> production-ready. They remain outside the SemVer policy.

## Running

### Keys

You will need an ML-DSA key pair to be used to sign log checkpoints. You can
generate ML-DSA key pairs in the correct `vkey` format with the
[`generate_keys`](https://github.com/transparency-dev/witness/blob/main/cmd/generate_keys/main.go)
command from the [witness](https://github.com/transparency-dev/witness) repo.

The command below will generate such a key pair, with an origin of
"oid/1.3.6.1.4.1.32473.106", writing the public and private keys out to
`/tmp/mtc.pub` and `/tmp/mtc.sec` respectively:

```bash
go run github.com/transparency-dev/witness/cmd/generate_keys@main \
  --mldsa \
  --origin "oid/1.3.6.1.4.1.32473.106" \
  --out_priv /tmp/mtc.sec \
  --out_pub /tmp/mtc.pub
```

> [!WARNING] Ensure that these keys are stored somewhere safe, and not in a
> location which could accidentally be made public when exporting the log data.
> If you're running this server in production, this key MUST be HSM-backed as
> per Chrome's draft CQRP policy.

### Starting the log server

An example command for starting the server is given below:

```bash
go run ./cmd/mtc/log/posix \
  --listen_addr="localhost:6962" \
  --storage_dir="/tmp/mtclog" \
  --ca_id="32473.106" \
  --log_number=1 \
  --private_key=/tmp/mtc.sec \
  --slog_level=-4
```

### Hammer

In a different terminal, start the hammer:

```bash
go run ./cmd/mtc/log/hammer \
  --log_url=file:///tmp/mtclog \
  --write_log_url=http://localhost:6962/ \
  --ca_id="32473.106" \
  --log_number=1 \
  --log_public_key=$(cat /tmp/mtc.pub) \
  --max_read_ops=10 \
  --num_readers_random=2 \
  --num_readers_full=2 \
  --num_writers=50 \
  --max_write_ops=50 \
  --leaf_write_goal=1000 \
  --show_ui=true
```
