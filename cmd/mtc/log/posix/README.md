# POSIX MTC Log

This directory contains an MTC ([`draft-ietf-plants-merkle-tree-certs`](https://ietf-plants-wg.github.io/merkle-tree-certs/draft-ietf-plants-merkle-tree-certs.html))
issuance log server backed by [Tessera's POSIX storage implementation](/storage/posix/).

This document contains [Documentation](#documentation) and a [Codelab](#codelab).

A matching POSIX Mirror implementation is available at [/cmd/mtc/mirror/posix](/cmd/mtc/mirror/posix).

> [!WARNING]
> This binary and the internal packages it uses are still under active
> development, and should be considered experimental and not
> production-ready. They remain outside the SemVer policy.

## Documentation

### Main functionalities

See [mtc/log/README.md](../README.md).

### API

#### HTTP Endpoints

The log server exposes the following HTTP endpoints:

- `POST /add-tbs`: Submits a JSON-encoded [`TBSCertificateLogEntry`](https://ietf-plants-wg.github.io/merkle-tree-certs/draft-ietf-plants-merkle-tree-certs.html#log-entries)
  to append to the log. Returns HTTP 201 Created with a JSON-encoded
  [`AddTBSRsp`](https://github.com/search?q=repo%3Atransparency-dev%2Ftessera+symbol%3AAddTBSRsp+path%3Amtc.go&type=code)
  containing the assigned entry `index` and a TLS-encoded [`MTCProof`](https://ietf-plants-wg.github.io/merkle-tree-certs/draft-ietf-plants-merkle-tree-certs.html#name-certificate-format),
  with subtree signatures.
- `GET /proof-to-landmark?index=<index>`: Fetches a landmark-relative
  [`MTCProof`](https://ietf-plants-wg.github.io/merkle-tree-certs/draft-ietf-plants-merkle-tree-certs.html#name-certificate-format)
  for the given entry index. Returns HTTP 200 OK with a [`ProofToLandmarkRsp`](https://github.com/search?q=repo%3Atransparency-dev%2Ftessera+symbol%3AProofToLandmarkRsp+path%3Amtc.go&type=code),
  containing a TLS-encoded [`MTCProof`](https://ietf-plants-wg.github.io/merkle-tree-certs/draft-ietf-plants-merkle-tree-certs.html#name-certificate-format),
  or HTTP 202 Accepted with a `Retry-After` header if an enclosing landmark has
  not been published yet.

#### Log data and Landmarks

Log data (checkpoints, tiles, leaves) and the `/landmarks` resource are
accessible through the underlying POSIX storage filesystem.

### Configuration

Inspect the [`main.go`](./main.go) file for a full list of flags.

Notable MTC-related flags are:

- `landmark_interval`: Interval between publishing landmarks. If 0, defaults
   to CQRP recommended interval for max_cert_lifetime.
- `ca_id`: The CA ID as per [draft-ietf-plants-merkle-tree-certs Section 5.1](https://ietf-plants-wg.github.io/merkle-tree-certs/draft-ietf-plants-merkle-tree-certs.html#name-certification-authority-ide)
  (e.g. 32473.106)
- `log_number`: The issuance log number (strictly positive).
- `private_key`: Location of private key file. If unset, uses the contents of
  the `LOG_PRIVATE_KEY` environment variable.
- `max_cert_lifetime`: Maximum validity duration allowed for submitted
  certificate entries.
- `mirror_policy`: File containing the mirror policy in tlog-policy format. If
  unset, no mirroring will be performed.

## Codelab

These instructions will help you bring up an MTC POSIX log, and send entries to
it using the [Hammer](../hammer/hammer.go). If you'd like, you can also run a
[Mirror](../../mirror/) alongside this log, and configure the log to use it.

### Keys

#### Log Key Pair
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

> [!WARNING]
> Ensure that these keys are stored somewhere safe, and not in a location
> which could accidentally be made public when exporting the log data.
> If you're running this server in production, this key MUST be HSM-backed as
> per Chrome's draft CQRP policy.

#### Mirror & Witness Cosigner Keys (Optional for Mirroring)
If you are running an MTC mirror alongside the log, you will also need a
separate cosigner key pairs for the mirror service:

```bash
go run github.com/transparency-dev/witness/cmd/generate_keys@main \
  --mldsa \
  --origin "oid/1.3.6.1.4.1.32473.312202" \
  --out_priv /tmp/mirror.sec \
  --out_pub /tmp/mirror.pub
```

### Running with an MTC Mirror

To run the log alongside an MTC mirror with a policy such that the log waits
for the mirror to catch up before publishing checkpoints:

#### 1. Create the Mirror Configuration
The mirror requires a configuration file identifying the log to accept requests
from:

```bash
cat <<EOF > /tmp/mirror_config
logs/v0

vkey $(cat /tmp/mtc.pub)
origin oid/1.3.6.1.4.1.32473.106.0.1
EOF
```

#### 2. Start the Mirror Server
Start the POSIX mirror server on port 6963:

```bash
go run ./cmd/mtc/mirror/posix \
  --listen_addr="localhost:6963" \
  --storage_dir="/tmp/mtcmirror" \
  --config_path="/tmp/mirror_config" \
  --mirror_cosigner_path=/tmp/mirror.sec \
  --slog_level=-4
```

#### 3. Create the Mirror Policy
The log uses a [c2sp.org/tlog-policy](https://c2sp.org/tlog-policy) file to
express the cosigner quorum to be reached. With this policy configured, the log
will send each new checkpoint and entries to the mirror. It will wait for its
cosignature before publishing a new checkpoint. It will also request a subtree
signature from them, which it includes in `AddTBS()` responses.

```bash
cat <<EOF > /tmp/mirror_policy
witness w1 $(cat /tmp/mirror.pub) http://localhost:6963/
group g1 all w1
quorum g1
EOF
```

#### 4. Start the Log Server with Mirror Policy
Start the log server configured with `--mirror_policy`:

```bash
go run ./cmd/mtc/log/posix \
  --listen_addr="localhost:6962" \
  --storage_dir="/tmp/mtclog" \
  --ca_id="32473.106" \
  --log_number=1 \
  --private_key=/tmp/mtc.sec \
  --mirror_policy="/tmp/mirror_policy" \
  --slog_level=-4
```

### Running Standalone (Without Mirror)

If running without a mirror or witness policy, start the log server with:

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
