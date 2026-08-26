# MTC Log Package

Package `log` provides the core logic and Go API for an MTC
([`draft-ietf-plants-merkle-tree-certs`](https://datatracker.ietf.org/doc/html/draft-ietf-plants-merkle-tree-certs))
issuance log server using [c2sp.org/mtc-tlog](https://c2sp.org/mtc-tlog). It
does not implement ACME-related features, nor any CA business logic.

It is meant to be used with a Tessera log. At the moment, it only works with a
[POSIX storage backend](/storage/posix/).

## Functionalities

This library:
 - Logs
   [`TBSCertificateLogEntry`](https://ietf-plants-wg.github.io/merkle-tree-certs/draft-ietf-plants-merkle-tree-certs.html#log-entries)
   and returns a corresponding
   [`MTCProof`](https://ietf-plants-wg.github.io/merkle-tree-certs/draft-ietf-plants-merkle-tree-certs.html#name-certificate-format)
   including cosignatures.
 - Pushes entries to mirrors and serves checkpoints with their cosignatures.
 - Publishes active [landmarks](https://ietf-plants-wg.github.io/merkle-tree-certs/draft-ietf-plants-merkle-tree-certs.html#section-6.4.3).
 - Builds inclusion proofs to active landmark sizes.


## API Surface

### Configuration

- **`NewMTCLog(ctx, appender, opts)`**: Initializes an `MTCLog` instance backed
  by a Tessera appender and configured with [`*Options`](./mtc.go).
- **`NewOptions()`**: Creates an `Options` builder to configure the landmarks
  storage backends, landmark intervals, maximum certificate lifetime, signers,
  and subtree witness groups. Default options can be customized with
  corresponding `With*` methods defined in [`mtc.go`](./mtc.go), such as
  `WithMaxCertLifetime`.

### MTC APIs

- **`AddTBS(ctx, tbs)`**: Validates and appends a [`TBSCertificateLogEntry`](https://ietf-plants-wg.github.io/merkle-tree-certs/draft-ietf-plants-merkle-tree-certs.html#log-entries)
  to the log. Returns an [`AddTBSRsp`](https://github.com/search?q=repo%3Atransparency-dev%2Ftessera+symbol%3AAddTBSRsp+path%3Amtc.go&type=code)
  containing the assigned leaf `Index` and a serialized [`MTCProof`](https://ietf-plants-wg.github.io/merkle-tree-certs/draft-ietf-plants-merkle-tree-certs.html#name-certificate-format)
  (with subtree signatures) to construct a standalone certificate.
- **`ProofToLandmark(ctx, index)`**: Generates a TLS-encoded landmark-relative
  [`MTCProof`](https://ietf-plants-wg.github.io/merkle-tree-certs/draft-ietf-plants-merkle-tree-certs.html#name-certificate-format)
  for the given entry index. If the enclosing landmark is still pending
  publication, returns a retry duration.

### Reads

- Log data is served as a [tlog-tiles](https://c2sp.org/tlog-tiles) log,
  through the APIs of the Tessera storage driver used.
- Landmarks are served through the same read APIs, at `/landmarks`.
