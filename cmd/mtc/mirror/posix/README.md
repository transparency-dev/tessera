# POSIX MTC Mirror

This directory contains an MTC mirror server backed by Tessera's POSIX storage implementation.
The server's HTTP API is intended to comply with the [`tlog-mirror`](https://c2sp.org/tlog-mirror)
specification.

A `tlog-mirror` is both:
- a regular `tlog-witness` server which can optionally be configured to sign consistent log checkpoints
- an additional "extended" `tlog-witness` which will sign a consistent checkpoint only once it has _also_
  stored a copy of the log content which that checkpoint commits to.

> [WARNING]
> This binary and the internal packages it uses are still under active development, and should be
> considered experimental and not production-ready. They remain outside the SemVer policy.

## Storage overview

Since this server is backed by Tessera's POSIX storage implementation, all data is stored in files
under a directory the operator provides via a flag.

The directory layout is as follows:
```
${ROOT}
├── private                                    # Private state of the mirror, not to be shared.
│   │
│   └── witness                                # State for the integrated Tessera witness used by the
│                                              # `add-checkpoint` endpoint.
│
└── public                                     # Publicly accessible state for the mirror.
    │
    └── mirrors                                # Mirrored log data root. This directory should be made
        │                                      # publicly accessible at the mirror's monitoring prefix.
        │                                      # Individual mirrored logs are stored in their own subdirectories.
        │
        ├── 2ee6a567373ee0c7d6783ae6633e61...  # Each subdirectory is named as the lower-cased SHA256 hash of the
        ├── cf89f0e5f78e09351f0fa84efa5bc3...  # corresponding log origin.
        └── ...
```

It is strongly recommended that each individual mirror's log data directory (i.e. those _under_ `public/mirrors/`)
be its own filesystem mountpoint, with ZFS being the recommended filesystem. This requires a small amount of
effort to set up per-mirror, but has a number of benefits:
  1. Disk resources are clearly delineated between mirror instances, and can be reallocated as needed.
  2. It's easy to place individual mirrors on different storage devices/volumes.
  3. When the mirror of a log is no longer required, e.g. because a source log has been retired and the mirror's
     copy is no longer useful, the filesystem containing that mirror's data can be unmounted and its storage
     reclaimed much quicker and more cheaply compared to attempting to delete the directory hierarchy.

> [NOTE]
> Feedback is very welcome, and we'd particularly love to hear opinions from potential operators on operational
> characteristics such as the storage details outlined above.

## Running

A quick getting-started guide is provided below. 

### Keys

You will need:
1. An MLDSA key to be used to cosign fully mirrored checkpoints.
2. Optionally, if you want your MTC mirror to also function as a "regular" witness, an additional MLDSA key to be
   used to cosign those checkpoints.

You can generate MLDSA key pairs in the correct `vkey` format with the
[`generate_keys`](https://github.com/transparency-dev/witness/blob/main/cmd/generate_keys/main.go) command from the
`[witness](https://github.com/transparency-dev/witness) repo.

The command below will generate such a key pair, with an origin of "mirror.example.com/mtctest", writing the public
and private keys out to `mirror.pub` and `mirror.sec` respectively:

```bash
$ go run github.com/transparency-dev/witness/cmd/generate_keys@main \
    --mldsa \
    --origin "mirror.example.com/mtctest" \
    --out_priv mirror.sec \
    --out_pub mirror.pub
```

> [WARNING]
> Ensure that these keys are stored somewhere safe, and not in a location which could accidentally be made
> public when exporting the mirrored log data!

### Configure log access

Currently, deciding which logs are permitted to be pushed to the mirror is done with a simple JSON config file.
An example file is provided as [`example_config.json`](./example_config.json).

Edit this or create your own with appropriate entries. Then, place the config file in a suitable location.

### Starting the mirror server

An example command for starting the server is given below:

```bash
$ go run ./cmd/mtc/mirror/posix \
    --storage_dir="/tmp/mirrorroot" \
    --mirror_cosigner_path=./mirror.sec \
    --config_path=cmd/mtc/mirror/posix/example_config.json \
    --slog_level=-9
```
