# Tessera on POSIX Filesystems

This document describes the storage implementation for running Tessera on a POSIX-compliant filesystem.

## Overview

POSIX provides for a small number of atomic operations on compliant filesystems.

This design leverages those to safely maintain a Merkle tree log on disk, in a format
which can be exposed directly via a read-only endpoint to clients of the log (for example,
using `nginx` or similar).

In contrast with some of other storage backends, sequencing and integration of entries into
the tree is synchronous.

The implementation uses a `.state/` directory to coordinate operation.
This directory does _not_ need to be visible to log clients, but it does not contain sensitive
data and so it isn't a problem if it is made visible.

## Life of a Leaf

In the description below, when we talk about writing to files - either appending or creating new ones,
the _actual_ process used always follows the following pattern:
1. Create a temporary file on the same filesystem as the target location
1. If we're appending data, copy the contents of the prefix location into the temporary file
1. Write any new/additional data into the temporary file
1. Close the temporary file
1. Rename the temporary file into the target location.

The final step in the dance above is atomic according to the POSIX spec, so in performing this sequence
of actions we can avoid corrupt or partially written files being part of the tree.

1. Leaves are submitted by the binary built using Tessera via a call the storage's `Add` func.
1. The storage library batches these entries up in memory, and, after a configurable period of time has elapsed
   or the batch reaches a configurable size threshold, the batch is sequenced and appended to the tree:
   1. An advisory lock is taken on `.state/treeState.lock` file.
      This helps prevent multiple frontends from stepping on each other, but isn't necessary for safety.
   1. Flushed entries are assigned contiguous sequence numbers, and written out into entry bundle files.
   1. Integrate newly added leaves into Merkle tree, and write tiles out as files.
   1. Update `./state/treeState` file with the new size & root hash.
1. Asynchronously, at an interval determined by the `WithCheckpointInterval` option, the `checkpoint` file
will be updated:
   1. An advisory lock is taken on `.state/publish.lock`
   1. If the last-modified date of the `checkpoint` file is older than the checkpoint update interval,
      a new checkpoint which commits to the latest tree state is produced and written to the `checkpoint`
      file.

## Filesystems

This implementation has been somewhat tested on local `ext4` and `ZFS` filesystems, and on a distributed
[CephFS](https://docs.ceph.com/en/reef/cephfs/) instance on GCP, in all cases with multiple
personality binaries attempting to add new entries concurrently.

Other POSIX compliant filesystems such as `XFS` _should_ work, but filesystems which do not offer strong
POSIX compliance (e.g. `s3fs` or `NFS`) are unlikely to result in long term happiness.

If in doubt, tools like https://github.com/saidsay-so/pjdfstest may help in determining whether a given
filesystem is suitable.
