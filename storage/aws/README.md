# AWS Design

This document describes how the storage implementation for running Tessera on Amazon Web Services
is intended to work.

## Overview

This design takes advantage of S3 for long term storage and low cost & complexity serving of read traffic,
but leverages something more transactional for coordinating writes.

New entries flow in from the binary built with Tessera into transactional storage, where they're held
temporarily to batch them up, and then assigned sequence numbers as each batch is flushed.
This allows the `Add` API call to quickly return with *durably assigned* sequence numbers.

From there, an async process derives the entry bundles and Merkle tree structure from the sequenced batches,
writes these to GCS for serving, before finally removing integrated bundles from the transactional storage.

Since entries are all sequenced by the time they're stored, and sequencing is done in "chunks", it's worth
noting that all tree derivations are therefore idempotent.

## Transactional storage

The transactional storage is implemented with Aurora MySQL, and uses a schema with 3 tables:

### `SeqCoord`
A table with a single row which is used to keep track of the next assignable sequence number.

### `Seq`
This holds batches of entries keyed by the sequence number assigned to the first entry in the batch.

### `IntCoord`
This table is used to coordinate integration of sequenced batches in the `Seq` table, and keep track of the current tree state.

## Life of a leaf

1. Leaves are submitted by the binary built using Tessera via a call the storage's `Add` func.
1. The storage library batches these entries up, and, after a configurable period of time has elapsed
   or the batch reaches a configurable size threshold, the batch is written to the `Seq` table which effectively
   assigns a sequence numbers to the entries using the following algorithm:
   In a transaction:
   1. selects next from `SeqCoord` with for update ← this blocks other FE from writing their pools, but only for a short duration.
   1. Inserts batch of entries into `Seq` with key `SeqCoord.next`
   1. Update `SeqCoord` with `next+=len(batch)`
1. Integrators periodically integrate new sequenced entries into the tree:
   In a transaction:
   1. select `seq` from `IntCoord` with for update ← this blocks other integrators from proceeding.
   1. Select one or more consecutive batches from `Seq` for update, starting at `IntCoord.seq`
   1. Write leaf bundles to S3 using batched entries
   1. Integrate in Merkle tree and write tiles to S3
   1. Update checkpoint in S3
   1. Delete consumed batches from `Seq`
   1. Update `IntCoord` with `seq+=num_entries_integrated` and the latest `rootHash`
1. Checkpoints representing the latest state of the tree are published at the configured interval.

## Dedup

Two experimental implementations have been tested which uses either Aurora MySQL,
or a local bbolt database to store the `<identity_hash>` --> `sequence` mapping.
They work well, but call for further stress testing and cost analysis.

### Alternatives considered

Other transactional storage systems are available on AWS, e.g. Redshift, RDS or
DynamoDB. Experiments were run using Aurora (MySQL, Serverless v2), RDS (MySQL),
and DynamoDB.

Aurora (MySQL) worked out to be a good compromise between cost, performance,
operational overhead, code complexity, and so was selected.

The alpha implementation was tested with entries of size 1KB each, at a write
rate of 1500/s. This was done using the smallest possible Aurora instance
available, `db.r5.large`, running `8.0.mysql_aurora.3.05.2`.

Aurora (Serverless v2) worked out well, but seems less cost effective than
provisioned Aurora for sustained traffic. For now, we decided not to explore this option further.

RDS (MySQL) worked out well, but requires more administrative overhead than
Aurora. For now, we decided not to explore this option further. 

DynamoDB worked out to be less cost efficient than Aurora and RDS. It also has
constraints that introduced a non trivial amount of complexity: max object size
is 400KB,  max transaction size is {4MB OR 25 rows for write OR 100 rows for
reads}, binary values must be base64 encoded, arrays of bytes are marshaled as
sets by default (as of Dec. 2024). We decided not to explore this option further.
