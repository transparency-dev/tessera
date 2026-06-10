# Tessera Pruning

Transparency logs are append-only data structures that grow indefinitely. For high-volume logs, such as Certificate Transparency, this can lead to significant storage costs and overhead for both operators and monitors.

The [tlog-tiles] specification introduces an optional mechanism to "prune" logs, allowing logs to make a prefix of the log unavailable while maintaining the Merkle tree structure and validity of existing proofs.

This document outlines the design for adding pruning support to Tessera, adhering to the principles of simplicity and low total cost of ownership (TCO).

## Spec Requirements & Ambiguities

The [tlog-tiles] spec (revision `1cd32db`) defines the rules for pruning but leaves operational details as an exercise for the implementation:

### What the Spec Defines
*   **Minimum Index**: The log maintains a minimum index which represents the inclusive lower bound of entries it makes available. Resources whose entries fall entirely below this bound may be pruned. This applies to entry bundles, and all tiles (level 0 and higher).
*   **Safety Recommendation**: The spec recommends denying HTTP requests for pruned resources rather than deleting them from storage, allowing for recovery and support for slow witnesses.
*   **Tree Invariants**: Pruning does not change the tree structure or invalidate existing proofs.

### Ambiguities and Gaps
*   **Discovery**: The spec has a `TODO` regarding how clients discover the current `minimum index`.
*   **Policy**: The spec does not define *when* a log should be pruned, deferring this to ecosystem-specific retention policies.

## Design

Tessera adopts a pragmatic, operator-driven approach to pruning, avoiding the need for complex runtime infrastructure on the read path.

### 1. Explicit Operator Action (CLI Tool)
Pruning is not an automated background process in Tessera. Instead, it is an explicit action performed by the operator using a CLI tool. This minimizes complexity in the core library and ensures operators are aware of the impact.

The tool performs the following steps:
1.  **Validation**: Ensures the new minimum index `N` is less than the current checkpoint size, greater than or equal to the current `min_index`, and enforces that `N` is a multiple of 256 (to align with tile boundaries).
2.  **State Commitment**: Writes the new `N` to a statically served `min_index` file at the root of the log.
3. **Archiving**: Identifies tiles and entry bundles whose entries fall entirely below `N` (i.e., their exclusive end index is `<= N`) and moves them to an archive location.

The operation is idempotent and can be safely rerun to continue a failed or interrupted operation.

### 2. State File (`min_index`)
To address the spec's ambiguity on discovery, Tessera introduces a static `min_index` file served at the root of the log (e.g., `<prefix>/min_index`), similar to the `checkpoint` file.
*   **Format**: A simple text file containing the decimal representation of the minimum index.
*   **Serving**: Clients can fetch this file to understand the current valid range of the log.

### 3. Archiving Strategy
To fulfill the spec's recommendation of not deleting data, the pruning tool moves files to an archive location while maintaining the exact same directory structure. This allows the archive to be used directly as a fallback log or for manual recovery.

*   **POSIX**: Files are moved to an archive directory that is not exposed by the web server serving the log. This directory would be flag-configured.
*   **Cloud (GCS/AWS)**: Files are moved to a separate, private bucket (e.g., `[log-bucket]-archive`). This ensures that direct reads to the public bucket return a native `404` without requiring a load balancer or proxy. See Alternatives Considered for an approach that archives within the same bucket.
*   **Ordering**: To maintain consistency with Tessera's existing Garbage Collection mechanism (which cleans up obsolete partial tiles), the tool should process resources in a bottom-up, left-to-right order: first entry bundles, then Level 0 tiles, and finally higher-level tiles. Within each resource type, it should proceed from the lowest index to the highest.

### 4. Safety Features
*   **Confirmation Prompt**: The tool scans the range to be pruned and prompts the operator with the count of files (tiles and entry bundles) to be moved before proceeding.
*   **Undo Command**: The tool provides an `undo` command that moves files back from the archive to the active area and restores the `min_index` file to its previous value (by storing the previous value in the archive during pruning), facilitating recovery from operator error. In the event of manual recovery, the operator can manually update the `min_index` file based on the data they have restored.

## Alternatives Considered

### Archive-in-Place (Renaming Files)
We considered keeping archived files in the same storage location (bucket or directory) but renaming them (e.g., appending `~` or moving to a sub-folder like `archive/` within the same bucket).

*   **Why it was not picked**: While this makes the "move" operation fast and atomic (metadata rename), it creates security risks for cloud storage users. If a GCS bucket is served publicly with uniform bucket-level access, everything in that bucket is public. Hiding an `archive/` folder or renamed files would require complex IAM conditions or putting a proxy/load balancer in front of the bucket. This violates Tessera's goal of zero-infrastructure read paths. Moving to a separate private bucket provides a much stronger security boundary with less operational complexity.

### Edge Workers on the Read Path
We considered using lightweight compute at the edge (e.g., Cloudflare Workers, Lambda@Edge) or CDN rules to intercept requests for tiles, check them against the current `min_index`, and return `404` for pruned tiles without moving or deleting the data.

*   **Why it was not picked**: This approach introduces significant infrastructure complexity. It requires logs to be served behind a Load Balancer or CDN rather than allowing direct access to object storage buckets (like GCS). This violates Tessera's goal of having "no additional services to manage" and maintaining a low TCO for operators.

### GCS Object Versioning
We considered relying on cloud-specific features like GCS Object Versioning. Under this model, the tool would simply delete the files from the public bucket. If Object Versioning is enabled, the deleted files would become noncurrent versions (returning `404` to public requests) and could be restored later.

*   **Why it was not picked**: 
    *   **Cost Inefficiency**: Object Versioning is not recommended for Tessera. Tessera writes many partial tiles and entry bundles during normal operation, and enabling Object Versioning would cause Tessera's existing Garbage Collection mechanism (which deletes obsolete partial tiles) to become ineffective at saving quota, as the deleted files would be retained as noncurrent versions.
    *   **Portability**: This approach relies on specific cloud provider features that do not translate well to simple POSIX filesystems or other cloud providers with different versioning semantics. The "move to archive" approach is universally applicable across all storage drivers supported by Tessera.

### GCS "Brave Mode" (Delete with Soft Delete)
We considered a mode for GCS deployments where the tool simply deletes the files from the public bucket instead of moving them to an archive. By relying on GCS's default **Soft Delete** feature, operators retain a time-limited recovery window (default 7 days) to undo errors, after which the files are permanently deleted.

*   **Why it was not picked**: While this avoids the cost and overhead of maintaining a perpetual archive bucket, it violates the spec's recommendation to not delete data from storage. Additionally, files retained by Soft Delete would still incur costs during the retention window, potentially interfering with the cost-saving goals of garbage collecting partial tiles (though only for a limited time).

[tlog-tiles]: https://c2sp.org/tlog-tiles
