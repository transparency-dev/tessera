# Tessera on MySQL

This directory originally contained the outline of a MySQL-based storage implementation.

This implementation was removed after the
[v1.0.2](https://github.com/transparency-dev/tessera/releases/tag/v1.0.2) release for a few reasons:
  * Primarily, it implements a pattern where the _read path_ of the log is still served directly
    from the same database that is also concerned with the coordination and write-throughput of the log.
    This has been the source of performance and scalability issues in previous t-log implementations.
  * Despite early initial interest from folks we polled prior to implementing, the completion of the MySQL
    support has not actually been requested nor, as far as we're aware, the current implementation deployed.

Alternative options include:
  * The "AWS" implementation, which was designed to run with any MySQL-compatible DBMS and any S3-compatible
    object storage system.
  * The POSIX implementation, which was designed to be as easy as possible to run while still being production-ready.

If these don't work, please get in touch/file an issue.
