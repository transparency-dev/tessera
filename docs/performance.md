# Tessera Storage Performance

Tessera is designed to scale to meet the needs of most currently envisioned workloads in a cost-effective manner.

All storage backends have been tested to meet the write-throughput of CT-scale loads without issue.
The read API of Tessera based logs scales extremely well due to the immutable resource based approach used, which allows for:
1. Aggressive caching to be applied, e.g. via CDN
2. Horizontal scaling of read infrastructure (e.g. object storage)[^1]

[^1]: The MySQL storage backend is different to the others in that reads must be served via the personality rather than directly,
      however, due to changes in how MySQL is used compared to Trillian v1, read performance should be far better, and _could_ still
      be scaled horizontally with additional MySQL read replicas & read-only personality instances.

Below are some indicative figures which show the rough scale of performance we've seen from deploying Tessera conformance
binaries in various environments.

## Performance factors

### Resources

Exact performance numbers are highly dependent on the exact infrastructure being used (e.g. storage type & locality, host resources
of the machine(s) running the personality binary, network speed and weather, etc.)  If in doubt, you should run your own performance
tests on infrastructure which is as close as possible to that which will ultimately be used to run the log in production.
The [conformance binaries](/cmd/conformance) and [hammer tool](/internal/hammer) are designed for this kind of performance testing.

### Antispam

Antispam is a feature which does best effort deduplication of incoming entries. While cheaper than _strong atomic_ deduplication would
be, it is still a somewhat expensive operation in terms of both storage and throughput.
Not all personality designs will require it, so Tessera is built such that you only incur these costs if they are necessary
for your design.

Leaving antispam disabled will greatly increase the throughput of the log, and decrease CPU and storage costs.


## Backends

The currently supported storage backends are listed below, with a rough idea of the expected performance figures.
Individual storage implementations may have more detailed information about performance in their respective directories.

### GCP

The main lever for cost vs performance on GCP is Spanner, in the form of "Performance Units" (PUs).
PUs can be allocated in blocks of 100, and 1000 PUs is equivalent to 1 Spanner Server.

The table below shows some rough numbers of measured performance:

| Spanner PUs | Num FEs | QPS no-antispam | QPS antispam |
|-------------|---------|--------------|-----------|
| 100         | 1       | > 3,000      | > 800     |
| 200         | 1       | not done     | > 1500    |
| 300         | 1       | not done     | > 3000    |
| 300         | 2       | not done     | > 5000    |


### POSIX

#### Local storage

A single local instance on an 12-core VM with 8GB of RAM writing to local filesystem stored on a mirrored pair of SAS disks.

Without antispam, it was able to sustain around 2900 writes/s.

```
┌────────────────────────────────────────────────────────────────────────────────────┐
│Read (8 workers): Current max: 20/s. Oversupply in last second: 0                   │
│Write (3000 workers): Current max: 3000/s. Oversupply in last second: 0             │
│TreeSize: 1470460 (Δ 2927qps over 30s)                                              │
│Time-in-queue: 136ms/1110ms/1356ms (min/avg/max)                                    │
│Observed-time-to-integrate: 583ms/6019ms/6594ms (min/avg/max)                       │
├────────────────────────────────────────────────────────────────────────────────────┤
```

With antispam enabled (badger), it was able to sustain around 1600 writes/s.

```
┌────────────────────────────────────────────────────────────────────────────────────┐
│Read (8 workers): Current max: 20/s. Oversupply in last second: 0                   │
│Write (1800 workers): Current max: 1800/s. Oversupply in last second: 0             │
│TreeSize: 2041087 (Δ 1664qps over 30s)                                              │
│Time-in-queue: 0ms/112ms/448ms (min/avg/max)                                        │
│Observed-time-to-integrate: 593ms/3232ms/5754ms (min/avg/max)                       │
├────────────────────────────────────────────────────────────────────────────────────┤
```


#### Network storage

A 4 node CephFS cluster (1 admin, 3x storage nodes) running on E2 nodes sustained > 1000qps of writes.

#### GCP Free Tier VM Instance

A small `e2-micro` free-tier VM is able to sustain > 1500 writes/s.

> [!NOTE]
> Virtual CPUs (vCPUs) in virtualized environments often share physical CPU cores with other vCPUs and introduce variability
> and potential performance impacts.

```
┌───────────────────────────────────────────────────────────────────────┐
│Read (184 workers): Current max: 0/s. Oversupply in last second: 0     │
│Write (600 workers): Current max: 1758/s. Oversupply in last second: 0 │
│TreeSize: 1882477 (Δ 1587qps over 30s)                                 │
│Time-in-queue: 149ms/371ms/692ms (min/avg/max)                         │
│Observed-time-to-integrate: 569ms/1191ms/1878ms (min/avg/max)          │
└───────────────────────────────────────────────────────────────────────┘
```

More details on Tessera POSIX performance can be found [here](/storage/posix/PERFORMANCE.md).


## MySQL

Figures below were measured using VMs on GCP in order to provide an idea of size of machine required to
achieve the results.

> [!NOTE]
> Note that for Tessera on GCP deployments, we **strongly recommended* using the Tessera GCP storage implementation instead.


### GCP free-tier + CloudSQL

Tessera running on an `e2-micro` free tier VM instance on GCP, using CloudSQL for storage can sustain around 2000 write/s.

```
┌───────────────────────────────────────────────────────────────────────┐
│Read (8 workers): Current max: 0/s. Oversupply in last second: 0       │
│Write (512 workers): Current max: 2571/s. Oversupply in last second: 0 │
│TreeSize: 2530480 (Δ 2047qps over 30s)                                 │
│Time-in-queue: 41ms/120ms/288ms (min/avg/max)                          │
│Observed-time-to-integrate: 568ms/636ms/782ms (min/avg/max)            │
└───────────────────────────────────────────────────────────────────────┘
```

### GCP free-tier VM only

Tessera + MySQL both running on an `e2-micro` free tier VM instance on GCP can sustain around 300 writes/s.

```
┌──────────────────────────────────────────────────────────────────────┐
│Read (8 workers): Current max: 0/s. Oversupply in last second: 0      │
│Write (256 workers): Current max: 409/s. Oversupply in last second: 0 │
│TreeSize: 240921 (Δ 307qps over 30s)                                  │
│Time-in-queue: 86ms/566ms/2172ms (min/avg/max)                        │
│Observed-time-to-integrate: 516ms/1056ms/2531ms (min/avg/max)         │
└──────────────────────────────────────────────────────────────────────┘
```

More details on Tessera MySQL performance can be found [here](/storage/mysql/PERFORMANCE.md).


## AWS

Coming soon.
