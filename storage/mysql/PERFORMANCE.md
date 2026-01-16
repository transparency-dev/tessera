# MySQL Performance

> [!TIP]
> The performance test result shows that Tessera can run on the free tier VM instance on GCP.

### GCP Free Tier VM Instance

**tl;dr:** Tessera (MySQL) can run on the free tier VM instance on GCP with around **300 write QPS**. The bottleneck comes from the lack of memory which is consumed by the dockerized MySQL instance.

**e2-micro**

- vCPU: 0.25-2 vCPU (1 shared core)
- Memory: 1 GB
- OS: Debian GNU/Linux 12 (bookworm)

> [!NOTE]
> Virtual CPUs (vCPUs) in virtualized environments often share physical CPU cores with other vCPUs and introduce variability and potential performance impacts.

#### Result

```
┌──────────────────────────────────────────────────────────────────────┐
│Read (8 workers): Current max: 0/s. Oversupply in last second: 0      │
│Write (256 workers): Current max: 409/s. Oversupply in last second: 0 │
│TreeSize: 240921 (Δ 307qps over 30s)                                  │
│Time-in-queue: 86ms/566ms/2172ms (min/avg/max)                        │
│Observed-time-to-integrate: 516ms/1056ms/2531ms (min/avg/max)         │
└──────────────────────────────────────────────────────────────────────┘
```

The bottleneck is at the dockerized MySQL instance, which consumes around 50% of the memory.

```
top - 20:07:16 up 9 min,  3 users,  load average: 0.55, 0.56, 0.29
Tasks: 103 total,   1 running, 102 sleeping,   0 stopped,   0 zombie
%Cpu(s):  3.5 us,  1.7 sy,  0.0 ni, 89.9 id,  2.9 wa,  0.0 hi,  2.0 si,  0.0 st 
MiB Mem :    970.0 total,     74.5 free,    932.7 used,     65.2 buff/cache     
MiB Swap:      0.0 total,      0.0 free,      0.0 used.     37.3 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
   1770 root      20   0 1231828  22808      0 S   8.6   2.3   0:18.35 conformance-mys
   1140 999       20   0 1842244 493652      0 S   4.0  49.7   0:13.93 mysqld
```

#### Steps

1. Create a [GCP free tier](https://cloud.google.com/free/docs/free-cloud-features#free-tier) e2-micro VM instance in us-central1 (Iowa).

1. [Install Go](https://go.dev/doc/install)
   
   ```sh
   instance:~$ wget https://go.dev/dl/go1.23.0.linux-amd64.tar.gz
   instance:~$ sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.23.0.linux-amd64.tar.gz
   instance:~$ export PATH=$PATH:/usr/local/go/bin
   instance:~$ go version
   go version go1.23.0 linux/amd64
   ```

1. [Install Docker using the `apt` repository](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository)

1. Install Git

   ```sh
   instance:~$ sudo apt-get install git -y -q
   ...
   instance:~$ git version
   git version 2.39.2
   ```

1. Clone the Tessera repository

   ```sh
   instance:~$ git clone https://github.com/transparency-dev/tessera.git
   Cloning into 'tessera'...
   ```

1. Run `cmd/conformance/mysql` and MySQL database via Docker compose

   ```sh
   instance:~/tessera$ sudo docker compose -f ./cmd/conformance/mysql/docker/compose.yaml up
   ```

1. Run `hammer` and get performance metrics

   ```sh
   hammer:~/tessera$ go run ./internal/hammer --log_public_key=transparency.dev/tessera/example+ae330e15+ASf4/L1zE859VqlfQgGzKy34l91Gl8W6wfwp+vKP62DW --log_url=http://10.128.0.3:2024 --max_read_ops=0 --num_writers=512 --max_write_ops=512
   ```

### GCP Free Tier VM Instance + Cloud SQL (MySQL)

**tl;dr:** Tessera (MySQL) can run on the free tier VM instance on GCP with around **2000 write QPS** when the MySQL database is run on Cloud SQL.

**e2-micro**

- vCPU: 0.25-2 vCPU (1 shared core)
- Memory: 1 GB
- OS: Debian GNU/Linux 12 (bookworm)

> [!NOTE]
> Virtual CPUs (vCPUs) in virtualized environments often share physical CPU cores with other vCPUs and introduce variability and potential performance impacts.

**Cloud SQL (MySQL 8.0.31)**

- vCPUs: 4
- Memory: 7.5 GB
- SSD storage: 10 GB

#### Result

```
┌───────────────────────────────────────────────────────────────────────┐
│Read (8 workers): Current max: 0/s. Oversupply in last second: 0       │
│Write (512 workers): Current max: 2571/s. Oversupply in last second: 0 │
│TreeSize: 2530480 (Δ 2047qps over 30s)                                 │
│Time-in-queue: 41ms/120ms/288ms (min/avg/max)                          │
│Observed-time-to-integrate: 568ms/636ms/782ms (min/avg/max)            │
└───────────────────────────────────────────────────────────────────────┘
```

The bottleneck comes from CPU usage of the `cmd/conformance/mysql` binary on the free tier VM instance. The Cloud SQL (MySQL) CPU usage is lower than 10%.

#### Steps

1. Create a MySQL instance on Cloud SQL.

1. Create a [GCP free tier](https://cloud.google.com/free/docs/free-cloud-features#free-tier) e2-micro VM instance in us-central1 (Iowa).

1. Setup VPC peering.

1. [Install Go](https://go.dev/doc/install)
   
   ```sh
   instance:~$ wget https://go.dev/dl/go1.23.0.linux-amd64.tar.gz
   instance:~$ sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.23.0.linux-amd64.tar.gz
   instance:~$ export PATH=$PATH:/usr/local/go/bin
   instance:~$ go version
   go version go1.23.0 linux/amd64
   ```

1. Install Git

   ```sh
   instance:~$ sudo apt-get install git -y -q
   ...
   instance:~$ git version
   git version 2.39.2
   ```

1. Clone the Tessera repository

   ```sh
   instance:~$ git clone https://github.com/transparency-dev/tessera.git
   Cloning into 'tessera'...
   ```

1. Run `cloud-sql-proxy`

   ```sh
   instance:~$ ./cloud-sql-proxy --port 3306 transparency-dev-playground:us-central1:mysql-dev-instance-1
   ```

1. Run `cmd/conformance/mysql`

   ```sh
   instance:~/tessera$ go run ./cmd/conformance/mysql --mysql_uri="root:root@tcp(127.0.0.1:3306)/test_tessera" --init_schema_path="./storage/mysql/schema.sql" --private_key_path="./cmd/conformance/mysql/docker/testdata/key" --db_max_open_conns=1024 --db_max_idle_conns=512
   ```

1. Run `hammer` and get performance metrics

   ```sh
   hammer:~/tessera$ go run ./internal/hammer --log_public_key=transparency.dev/tessera/example+ae330e15+ASf4/L1zE859VqlfQgGzKy34l91Gl8W6wfwp+vKP62DW --log_url=http://10.128.0.3:2024 --max_read_ops=0 --num_writers=512 --max_write_ops=512
   ```

## POSIX

### GCP Free Tier VM Instance

**e2-micro**

- vCPU: 0.25-2 vCPU (1 shared core)
- Memory: 1 GB
- OS: Debian GNU/Linux 12 (bookworm)

> [!NOTE]
> Virtual CPUs (vCPUs) in virtualized environments often share physical CPU cores with other vCPUs and introduce variability and potential performance impacts.

#### Result

```
┌───────────────────────────────────────────────────────────────────────┐
│Read (184 workers): Current max: 0/s. Oversupply in last second: 0     │
│Write (600 workers): Current max: 1758/s. Oversupply in last second: 0 │
│TreeSize: 1882477 (Δ 1587qps over 30s)                                 │
│Time-in-queue: 149ms/371ms/692ms (min/avg/max)                         │
│Observed-time-to-integrate: 569ms/1191ms/1878ms (min/avg/max)          │
└───────────────────────────────────────────────────────────────────────┘
```

```
top - 20:45:35 up 47 min,  3 users,  load average: 1.89, 0.88, 1.03
Tasks:  97 total,   1 running,  96 sleeping,   0 stopped,   0 zombie
%Cpu(s): 47.2 us, 24.7 sy,  0.0 ni,  0.0 id,  0.0 wa,  0.0 hi, 28.1 si,  0.0 st 
MiB Mem :    970.0 total,    158.7 free,    566.8 used,    409.3 buff/cache     
MiB Swap:      0.0 total,      0.0 free,      0.0 used.    403.2 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
   8336 root      20   0 1231800  34784   5080 S 200.0   3.5   2:59.50 conformance-pos
    409 root      20   0 2442648  79112  26836 S   1.0   8.0   0:49.10 dockerd
   6848 root      20   0 1800176  34376  16940 S   0.7   3.5   0:12.57 docker-compose
```

#### Steps

1. Create a [GCP free tier](https://cloud.google.com/free/docs/free-cloud-features#free-tier) e2-micro VM instance in us-central1 (Iowa).

1. [Install Go](https://go.dev/doc/install)
   
   ```sh
   instance:~$ wget https://go.dev/dl/go1.23.0.linux-amd64.tar.gz
   instance:~$ sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.23.0.linux-amd64.tar.gz
   instance:~$ export PATH=$PATH:/usr/local/go/bin
   instance:~$ go version
   go version go1.23.0 linux/amd64
   ```

1. [Install Docker using the `apt` repository](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository)

1. Install Git

   ```sh
   instance:~$ sudo apt-get install git -y -q
   ...
   instance:~$ git version
   git version 2.39.2
   ```

1. Clone the Tessera repository

   ```sh
   instance:~$ git clone https://github.com/transparency-dev/tessera.git
   Cloning into 'tessera'...
   ```

1. Run `cmd/conformance/posix` via Docker compose

   ```sh
   instance:~/tessera$ sudo docker compose -f ./cmd/conformance/posix/docker/compose.yaml up
   ```

1. Run `hammer` and get performance metrics

   ```sh
   hammer:~/tessera$ go run ./internal/hammer --log_public_key=example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx --log_url=http://localhost:2025 --max_read_ops=0 --num_writers=512 --max_write_ops=512
   ```

## GCP

Coming soon.

