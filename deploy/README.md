# Observability

Shows where request time goes inside the engine: WAL fsync, lock wait, memtable, flush, compaction, SSTable scans.

## Run

```sh
# 1. server on the host (exposes /metrics and /debug/pprof on :8080; override with KLSM_ADDR=:8081)
go run ./cmd/server

# 2. Prometheus + Grafana
docker compose -f deploy/docker-compose.yml up -d

# 3. load
go run ./cmd/gen put-delete 100000      # writes put-delete.txt
go run ./cmd/client put-delete.txt
```

Open http://localhost:3000 (no login) and pick the **k-lsm-engine** dashboard. Prometheus itself is at http://localhost:9090.

## What to look at

- **Time per request by stage** (stacked): the widest band is the bottleneck. Pick `op` (put / delete / get) at the top.
- **p99 per stage**: separates stages that are always slow (WAL fsync) from ones that are rarely but hugely slow (flush, compaction run inline in the request).
- **Flush / compaction** markers line up with latency spikes on the request panels.
- **GET source** and **SSTable files per GET** show read amplification once data has been flushed out of the memtable.

## CPU profile

Histograms show wall time, including I/O waits such as fsync. For CPU hot spots (JSON encoding, logging) use pprof:

```sh
go tool pprof -top http://localhost:8080/debug/pprof/profile?seconds=10
```

## Notes

- `cmd/client` sends one request at a time (`sem` of size 1), so `lock_wait` stays near zero until that is raised.
- Metrics are defined in `internal/storage/metrics.go`; stage boundaries are in `engine.go`, `wal.go` and `sstable.go`.
