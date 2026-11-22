# kbchange

**Read-only data-change detection tool** for KingbaseES / PostgreSQL. It answers one question:

> After a given point in time, has anything been written to this database (or to specific tables)? Which tables changed?

Typical scenario: after a cluster split-brain or an abnormal recovery, you need to know which tables on each node received new writes — with **zero side effects**: no objects created, no configuration changes, no data written.

- Language: Go. The build output is a **single static binary** (~6 MB); copy it to the database host and run — no runtime dependencies.
- Compatibility: KingbaseES V8R6 (including the cluster edition, verified on Kunpeng/ARM) and PostgreSQL 12+.
- License: MIT (see [LICENSE](LICENSE)).
- 手册（中文）：[README.md](README.md), [docs/USAGE_ZH.md](docs/USAGE_ZH.md) ｜ Manual (English): [docs/USAGE_EN.md](docs/USAGE_EN.md)

## Four detection paths

| Path | Mechanism | Reliability | Prerequisites |
|---|---|---|---|
| 1 | In-table timestamp columns / `xmin` commit time | Direct evidence | Table has a time column, or `track_commit_timestamp=on` |
| 2 | WAL parsing via `sys_waldump`/`pg_waldump`, transactions located by COMMIT time | Strong (if coverage is complete) | Tool runs on the data host; WAL segments not recycled |
| 3 | Counter deltas in `pg_stat_user_tables` | Reference only (candidate when the stats baseline is insufficient) | None |
| 4 | Data-file mtime | Physical signal only (never merged into the conclusion, to avoid VACUUM FULL false positives) | Read access to the data directory |

The paths complement each other: UPDATEs that leave the timestamp untouched, net-zero changes (insert then delete), partitioned/INHERITS parent tables, TOAST values, TRUNCATE, in-flight transactions — each case has a designated fallback path. See the manual's "Capabilities and known limitations".

## Quick start

```bash
# Build (static, works on aarch64/x86_64)
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o kbchange .

# Which tables changed after 2026-11-18 08:00 in database kingbase?
./kbchange -dsn "host=127.0.0.1 port=54321 user=system password=*** dbname=kingbase sslmode=disable" \
    -since "2026-11-18 08:00:00"

# Full run: enable WAL parsing (add -wal-root inside containers) and the data directory (path 4)
./kbchange -dsn "..." -since "2026-11-18 08:00:00" \
    -wal-enable -pgdata /home/kingbase/cluster/data -json
```

Time semantics: `-since` is interpreted in the **operating system timezone**; the tool converts it to the database timezone automatically and states the conversion explicitly whenever the two differ.

## Flags

| Flag | Meaning |
|---|---|
| `-dsn` | Connection string (single database) |
| `-since` | Start time; OS timezone input, converted automatically |
| `-schema` | Schemas to check, comma-separated (default public) |
| `-tables` | Only these tables `schema.table`; empty = auto-discovery |
| `-time-col` | Force the path-1 time column |
| `-wal-enable` | Enable path 2 (WAL parsing) |
| `-wal` | WAL directory override (default `sys_wal`/`pg_wal` under the data directory) |
| `-wal-bin` | Path to the waldump executable |
| `-wal-root` | Container root (`/proc/<pid>/root`) when the in-container waldump misses libraries |
| `-pgdata` | Data directory override for path 4 |
| `-limit` | Max tables checked in auto-discovery (default 500) |
| `-json` | JSON output |
| `-verbose` | Print per-table verdicts for each path |

## Reading the conclusion

The `conclusion` line carries the merged verdict (deduplicated). **"Changed" is trustworthy**. Before accepting **"no change"**, verify none of these appear:

- `通路2 覆盖=none/partial` / "未产出结果" — WAL not fully read, cannot count as "no change"
- `未提交事务` — transactions in flight; they may flip the verdict after commit
- `时间线切换` — pre-switch data ignored; "no change" does not cover the pre-switch window
- `通路1 未判定 N 张` — date precision / NULL time values / xmin unavailable
- `另有 N 张文件 mtime…` — path-4 physical signal (TRUNCATE only shows up here)

## Known limitations (stated plainly)

- TRUNCATE: KingbaseES writes no user-table DML record for TRUNCATE; path 2 cannot see it. It surfaces through the path-4 mtime signal.
- UNLOGGED tables: no WAL, dirty pages not checkpointed — no signal on paths 1/2/4 until pages hit disk.
- Path 1 cannot see UPDATEs that leave the time column untouched (paths 2/3/4 cover it).
- Timelines: after a detected switch, old-timeline WAL is ignored entirely (ownership cannot be proven); real writes made before the fork point are ignored too and must be verified manually per the warning.

## Testing

```bash
go test -count=1 ./...
go run ./cmd/matrix -dsn "..." -bin ./kbchange   # 34-case scenario matrix
```

Regression passes on both a containerized instance and a real two-node KingbaseES V8R6 cluster (ARM).
