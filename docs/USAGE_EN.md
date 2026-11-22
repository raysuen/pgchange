# kbchange User Manual

Version: v1.0 (trial release) ｜ License: MIT ｜ 中文: [USAGE_ZH.md](USAGE_ZH.md)

## 1. What it does

kbchange answers one question: **which tables were written to after a given point in time**.

It targets database incident-recovery scenarios (split-brain, failover audits, point-in-time checks) and is **strictly read-only**: no objects created, no configuration changes, no replication slots consumed, no data written. Output is human-readable text, plus structured JSON with `-json`.

## 2. Install & where to run

- One static binary, no runtime dependencies. Run it on the **database host** (or the container host sharing the data directory).
- Build: `CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o kbchange .`
- Paths 2 and 4 read the data directory — **remote connections can only use paths 1/3**. When they are unavailable the tool says so explicitly; it never silently skips.

## 3. Four paths and their semantics

| Path | Mechanism | Verdict semantics |
|---|---|---|
| 1 | Max in-table timestamp / `xmin` commit time (`track_commit_timestamp`) | Direct evidence when a time column exists; SKIP with explanation otherwise |
| 2 | Read-only `sys_waldump`/`pg_waldump` parsing; transactions located by COMMIT time, DML mapped by relfilenode | Coverage is probed first; recycled segments, missing commits and orphan transactions all warn |
| 3 | `pg_stat_user_tables` counters (n_tup_ins/upd/del) | When the stats baseline predates the checkpoint, counts are "candidates" only |
| 4 | Data-file mtime | Physical signal, **never merged into the conclusion** (VACUUM FULL rewrites files — false positive verified in testing) |

Merged verdict = path 1 ∪ path 2 (deduplicated); trusted and candidate path-3 results are listed separately; path 4 is informational only.

## 4. Time & timezones

- `-since` is interpreted in the **OS timezone**. The tool reads the DB session timezone and converts automatically; when they differ, `timezone.compare` states the conversion.
- `timestamptz` compares by absolute instant; `timestamp`/`date` compare by wall-clock time in the DB timezone; `date` has day precision, so same-day writes cannot be pinned to the instant — such columns are reported "undetermined", never "no change".

## 5. Common invocations

```bash
# Minimal (paths 1 + 3)
./kbchange -dsn "..." -since "2026-11-18 08:00:00"

# Recommended: all paths + JSON (on the data host)
./kbchange -dsn "..." -since "2026-11-18 08:00:00" \
    -wal-enable -pgdata /home/kingbase/cluster/data -json > result.json

# Specific tables only
./kbchange -dsn "..." -since "..." -tables "public.orders,public.payments"

# Inside containers (borrow the container loader when waldump misses libraries)
./kbchange -dsn "..." -since "..." -wal-enable \
    -wal-root /proc/<pid>/root -pgdata /proc/<pid>/root/<data dir>
```

## 6. Reading the conclusion (important)

The conclusion is the single entry point. **"Changed" is trustworthy; "no change" must first clear the warnings**:

| If the conclusion says | Meaning | Trust "no change"? |
|---|---|---|
| `通路2 覆盖=full` | WAL fully covers the window after the given time | Yes, within coverage |
| `覆盖=partial` / earlier than WAL start | Only part of the window was read | No — the uncovered window was not checked |
| `通路2 未产出结果` | WAL unreachable / parse failed | That path did not run |
| `N 个未提交事务` | Transactions in flight (orphans) | May flip after commit; re-check afterwards |
| `时间线切换（TLI a→b）` | Instance went through a promote/recovery switch | Pre-switch data ignored; "no change" does not cover it |
| `通路1 未判定 N 张` | date precision / NULL values / xmin unavailable | Path 1 did not check those tables |
| `另有 N 张文件 mtime…` | Path-4 physical signal | Not merged; TRUNCATE only appears here; may be VACUUM FULL |

## 7. Capabilities and known limitations (verified by testing)

**Detected**: INSERT/UPDATE/DELETE, bulk COPY, SAVEPOINT and rollbacks, in-flight transactions (orphans until COMMIT, then located by commit time), partitioned/INHERITS parent tables (child DML attributed to the parent; named children not double-counted), multi-level partitions, materialized-view REFRESH, TOAST values, new writes after DROP+recreate of a same-named table, net-zero changes (insert then delete), pure DDL (no false positives), timeline-switch detection.

**Not detected / limited** (always surfaced in warnings, never silent):
- **TRUNCATE**: KingbaseES writes no user-table DML record (it is a Storage CREATE of a new file plus a pg_class update); path 2 cannot see it, path 1 sees an empty table, path 3 counters do not move. The only signal is the path-4 mtime of the new file.
- **UNLOGGED tables**: no WAL, and CHECKPOINT does not flush their dirty pages — no signal on paths 1/2/4 until pages reach disk; path 3 shows candidates only.
- Path 1 misses **UPDATEs that leave the time column untouched** (paths 2/3/4 cover it).
- Path 1 cannot see **DELETEs** (old time values remain; paths 2/3 cover it).
- Timelines: after a detected switch, old-timeline WAL is ignored wholesale — writes made before the fork point are ignored too (ownership cannot be proven); verify manually per the warning.

## 8. Troubleshooting

| Symptom | Cause & fix |
|---|---|
| `waldump … invalid record length`, exit 1 | Reading the partially-filled tail segment — **normal shutdown of the scan**, the warning says it is not a fault |
| Container waldump missing libraries | Pass `-wal-root /proc/<pid>/root` to borrow the container loader |
| `WAL 段不可读或无段文件` | Wrong data dir, or remote access (paths 2/4 must run on the data host) |
| `could not find file 00000001…` | Old-timeline segments recycled after a switch; current builds pass `-t <new TLI>` automatically — upgrade |
| Time column reported "no time field" | KingbaseES cluster edition returns data_type=USER-DEFINED; current builds fall back to udt_name — upgrade |
| `since 早于 WAL 起点` | The requested time precedes the oldest surviving WAL; recycled history is uncheckable and warned |

## 9. Exit codes

- `0`: results produced (warnings included).
- Non-zero: bad arguments / connection failure / fatal error; stderr explains.

## 10. Testing & regression

```bash
go test -count=1 ./...          # unit tests
go run ./cmd/matrix -dsn "..." -bin ./kbchange          # 34-case matrix
go run ./cmd/matrix -dsn "..." -bin ./kbchange -pgdata ... # full (incl. path-4 assertions)
```

The matrix crosses table kinds (plain/range-partitioned/multi-level/INHERITS/views/matviews) × time columns × transaction shapes × fault injection. Regression passes on both a containerized instance and a real two-node KingbaseES V8R6 cluster (including one node with a genuine timeline switch).
