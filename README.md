# kbchange

金仓数据库（KingbaseES）/ PostgreSQL **只读数据变化检测工具**。回答一个问题：

> 指定时间点之后，这个库（或指定的表）有没有写入？哪些表变了？

典型场景：数据库集群脑裂、异常恢复后，需要判断各节点库里哪些表有新数据、数据新旧程度——全程**只读**，不建对象、不改配置、不写任何数据。

- 语言：Go，编译产物是**单个静态二进制**（约 6MB），拷贝到数据库所在机器即可运行，无任何运行时依赖
- 版本：金仓 V8R6（含集群版，鲲鹏/ARM 实测）与 PostgreSQL 12+ 兼容
- 许可：MIT（见 [LICENSE](LICENSE)）
- 使用手册：[docs/USAGE_ZH.md](docs/USAGE_ZH.md) ｜ English: [README_EN.md](README_EN.md), [docs/USAGE_EN.md](docs/USAGE_EN.md)

## 四条通路

| 通路 | 原理 | 可信度 | 前置条件 |
|---|---|---|---|
| 通路1 | 表内时间字段 / `xmin` 提交时间 | 直接证据 | 表有时间列，或 `track_commit_timestamp=on` |
| 通路2 | 解析 WAL（`sys_waldump`/`pg_waldump`），按 COMMIT 时间定位 | 强（需覆盖完整） | 工具与数据目录同机；WAL 段未被回收 |
| 通路3 | `pg_stat_user_tables` 计数增量 | 参考（统计基线不足时只作候选） | 无 |
| 通路4 | 数据文件 mtime | 物理信号（不并入结论，防 VACUUM FULL 假阳性） | 与数据目录同机且有读权限 |

多通路自动互补：时间列旧值的 UPDATE、净零变化（插入又删光）、分区/INHERITS 父子表、TOAST 大字段、TRUNCATE、未提交事务等场景各有对应通路兜底，详见手册"能力与已知局限"。

## 快速开始

```bash
# 构建（aarch64/x86_64 通用，纯静态）
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o kbchange .

# 检查：2026-11-18 08:00 之后，kingbase 库里有哪些表有写入
./kbchange -dsn "host=127.0.0.1 port=54321 user=system password=*** dbname=kingbase sslmode=disable" \
    -since "2026-11-18 08:00:00"

# 全量：开 WAL 解析（容器场景加 -wal-root），带数据目录（通路4）
./kbchange -dsn "..." -since "2026-11-18 08:00:00" \
    -wal-enable -pgdata /home/kingbase/cluster/data -json
```

时间口径：`-since` 按**操作系统时区**输入，程序自动换算到数据库时区比对（DB 与 OS 时区不一致时会在输出中明确说明换算关系）。

## 参数速查

| 参数 | 说明 |
|---|---|
| `-dsn` | 连接串（只连单库） |
| `-since` | 起始时间点，OS 时区输入，自动换算 |
| `-schema` | 要检查的模式，逗号分隔，默认 public |
| `-tables` | 只查这些表 `schema.table`；留空=自动发现 |
| `-time-col` | 强制指定通路1 时间字段 |
| `-wal-enable` | 启用通路2 WAL 解析 |
| `-wal` | WAL 目录覆盖（默认数据目录下 sys_wal/pg_wal） |
| `-wal-bin` | waldump 可执行文件路径 |
| `-wal-root` | 容器 root 路径（`/proc/<pid>/root`），容器内 waldump 缺库时借用 |
| `-pgdata` | 通路4 数据目录覆盖 |
| `-limit` | 自动发现最多检查多少表（默认 500） |
| `-json` | JSON 输出 |
| `-verbose` | 打印各通路判定明细 |

## 结论怎么读

`结论` 一行给出合并判定（已去重）。**有变化**是可信的；**"无变化"采信前**必须核对结论里没有这些提示：

- `通路2 覆盖=none/partial` 或"未产出结果"——WAL 没读全，不能当无变化
- `未提交事务`——有事务在跑，提交后可能翻盘
- `时间线切换`——切换前数据已忽略，"无变化"不覆盖切换前窗口
- `通路1 未判定 N 张`——date 精度/NULL 时间列/xmin 不可用
- `另有 N 张文件 mtime…`——通路4 物理信号（TRUNCATE 只在这里出现）

## 已知局限（如实告知）

- TRUNCATE：金仓 WAL 无对应 DML 记录，通路2 检不出，靠通路4 mtime 信号提示
- UNLOGGED 表：不写 WAL、脏页不落盘，落盘前通路1/2/4 全无信号
- 通路1 不更新时间字段的 UPDATE 查不到（通路2/3/4 补位）
- timeline：检出切换后旧时间线整段忽略（不猜归属），旧时间线上 fork 点前的真实写入也一并忽略，靠告警交人工核对

## 测试

```bash
go test -count=1 ./...
go run ./cmd/matrix -dsn "..." -bin ./kbchange   # 34 格场景矩阵（表种类×时间列×事务×故障注入）
```

在金仓 V8R6 集群版（ARM）真实双机环境与容器环境均通过回归。
