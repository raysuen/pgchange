# kbchange 使用手册

版本：v1.0（试用发布）｜ 许可：MIT ｜ English: [USAGE_EN.md](USAGE_EN.md)

## 1. 工具是干什么的

kbchange 回答一个问题：**指定时间点之后，这个库里哪些表有写入**。

它面向数据库故障恢复场景（脑裂、异常切换、回档核对），特点是**全程只读**：不建对象、不改配置、不消费复制槽、不写任何数据。输出既有人读的文本，也有 `-json` 结构化结果。

## 2. 安装与运行方式

- 单个静态二进制，无运行时依赖。放到**数据库所在机器**（或与数据目录同机的容器宿主）运行。
- 构建：`CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o kbchange .`
- 通路2/通路4 需要读数据目录，**远程连接只能跑通路1/3**——跨机使用时工具会明确告警"不可达"，不会静默跳过。

## 3. 四条通路与可信度

| 通路 | 原理 | 判定语义 |
|---|---|---|
| 通路1 | 表内时间字段 max 值 / `xmin` 提交时间（`track_commit_timestamp`） | 有时间列 → 直接证据；无时间列且 xmin 不可用 → SKIP 并说明 |
| 通路2 | 只读调用 `sys_waldump`/`pg_waldump` 解析 WAL，按 COMMIT 时间定位事务，DML 行按 relfilenode 映射表名 | 需先探覆盖区间；段被回收/无提交/孤儿事务都会告警 |
| 通路3 | `pg_stat_user_tables` 计数（n_tup_ins/upd/del） | 统计起点早于比对时点时只作"候选"，不采信为"有" |
| 通路4 | 数据文件 mtime | 物理信号，**永不并入合并结论**（VACUUM FULL 会重写文件造成假阳性，实测确认） |

合并判定 = 通路1 ∪ 通路2（去重），通路3 采信部分与候选单独列出，通路4 只提示。

## 4. 时间与时区

- `-since` 按**操作系统时区**输入。程序读取 DB 会话时区并自动换算；两边不一致时输出 `timezone.compare` 明说换算关系。
- `timestamptz` 列按绝对时刻比较；`timestamp`/`date` 按 DB 时区墙上时间比较；`date` 只有天精度，当天写入无法定位到时点 → 单列"未判定"，不当"无变化"。

## 5. 常用姿势

```bash
# 最小用法（通路1+3）
./kbchange -dsn "..." -since "2026-11-18 08:00:00"

# 推荐：全通路 + JSON（与库同机）
./kbchange -dsn "..." -since "2026-11-18 08:00:00" \
    -wal-enable -pgdata /home/kingbase/cluster/data -json > result.json

# 只查几张表
./kbchange -dsn "..." -since "..." -tables "public.orders,public.payments"

# 容器场景（waldump 在容器内缺库时借 loader 启动）
./kbchange -dsn "..." -since "..." -wal-enable \
    -wal-root /proc/<容器PID>/root -pgdata /proc/<容器PID>/root/<数据目录>
```

## 6. 结论怎么读（重要）

结论是唯一入口。**"有变化"直接可信；"无变化"必须先核对提示**：

| 结论里出现 | 含义 | 该不该信"无变化" |
|---|---|---|
| `通路2 覆盖=full` | WAL 完整覆盖指定时点之后 | 覆盖内的"无变化"可信 |
| `覆盖=partial` / `早于 WAL 起点` | 只覆盖部分区间 | 覆盖外的时间窗**没查**，不可信 |
| `通路2 未产出结果` | WAL 不可达/解析失败 | 该通路没跑，不能当"无变化" |
| `N 个未提交事务` | 有事务在跑（孤儿） | 提交后可能翻盘，待其提交后复核 |
| `时间线切换（TLI a→b）` | 实例经历过 promote/恢复切换 | 切换前数据已忽略，"无变化"不覆盖切换前窗口 |
| `通路1 未判定 N 张` | date 精度/NULL 时间列/xmin 不可用 | 这些表通路1 没查成，不能当"无变化" |
| `另有 N 张文件 mtime…` | 通路4 物理信号 | 不并入合并数；TRUNCATE 只在这里出现；也可能是 VACUUM FULL |

## 7. 能力与已知局限（实测确认）

**能检出**：INSERT/UPDATE/DELETE、批量 COPY、SAVEPOINT 与回滚、未提交事务（判孤儿不计入、提交后按 COMMIT 时间检出）、分区/INHERITS 父子表（DML 归父表、子表点名不重复）、多级分区、物化视图 REFRESH、TOAST 大字段、DROP 后重建同名表的新写入、净零变化（插入又删光）、纯 DDL 不误报、时间线切换检出。

**检不出/受限**（都有文案兜底，不静默）：
- **TRUNCATE**：金仓 WAL 无用户表 DML 记录（实为 Storage CREATE 新文件+pg_class 更新），通路2 检不出；通路1 表空、通路3 计数不动。唯一信号是通路4 新文件 mtime。
- **UNLOGGED 表**：不写 WAL、CHECKPOINT 不刷其脏页——落盘前通路1/2/4 全无信号，通路3 只有候选。落盘后通路4 才有延迟信号。
- 通路1 查不到**不更新时间字段的 UPDATE**（通路2/3/4 补位）。
- 通路1 查不到 **DELETE**（时间列在旧值上；通路2/3 补位）。
- timeline：检出切换后旧时间线整段忽略——fork 点前真实写入也一并忽略（归属无法证明），靠告警交人工。

## 8. 故障排查

| 现象 | 原因与处理 |
|---|---|
| `waldump … invalid record length` 且 exit 1 | 读到 WAL 末尾未写满区段，**属正常收尾**，告警已注明不是故障 |
| 容器内 waldump 缺动态库 | 用 `-wal-root /proc/<pid>/root` 借容器 loader 启动 |
| `WAL 段不可读或无段文件` | 数据目录路径错，或跨机访问（通路2/4 须与库同机） |
| `could not find file 00000001…` | 时间线切换后旧段被回收；新版已自动 `-t 新TLI`，请升级工具 |
| 时间列被判"无时间字段" | 金仓集群版 data_type 为 USER-DEFINED；新版已用 udt_name 兜底，请升级工具 |
| `since 早于 WAL 起点` | 想查的时间早于现存最早 WAL，被回收的部分查不了，告警会注明 |

## 9. 退出码

- `0`：正常产出（含告警产出）。
- 非 0：参数错误 / 连接失败 / 严重故障，stderr 有原因。

## 10. 测试与回归

```bash
go test -count=1 ./...          # 单元测试
go run ./cmd/matrix -dsn "..." -bin ./kbchange          # 34 格场景矩阵
go run ./cmd/matrix -dsn "..." -bin ./kbchange -pgdata ... # 全量（含通路4 断言）
```

矩阵覆盖：表种类（普通/分区/多级分区/INHERITS/视图/物化视图）× 时间列（timestamptz/timestamp/date/NULL/多列/无列）× 事务形状（SAVEPOINT/回滚/TRUNCATE/长事务/批量）× 故障注入（坏 wal 目录/waldump 失败/since 异常/limit 截断）。已在容器环境与金仓 V8R6 集群版双机（含真实 TLI 切换环境）回归通过。
