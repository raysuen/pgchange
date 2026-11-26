package report

import (
	"encoding/json"
	"fmt"
	"kbchange/internal/filecheck"
	"kbchange/internal/path1"
	"kbchange/internal/tz"
	"kbchange/internal/wal"
	"os"
	"testing"
)

func TestPrintExampleOutput(t *testing.T) {
	r := Report{
		Schema: "public",
		Since:  "2026-11-16 10:00:00",
		TZ: tz.TZInfo{
			OSLocal:    "Local(+08:00)",
			SinceTZ:    "Local(+08:00)(自动=OS)",
			SinceInput: "2026-11-16 18:00:00",
			DBTimeZone: "UTC",
			DBOffset:   "+00:00",
			OSOffset:   "+08:00",
			Compare:    "不一致：DB 会话时区 UTC=+00:00，操作系统时区 Local(+08:00)，相差 -08:00（以 since 时刻计）",
			SinceUTC:   "2026-11-16 10:00:00",
			Note:       "since 按操作系统时区输入，程序自动换算成 DB 会话时区后比对；timestamptz/xmin 按绝对时刻比较并换算到 DB 时区显示；timestamp/date 按 DB 时区墙上时间比较",
		},
		TableCount:     8,
		StatsReset:     "2026-11-16 19:00:00",
		StatsBaseline:  "partial",
		Path1Count:     3,
		Path3Count:     4,
		Path3Reliable:  4,
		Path3Candidate: 0,
		CombinedCount:  5,
		Path1Note:      "按表内时间字段/xmin 提交时间查指定时点后的行，属直接证据；只覆盖时间字段随写入更新的场景，不更新时间字段的 UPDATE 查不到；查不了 DELETE/TRUNCATE；date 列精度只到天——当天写入无法定位到时点，单列'未判定'、不当无变化；时间字段允许 NULL 且确实存在 NULL 行时，这些行同样定位不到时点，也列'未判定'；无时间字段且 xmin 不可用时 SKIP",
		Path3Note:      "pg_stat_user_tables 累计计数，只报窗口内增改删总数、无时间点；partial（统计重置晚于指定时间，只能覆盖部分区间，属下界）；单向可信——stale 只可信'无'、partial 只可信'有'，不可信的一半仅列候选不并入合并数；父表的计数为未被单独检查的子表合计（含分区与 INHERITS）",
		Path4Note:      "数据文件 mtime，刷盘时间会被 checkpoint/vacuum 污染，仅供参考，不并入合并数；远程库/无读权限时不可达",
		Path1: []path1.Path1Result{
			{Table: "public.orders", Basis: "update_time", ColKind: "timestamptz", RowsAfter: 12, MaxTime: "2026-11-16 20:15:33", Status: "OK", Changed: true},
			{Table: "public.order_items", Basis: "create_time", ColKind: "timestamp", RowsAfter: 47, MaxTime: "2026-11-16 20:15:33", Status: "OK", Changed: true},
			{Table: "public.refund", Basis: "update_time", ColKind: "timestamptz", RowsAfter: 3, MaxTime: "2026-11-16 19:42:10", Status: "OK", Changed: true},
			{Table: "public.product", Basis: "update_time", ColKind: "timestamp", RowsAfter: 0, MaxTime: "2026-11-16 17:30:00", Status: "OK"},
			{Table: "public.sys_config", Basis: "none", Status: "SKIP", Note: "无时间字段且 xmin 提交时间不可用"},
		},
		Path3: []Path3Result{
			{Table: "public.orders", Ins: 12, Upd: 5, Del: 0, Changed: true, Reliable: true, Qualifier: "统计起点晚于比对时点，计数全部落在时点之后，可判定有变化"},
			{Table: "public.order_items", Ins: 47, Upd: 0, Del: 1, Changed: true, Reliable: true, Qualifier: "统计起点晚于比对时点，计数全部落在时点之后，可判定有变化"},
			{Table: "public.refund", Ins: 3, Upd: 1, Del: 0, Changed: true, Reliable: true, Qualifier: "统计起点晚于比对时点，计数全部落在时点之后，可判定有变化"},
			{Table: "public.stock_log", Ins: 0, Upd: 0, Del: 9, Changed: true, Reliable: true, Qualifier: "统计起点晚于比对时点，计数全部落在时点之后，可判定有变化"},
			{Table: "public.product", Ins: 0, Upd: 0, Del: 0},
		},
		Path4: filecheck.FileCheckReport{
			Available:    true,
			DataDir:      "/opt/kingbase/data",
			DBOID:        "16384",
			ChangedCount: 2,
			Results: []filecheck.FileResult{
				{Table: "public.orders", Relfilenode: "24576", LatestMTime: "2026-11-16 20:15:40", Changed: true, Status: "OK"},
				{Table: "public.order_items", Relfilenode: "24577", LatestMTime: "2026-11-16 20:15:40", Changed: true, Status: "OK"},
				{Table: "public.product", Relfilenode: "24578", LatestMTime: "2026-11-16 17:00:00", Changed: false, Status: "OK"},
			},
		},
		Path2: wal.WalCheckReport{
			Enabled:      true,
			Available:    true,
			Coverage:     wal.WalFull,
			WALDir:       "/opt/kingbase/data/sys_wal",
			Waldump:      "/opt/kingbase/bin/sys_waldump",
			RangeStart:   "2026-11-16 08:00:00",
			RangeEnd:     "2026-11-16 21:00:00",
			CompareFrom:  "2026-11-16 10:00:00",
			Scanned:      15234,
			ChangedCount: 2,
			Changed: []wal.WalTableDML{
				{Table: "public.orders", Ins: 12, Upd: 5, Del: 0, LastLSN: "0/1A2B3C4"},
				{Table: "public.stock_log", Ins: 0, Upd: 0, Del: 9, LastLSN: "0/1A2B9F0"},
			},
		},
		Path2Note: "只读解析 WAL：COMMIT 时间定位事务，DML 行取 relfilenode 映射表名（分区/INHERITS 父表把子表 DML 归到父表名下），覆盖 Heap 增删改（含 Kingbase INPLACE 原地改、COPY 的 MULTI_INSERT）；SAVEPOINT 子事务按父事务 COMMIT 行的 subxacts 归并、回滚事务(ABORT)不计入；两阶段提交(COMMIT PREPARED 无提交时间)按'有变化'保守计入并提示；先探覆盖区间，段被回收/无提交/孤儿事务(有 DML 但既无 COMMIT 也无 ABORT)会告警，waldump 异常退出/段缺口/读取中断会把覆盖从 full 降级为 partial；TRUNCATE/DROP 等改 relfilenode 的 DDL 不在覆盖范围",
	}
	r.Conclusion = BuildConclusion(r)

	fmt.Println("===== 默认输出（示意数据） =====")
	PrintReport(r, false)
	fmt.Println()
	fmt.Println("===== --verbose 输出（示意数据） =====")
	PrintReport(r, true)
	fmt.Println()
	fmt.Println("===== -json 输出（示意数据，节选见下） =====")
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		t.Fatal(err)
	}
}
