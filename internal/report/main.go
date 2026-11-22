package report

import (
	"database/sql"
	"fmt"
	"kbchange/internal/filecheck"
	"kbchange/internal/path1"
	"kbchange/internal/table"
	"kbchange/internal/tz"
	"kbchange/internal/wal"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
)

type Path3Result struct {
	Table     string `json:"table"`
	Ins       int64  `json:"ins"`
	Upd       int64  `json:"upd"`
	Del       int64  `json:"del"`
	Changed   bool   `json:"changed"`   // 原始信号：统计窗口内计数>0
	Reliable  bool   `json:"reliable"`  // 能否据此确认指定时点后有变化
	Qualifier string `json:"qualifier"` // 口径说明（写进结果）
	// FromPartition 表示这行计数来自"未被单独检查的子表"汇总（分区父表/继承父表）。
	// 父表在 pg_stat_user_tables 里没有自己的行，不汇总就是彻底空白。
	FromPartition bool `json:"from_partition,omitempty"`
}

type Report struct {
	Schema         string                    `json:"schema"`
	Since          string                    `json:"since"`
	TZ             tz.TZInfo                 `json:"timezone"`
	TableCount     int                       `json:"table_count"`
	DiscoveryNote  string                    `json:"discovery_note,omitempty"` // 表发现被 -limit 截断的告警
	ScanScope      string                    `json:"scan_scope,omitempty"`     // 自动发现的表范围说明（物化视图/视图不在自动发现内）
	PartitionNote  string                    `json:"partition_note,omitempty"` // 父表展开说明（分区/继承子表的 DML、计数归到父表名下）
	StatsReset     string                    `json:"stats_reset"`
	StatsBaseline  string                    `json:"stats_baseline"`
	Path1Count     int                       `json:"path1_changed_tables"`
	Path1Error     int                       `json:"path1_error_tables"`     // 通路1 检查失败的表数（不能当成"无变化"）
	Path1Skip      int                       `json:"path1_skip_tables"`      // 通路1 无时间字段且 xmin 不可用的表数
	Path1Ambiguous int                       `json:"path1_ambiguous_tables"` // 通路1 定位不到时点的表数（date 只有天精度 / 时间字段有 NULL 行），不能当成"无变化"
	Path3Count     int                       `json:"path3_changed_tables"`   // 原始信号：计数>0 的表数
	Path3Reliable  int                       `json:"path3_reliable_tables"`  // 通路3 中可采信为"有变化"的表数
	Path3Candidate int                       `json:"path3_candidate_tables"` // 通路3 中仅候选、不采信的表数
	CombinedCount  int                       `json:"combined_changed_tables"`
	Path1          []path1.Path1Result       `json:"path1"`
	Path3          []Path3Result             `json:"path3"`
	Path4          filecheck.FileCheckReport `json:"path4"`
	Path2          wal.WalCheckReport        `json:"path2"`
	Path1Note      string                    `json:"path1_note"` // 通路1 口径说明
	Path3Note      string                    `json:"path3_note"` // 通路3 口径说明（含单向可信规则）
	Path4Note      string                    `json:"path4_note"` // 通路4 口径说明
	Path2Note      string                    `json:"path2_note"`
	Conclusion     string                    `json:"conclusion"`
}

func Path3Reliability(baseline string, changed bool) (reliable bool, qualifier string) {
	switch baseline {
	case "partial":
		if changed {
			return true, "统计起点晚于比对时点，计数全部落在时点之后，可判定有变化"
		}
		return false, "统计起点晚于比对时点，仅覆盖部分区间，未计数不代表时点前无变化（可能低估）"
	case "stale":
		if changed {
			return false, "统计起点早于比对时点，计数可能来自时点之前，不能判定有变化（候选，需人工核对）"
		}
		return true, "统计窗口内无计数，可判定时点后无变化"
	default:
		if changed {
			return false, "统计基线未知，计数无法定位时间点，仅供参考"
		}
		return false, "统计基线未知"
	}
}

// Path3Raw 是 pg_stat_user_tables 的一行原始计数。

type Path3Raw struct {
	Schema string
	Name   string
	Ins    int64
	Upd    int64
	Del    int64
}

// AggregatePath3Rows 按"归属表"汇总统计行。
// 分区父表在 pg_stat_user_tables 里没有自己的行（实测金仓 V009：ray.px 无行、只有 ray.px_p1），
// 不把子表计数并进来，父表在通路3 里就是彻底空白——通路1 也看不到时就成了静默假阴性。
// owner 为 nil 时按表名一一对应（分区关系查不到时的兜底）。
// 返回顺序按 refs，只含查得到统计行的表。

func AggregatePath3Rows(refs []table.TableRef, owner map[string]string, rows []Path3Raw) []Path3Result {
	reportOf := func(key string) (string, bool) {
		if owner == nil {
			return key, true
		}
		v, ok := owner[key]
		return v, ok
	}
	type acc struct {
		ins, upd, del int64
		fromParts     bool
	}
	agg := map[string]*acc{}
	for _, row := range rows {
		key := row.Schema + "." + row.Name
		rep, ok := reportOf(key)
		if !ok {
			continue
		}
		a := agg[rep]
		if a == nil {
			a = &acc{}
			agg[rep] = a
		}
		a.ins += row.Ins
		a.upd += row.Upd
		a.del += row.Del
		if key != rep {
			a.fromParts = true
		}
	}
	out := make([]Path3Result, 0, len(refs))
	for _, r := range refs {
		a, ok := agg[r.String()]
		if !ok {
			continue
		}
		out = append(out, Path3Result{
			Table:         r.String(),
			Ins:           a.ins,
			Upd:           a.upd,
			Del:           a.del,
			Changed:       a.ins+a.upd+a.del > 0,
			FromPartition: a.fromParts,
		})
	}
	return out
}

// 通路3：pg_stat_user_tables 统计计数，补通路1查不到的 DELETE。
// stats_reset 决定基线：晚于 since = partial（只覆盖部分区间），早于 since = stale（含 since 前数据）。
// owner 是分区归属映射（表 key -> 报告表 key）；nil 表示按表名一一对应。

func CheckPath3(db *sql.DB, refs []table.TableRef, owner map[string]string, since time.Time, sinceLoc *time.Location) ([]Path3Result, string, string, error) {
	statsReset := "-"
	baseline := "unknown"
	var reset sql.NullTime
	if err := db.QueryRow(`SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()`).Scan(&reset); err != nil {
		return nil, statsReset, "unavailable", err
	}
	if reset.Valid {
		// stats_reset 是 timestamptz，按用户指定时区显示，跟 since 同口径。
		statsReset = reset.Time.In(sinceLoc).Format("2006-01-02 15:04:05")
		switch {
		case !reset.Time.Before(since):
			baseline = "partial"
		default:
			baseline = "stale"
		}
	}

	// 查询范围 = 被检查的表 + 归到它们名下的子分区（分区父表要拿子分区的计数）。
	// 用 key 集合而不是按 refs 拼名单：子分区可能不在 -schema 里。
	keySet := map[string]bool{}
	for _, r := range refs {
		keySet[r.String()] = true
	}
	for key := range owner {
		keySet[key] = true
	}
	if len(keySet) == 0 {
		return []Path3Result{}, statsReset, baseline, nil
	}
	schemaSet := map[string]bool{}
	nameSet := map[string]bool{}
	for key := range keySet {
		s, n := table.SplitTableKey(key)
		schemaSet[s] = true
		nameSet[n] = true
	}
	schemas := make([]string, 0, len(schemaSet))
	for s := range schemaSet {
		schemas = append(schemas, s)
	}
	sort.Strings(schemas)
	names := make([]string, 0, len(nameSet))
	for n := range nameSet {
		names = append(names, n)
	}
	sort.Strings(names)

	rows, err := db.Query(`
		SELECT schemaname, relname, n_tup_ins, n_tup_upd, n_tup_del
		FROM pg_stat_user_tables
		WHERE schemaname = ANY($1::text[]) AND relname = ANY($2::text[])`,
		pq.Array(schemas), pq.Array(names))
	if err != nil {
		return nil, statsReset, baseline, err
	}
	defer rows.Close()

	var raw []Path3Raw
	for rows.Next() {
		var schema, name string
		var ins, upd, del int64
		if err := rows.Scan(&schema, &name, &ins, &upd, &del); err != nil {
			return nil, statsReset, baseline, err
		}
		if !keySet[schema+"."+name] {
			continue
		}
		raw = append(raw, Path3Raw{Schema: schema, Name: name, Ins: ins, Upd: upd, Del: del})
	}
	if err := rows.Err(); err != nil {
		return nil, statsReset, baseline, err
	}

	out := AggregatePath3Rows(refs, owner, raw)
	for i := range out {
		reliable, qualifier := Path3Reliability(baseline, out[i].Changed)
		if out[i].FromPartition {
			qualifier = "父表，计数为未被单独检查的子表合计；" + qualifier
		}
		out[i].Reliable = reliable
		out[i].Qualifier = qualifier
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Table < out[j].Table })
	return out, statsReset, baseline, nil
}

// table.SplitTableKey 把 "schema.name" 拆成两段。schema 名里不含点，按第一个点拆即可。

func BaselineText(s string) string {
	switch s {
	case "partial":
		return "partial（统计重置晚于指定时间，只能覆盖部分区间，属下界）"
	case "stale":
		return "stale（统计重置早于指定时间，含指定时间前的写入，属上界）"
	case "unavailable":
		return "unavailable（统计信息不可用）"
	default:
		return "unknown"
	}
}

func BuildConclusion(r Report) string {
	// 一张表都没检查（模式/表名匹配不到、-limit 截断）时不能写"0 张变化"：
	// 那是"查过且没变化"的口径，实际什么都没查，会被读成确认无变化。
	if r.TableCount == 0 {
		return "未检查到任何表（模式或表名匹配不到 / -limit 截断 / 无权限），本次结果不能当成'无变化'；请核对 -schema、-tables、-limit"
	}
	// 通路2 没启用时不能写"通路2 0 张"——那是"查了没变化"的口径，
	// 而实际是根本没跑。跟"启用但不可达"一样，都属于不参与判定的通路。
	parts := []string{fmt.Sprintf("通路1 %d 张", r.Path1Count)}
	switch {
	case r.Path2.Enabled && r.Path2.Available:
		parts = append(parts, fmt.Sprintf("通路2 %d 张", r.Path2.ChangedCount))
	case r.Path2.Enabled:
		// 启用了但没产出（WAL 不可达 / 目标表没有数据文件）：写"0 张"会被读成"查了没变化"。
		// 实测 -tables 打错表名时，括号里正是"通路2 0 张"，后面才补一句"未产出结果"，前后打脸。
		parts = append(parts, "通路2 未产出")
	default:
		parts = append(parts, "通路2 未启用")
	}
	// 通路3 查询失败（baseline=unavailable）时同样不能写"采信 0 张"——那是"查了没变化"，
	// 实际是一条结果都没产出，混起来会高估结论置信度。
	p3Part := fmt.Sprintf("通路3 采信 %d 张", r.Path3Reliable)
	if r.StatsBaseline == "unavailable" {
		p3Part = "通路3 未产出（查询失败）"
	}
	parts = append(parts, p3Part)
	base := fmt.Sprintf("指定时间后有数据变化的表：%d 张（%s，已去重）",
		r.CombinedCount, strings.Join(parts, "，"))
	if r.Path2.Enabled {
		if !r.Path2.Available {
			// 不可达 != 覆盖none：前者是没读成、不参与判定，后者是读到了但时点后无提交。
			// 混为一谈会让人误以为通路2 也确认了"无变化"，从而高估结论置信度。
			base += "；通路2 未产出结果（WAL 不可达/未解析），不参与判定、不能当成'无变化'"
		} else {
			base += fmt.Sprintf("；通路2 覆盖=%s", wal.CoverageText(r.Path2.Coverage))
		}
	}
	if r.Path1Error+r.Path1Skip+r.Path1Ambiguous > 0 {
		base += fmt.Sprintf("；通路1 未判定 %d 张（失败 %d / 跳过 %d / 时点不确定 %d：date 只到天或时间字段有 NULL），这些表通路1 未查成或无法定位时点、不能当成'无变化'，可用其他通路或 -time-col 核对",
			r.Path1Error+r.Path1Skip+r.Path1Ambiguous, r.Path1Error, r.Path1Skip, r.Path1Ambiguous)
	}
	if r.Path3Candidate > 0 {
		base += fmt.Sprintf("；通路3 另有 %d 张仅候选未采信（统计基线不足，不能判定时点后有变化，需人工核对）", r.Path3Candidate)
	}
	// 通路4 判定有物理变化但不并入合并数——合并数只收"已证实"的通路。
	// 但一句不提会把 TRUNCATE 这类"通路1/2/3 都抓不到、只有通路4 抓到"的表完全藏住：
	// 实测金仓 TRUNCATE 的 WAL 只有 Storage CREATE 新文件 + pg_class 更新，
	// 通路2 的 DML 正则匹配不到，通路1 表空查不到行，通路3 计数不动，只有通路4 mtime 有信号。
	// 同时通路4 有已知假阳性（VACUUM FULL 重写文件），所以只作提示、不改变合并数。
	if r.Path4.Available && r.Path4.ChangedCount > 0 {
		base += fmt.Sprintf("；另有 %d 张文件 mtime 晚于指定时间（通路4 仅物理信号，未并入合并数：可能是写入落盘，也可能是 TRUNCATE / VACUUM FULL / 重建等操作，需人工核对）", r.Path4.ChangedCount)
	}
	// 孤儿事务：有目标表 DML 但窗口内找不到 COMMIT/ABORT——事务还在跑。
	// 它一提交，"无变化"的表就可能变成"有变化"；不提示会把无变化说得太满。
	if r.Path2.Enabled && r.Path2.Available && r.Path2.OrphanTx > 0 {
		base += fmt.Sprintf("；通路2 检到 %d 个未提交事务（%d 条目标表 DML，事务运行中），提交后可能新增变化表，'无变化'需待其提交后复核", r.Path2.OrphanTx, r.Path2.OrphanDML)
	}
	// 时间线切换：切换前数据已整段忽略（旧时间线的 COMMIT 分不清真实写入与被撤回的事务）。
	// 漏了这句，"无变化"会被读成"切换前也没写入"——实际那段时间根本没查。
	// 不要求 Available：即使 waldump 全失败，切换事件本身已经成立，照样要提示。
	if r.Path2.Enabled && r.Path2.TLI != nil {
		base += fmt.Sprintf("；通路2 检到时间线切换（TLI %s→%s），切换前数据已忽略，'无变化'结论不覆盖切换前窗口，需人工核对", r.Path2.TLI.From, r.Path2.TLI.To)
	}
	switch r.StatsBaseline {
	case "stale":
		base += "；stale 基线只可信'无'、不可信'有'"
	case "partial":
		base += "；partial 基线只可信'有'、不可信'无'"
	case "unavailable":
		base += "；通路3 本轮查询失败，未采信任何结果，不能当成'无变化'"
	}
	return base
}

func Path4Summary(r filecheck.FileCheckReport) string {
	if !r.Available {
		return "不可达（" + r.Note + "）"
	}
	return fmt.Sprintf("%d 张表 mtime 晚于指定时间（仅供参考，不并入合并数）", r.ChangedCount)
}

// WalWindowText 拼 WAL 时间窗；没有 COMMIT 时区间为空，不能打印成 " ~ "。

func WalWindowText(start, end string) string {
	if start == "" || end == "" {
		return "WAL 时间窗未知（无可解析 COMMIT）"
	}
	return fmt.Sprintf("WAL 时间窗 %s ~ %s（DB时区）", start, end)
}

func Path2Summary(r wal.WalCheckReport) string {
	if !r.Enabled {
		return "未启用（加 -wal-enable 开启）"
	}
	if !r.Available {
		// 不止"WAL 不可达"一种：也可能是目标表没有数据文件映射（表不存在/无子分区的分区父表）。
		// 统一写"未产出结果"，具体原因用冒号接在后面（原因本身常带括号，套两层读起来费劲）。
		return "未产出结果：" + r.Warning
	}
	s := fmt.Sprintf("覆盖=%s，%d 张表有 DML 变化；%s，扫描 %d 条记录",
		wal.CoverageText(r.Coverage), r.ChangedCount, WalWindowText(r.RangeStart, r.RangeEnd), r.Scanned)
	if r.Warning != "" {
		s += "；告警：" + r.Warning
	}
	return s
}

func PrintReport(r Report, verbose bool) {
	fmt.Printf("模式：%s    比对时点(DB时区)：%s    输入：%s(%s)    检查表数：%d\n",
		r.Schema, r.Since, r.TZ.SinceInput, r.TZ.SinceTZ, r.TableCount)
	if r.DiscoveryNote != "" {
		fmt.Printf("表发现告警：%s\n", r.DiscoveryNote)
	}
	if r.ScanScope != "" {
		fmt.Printf("扫描范围：%s\n", r.ScanScope)
	}
	if r.PartitionNote != "" {
		fmt.Printf("分区：%s\n", r.PartitionNote)
	}
	fmt.Printf("时区：输入按=%s    DB会话=%s(%s)    OS=%s    since绝对时刻=%s UTC\n",
		r.TZ.SinceTZ, r.TZ.DBTimeZone, r.TZ.DBOffset, r.TZ.OSOffset, r.TZ.SinceUTC)
	fmt.Printf("DB/OS 时区对比：%s\n", r.TZ.Compare)
	fmt.Printf("换算口径：%s\n", r.TZ.Note)
	p3Line := fmt.Sprintf("通路3：%d 张表有变化（采信 %d / 候选 %d）", r.Path3Count, r.Path3Reliable, r.Path3Candidate)
	if r.StatsBaseline == "unavailable" {
		p3Line = "通路3：未产出（查询失败）"
	}
	fmt.Printf("通路1：%d 张表有变化（未判定 %d：失败 %d / 跳过 %d / 时点不确定 %d）    %s\n",
		r.Path1Count, r.Path1Error+r.Path1Skip+r.Path1Ambiguous, r.Path1Error, r.Path1Skip, r.Path1Ambiguous, p3Line)
	fmt.Printf("统计基线：%s（stats_reset = %s）\n", BaselineText(r.StatsBaseline), r.StatsReset)
	fmt.Printf("通路4（数据文件 mtime）：%s\n", Path4Summary(r.Path4))
	fmt.Printf("通路2（WAL 解析）：%s\n", Path2Summary(r.Path2))
	fmt.Printf("结论：%s\n", r.Conclusion)
	fmt.Println("各通路口径：")
	fmt.Printf("  [通路1] %s\n", r.Path1Note)
	fmt.Printf("  [通路3] %s\n", r.Path3Note)
	fmt.Printf("  [通路4] %s\n", r.Path4Note)
	fmt.Printf("  [通路2] %s\n", r.Path2Note)

	if !verbose {
		return
	}
	fmt.Println()
	fmt.Printf("通路1 变化表（%d）：\n", r.Path1Count)
	n := 0
	for _, t := range r.Path1 {
		if !t.Changed {
			continue
		}
		n++
		kind := t.ColKind
		if kind == "" {
			kind = "?"
		}
		fmt.Printf("  %d. %-34s 依据=%-20s[%s] T后行数=%d 最后=%s\n", n, t.Table, t.Basis, kind, t.RowsAfter, t.MaxTime)
	}
	if n == 0 {
		fmt.Println("  （无）")
	}
	if r.Path1Error+r.Path1Skip+r.Path1Ambiguous > 0 {
		fmt.Printf("通路1 未判定表（%d，失败/跳过/时点不确定，非'无变化'）：\n", r.Path1Error+r.Path1Skip+r.Path1Ambiguous)
		for _, t := range r.Path1 {
			if t.Status == "OK" {
				continue
			}
			fmt.Printf("  [%s] %-34s %s\n", t.Status, t.Table, t.Note)
		}
	}
	fmt.Printf("通路3 变化表（%d，采信 %d / 候选 %d）：\n", r.Path3Count, r.Path3Reliable, r.Path3Candidate)
	n = 0
	for _, t := range r.Path3 {
		if !t.Changed {
			continue
		}
		n++
		mark := "候选"
		if t.Reliable {
			mark = "采信"
		}
		fmt.Printf("  %d. %-34s 增=%d 改=%d 删=%d  [%s] %s\n", n, t.Table, t.Ins, t.Upd, t.Del, mark, t.Qualifier)
	}
	if n == 0 {
		fmt.Println("  （无）")
	}
	fmt.Printf("通路4 mtime 变化表（%d，仅供参考）：\n", r.Path4.ChangedCount)
	if !r.Path4.Available {
		fmt.Printf("  不可达：%s\n", r.Path4.Note)
	} else {
		n = 0
		for _, t := range r.Path4.Results {
			if !t.Changed {
				continue
			}
			n++
			fmt.Printf("  %d. %-34s relfilenode=%-12s 最新mtime=%s\n", n, t.Table, t.Relfilenode, t.LatestMTime)
		}
		if n == 0 {
			fmt.Println("  （无）")
		}
		fmt.Println("  注意：mtime 是刷盘时间，checkpoint 会污染，不能据此判定 DML 时间")
	}
	if r.Path2.Enabled {
		fmt.Printf("  WAL目录=%s  waldump=%s\n", r.Path2.WALDir, r.Path2.Waldump)
		if !r.Path2.Available {
			// 没跑起来就别打印"（无）"——那会被读成"查过了没变化"，实际根本没读。
			fmt.Printf("  未产出结果（不可达/未解析）：%s\n", r.Path2.Warning)
		} else {
			if r.Path2.RangeStart != "" && r.Path2.RangeEnd != "" {
				fmt.Printf("  覆盖区间：%s ~ %s（DB时区） 实际比对起点：%s\n",
					r.Path2.RangeStart, r.Path2.RangeEnd, r.Path2.CompareFrom)
				if r.Path2.SubxactMerged > 0 {
					// 这是整段 WAL 的统计，不是目标表的统计：说清楚，免得和上面的"变化表"对不上号。
					fmt.Printf("  子事务：WAL 内 %d 个子事务已按父事务 COMMIT 时间归并（落在目标表上才算变化）\n", r.Path2.SubxactMerged)
				}
			} else {
				fmt.Printf("  %s\n", WalWindowText(r.Path2.RangeStart, r.Path2.RangeEnd))
			}
			fmt.Printf("  WAL 变化表（%d）：\n", r.Path2.ChangedCount)
			if len(r.Path2.Changed) == 0 {
				fmt.Println("    （无）")
			} else {
				n = 0
				for _, t := range r.Path2.Changed {
					n++
					fmt.Printf("    %d. %-34s 增=%d 改=%d 删=%d\n", n, t.Table, t.Ins, t.Upd, t.Del)
				}
			}
		}
	}
}
