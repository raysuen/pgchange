// cmd/matrix 是 kbchange 的场景矩阵回归工具，不参与发布。
//
// 目的：把"每轮靠灵感想一个场景"换成"一次跑完一张表"。
// 四个轴：
//
//	表种类  普通表 / 分区父 / 分区子 / 多级分区 / INHERITS 父 / 物化视图 / 视图
//	时间列  timestamp / timestamptz / date(当天) / date(昨天) / 无时间列 / 可空含 NULL / 多时间列
//	事务    单条 INSERT / 批量 INSERT(多行) / 不改时间列的 UPDATE / DELETE / SAVEPOINT 提交 /
//	        SAVEPOINT 回滚后再提交 / 整体 ROLLBACK / TRUNCATE
//	故障    wal 目录不存在 / waldump 失败 / since 未来 / since 早于 WAL 起点 / 表不存在 / 模式不存在 / -limit 截断
//
// 每个格子：造对象 -> 写数据（提交时间就在刚刚）-> 跑 kbchange -json -> 按期望逐项对比。
// 期望写的是"正确行为"，不是"当前行为"。跑出 FAIL 就是真问题，不是脚本问题。
//
// 用法：
//
//	go run ./cmd/matrix -dsn '...' -bin ./kbchange -wal-root /proc/<pid>/root
//	go run ./cmd/matrix -dsn '...' -only B01,D05        # 只跑指定格子
package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

const (
	schemaMain = "mx"  // 逐表指定场景
	schemaScan = "mxs" // 扫描模式场景，单独一个模式，免得别的表混进 table_count
)

// ---------- kbchange -json 的最小映射（只取要断言的字段） ----------

type p1Row struct {
	Table     string `json:"table"`
	Basis     string `json:"basis"`
	ColKind   string `json:"col_kind"`
	RowsAfter int64  `json:"rows_after"`
	DayHit    int64  `json:"day_hit"`
	NullTime  int64  `json:"null_time"`
	MaxTime   string `json:"max_time"`
	Status    string `json:"status"`
	Note      string `json:"note"`
	Changed   bool   `json:"changed"`
}

type p2Row struct {
	Table string `json:"table"`
	Ins   int64  `json:"ins"`
	Upd   int64  `json:"upd"`
	Del   int64  `json:"del"`
}

type p2Report struct {
	Enabled       bool    `json:"enabled"`
	Available     bool    `json:"available"`
	Coverage      string  `json:"coverage"`
	WALDir        string  `json:"wal_dir"`
	Waldump       string  `json:"waldump"`
	RangeStart    string  `json:"range_start"`
	RangeEnd      string  `json:"range_end"`
	CompareFrom   string  `json:"compare_from"`
	Warning       string  `json:"warning"`
	Note          string  `json:"note"`
	Changed       []p2Row `json:"changed_tables"`
	ChangedCount  int     `json:"changed_count"`
	Scanned       int     `json:"scanned_records"`
	OrphanTx      int     `json:"orphan_tx"`
	OrphanDML     int     `json:"orphan_dml"`
	SubxactMerged int     `json:"subxact_merged"`
}

type p3Row struct {
	Table         string `json:"table"`
	Ins           int64  `json:"ins"`
	Upd           int64  `json:"upd"`
	Del           int64  `json:"del"`
	Changed       bool   `json:"changed"`
	Reliable      bool   `json:"reliable"`
	Qualifier     string `json:"qualifier"`
	FromPartition bool   `json:"from_partition"`
}

type p4Report struct {
	Available    bool       `json:"available"`
	DataDir      string     `json:"data_dir"`
	ChangedCount int        `json:"changed_count"`
	Note         string     `json:"note"`
	Results      []p4Result `json:"results"`
}

type p4Result struct {
	Table   string `json:"table"`
	Changed bool   `json:"changed"`
	Note    string `json:"note"`
}

type report struct {
	Schema         string   `json:"schema"`
	Since          string   `json:"since"`
	TableCount     int      `json:"table_count"`
	DiscoveryNote  string   `json:"discovery_note"`
	ScanScope      string   `json:"scan_scope"`
	PartitionNote  string   `json:"partition_note"`
	StatsReset     string   `json:"stats_reset"`
	StatsBaseline  string   `json:"stats_baseline"`
	Path1Count     int      `json:"path1_changed_tables"`
	Path1Error     int      `json:"path1_error_tables"`
	Path1Skip      int      `json:"path1_skip_tables"`
	Path1Ambiguous int      `json:"path1_ambiguous_tables"`
	Path3Count     int      `json:"path3_changed_tables"`
	Path3Reliable  int      `json:"path3_reliable_tables"`
	Path3Candidate int      `json:"path3_candidate_tables"`
	CombinedCount  int      `json:"combined_changed_tables"`
	Path1          []p1Row  `json:"path1"`
	Path3          []p3Row  `json:"path3"`
	Path4          p4Report `json:"path4"`
	Path2          p2Report `json:"path2"`
	Conclusion     string   `json:"conclusion"`
}

// ---------- 期望：声明式，写"正确行为" ----------

type opCount struct{ Ins, Upd, Del int64 }

type expect struct {
	p1Status    map[string]string // 表 -> OK/AMBIG/SKIP/ERROR（必须在 path1 里出现）
	p1Changed   []string          // 通路1 判定有变化
	p1NoChange  []string          // 通路1 判定无变化（status 仍可为 OK）
	p1NoteHas   map[string]string // 表 -> note 必含子串
	p1Basis     map[string]string // 表 -> basis 列名
	p1RowsAfter map[string]int64

	p2Unavailable  bool
	coverageAbsent bool // 通路2 失败时 JSON 里不得出现 coverage（none 只属于"读到了但无提交"）
	p2Has          map[string]opCount
	p2NoTable      []string
	coverage       string
	p2WarningHas   string
	subxactMin     int
	orphanZero     bool

	conclusionHas    []string
	conclusionNotHas []string

	tableCount   int    // -1 = 不校验
	countSchema  string // 非空：table_count 必须等于该模式下 relkind in (r,p) 的实际数量
	zeroTables   bool   // table_count == 0
	discoveryHas string
	scanScopeHas string
	partitionHas string

	path4Available  bool
	p4Changed       []string          // 通路4 判定有变化的表（仅在 -pgdata 可用时校验）
	p4NoteHas       map[string]string // 表 -> path4 note 必含子串
	p4ConclusionHas []string          // 结论必含子串（仅在 -pgdata 可用时校验：通路4 信号句）
	exitCode        int               // 0 默认；其它值需要显式给
}

func (e expect) check(rep *report, exit int, stderr string) []string {
	var errs []string
	add := func(f string, a ...any) { errs = append(errs, fmt.Sprintf(f, a...)) }

	for tbl, want := range e.p1Status {
		r := findP1(rep, tbl)
		if r == nil {
			add("path1 里没有 %s（期望 status=%s）", tbl, want)
			continue
		}
		if r.Status != want {
			add("%s status=%s 期望 %s（note=%q）", tbl, r.Status, want, r.Note)
		}
	}
	for _, tbl := range e.p1Changed {
		r := findP1(rep, tbl)
		if r == nil || !r.Changed {
			changed := "表不在 path1 结果里"
			if r != nil {
				changed = fmt.Sprintf("status=%s rows_after=%d note=%q", r.Status, r.RowsAfter, r.Note)
			}
			add("%s 期望通路1 判定有变化，实际无（%s）", tbl, changed)
		}
	}
	for _, tbl := range e.p1NoChange {
		if r := findP1(rep, tbl); r != nil && r.Changed {
			add("%s 期望通路1 判定无变化，实际 rows_after=%d", tbl, r.RowsAfter)
		}
	}
	for tbl, sub := range e.p1NoteHas {
		r := findP1(rep, tbl)
		if r == nil {
			add("path1 里没有 %s（期望 note 含 %q）", tbl, sub)
			continue
		}
		if !strings.Contains(r.Note, sub) {
			add("%s note 不含 %q，实际 %q", tbl, sub, r.Note)
		}
	}
	for tbl, want := range e.p1Basis {
		if r := findP1(rep, tbl); r == nil || r.Basis != want {
			got := "(表不在 path1)"
			if r != nil {
				got = r.Basis
			}
			add("%s basis=%s 期望 %s", tbl, got, want)
		}
	}
	for tbl, want := range e.p1RowsAfter {
		if r := findP1(rep, tbl); r != nil && r.RowsAfter != want {
			add("%s rows_after=%d 期望 %d", tbl, r.RowsAfter, want)
		}
	}

	if e.p2Unavailable && rep.Path2.Available {
		add("期望通路2 不可用/未产出，实际 available=true coverage=%s", rep.Path2.Coverage)
	}
	for tbl, want := range e.p2Has {
		r := findP2(rep, tbl)
		if r == nil {
			add("通路2 没检出 %s（期望 ins=%d upd=%d del=%d，结论=%s）",
				tbl, want.Ins, want.Upd, want.Del, rep.Conclusion)
			continue
		}
		if r.Ins != want.Ins || r.Upd != want.Upd || r.Del != want.Del {
			add("%s 通路2 ins/upd/del=%d/%d/%d 期望 %d/%d/%d",
				tbl, r.Ins, r.Upd, r.Del, want.Ins, want.Upd, want.Del)
		}
	}
	for _, tbl := range e.p2NoTable {
		if r := findP2(rep, tbl); r != nil {
			add("%s 不该出现在通路2 变化表，实际 ins=%d upd=%d del=%d", tbl, r.Ins, r.Upd, r.Del)
		}
	}
	if e.coverage != "" && rep.Path2.Coverage != e.coverage {
		add("通路2 coverage=%q 期望 %q（warning=%q）", rep.Path2.Coverage, e.coverage, rep.Path2.Warning)
	}
	if e.coverageAbsent && !rep.Path2.Enabled {
		add("coverageAbsent 校验要求 -wal-enable，但通路2 未启用")
	}
	if e.coverageAbsent && rep.Path2.Enabled && rep.Path2.Coverage != "" {
		add("通路2 未产出（available=%v）但 coverage=%q：none 只属于'读到了但无提交'", rep.Path2.Available, rep.Path2.Coverage)
	}
	for _, t := range e.p4Changed {
		found := false
		for _, fr := range rep.Path4.Results {
			if fr.Table == t && fr.Changed {
				found = true
				break
			}
		}
		if rep.Path4.Available && !found {
			add("通路4 应判定 %s 有变化，实际 %v", t, rep.Path4.Results)
		}
	}
	for _, sub := range e.p4ConclusionHas {
		if rep.Path4.Available && !strings.Contains(rep.Conclusion, sub) {
			add("结论（-pgdata 可用）不含 %q，实际 %q", sub, rep.Conclusion)
		}
	}
	for t, sub := range e.p4NoteHas {
		found := false
		for _, fr := range rep.Path4.Results {
			if fr.Table == t && strings.Contains(fr.Note, sub) {
				found = true
				break
			}
		}
		if rep.Path4.Available && !found {
			add("通路4 %s 的 note 应含 %q", t, sub)
		}
	}
	if e.p2WarningHas != "" && !strings.Contains(rep.Path2.Warning, e.p2WarningHas) {
		add("通路2 warning 不含 %q，实际 %q", e.p2WarningHas, rep.Path2.Warning)
	}
	if e.subxactMin > 0 && rep.Path2.SubxactMerged < e.subxactMin {
		add("subxact_merged=%d 期望 >=%d", rep.Path2.SubxactMerged, e.subxactMin)
	}
	if e.orphanZero && (rep.Path2.OrphanTx != 0 || rep.Path2.OrphanDML != 0) {
		add("不该有孤儿事务，实际 orphan_tx=%d orphan_dml=%d warning=%q",
			rep.Path2.OrphanTx, rep.Path2.OrphanDML, rep.Path2.Warning)
	}

	for _, sub := range e.conclusionHas {
		if !strings.Contains(rep.Conclusion, sub) {
			add("结论不含 %q，实际 %q", sub, rep.Conclusion)
		}
	}
	for _, sub := range e.conclusionNotHas {
		if strings.Contains(rep.Conclusion, sub) {
			add("结论不该含 %q，实际 %q", sub, rep.Conclusion)
		}
	}

	// tableCount 只在 >0 时校验：0 是"不校验"的零值，真要校验 0 用 zeroTables。
	if e.tableCount > 0 && rep.TableCount != e.tableCount {
		add("table_count=%d 期望 %d", rep.TableCount, e.tableCount)
	}
	if e.zeroTables && rep.TableCount != 0 {
		add("期望 table_count=0，实际 %d", rep.TableCount)
	}
	if e.discoveryHas != "" && !strings.Contains(rep.DiscoveryNote, e.discoveryHas) {
		add("discovery_note 不含 %q，实际 %q", e.discoveryHas, rep.DiscoveryNote)
	}
	if e.scanScopeHas != "" && !strings.Contains(rep.ScanScope, e.scanScopeHas) {
		add("scan_scope 不含 %q，实际 %q", e.scanScopeHas, rep.ScanScope)
	}
	if e.partitionHas != "" && !strings.Contains(rep.PartitionNote, e.partitionHas) {
		add("partition_note 不含 %q，实际 %q", e.partitionHas, rep.PartitionNote)
	}
	if e.path4Available && !rep.Path4.Available {
		add("通路4 期望可用，实际 note=%q", rep.Path4.Note)
	}
	if exit != e.exitCode {
		add("退出码 %d 期望 %d（stderr=%s）", exit, e.exitCode, firstLine(stderr))
	}
	return errs
}

func findP1(rep *report, tbl string) *p1Row {
	for i := range rep.Path1 {
		if rep.Path1[i].Table == tbl {
			return &rep.Path1[i]
		}
	}
	return nil
}

func findP2(rep *report, tbl string) *p2Row {
	for i := range rep.Path2.Changed {
		if rep.Path2.Changed[i].Table == tbl {
			return &rep.Path2.Changed[i]
		}
	}
	return nil
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// ---------- 场景定义 ----------

type testCase struct {
	ID      string
	Kind    string // 表种类轴
	TimeCol string // 时间列轴
	Tx      string // 事务轴
	Fault   string // 故障轴
	Setup   []string
	Args    []string
	SinceIn time.Duration // since = 现在 + 该值
	WAL     bool          // 是否加 -wal-enable（默认自动）
	NoWAL   bool          // 显式不带 -wal-enable
	Exp     expect
	Note    string
}

func (c testCase) axis() string {
	kind, tc, tx, ft := c.Kind, c.TimeCol, c.Tx, c.Fault
	if kind == "" {
		kind = "-"
	}
	if tc == "" {
		tc = "-"
	}
	if tx == "" {
		tx = "-"
	}
	if ft == "" {
		ft = "-"
	}
	return fmt.Sprintf("%s | %s | %s | %s", kind, tc, tx, ft)
}

// buildCases 生成全部格子。nowUTC / today 由库里取，避免脚本和库时区不一致。
func buildCases(nowUTC string) []testCase {
	const (
		old = "2020-01-01 00:00:00"
	)
	cs := []testCase{
		// ===== 轴1：普通表 × 时间列 =====
		{
			ID: "A01", Kind: "普通表", TimeCol: "timestamp", Tx: "单条INSERT",
			Setup: []string{`CREATE TABLE mx.a01(id int, update_time timestamp);
				INSERT INTO mx.a01 VALUES (1, '` + nowUTC + `')`},
			Args: []string{"-tables", "mx.a01"}, NoWAL: true,
			Exp: expect{p1Changed: []string{"mx.a01"}, p1Status: map[string]string{"mx.a01": "OK"},
				p1Basis:     map[string]string{"mx.a01": "update_time"},
				p1RowsAfter: map[string]int64{"mx.a01": 1}, tableCount: 1,
				conclusionHas: []string{"通路1 1 张"}},
			Note: "时间列值在时点后，通路1 直接命中",
		},
		{
			ID: "A02", Kind: "普通表", TimeCol: "timestamp", Tx: "单条INSERT",
			Setup: []string{`CREATE TABLE mx.a02(id int, update_time timestamp);
				INSERT INTO mx.a02 VALUES (1, '` + old + `')`},
			Args: []string{"-tables", "mx.a02"}, WAL: true,
			Exp: expect{p1NoChange: []string{"mx.a02"}, p1Status: map[string]string{"mx.a02": "OK"},
				p2Has:         map[string]opCount{"mx.a02": {Ins: 1}},
				coverage:      "full",
				conclusionHas: []string{"通路2 1 张"}},
			Note: "写入时间字段是旧值，通路1 看不到（口径内），要靠通路2 补",
		},
		{
			ID: "A03", Kind: "普通表", TimeCol: "timestamptz", Tx: "单条INSERT",
			Setup: []string{`CREATE TABLE mx.a03(id int, update_time timestamptz);
				INSERT INTO mx.a03 VALUES (1, now())`},
			Args: []string{"-tables", "mx.a03"}, NoWAL: true,
			Exp: expect{p1Changed: []string{"mx.a03"}, p1Status: map[string]string{"mx.a03": "OK"},
				p1RowsAfter: map[string]int64{"mx.a03": 1}},
			Note: "带时区列走绝对时刻比较",
		},
		{
			ID: "A04", Kind: "普通表", TimeCol: "date", Tx: "单条INSERT",
			Setup: []string{`CREATE TABLE mx.a04(id int, d date);
				INSERT INTO mx.a04 VALUES (1, current_date)`},
			Args: []string{"-tables", "mx.a04"}, NoWAL: true,
			Exp: expect{p1Status: map[string]string{"mx.a04": "AMBIG"},
				p1NoChange: []string{"mx.a04"}, conclusionHas: []string{"时点不确定"}},
			Note: "date 只有天精度，当天写入不能当无变化",
		},
		{
			ID: "A05", Kind: "普通表", TimeCol: "date", Tx: "单条INSERT",
			Setup: []string{`CREATE TABLE mx.a05(id int, d date);
				INSERT INTO mx.a05 VALUES (1, current_date - 1)`},
			Args: []string{"-tables", "mx.a05"}, NoWAL: true,
			Exp:  expect{p1Status: map[string]string{"mx.a05": "OK"}, p1NoChange: []string{"mx.a05"}},
			Note: "昨天写入，date 比较确实无变化，也不该误报 AMBIG",
		},
		{
			ID: "A06", Kind: "普通表", TimeCol: "无时间列", Tx: "单条INSERT",
			Setup: []string{`CREATE TABLE mx.a06(id int); INSERT INTO mx.a06 VALUES (1)`},
			Args:  []string{"-tables", "mx.a06"}, NoWAL: true,
			Exp: expect{p1Status: map[string]string{"mx.a06": "SKIP"},
				p1NoteHas: map[string]string{"mx.a06": "表内无时间类型字段"}},
			Note: "track_commit_timestamp=off，无时间列只能 SKIP 且要说清前因",
		},
		{
			ID: "A07", Kind: "普通表", TimeCol: "可空含NULL", Tx: "单条INSERT",
			Setup: []string{`CREATE TABLE mx.a07(id int, update_time timestamp);
				INSERT INTO mx.a07 VALUES (1, NULL)`},
			Args: []string{"-tables", "mx.a07"}, NoWAL: true,
			Exp: expect{p1Status: map[string]string{"mx.a07": "AMBIG"},
				p1NoChange:    []string{"mx.a07"},
				p1NoteHas:     map[string]string{"mx.a07": "NULL"},
				conclusionHas: []string{"时点不确定"}},
			Note: "时间列为 NULL 的行定位不到时点，不能报 OK/0 行",
		},
		{
			ID: "A08", Kind: "普通表", TimeCol: "NOT NULL 旧值", Tx: "单条INSERT",
			Setup: []string{`CREATE TABLE mx.a08(id int, update_time timestamp NOT NULL);
				INSERT INTO mx.a08 VALUES (1, '` + old + `')`},
			Args: []string{"-tables", "mx.a08"}, NoWAL: true,
			Exp:  expect{p1Status: map[string]string{"mx.a08": "OK"}, p1NoChange: []string{"mx.a08"}},
			Note: "NOT NULL 列不该被 NULL 补查带偏成 AMBIG",
		},
		{
			ID: "A09", Kind: "普通表", TimeCol: "多时间列(两 date)", Tx: "单条INSERT",
			Setup: []string{`CREATE TABLE mx.a09(id int, start_day date, end_day date);
				INSERT INTO mx.a09 VALUES (1, current_date - 2, current_date + 1)`},
			Args: []string{"-tables", "mx.a09"}, NoWAL: true,
			Exp: expect{p1Status: map[string]string{"mx.a09": "ERROR"},
				p1NoteHas: map[string]string{"mx.a09": "-time-col"}},
			Note: "两个非候选时间列无法自动选，必须提示用 -time-col 而不是硬挑一个",
		},
		{
			ID: "A10", Kind: "普通表", TimeCol: "-time-col 指到非时间列", Tx: "单条INSERT",
			Setup: []string{`CREATE TABLE mx.a10(id int, update_time timestamp);
				INSERT INTO mx.a10 VALUES (1, '` + nowUTC + `')`},
			Args: []string{"-tables", "mx.a10", "-time-col", "id"}, NoWAL: true,
			Exp: expect{p1Status: map[string]string{"mx.a10": "ERROR"},
				p1NoteHas: map[string]string{"mx.a10": "不是时间类型"}},
			Note: "指错列要分诊成'类型不对'，不能笼统说'列不存在'",
		},
		{
			ID: "A11", Kind: "普通表", TimeCol: "-time-col 指定 date", Tx: "单条INSERT",
			Setup: []string{`CREATE TABLE mx.a11(id int, start_day date, end_day date);
				INSERT INTO mx.a11 VALUES (1, current_date - 2, current_date + 1)`},
			Args: []string{"-tables", "mx.a11", "-time-col", "end_day"}, NoWAL: true,
			Exp:  expect{p1Changed: []string{"mx.a11"}, p1Status: map[string]string{"mx.a11": "OK"}},
			Note: "指定列后应能正常判定（end_day 是明天，确定在时点后）",
		},

		// ===== 轴2：分区 / 继承 / 物化视图 =====
		{
			ID: "B01", Kind: "分区父表", TimeCol: "timestamp", Tx: "单条INSERT(落子表)",
			Setup: []string{`CREATE TABLE mx.b01(id int, update_time timestamp) PARTITION BY RANGE (id);
				CREATE TABLE mx.b01_p1 PARTITION OF mx.b01 FOR VALUES FROM (0) TO (100);
				INSERT INTO mx.b01 VALUES (5, '` + old + `')`},
			Args: []string{"-tables", "mx.b01"}, WAL: true,
			Exp: expect{p2Has: map[string]opCount{"mx.b01": {Ins: 1}}, coverage: "full",
				partitionHas: "展开 1 个子表"},
			Note: "父表 relfilenode=0，DML 挂子表上，必须归到父表名下",
		},
		{
			ID: "B02", Kind: "分区子表", TimeCol: "timestamp", Tx: "单条INSERT(落子表)",
			Setup: []string{`CREATE TABLE mx.b02(id int, update_time timestamp) PARTITION BY RANGE (id);
				CREATE TABLE mx.b02_p1 PARTITION OF mx.b02 FOR VALUES FROM (0) TO (100);
				INSERT INTO mx.b02 VALUES (5, '` + old + `')`},
			Args: []string{"-tables", "mx.b02_p1"}, WAL: true,
			Exp: expect{p2Has: map[string]opCount{"mx.b02_p1": {Ins: 1}},
				p2NoTable: []string{"mx.b02"}},
			Note: "单独点名子表时按子表成行，不该再算到父表",
		},
		{
			ID: "B03", Kind: "多级分区", TimeCol: "timestamp", Tx: "单条INSERT(落孙表)",
			Setup: []string{`CREATE TABLE mx.b03(id int, update_time timestamp) PARTITION BY RANGE (id);
				CREATE TABLE mx.b03_a PARTITION OF mx.b03 FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (id);
				CREATE TABLE mx.b03_a1 PARTITION OF mx.b03_a FOR VALUES FROM (0) TO (50);
				INSERT INTO mx.b03 VALUES (10, '` + old + `')`},
			Args: []string{"-tables", "mx.b03"}, WAL: true,
			Exp: expect{p2Has: map[string]opCount{"mx.b03": {Ins: 1}}, coverage: "full",
				partitionHas: "展开 2 个子表"},
			Note: "两层分区只指定顶层，孙表也要归到顶层",
		},
		{
			ID: "B04", Kind: "分区父子同查", TimeCol: "timestamp", Tx: "单条INSERT(落子表)",
			Setup: []string{`CREATE TABLE mx.b04(id int, update_time timestamp) PARTITION BY RANGE (id);
				CREATE TABLE mx.b04_p1 PARTITION OF mx.b04 FOR VALUES FROM (0) TO (100);
				INSERT INTO mx.b04 VALUES (5, '` + old + `')`},
			Args: []string{"-tables", "mx.b04,mx.b04_p1"}, WAL: true,
			Exp: expect{p2Has: map[string]opCount{"mx.b04_p1": {Ins: 1}},
				p2NoTable:    []string{"mx.b04"},
				partitionHas: "父表那行不再重复计入"},
			Note: "父子同查：子表自己成行，父表不再重复计入，提示要写清口径",
		},
		{
			ID: "B05", Kind: "INHERITS 父表", TimeCol: "timestamp", Tx: "单条INSERT(落子表)",
			Setup: []string{`CREATE TABLE mx.b05(id int, update_time timestamp);
				CREATE TABLE mx.b05_c() INHERITS (mx.b05);
				INSERT INTO mx.b05_c VALUES (1, '` + old + `')`},
			Args: []string{"-tables", "mx.b05"}, WAL: true,
			Exp: expect{p2Has: map[string]opCount{"mx.b05": {Ins: 1}}, coverage: "full",
				p4Changed: []string{"mx.b05"},
				p4NoteHas: map[string]string{"mx.b05": "子表文件"}},
			Note: "老式继承也走 pg_inherits，行为应与分区父表一致；写入在子表文件上，通路4 汇总子表 mtime，父表不得漏报",
		},
		{
			ID: "C01", Kind: "物化视图", TimeCol: "timestamp", Tx: "建视图(无新写入)",
			Setup: []string{`CREATE TABLE mx.c01_src(id int, update_time timestamp);
				INSERT INTO mx.c01_src VALUES (1, '` + nowUTC + `');
				CREATE MATERIALIZED VIEW mx.c01 AS SELECT id, update_time FROM mx.c01_src`},
			Args: []string{"-tables", "mx.c01"}, NoWAL: true,
			Exp: expect{p1Status: map[string]string{"mx.c01": "OK"},
				p1Basis: map[string]string{"mx.c01": "update_time"}},
			Note: "金仓信息模式不含物化视图列，必须走系统目录兜底，否则报 SKIP 误导",
		},
		{
			ID: "C02", Kind: "视图", TimeCol: "无时间列", Tx: "建视图",
			Setup: []string{`CREATE VIEW mx.c02 AS SELECT 1 AS id`},
			Args:  []string{"-tables", "mx.c02"}, NoWAL: true,
			Exp: expect{p1Status: map[string]string{"mx.c02": "SKIP"},
				p1NoteHas:     map[string]string{"mx.c02": "这是视图"},
				conclusionHas: []string{"未判定"}},
			Note: "视图无时间列应 SKIP",
		},

		{
			ID: "C04", Kind: "物化视图", TimeCol: "timestamp", Tx: "REFRESH",
			Setup: []string{`CREATE TABLE mx.c04_src(id int, update_time timestamp);
				INSERT INTO mx.c04_src VALUES (1, '` + old + `');
				CREATE MATERIALIZED VIEW mx.c04 AS SELECT id, update_time FROM mx.c04_src WITH NO DATA;
				REFRESH MATERIALIZED VIEW mx.c04`},
			Args: []string{"-tables", "mx.c04"}, WAL: true,
			Exp:  expect{p2Has: map[string]opCount{"mx.c04": {Ins: 1}}, coverage: "full"},
			Note: "REFRESH 换新 relfilenode，能映射到当前值才检得出（口径里写过，之前没实测）",
		},

		// ===== 轴3：事务形状（通路2） =====
		{
			ID: "D01", Kind: "普通表", TimeCol: "timestamp", Tx: "批量INSERT(多行)",
			Setup: []string{`CREATE TABLE mx.d01(id int, update_time timestamp);
				INSERT INTO mx.d01 SELECT g, '` + old + `' FROM generate_series(1,3) g`},
			Args: []string{"-tables", "mx.d01"}, WAL: true,
			Exp:  expect{p2Has: map[string]opCount{"mx.d01": {Ins: 3}}, coverage: "full"},
			Note: "一条 WAL 记录塞多行（MULTI_INSERT），要按 num 计行数而不是计记录数",
		},
		{
			ID: "D02", Kind: "普通表", TimeCol: "timestamp", Tx: "UPDATE 不改时间列",
			Setup: []string{`CREATE TABLE mx.d02(id int, v int, update_time timestamp);
				INSERT INTO mx.d02 VALUES (1, 1, '` + old + `');
				UPDATE mx.d02 SET v = 2 WHERE id = 1`},
			Args: []string{"-tables", "mx.d02"}, WAL: true,
			// INSERT 和 UPDATE 在同一个隐式事务里提交，两条都落在窗口内，所以是 1/1；
			// 这个格子只关心"UPdATE 有没有被检出"，不追求只出现 UPDATE。
			Exp: expect{p1NoChange: []string{"mx.d02"},
				p2Has:    map[string]opCount{"mx.d02": {Ins: 1, Upd: 1}},
				coverage: "full"},
			Note: "不改时间列的 UPDATE 是通路1 的已知盲点，通路2 必须补上",
		},
		{
			ID: "D03", Kind: "普通表", TimeCol: "timestamp", Tx: "DELETE",
			Setup: []string{`CREATE TABLE mx.d03(id int, update_time timestamp);
				INSERT INTO mx.d03 VALUES (1, '` + old + `');
				DELETE FROM mx.d03 WHERE id = 1`},
			Args: []string{"-tables", "mx.d03"}, WAL: true,
			Exp:  expect{p2Has: map[string]opCount{"mx.d03": {Ins: 1, Del: 1}}, coverage: "full"},
			Note: "DELETE 只在通路2 可见（通路1 数行数查不出）",
		},
		{
			ID: "D04", Kind: "普通表", TimeCol: "timestamp", Tx: "SAVEPOINT 提交",
			Setup: []string{`CREATE TABLE mx.d04(id int, update_time timestamp);
				BEGIN; SAVEPOINT s1; INSERT INTO mx.d04 VALUES (1, '` + old + `'); RELEASE SAVEPOINT s1; COMMIT`},
			Args: []string{"-tables", "mx.d04"}, WAL: true,
			Exp: expect{p2Has: map[string]opCount{"mx.d04": {Ins: 1}}, coverage: "full",
				subxactMin: 1, orphanZero: true},
			Note: "子事务 DML 记子 xid，COMMIT 记父 xid，不解析 subxacts 就整条漏掉",
		},
		{
			ID: "D05", Kind: "普通表", TimeCol: "timestamp", Tx: "SAVEPOINT 回滚后再提交",
			Setup: []string{`CREATE TABLE mx.d05(id int, update_time timestamp);
				BEGIN; SAVEPOINT s1; INSERT INTO mx.d05 VALUES (1, '` + old + `'); ROLLBACK TO SAVEPOINT s1;
				INSERT INTO mx.d05 VALUES (2, '` + old + `'); COMMIT`},
			Args: []string{"-tables", "mx.d05"}, WAL: true,
			Exp:  expect{p2Has: map[string]opCount{"mx.d05": {Ins: 1}}, coverage: "full"},
			Note: "回滚掉的那行不该算，剩下的 1 行要算",
		},
		{
			ID: "D06", Kind: "普通表", TimeCol: "timestamp", Tx: "整体 ROLLBACK",
			Setup: []string{`CREATE TABLE mx.d06(id int, update_time timestamp);
				BEGIN; INSERT INTO mx.d06 VALUES (1, '` + old + `'); ROLLBACK`},
			Args: []string{"-tables", "mx.d06"}, WAL: true,
			Exp:  expect{p2NoTable: []string{"mx.d06"}, orphanZero: true},
			Note: "回滚事务的 DML 不该计入，也不该报'找不到 COMMIT'的孤儿告警",
		},
		{
			ID: "D07", Kind: "普通表", TimeCol: "timestamp", Tx: "TRUNCATE",
			Setup: []string{`CREATE TABLE mx.d07(id int, update_time timestamp);
				INSERT INTO mx.d07 VALUES (1, '` + old + `'); TRUNCATE mx.d07`},
			Args: []string{"-tables", "mx.d07"}, WAL: true,
			Exp: expect{p2Has: map[string]opCount{"mx.d07": {Ins: 1}},
				// 金仓 TRUNCATE 只有 Storage CREATE 新文件 + pg_class 更新，通路2 无 DML 可匹配、
				// 通路1 表空、通路3 计数不动——唯一信号是通路4 新文件 mtime，结论必须把它亮出来。
				p4Changed:       []string{"mx.d07"},
				p4ConclusionHas: []string{"另有 1 张文件 mtime 晚于指定时间", "通路4 仅物理信号"}},
			Note: "TRUNCATE 改 relfilenode，通路2 不覆盖；INSERT 部分锁定'不被误报成 DELETE'，TRUNCATE 本身靠通路4 信号提示",
		},

		// ===== 轴4：故障注入 =====
		{
			ID: "E01", Kind: "普通表", TimeCol: "timestamp", Fault: "wal 目录不存在",
			Setup: []string{`CREATE TABLE mx.e01(id int, update_time timestamp);
				INSERT INTO mx.e01 VALUES (1, '` + old + `')`},
			Args: []string{"-tables", "mx.e01", "-wal-enable", "-wal", "/nope/wal"}, NoWAL: true,
			Exp:  expect{p2Unavailable: true, coverageAbsent: true, conclusionHas: []string{"通路2 未产出"}},
			Note: "读不到 WAL 要说'未产出'，不能给'0 张'让人以为查过了；JSON 里也不得出现 coverage=none",
		},
		{
			ID: "E02", Kind: "普通表", TimeCol: "timestamp", Fault: "waldump 执行失败",
			Setup: []string{`CREATE TABLE mx.e02(id int, update_time timestamp);
				INSERT INTO mx.e02 VALUES (1, '` + old + `')`},
			Args: []string{"-tables", "mx.e02", "-wal-enable", "-wal-bin", "/bin/false"}, NoWAL: true,
			Exp:  expect{p2Unavailable: true, coverageAbsent: true, conclusionHas: []string{"通路2 未产出"}},
			Note: "waldump 挂了不能被覆盖判定覆盖成 0 张；JSON 里也不得出现 coverage=none",
		},
		{
			ID: "E03", Kind: "普通表", TimeCol: "timestamp", Fault: "since 在未来",
			Setup: []string{`CREATE TABLE mx.e03(id int, update_time timestamp);
				INSERT INTO mx.e03 VALUES (1, '` + old + `')`},
			Args: []string{"-tables", "mx.e03"}, WAL: true, SinceIn: 24 * time.Hour,
			Exp:  expect{coverage: "none", p2NoTable: []string{"mx.e03"}},
			Note: "时点在未来，应判 none 且不列出任何变化",
		},
		{
			ID: "E04", Kind: "普通表", TimeCol: "timestamp", Fault: "since 早于 WAL 起点",
			Setup: []string{`CREATE TABLE mx.e04(id int, update_time timestamp);
				INSERT INTO mx.e04 VALUES (1, '` + old + `')`},
			Args: []string{"-tables", "mx.e04", "-since", "2000-01-01 00:00:00"},
			WAL:  true,
			Exp: expect{coverage: "partial", p2Has: map[string]opCount{"mx.e04": {Ins: 1}},
				p2WarningHas: "不在可读 WAL 内"},
			Note: "since 早于可读 WAL，必须告警 partial，不能假装全覆盖",
		},
		{
			ID: "E05", Kind: "普通表", TimeCol: "-", Fault: "表名不存在",
			Setup: []string{`CREATE TABLE mx.e05(id int)`},
			Args:  []string{"-tables", "mx.e05_nosuch"}, WAL: true,
			Exp: expect{p1Status: map[string]string{"mx.e05_nosuch": "SKIP"},
				p2Unavailable: true, conclusionHas: []string{"通路2 未产出"}},
			Note: "表不存在时通路2 白名单为空，不能报'覆盖=full、0 张'",
		},
		{
			ID: "E06", Kind: "普通表", TimeCol: "-", Fault: "模式不存在",
			Setup: []string{`CREATE TABLE mx.e06(id int)`},
			Args:  []string{"-schema", "nosuchschema"}, NoWAL: true,
			Exp: expect{zeroTables: true, conclusionHas: []string{"未检查到任何表"},
				conclusionNotHas: []string{"有数据变化的表：0 张"}},
			Note: "一张表都没查，不能输出'0 张变化'这种'查过没变'的措辞",
		},
		{
			ID: "E07", Kind: "普通表", TimeCol: "-", Fault: "-limit 截断",
			Setup: []string{`CREATE TABLE mxs.e07a(id int); CREATE TABLE mxs.e07b(id int)`},
			Args:  []string{"-schema", schemaScan, "-limit", "1"}, NoWAL: true,
			Exp:  expect{tableCount: 1, discoveryHas: "仅检查按名称排序的前"},
			Note: "-limit 截断必须告警，否则漏表静默",
		},
		{
			ID: "C03", Kind: "扫描模式", TimeCol: "timestamp", Fault: "扫描范围说明",
			Setup: []string{`CREATE TABLE mxs.c03(id int, update_time timestamp);
				INSERT INTO mxs.c03 VALUES (1, '` + old + `');
				CREATE MATERIALIZED VIEW mxs.c03_mv AS SELECT * FROM mxs.c03`},
			Args: []string{"-schema", schemaScan, "-limit", "50"}, NoWAL: true,
			Exp:  expect{scanScopeHas: "物化视图", countSchema: schemaScan},
			Note: "扫描模式只含普通表/分区表，物化视图要明说不在自动发现内",
		},
	}
	return cs
}

// ---------- 执行 ----------

type result struct {
	c      testCase
	errs   []string
	stdout string
	stderr string
}

func main() {
	dsn := flag.String("dsn", "", "库连接串")
	bin := flag.String("bin", "./kbchange", "kbchange 二进制")
	walRoot := flag.String("wal-root", "", "容器 root，如 /proc/<pid>/root")
	pgdata := flag.String("pgdata", "", "通路4 数据目录（可选）")
	only := flag.String("only", "", "只跑这些 ID，逗号分隔")
	verbose := flag.Bool("v", false, "FAIL 时打印完整 JSON")
	keep := flag.Bool("keep", false, "跑完不清理 mx/mxs 模式（排障用）")
	flag.Parse()

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "必须给 -dsn")
		os.Exit(2)
	}
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "连接失败:", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		fmt.Fprintln(os.Stderr, "连接失败:", err)
		os.Exit(1)
	}

	// 库里的"现在"和"今天"：timestamp 是墙上时间，必须按库时区取值，
	// 否则脚本按 OS 时区写进去的时间会在 DB 时区里偏 8 小时。
	var nowUTC, today string
	if err := db.QueryRow(`SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), to_char(current_date, 'YYYY-MM-DD')`).
		Scan(&nowUTC, &today); err != nil {
		fmt.Fprintln(os.Stderr, "取库时间失败:", err)
		os.Exit(1)
	}

	fmt.Printf("目标库时间：%s（库时区）  今天=%s\n", nowUTC, today)
	if err := resetSchemas(db); err != nil {
		fmt.Fprintln(os.Stderr, "初始化模式失败:", err)
		os.Exit(1)
	}
	defer func() {
		if *keep {
			fmt.Println("已保留 mx/mxs 模式（-keep）")
			return
		}
		// 收尾只删不建：resetSchemas 建完就走，会留下两个空模式。
		if _, err := db.Exec(`DROP SCHEMA IF EXISTS mx CASCADE; DROP SCHEMA IF EXISTS mxs CASCADE`); err != nil {
			fmt.Fprintln(os.Stderr, "清理失败:", err)
		}
	}()

	filter := map[string]bool{}
	for _, s := range strings.Split(*only, ",") {
		if s = strings.TrimSpace(s); s != "" {
			filter[s] = true
		}
	}

	var results []result
	for _, c := range buildCases(nowUTC) {
		if len(filter) > 0 && !filter[c.ID] {
			continue
		}
		results = append(results, runCase(db, *dsn, c, *bin, *walRoot, *pgdata))
	}

	// 输出
	fails := 0
	fmt.Println()
	fmt.Println("== 逐格结果 ==")
	for _, r := range results {
		tag := "PASS"
		if len(r.errs) > 0 {
			tag = "FAIL"
			fails++
		}
		fmt.Printf("%s %-4s %-58s %s\n", tag, r.c.ID, r.c.axis(), r.c.Note)
		for _, e := range r.errs {
			fmt.Printf("       - %s\n", e)
		}
		if len(r.errs) > 0 && *verbose {
			fmt.Printf("       stdout: %s\n", strings.ReplaceAll(r.stdout, "\n", " "))
			if r.stderr != "" {
				fmt.Printf("       stderr: %s\n", strings.ReplaceAll(r.stderr, "\n", " "))
			}
		}
	}

	printAxisSummary(results)

	fmt.Printf("\n合计 %d 格：PASS %d，FAIL %d\n", len(results), len(results)-fails, fails)
	if fails > 0 {
		os.Exit(1)
	}
}

func runCase(db *sql.DB, dsn string, c testCase, bin, walRoot, pgdata string) result {
	res := result{c: c}
	for _, s := range c.Setup {
		if _, err := db.Exec(s); err != nil {
			res.errs = append(res.errs, "setup 失败: "+err.Error())
			return res
		}
	}
	// 给 WAL / 统计留一点落地时间：提交立刻可见，但 WAL 段刷盘和统计行建立不是同步的。
	time.Sleep(150 * time.Millisecond)

	since := time.Now().Add(-10 * time.Minute)
	if c.SinceIn != 0 {
		since = time.Now().Add(c.SinceIn)
	}
	// 显式给了 -since 的场景不覆盖
	hasSince := false
	for i := 0; i < len(c.Args); i++ {
		if c.Args[i] == "-since" {
			hasSince = true
		}
	}

	args := append([]string{"-dsn", dsn}, c.Args...)
	if !hasSince {
		args = append(args, "-since", since.Format("2006-01-02 15:04:05"))
	}
	args = append(args, "-json")
	if c.WAL && !c.NoWAL {
		args = append(args, "-wal-enable")
		if walRoot != "" {
			args = append(args, "-wal-root", walRoot)
		}
	}
	if pgdata != "" {
		args = append(args, "-pgdata", pgdata)
	}

	cmd := exec.Command(bin, args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	cmd.Env = os.Environ()
	runErr := cmd.Run()
	exit := 0
	if ee, ok := runErr.(*exec.ExitError); ok {
		exit = ee.ExitCode()
	} else if runErr != nil {
		res.errs = append(res.errs, "启动 kbchange 失败: "+runErr.Error())
		return res
	}
	res.stdout = out.String()
	res.stderr = errb.String()

	var rep report
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		res.errs = append(res.errs, "JSON 解析失败: "+err.Error())
		return res
	}
	res.errs = append(res.errs, c.Exp.check(&rep, exit, res.stderr)...)
	if sch := c.Exp.countSchema; sch != "" {
		var real int
		q := `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relkind IN ('r','p')`
		if err := db.QueryRow(q, sch).Scan(&real); err != nil {
			res.errs = append(res.errs, "取真实表数失败: "+err.Error())
		} else if rep.TableCount != real {
			res.errs = append(res.errs, fmt.Sprintf("table_count=%d 与库里 relkind r,p 的实际数量 %d 不一致",
				rep.TableCount, real))
		}
	}
	return res
}

func resetSchemas(db *sql.DB) error {
	_, err := db.Exec(`DROP SCHEMA IF EXISTS mx CASCADE; DROP SCHEMA IF EXISTS mxs CASCADE;
		CREATE SCHEMA mx; CREATE SCHEMA mxs;`)
	return err
}

// printAxisSummary 按轴聚合，直接露出"哪一类没覆盖/挂了"。
func printAxisSummary(rs []result) {
	type bucket struct{ pass, fail int }
	axes := []struct {
		name string
		get  func(testCase) string
	}{
		{"表种类", func(c testCase) string { return orDash(c.Kind) }},
		{"时间列", func(c testCase) string { return orDash(c.TimeCol) }},
		{"事务  ", func(c testCase) string { return orDash(c.Tx) }},
		{"故障  ", func(c testCase) string { return orDash(c.Fault) }},
	}
	for _, ax := range axes {
		m := map[string]*bucket{}
		for _, r := range rs {
			k := ax.get(r.c)
			if m[k] == nil {
				m[k] = &bucket{}
			}
			if len(r.errs) > 0 {
				m[k].fail++
			} else {
				m[k].pass++
			}
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var parts []string
		for _, k := range keys {
			b := m[k]
			if b.fail > 0 {
				parts = append(parts, fmt.Sprintf("%s %d/%d✗", k, b.pass, b.pass+b.fail))
			} else {
				parts = append(parts, fmt.Sprintf("%s %d/%d", k, b.pass, b.pass+b.fail))
			}
		}
		fmt.Printf("  %s：%s\n", ax.name, strings.Join(parts, "  "))
	}
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
