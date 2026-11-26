package wal

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSegNameToStartLSN(t *testing.T) {
	cases := []struct {
		name    string
		segSize uint64
		want    string
	}{
		{"000000010000000000000006", 16 * 1024 * 1024, "0/6000000"},
		{"00000001000000000000000A", 16 * 1024 * 1024, "0/A000000"},
		{"000000010000000100000000", 16 * 1024 * 1024, "1/0"},
		{"000000010000000000000001", 64 * 1024 * 1024, "0/4000000"},
	}
	for _, c := range cases {
		got, err := SegNameToStartLSN(c.name, c.segSize)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s segSize=%d: got %s, want %s", c.name, c.segSize, got, c.want)
		}
	}
	if _, err := SegNameToStartLSN("bad", 0); err == nil {
		t.Error("bad seg name should error")
	}
}

func TestParseCommitTime(t *testing.T) {
	cases := []struct {
		ts, zone string
		want     string
	}{
		{"2026-11-17 08:45:31.473489", "CST", "2026-11-17 08:45:31"},
		{"2026-11-17 00:45:31", "UTC", "2026-11-17 00:45:31"},
	}
	for _, c := range cases {
		got, err := ParseCommitTime(c.ts, c.zone, time.UTC)
		if err != nil {
			t.Errorf("%s: %v", c.ts, err)
			continue
		}
		if f := got.Format("2006-01-02 15:04:05"); f != c.want {
			t.Errorf("%s %s: got %s, want %s", c.ts, c.zone, f, c.want)
		}
	}
	// PG 风格：偏移紧贴时间，zone 被正则抓成 +08
	got, err := ParseCommitTime("2026-11-17 08:45:31.473489", "+08", time.UTC)
	if err != nil {
		t.Fatalf("+08 parse: %v", err)
	}
	if want := "2026-11-17 00:45:31 UTC"; got.UTC().Format("2006-01-02 15:04:05 MST") != want {
		t.Errorf("+08 => %s, want %s", got.UTC().Format("2006-01-02 15:04:05 MST"), want)
	}
	if _, err := ParseCommitTime("not-a-time", "UTC", time.UTC); err == nil {
		t.Error("invalid time should error")
	}
}

func TestResolveWALOverride(t *testing.T) {
	root := "/proc/123/root"
	dataDir := root + "/home/kingbase/userdata/data"
	// 相对名按数据目录拼
	if got := ResolveWALOverride("sys_wal", root, dataDir); got != dataDir+"/sys_wal" {
		t.Errorf("relative = %q", got)
	}
	// 容器内绝对路径加 root
	if got := ResolveWALOverride("/home/kingbase/userdata/data/sys_wal", root, dataDir); got != dataDir+"/sys_wal" {
		t.Errorf("abs = %q", got)
	}
	// 已经是宿主机路径不动
	host := root + "/tmp/wal"
	if got := ResolveWALOverride(host, root, dataDir); got != host {
		t.Errorf("host path = %q", got)
	}
	// 没给 root 时原样返回
	if got := ResolveWALOverride("/tmp/wal", "", dataDir); got != "/tmp/wal" {
		t.Errorf("no root = %q", got)
	}
}

func TestScanWaldumpPGCommitWithOffset(t *testing.T) {
	input := `rmgr: Transaction len (rec/tot): 34/34, tx: 5, lsn: 0/1000028, prev 0/0, desc: COMMIT 2026-11-17 08:45:31+08
`
	res := ScanWaldumpOutput(strings.NewReader(input), time.UTC, 0, nil)
	if len(res.Commits) != 1 {
		t.Fatalf("PG 风格 COMMIT 没解析出来: %+v", res)
	}
	want := "2026-11-17 00:45:31"
	if got := res.Commits[5].UTC().Format("2006-01-02 15:04:05"); got != want {
		t.Errorf("commit = %s, want %s", got, want)
	}
}

func TestScanWaldumpOutput(t *testing.T) {
	input := `could not identify current directory: No such file or directory
rmgr: XLOG        len (rec/tot):    114/   114, tx:          0, lsn: 0/06000028, prev 0/0504E8A8, desc: CHECKPOINT_SHUTDOWN redo 0/6000028
rmgr: Heap        len (rec/tot):     54/  1950, tx:        890, lsn: 0/06000120, prev 0/060000F0, desc: INSERT off 8 flags 0x00, blkref #0: rel 1664/0/1262 blk 0 FPW
rmgr: Heap        len (rec/tot):     54/   3798, tx:        890, lsn: 0/06000CC8, prev 0/06000AD0, desc: INSERT off 28 flags 0x01, blkref #0: rel 1663/16384/2615 blk 0 FPW
rmgr: Heap        len (rec/tot):     54/   3798, tx:        890, lsn: 0/06000CC9, prev 0/06000CC8, desc: UPDATE off 3, blkref #0: rel 1663/16384/2615 blk 0
rmgr: Heap        len (rec/tot):     54/   3798, tx:        891, lsn: 0/06000DD0, prev 0/06000CC9, desc: DELETE off 5, blkref #0: rel 1663/16384/2615 blk 0
rmgr: Heap        len (rec/tot):     54/   3798, tx:        892, lsn: 0/06000EE0, prev 0/06000DD0, desc: INSERT off 6, blkref #0: rel 1663/99999/2615 blk 0
rmgr: Transaction len (rec/tot):     66/    66, tx:        890, lsn: 0/06000C50, prev 0/06000BD8, desc: COMMIT 2026-11-17 08:45:31.473489 UTC; inval SIMsgs: catcache 24
rmgr: Transaction len (rec/tot):     34/    34, tx:        891, lsn: 0/06000C60, prev 0/06000C50, desc: COMMIT 2026-11-17 08:47:52 UTC
`
	res := ScanWaldumpOutput(strings.NewReader(input), time.UTC, 16384, nil)
	if res.Scanned != 8 {
		t.Errorf("scanned = %d, want 8", res.Scanned)
	}
	if len(res.Commits) != 2 {
		t.Errorf("commits = %d, want 2", len(res.Commits))
	}
	if res.ParseErrs != 0 {
		t.Errorf("parse errs = %d", res.ParseErrs)
	}
	// tx 890：db16384 的 2615 = 1 insert + 1 update；db0 的 1262 应被 currentDBOID 过滤掉
	m890 := res.DMLByTx[890]
	if got := m890[WalDMLKey{DBOID: 16384, TS: 1663, RelFile: 2615, Op: "ins"}]; got != 1 {
		t.Errorf("tx890 ins = %d, want 1", got)
	}
	if got := m890[WalDMLKey{DBOID: 16384, TS: 1663, RelFile: 2615, Op: "upd"}]; got != 1 {
		t.Errorf("tx890 upd = %d, want 1", got)
	}
	if got := m890[WalDMLKey{DBOID: 0, RelFile: 1262, Op: "ins"}]; got != 0 {
		t.Errorf("db0 should be filtered, got %d", got)
	}
	// tx 891：delete
	if got := res.DMLByTx[891][WalDMLKey{DBOID: 16384, TS: 1663, RelFile: 2615, Op: "del"}]; got != 1 {
		t.Errorf("tx891 del = %d, want 1", got)
	}
	// tx 892：db99999 应被过滤
	if len(res.DMLByTx[892]) != 0 {
		t.Errorf("tx892 should be empty after db filter, got %v", res.DMLByTx[892])
	}
	// 时间窗
	if res.CommitMin.Format("2006-01-02 15:04:05") != "2026-11-17 08:45:31" {
		t.Errorf("commitMin = %s", res.CommitMin)
	}
	if res.CommitMax.Format("2006-01-02 15:04:05") != "2026-11-17 08:47:52" {
		t.Errorf("commitMax = %s", res.CommitMax)
	}
}

func TestScanWaldumpOutputCurrentDBOIDZero(t *testing.T) {
	input := `rmgr: Heap        len (rec/tot): 54/54, tx: 1, lsn: 0/1000028, prev 0/0, desc: INSERT off 1, blkref #0: rel 1664/0/1262 blk 0
`
	res := ScanWaldumpOutput(strings.NewReader(input), time.UTC, 0, nil)
	if got := res.DMLByTx[1][WalDMLKey{DBOID: 0, TS: 1664, RelFile: 1262, Op: "ins"}]; got != 1 {
		t.Errorf("with OID=0 no filter expected, got %d", got)
	}
}

func TestLocateWALDir(t *testing.T) {
	// override 直接用（目录存在）
	got, err := LocateWALDir("/no/such/data", "/tmp")
	if err != nil || got != "/tmp" {
		t.Errorf("override: got %s err %v", got, err)
	}
	// 指定不存在的路径报错
	if _, err := LocateWALDir("/no/such/data", "/no/such/wal"); err == nil {
		t.Error("nonexistent override should error")
	}
	// 无数据目录报错
	if _, err := LocateWALDir("", ""); err == nil {
		t.Error("empty data dir should error")
	}
}

func TestCoverageDecision(t *testing.T) {
	// 直接测生产函数，别在测试里复制一份逻辑。
	// since 落在 [commitMin, commitMax] => full（WAL 跨过指定时点）
	// since < commitMin => partial（WAL 起点晚于指定时点）
	// since >= commitMax => none（时点后没有提交）
	base := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	commitMin := base.Add(1 * time.Hour) // 01:00
	commitMax := base.Add(3 * time.Hour) // 03:00
	cases := []struct {
		since time.Time
		want  WalCoverage
	}{
		{base.Add(2 * time.Hour), WalFull},
		{base, WalPartial},
		{base.Add(4 * time.Hour), WalNone},
		{commitMin, WalFull},
		{commitMax, WalNone},
	}
	for _, c := range cases {
		got, from, warn := DecideWALCoverage(c.since, time.UTC, commitMin, commitMax)
		if got != c.want {
			t.Errorf("since=%s: got %s, want %s (from=%s warn=%s)", c.since, got, c.want, from, warn)
		}
		if from == "" {
			t.Errorf("since=%s: compare_from 空", c.since)
		}
	}
	// partial/none 必须带告警；full 不应告警。
	if _, _, w := DecideWALCoverage(base, time.UTC, commitMin, commitMax); !strings.Contains(w, "晚于指定时点") {
		t.Errorf("partial 告警异常: %s", w)
	}
	if _, _, w := DecideWALCoverage(base.Add(4*time.Hour), time.UTC, commitMin, commitMax); !strings.Contains(w, "时点后无已提交事务") {
		t.Errorf("none 告警异常: %s", w)
	}
	if _, _, w := DecideWALCoverage(base.Add(2*time.Hour), time.UTC, commitMin, commitMax); w != "" {
		t.Errorf("full 不应有告警: %s", w)
	}
}

func TestParseSizeText(t *testing.T) {
	cases := map[string]uint64{
		"16MB":     16 * 1024 * 1024,
		"16MB ":    16 * 1024 * 1024,
		"64mb":     64 * 1024 * 1024,
		"65536kB":  64 * 1024 * 1024,
		"16777216": 16 * 1024 * 1024,
		"1GB":      1 << 30,
	}
	for in, want := range cases {
		got, err := ParseSizeText(in)
		if err != nil || got != want {
			t.Errorf("ParseSizeText(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "abc", "MB", "16XB"} {
		if _, err := ParseSizeText(in); err == nil {
			t.Errorf("ParseSizeText(%q) 应失败", in)
		}
	}
}

func TestWalSegmentGaps(t *testing.T) {
	segs := []string{
		"000000010000000000000006",
		"000000010000000000000007",
		"000000010000000000000009", // 缺 8
		"00000001000000000000000A",
	}
	gaps := WalSegmentGaps(segs, 16*1024*1024)
	if len(gaps) != 1 || !strings.Contains(gaps[0], "缺 1 个段") {
		t.Errorf("gaps = %v", gaps)
	}
	if got := WalSegmentGaps(segs[:2], 16*1024*1024); len(got) != 0 {
		t.Errorf("连续段不应有缺口: %v", got)
	}
}

func TestScanWaldumpMultiInsert(t *testing.T) {
	input := `rmgr: Heap len (rec/tot): 54/54, tx: 9, lsn: 0/06000120, prev 0/0, desc: MULTI_INSERT off 0 num 3, blkref #0: rel 1663/16384/2615 blk 0
rmgr: Transaction len (rec/tot): 34/34, tx: 9, lsn: 0/06000130, prev 0/06000120, desc: COMMIT 2026-11-17 01:00:00 UTC
`
	res := ScanWaldumpOutput(strings.NewReader(input), time.UTC, 16384, nil)
	key := WalDMLKey{DBOID: 16384, TS: 1663, RelFile: 2615, Op: "ins"}
	if got := res.DMLByTx[9][key]; got != 3 {
		t.Errorf("MULTI_INSERT num=3 应记 3 行 ins，实际 %d", got)
	}
}

func TestLastLSNCollected(t *testing.T) {
	input := `rmgr: Heap len (rec/tot): 54/54, tx: 7, lsn: 0/06000120, prev 0/0, desc: INSERT off 1, blkref #0: rel 1663/16384/2615 blk 0
rmgr: Heap len (rec/tot): 54/54, tx: 7, lsn: 0/06000220, prev 0/06000120, desc: INSERT off 1, blkref #0: rel 1663/16384/9999 blk 0
rmgr: Heap len (rec/tot): 54/54, tx: 7, lsn: 0/06010F20, prev 0/06000220, desc: UPDATE off 1, blkref #0: rel 1663/16384/2615 blk 0
rmgr: Transaction len (rec/tot): 34/34, tx: 7, lsn: 0/06010F30, prev 0/06010F20, desc: COMMIT 2026-11-17 01:00:00 UTC
`
	allowed := map[string]bool{"1663/2615": true}
	res := ScanWaldumpOutput(strings.NewReader(input), time.UTC, 16384, allowed)
	if got := res.LastLSN[WalDMLKey{DBOID: 16384, TS: 1663, RelFile: 2615, Op: "upd"}]; got != "0/06010F20" {
		t.Errorf("upd last lsn = %q", got)
	}
	// 不在 allowedRF 里的表不应进内存
	if len(res.DMLByTx[7]) != 2 { // 2615 的 ins/upd 留下，9999 被过滤
		t.Errorf("allowedRF 未生效: %v", res.DMLByTx[7])
	}
}

func TestScanWaldumpINPLACE(t *testing.T) {
	input := `rmgr: Heap len (rec/tot): 54/54, tx: 12, lsn: 0/06000120, prev 0/0, desc: INPLACE off 3, blkref #0: rel 1663/16384/2615 blk 0
rmgr: Transaction len (rec/tot): 34/34, tx: 12, lsn: 0/06000130, prev 0/06000120, desc: COMMIT 2026-11-17 01:00:00 UTC
`
	res := ScanWaldumpOutput(strings.NewReader(input), time.UTC, 16384, nil)
	key := WalDMLKey{DBOID: 16384, TS: 1663, RelFile: 2615, Op: "upd"}
	if got := res.DMLByTx[12][key]; got != 1 {
		t.Errorf("INPLACE 应记 1 条 upd, 实际 %d", got)
	}
}

func TestOrphanDMLCounted(t *testing.T) {
	// tx 30 的 DML 找不到 COMMIT（子事务/段缺口吞掉 COMMIT），不能静默丢：
	// ScanWaldumpOutput 只负责收集，孤儿判定在 CheckPath2 汇总；这里验证 DML 确实被收进来了。
	input := `rmgr: Heap len (rec/tot): 54/54, tx: 30, lsn: 0/06000120, prev 0/0, desc: INSERT off 1, blkref #0: rel 1663/16384/2615 blk 0
rmgr: Heap len (rec/tot): 54/54, tx: 30, lsn: 0/06000220, prev 0/06000120, desc: INSERT off 2, blkref #0: rel 1663/16384/2615 blk 0
`
	res := ScanWaldumpOutput(strings.NewReader(input), time.UTC, 16384, nil)
	if _, ok := res.Commits[30]; ok {
		t.Error("不该有 COMMIT")
	}
	if got := res.DMLByTx[30][WalDMLKey{DBOID: 16384, TS: 1663, RelFile: 2615, Op: "ins"}]; got != 2 {
		t.Errorf("孤儿事务的 2 条 DML 应被收集, 实际 %d", got)
	}
}

func TestCountOrphansNoCommitAtAll(t *testing.T) {
	// 一条 COMMIT 都没有（none 覆盖提前 return 的场景）：目标表 DML 全是孤儿，
	// 必须照样计数，否则最该报的"有 DML 全无 COMMIT"反而静默。
	dml := map[uint32]map[WalDMLKey]int64{
		30: {WalDMLKey{DBOID: 16384, TS: 1663, RelFile: 2615, Op: "ins"}: 2},
		31: {WalDMLKey{DBOID: 16384, TS: 1663, RelFile: 2615, Op: "del"}: 1},
	}
	tx, n := CountOrphans(dml, map[uint32]time.Time{}, nil, nil) // 空 commits
	if tx != 2 || n != 3 {
		t.Errorf("全无 COMMIT: want tx=2 dml=3, got tx=%d dml=%d", tx, n)
	}
	w := OrphanWarningText(tx, n)
	if !strings.Contains(w, "可能低估") {
		t.Errorf("孤儿告警应提示可能低估, got %q", w)
	}
	// 有 COMMIT 的事务不算孤儿。
	tx, n = CountOrphans(dml, map[uint32]time.Time{30: time.Now()}, nil, nil)
	if tx != 1 || n != 1 {
		t.Errorf("tx30 有COMMIT后: want tx=1 dml=1, got tx=%d dml=%d", tx, n)
	}
}

func TestCoverageNoneTextDistinguishesUnavailable(t *testing.T) {
	// none 的措辞不能说成"无可用 WAL"——那和"不可达/未解析"混了。
	s := CoverageText(WalNone)
	if strings.Contains(s, "无可用 WAL") {
		t.Errorf("none 措辞不应包含'无可用 WAL', got %q", s)
	}
	if !strings.Contains(s, "已读到 WAL") {
		t.Errorf("none 措辞应强调已读到 WAL, got %q", s)
	}
}

func TestWaldumpExitText(t *testing.T) {
	tail := "could not identify current directory: No such file or directory\n" +
		"sys_waldump: fatal: error in WAL tmpRec at 0/612F338: invalid record length at 0/612F368: wanted 24, got 0"
	msg, benign := WaldumpExitText(errors.New("exit status 1"), tail, 3157)
	if !benign {
		t.Errorf("读到 WAL 末尾未写满区段属正常收尾，不该判失败: %q", msg)
	}
	if !strings.Contains(msg, "正常收尾") || !strings.Contains(msg, "3157") {
		t.Errorf("收尾文案应说明非故障且带已解析条数: %q", msg)
	}
	// 环境噪音行必须被滤掉，不能出现在告警里误导排障。
	if strings.Contains(msg, "could not identify current directory") {
		t.Errorf("getcwd 噪音应被过滤: %q", msg)
	}

	// 真失败：权限/文件读不了 —— 必须报"执行失败"。
	msg, benign = WaldumpExitText(errors.New("exit status 2"), "sys_waldump: fatal: could not open file \"000000010000000000000009\": Permission denied", 100)
	if benign {
		t.Errorf("权限错误不该判成正常收尾: %q", msg)
	}
	if !strings.Contains(msg, "执行失败") || !strings.Contains(msg, "Permission denied") {
		t.Errorf("失败文案应保留原始错误: %q", msg)
	}

	// 正常退出：无告警。
	if msg, _ = WaldumpExitText(nil, "", 10); msg != "" {
		t.Errorf("正常退出不该有告警: %q", msg)
	}
}

func TestJoinWarn(t *testing.T) {
	if got := JoinWarn("", "", ""); got != "" {
		t.Errorf("全空应返回空串, got %q", got)
	}
	if got := JoinWarn("", "a", "", "b"); got != "a；b" {
		t.Errorf("应跳过空项且无多余分号, got %q", got)
	}
	// waldump 失败告警必须排在最前面，不能被覆盖分支吞掉。
	got := JoinWarn("waldump 执行失败", "覆盖告警")
	if !strings.HasPrefix(got, "waldump 执行失败") {
		t.Errorf("失败告警应在最前, got %q", got)
	}
}

func TestScanWaldumpSubxactsMerged(t *testing.T) {
	input := `rmgr: Heap        len (rec/tot):     71/    71, tx:        973, lsn: 0/061F0710, prev 0/061F06E0, desc: INSERT off 2 flags 0x00, blkref #0: rel 1663/16384/16585 blk 1
rmgr: Transaction len (rec/tot):     46/    46, tx:        972, lsn: 0/061F0758, prev 0/061F0710, desc: COMMIT 2026-11-17 13:55:34.584745 UTC; subxacts: 973
`
	res := ScanWaldumpOutput(strings.NewReader(input), time.UTC, 16384, nil)
	ct, ok := res.Commits[973]
	if !ok {
		t.Fatalf("子事务 973 应归到父事务提交, commits=%v", res.Commits)
	}
	if got := ct.UTC().Format("2006-01-02 15:04:05"); got != "2026-11-17 13:55:34" {
		t.Errorf("子事务提交时间 = %s, want 2026-11-17 13:55:34", got)
	}
	if res.SubxactMerged != 1 {
		t.Errorf("SubxactMerged = %d, want 1", res.SubxactMerged)
	}
	if tx, dml := CountOrphans(res.DMLByTx, res.Commits, res.Aborted, res.CommitsPrepared); tx != 0 || dml != 0 {
		t.Errorf("已归并的子事务不该算孤儿, got tx=%d dml=%d", tx, dml)
	}
	// 冒号后多个 subxact、逗号分隔也要认。
	input2 := `rmgr: Transaction len (rec/tot): 46/46, tx: 20, lsn: 0/1000010, prev 0/0, desc: COMMIT 2026-11-17 01:00:00 UTC; subxacts: 21, 22
`
	res2 := ScanWaldumpOutput(strings.NewReader(input2), time.UTC, 0, nil)
	if _, ok := res2.Commits[21]; !ok {
		t.Errorf("subxacts 逗号分隔没解析: %v", res2.Commits)
	}
	if _, ok := res2.Commits[22]; !ok {
		t.Errorf("subxacts 逗号分隔没解析: %v", res2.Commits)
	}
	if res2.SubxactMerged != 2 {
		t.Errorf("SubxactMerged = %d, want 2", res2.SubxactMerged)
	}
}

// 回滚事务的 DML 不该报"找不到 COMMIT"：实测 BEGIN; INSERT; ROLLBACK 会留下 INSERT 记录 + ABORT 记录，
// 老实现把它算孤儿，告警说"可能低估"，其实一行都没变。

func TestScanWaldumpAbortNotOrphan(t *testing.T) {
	input := `rmgr: Heap        len (rec/tot):     71/    71, tx:        971, lsn: 0/061F0550, prev 0/061F0520, desc: INSERT+INIT off 1 flags 0x00, blkref #0: rel 1663/16384/16585 blk 0
rmgr: Transaction len (rec/tot):     34/    34, tx:        971, lsn: 0/061F0598, prev 0/061F0550, desc: ABORT 2026-11-17 13:53:59.026899 UTC
`
	res := ScanWaldumpOutput(strings.NewReader(input), time.UTC, 16384, nil)
	if !res.Aborted[971] {
		t.Fatalf("ABORT 没解析出来: %+v", res.Aborted)
	}
	if _, ok := res.Commits[971]; ok {
		t.Error("ABORT 不该被当成 COMMIT")
	}
	if tx, dml := CountOrphans(res.DMLByTx, res.Commits, res.Aborted, res.CommitsPrepared); tx != 0 || dml != 0 {
		t.Errorf("回滚事务不该算孤儿, got tx=%d dml=%d", tx, dml)
	}
	// 真孤儿（既无 COMMIT 也无 ABORT）仍要报。
	dml := map[uint32]map[WalDMLKey]int64{
		40: {WalDMLKey{DBOID: 16384, TS: 1663, RelFile: 2615, Op: "ins"}: 1},
	}
	if tx, n := CountOrphans(dml, map[uint32]time.Time{}, map[uint32]bool{}, nil); tx != 1 || n != 1 {
		t.Errorf("真孤儿应保留, got tx=%d dml=%d", tx, n)
	}
}

// relfilenode 键必须带表空间段：relfilenode 只在"表空间+库"内唯一，
// 同库跨表空间可能撞号。不带表空间的话，两个表的 DML 会并进同一个 key，
// 撞号时 A 表的变化会被记到 B 表头上。

func TestScanWaldumpTablespaceKey(t *testing.T) {
	input := `rmgr: Heap len (rec/tot): 54/54, tx: 7, lsn: 0/06000120, prev 0/0, desc: INSERT off 1, blkref #0: rel 1663/16384/2615 blk 0
rmgr: Heap len (rec/tot): 54/54, tx: 7, lsn: 0/06000220, prev 0/06000120, desc: INSERT off 1, blkref #0: rel 9000/16384/2615 blk 0
rmgr: Transaction len (rec/tot): 34/34, tx: 7, lsn: 0/06000330, prev 0/06000220, desc: COMMIT 2026-11-17 01:00:00 UTC
`
	allowed := map[string]bool{"1663/2615": true, "9000/2615": true}
	res := ScanWaldumpOutput(strings.NewReader(input), time.UTC, 16384, allowed)
	a := res.DMLByTx[7][WalDMLKey{DBOID: 16384, TS: 1663, RelFile: 2615, Op: "ins"}]
	b := res.DMLByTx[7][WalDMLKey{DBOID: 16384, TS: 9000, RelFile: 2615, Op: "ins"}]
	if a != 1 || b != 1 {
		t.Errorf("不同表空间的同号 relfilenode 应分开计数，实际 a=%d b=%d", a, b)
	}
	// 只放行其中一个表空间的键时，另一个必须被过滤。
	res2 := ScanWaldumpOutput(strings.NewReader(input), time.UTC, 16384, map[string]bool{"1663/2615": true})
	if got := res2.DMLByTx[7][WalDMLKey{DBOID: 16384, TS: 9000, RelFile: 2615, Op: "ins"}]; got != 0 {
		t.Errorf("allowedKeys 未按表空间过滤: %d", got)
	}
	if res2.DMLByTx[7][WalDMLKey{DBOID: 16384, TS: 1663, RelFile: 2615, Op: "ins"}] != 1 {
		t.Error("放行的键被误过滤")
	}
}

// 时间线切换：段名前 8 位 TLI 不一致时，只保留新 TLI 的段，旧时间线整段忽略。
// 真库造不出 timeline fork（promote 需要备用实例），用伪造段文件锁逻辑。

func TestTLISplitSingleTimeline(t *testing.T) {
	segs := []string{
		"000000010000000000000006",
		"000000010000000000000007",
		"000000010000000000000008",
	}
	kept, fork := TliSplit("", segs)
	if fork != nil {
		t.Errorf("单时间线不应返回 fork: %+v", fork)
	}
	if len(kept) != 3 {
		t.Errorf("单时间线段不应被剔除: %v", kept)
	}
}

func TestTLISplitForkKeepsOnlyNewTLI(t *testing.T) {
	dir := t.TempDir()
	segs := []string{
		"000000010000000000000006",
		"000000010000000000000007",
		"00000002000000000000000A", // 切换后的新时间线段
	}
	// history 记录 TLI 2 从 TLI 1 的 0/5000009 切过来。
	if err := os.WriteFile(filepath.Join(dir, "00000002.history"),
		[]byte("1\t0/5000009\tno recovery target specified\n"), 0644); err != nil {
		t.Fatal(err)
	}
	kept, fork := TliSplit(dir, segs)
	if fork == nil {
		t.Fatal("混合 TLI 应检出时间线切换")
	}
	if fork.From != "00000001" || fork.To != "00000002" {
		t.Errorf("fork 信息错: %+v", fork)
	}
	if fork.ForkLSN != "0/5000009" {
		t.Errorf("history 缺失切换点 LSN: %+v", fork)
	}
	if len(kept) != 1 || kept[0] != segs[2] {
		t.Errorf("应只保留新 TLI 的段: %v", kept)
	}
}

func TestTLISplitHistoryMissing(t *testing.T) {
	dir := t.TempDir()
	segs := []string{
		"000000010000000000000006",
		"00000002000000000000000A",
	}
	kept, fork := TliSplit(dir, segs)
	if fork == nil {
		t.Fatal("混合 TLI 应检出时间线切换")
	}
	if fork.ForkLSN != "" {
		t.Errorf("history 缺失时 ForkLSN 应为空: %+v", fork)
	}
	if len(kept) != 1 {
		t.Errorf("history 缺失不应阻塞剔除旧时间线: %v", kept)
	}
}

// 多级切换链（TLI 1→2→3）：history 取 parent 最大的行——那是进入当前 TLI 的切换点。

func TestTLISplitMultiGenerationHistory(t *testing.T) {
	dir := t.TempDir()
	segs := []string{
		"000000010000000000000002",
		"00000003000000000000000C", // 直接从 TLI 2 切来，但 TLI 2 的段已回收
	}
	if err := os.WriteFile(filepath.Join(dir, "00000003.history"),
		[]byte("1\t0/3000009\treason A\n2\t0/9000060\treason B\n"), 0644); err != nil {
		t.Fatal(err)
	}
	_, fork := TliSplit(dir, segs)
	if fork == nil {
		t.Fatal("应检出切换")
	}
	// 进入 TLI 3 的切换点来自 parent=2 那行（0/9000060），不是更早的 1→2。
	if fork.ForkLSN != "0/9000060" {
		t.Errorf("应取 parent 最大的行的 LSN: %+v", fork)
	}
}

// 超长单行（超过旧 bufio.Scanner 的 4MB 上限）不能再中断扫描——
// 旧实现停在超长行，其后 WAL 全部不解析，覆盖还可能报 full（BUG-1 之 3）。
func TestScanWaldumpHugeLineContinues(t *testing.T) {
	huge := strings.Repeat("x", 5*1024*1024) // 5MB，超过旧 4MB 上限
	in := "rmgr: Heap        len (rec/tot):     99/    99, tx:      701, lsn: 0/06000120, prev 0/06000100, desc: " + huge + "\n" +
		"rmgr: Transaction        len (rec/tot):    54/    54, tx:      701, lsn: 0/06000180, prev 0/06000120, desc: COMMIT 2026-11-24 08:00:00.123456 UTC; inval messages: rec 0\n"
	res := ScanWaldumpOutput(strings.NewReader(in), time.UTC, 16060, nil)
	if _, ok := res.Commits[701]; !ok {
		t.Fatal("超长行后面的 COMMIT 必须仍被解析（扫描不得中断）")
	}
}

// 两阶段提交：COMMIT PREPARED 无提交时间戳，必须单独识别（BUG-3）。
// 目标表 DML 不算孤儿，由调用方按"有变化"保守计入。
func TestScanWaldumpCommitPrepared(t *testing.T) {
	in := "rmgr: Heap        len (rec/tot):     99/    99, tx:      500, lsn: 0/06000120, prev 0/06000100, desc: INSERT off 72 flags 0x00, blkref #0: rel 1663/16060/19161 blk 2024\n" +
		"rmgr: Transaction        len (rec/tot):    54/    54, tx:      500, lsn: 0/06000180, prev 0/06000120, desc: COMMIT PREPARED 500\n"
	res := ScanWaldumpOutput(strings.NewReader(in), time.UTC, 16060, nil)
	if !res.CommitsPrepared[500] {
		t.Fatal("COMMIT PREPARED 应记入 CommitsPrepared")
	}
	if _, ok := res.Commits[500]; ok {
		t.Fatal("COMMIT PREPARED 无时间戳，不应进常规 Commits")
	}
	if tx, dml := CountOrphans(res.DMLByTx, res.Commits, res.Aborted, res.CommitsPrepared); tx != 0 || dml != 0 {
		t.Errorf("prepared 事务不算孤儿，got tx=%d dml=%d", tx, dml)
	}
}

// ABORT PREPARED：已回滚的两阶段事务，DML 排除、也不算孤儿（reAbort 的 \b 命中）。
func TestScanWaldumpAbortPrepared(t *testing.T) {
	in := "rmgr: Heap        len (rec/tot):     99/    99, tx:      501, lsn: 0/06000120, prev 0/06000100, desc: INSERT off 1 flags 0x00, blkref #0: rel 1663/16060/19161 blk 2024\n" +
		"rmgr: Transaction        len (rec/tot):    42/    42, tx:      501, lsn: 0/06000180, prev 0/06000120, desc: ABORT PREPARED 501\n"
	res := ScanWaldumpOutput(strings.NewReader(in), time.UTC, 16060, nil)
	if !res.Aborted[501] {
		t.Fatal("ABORT PREPARED 应记入 Aborted")
	}
	if tx, _ := CountOrphans(res.DMLByTx, res.Commits, res.Aborted, res.CommitsPrepared); tx != 0 {
		t.Errorf("已回滚的 prepared 事务不算孤儿")
	}
}

// 覆盖降级：full 只在 waldump 干净退出、无段缺口、读取无中断时成立（BUG-1）。
func TestFullCoverageBlocked(t *testing.T) {
	cases := []struct {
		name       string
		err        string
		benign     bool
		gaps       int
		parseErrs  int
		wantReason int
	}{
		{"全干净", "", false, 0, 0, 0},
		{"读到末尾正常收尾", "exit 1 (tail)", true, 0, 0, 0},
		{"中途异常退出", "waldump crashed", false, 0, 0, 1},
		{"段缺口", "", false, 2, 0, 1},
		{"读取中断", "", false, 0, 1, 1},
		{"三重叠加", "err", false, 1, 1, 3},
	}
	for _, c := range cases {
		got := FullCoverageBlocked(c.err, c.benign, c.gaps, c.parseErrs)
		if len(got) != c.wantReason {
			t.Errorf("%s: 期望 %d 条理由，got %d (%v)", c.name, c.wantReason, len(got), got)
		}
	}
}

// PreparedWarningText：有 prepared 才有文案。
func TestPreparedWarningText(t *testing.T) {
	if s := PreparedWarningText(map[uint32]bool{}); s != "" {
		t.Errorf("空 prepared 应返回空串，got %q", s)
	}
	s := PreparedWarningText(map[uint32]bool{1: true, 2: true})
	if !strings.Contains(s, "2 个两阶段提交") {
		t.Errorf("文案应包含计数，got %q", s)
	}
}
