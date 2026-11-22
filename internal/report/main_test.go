package report

import (
	"kbchange/internal/filecheck"
	"kbchange/internal/wal"
	"strings"
	"testing"
)

func TestBuildConclusionCounts(t *testing.T) {
	r := Report{TableCount: 8, CombinedCount: 5, Path1Count: 3, Path3Count: 4, Path3Reliable: 4, StatsBaseline: "partial"}
	c := BuildConclusion(r)
	for _, want := range []string{"5 张", "通路1 3 张", "通路3 采信 4 张", "partial 基线只可信"} {
		if !contains(c, want) {
			t.Errorf("conclusion %q missing %q", c, want)
		}
	}
}

func TestBuildConclusionPath2Disabled(t *testing.T) {
	// 通路2 压根没启用：结论不能写"通路2 0 张"——那读起来是"查了没变化"，
	// 实际根本没跑。必须明确写"未启用"。
	r := Report{TableCount: 3, CombinedCount: 1, Path1Count: 1, Path2: wal.WalCheckReport{Enabled: false}}
	c := BuildConclusion(r)
	if strings.Contains(c, "通路2 0 张") {
		t.Errorf("未启用时不应写通路2 0 张, got %q", c)
	}
	if !contains(c, "通路2 未启用") {
		t.Errorf("conclusion %q missing 通路2 未启用", c)
	}
	// 启用后仍要报张数。
	r.Path2 = wal.WalCheckReport{Enabled: true, Available: true, Coverage: wal.WalFull, ChangedCount: 2}
	c = BuildConclusion(r)
	if !contains(c, "通路2 2 张") {
		t.Errorf("启用时应报通路2 张数, got %q", c)
	}
}

func TestBuildConclusionPath2Unavailable(t *testing.T) {
	// 通路2 启用了但不可达：结论不能写"覆盖=none"（那是已判定无变化），
	// 必须明确"未产出结果、不参与判定"。
	r := Report{
		TableCount: 3, CombinedCount: 1, Path1Count: 1,
		Path2: wal.WalCheckReport{Enabled: true, Available: false, Coverage: wal.WalNone},
	}
	c := BuildConclusion(r)
	if strings.Contains(c, "覆盖=none") {
		t.Errorf("不可达时不应写覆盖=none, got %q", c)
	}
	// 括号里的摘要也不能写"通路2 0 张"——那是"查了没变化"，实际一条都没读到。
	if strings.Contains(c, "通路2 0 张") {
		t.Errorf("不可达时摘要不应写通路2 0 张, got %q", c)
	}
	if !contains(c, "通路2 未产出") {
		t.Errorf("不可达时摘要应写通路2 未产出, got %q", c)
	}
	for _, want := range []string{"未产出结果", "不参与判定", "不能当成"} {
		if !contains(c, want) {
			t.Errorf("conclusion %q missing %q", c, want)
		}
	}
	// 正常读到 WAL 时仍应写覆盖。
	r.Path2.Available = true
	c = BuildConclusion(r)
	if !contains(c, "覆盖=none") {
		t.Errorf("可读但无提交时应写覆盖=none, got %q", c)
	}
}

// TRUNCATE 场景：通路1 表空、通路2 无 DML、通路3 计数不动，只有通路4 新文件 mtime 有信号。
// 通路4 不并入合并数是对的（VACUUM FULL 假阳性），但结论一句不提会把这张表完全藏住。

func TestBuildConclusionPath4Signal(t *testing.T) {
	r := Report{TableCount: 1, Path2: wal.WalCheckReport{Enabled: true, Available: true, Coverage: wal.WalFull},
		Path4: filecheck.FileCheckReport{Available: true, ChangedCount: 1}, StatsBaseline: "stale"}
	c := BuildConclusion(r)
	for _, want := range []string{"另有 1 张文件 mtime 晚于指定时间", "通路4 仅物理信号", "TRUNCATE"} {
		if !contains(c, want) {
			t.Errorf("conclusion %q missing %q", c, want)
		}
	}
	// 通路4 不可用或没有变化时不能出现该句。
	r.Path4 = filecheck.FileCheckReport{Available: false, ChangedCount: 0}
	if contains(BuildConclusion(r), "另有") {
		t.Errorf("通路4 不可用时结论不应出现 mtime 信号句")
	}
}

// 孤儿事务：有 DML 但没 COMMIT（事务还在跑）。它一提交"无变化"就可能翻盘，结论必须提示。

func TestBuildConclusionOrphanTx(t *testing.T) {
	r := Report{TableCount: 2, Path2: wal.WalCheckReport{Enabled: true, Available: true, Coverage: wal.WalFull, OrphanTx: 1, OrphanDML: 1}}
	c := BuildConclusion(r)
	for _, want := range []string{"未提交事务", "提交后可能新增变化表"} {
		if !contains(c, want) {
			t.Errorf("conclusion %q missing %q", c, want)
		}
	}
	// 没孤儿时不出现该句。
	r.Path2.OrphanTx = 0
	if contains(BuildConclusion(r), "未提交事务") {
		t.Errorf("无孤儿时结论不应出现未提交事务提示")
	}
}

func TestBuildConclusionUndetermined(t *testing.T) {
	r := Report{TableCount: 5, CombinedCount: 0, Path1Error: 3, Path1Skip: 2}
	c := BuildConclusion(r)
	for _, want := range []string{"未判定 5 张", "失败 3", "跳过 2", "不能当成"} {
		if !contains(c, want) {
			t.Errorf("conclusion %q missing %q", c, want)
		}
	}
}

func TestBaselineText(t *testing.T) {
	cases := map[string]string{
		"partial": "partial",
		"stale":   "stale",
		"unknown": "unknown",
	}
	for in, want := range cases {
		if got := BaselineText(in); !contains(got, want) {
			t.Errorf("BaselineText(%q) = %q, want contain %q", in, got, want)
		}
	}
}

func TestPath3ReliabilityMatrix(t *testing.T) {
	// stale(R<T)：只信"无"，N>0 是候选；partial(R>=T)：只信"有"，N=0 不可信。
	cases := []struct {
		baseline  string
		changed   bool
		reliable  bool
		qualifier string // 说明里必须出现的关键词
	}{
		{"stale", true, false, "不能判定"},
		{"stale", false, true, "无计数"},
		{"partial", true, true, "可判定有变化"},
		{"partial", false, false, "未计数不代表"},
		{"unavailable", true, false, "仅供参考"},
		{"unknown", false, false, "基线未知"},
	}
	for _, c := range cases {
		got, q := Path3Reliability(c.baseline, c.changed)
		if got != c.reliable {
			t.Errorf("Path3Reliability(%s,%v) reliable=%v, want %v", c.baseline, c.changed, got, c.reliable)
		}
		if !contains(q, c.qualifier) {
			t.Errorf("Path3Reliability(%s,%v) qualifier=%q missing %q", c.baseline, c.changed, q, c.qualifier)
		}
	}
}

func TestBuildConclusionCandidate(t *testing.T) {
	// stale 下有 1 张通路3 候选未采信，结论要写清楚，不能当成确定变化。
	r := Report{TableCount: 2, CombinedCount: 0, Path1Count: 0, Path3Reliable: 0, Path3Candidate: 1, Path3Count: 1, StatsBaseline: "stale"}
	c := BuildConclusion(r)
	for _, want := range []string{"候选", "人工核对", "stale"} {
		if !contains(c, want) {
			t.Errorf("conclusion %q missing %q", c, want)
		}
	}
}

func TestBuildConclusionNoTables(t *testing.T) {
	// 模式匹配不到表 / -limit 截断成 0 张：一张表都没查，结论不能写"0 张变化"。
	// 实测（-schema nosuchschema）原来输出"指定时间后有数据变化的表：0 张"，会被读成确认无变化。
	r := Report{TableCount: 0, CombinedCount: 0, Path1Count: 0, StatsBaseline: "stale"}
	c := BuildConclusion(r)
	if !contains(c, "未检查到任何表") || !contains(c, "不能当成") {
		t.Errorf("0 张表时结论应明确未检查到任何表, got %q", c)
	}
	if contains(c, "有数据变化的表：0 张") {
		t.Errorf("0 张表时不该写'有数据变化的表：0 张', got %q", c)
	}
}

func TestBuildConclusionPath3Failed(t *testing.T) {
	// 通路3 查询失败（baseline=unavailable）：一条结果都没产出，
	// 不能写"通路3 采信 0 张"——那是"查了没变化"的口径。
	r := Report{TableCount: 3, CombinedCount: 0, Path1Count: 0, StatsBaseline: "unavailable"}
	c := BuildConclusion(r)
	if contains(c, "通路3 采信 0 张") {
		t.Errorf("查询失败时不该写'采信 0 张', got %q", c)
	}
	for _, want := range []string{"通路3 未产出", "查询失败", "不能当成"} {
		if !contains(c, want) {
			t.Errorf("conclusion %q missing %q", c, want)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
