// 跨包测试汇总：验证 report 包的 summary/打印函数（数据构造借 wal/filecheck/tz 类型）。
package report

import (
	"kbchange/internal/filecheck"
	"kbchange/internal/tz"
	"kbchange/internal/wal"
	"strings"
	"testing"
)

func TestPath2SummaryDisabled(t *testing.T) {
	r := wal.WalCheckReport{Enabled: false}
	s := Path2Summary(r)
	if !strings.Contains(s, "未启用") {
		t.Errorf("summary = %q", s)
	}
}

// COMMIT 行里的 subxacts 必须归到父事务：实测 BEGIN; SAVEPOINT; INSERT; RELEASE; COMMIT
// 这一行是真入库的，但子事务的 DML 记的是子 xid，不认 subxacts 就会被当孤儿丢掉。

func TestPath4SummaryUnavailable(t *testing.T) {
	r := filecheck.FileCheckReport{Available: false, Note: "远程库不可达"}
	got := Path4Summary(r)
	if !contains(got, "不可达") || !contains(got, "远程库不可达") {
		t.Errorf("Path4Summary = %q", got)
	}
}

func TestPath4SummaryAvailable(t *testing.T) {
	r := filecheck.FileCheckReport{Available: true, ChangedCount: 7}
	got := Path4Summary(r)
	if !contains(got, "7 张表") || !contains(got, "不并入合并数") {
		t.Errorf("Path4Summary = %q", got)
	}
}

func TestTZInfoInReport(t *testing.T) {
	// 确认 Report 携带时区信息且打印不 panic。
	r := Report{Schema: "public", Since: "2026-11-16 18:00:00", TZ: tz.TZInfo{
		OSLocal: "Local(+08:00)", SinceTZ: "UTC", DBTimeZone: "PRC",
		SinceUTC: "2026-11-16 18:00:00", Note: "test",
	}}
	PrintReport(r, false)
}
