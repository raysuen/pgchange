package path1

import (
	"kbchange/internal/tz"
	"strings"
	"testing"
	"time"
)

func TestTimeColKind(t *testing.T) {
	cases := map[string]string{
		"timestamp with time zone":    "timestamptz",
		"timestamptz":                 "timestamptz",
		"timestamp without time zone": "timestamp",
		"timestamp":                   "timestamp",
		"date":                        "date",
		"integer":                     "",
	}
	for in, want := range cases {
		if got := TimeColKind(in); got != want {
			t.Errorf("TimeColKind(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSinceArgForCol(t *testing.T) {
	// 用户场景：OS +08 输入 18:00，程序换算成 DB 时区(UTC) 10:00 后比对。
	osLoc, _ := tz.LoadLocation("+08:00")
	dbLoc, _ := tz.LoadLocation("UTC")
	since, err := tz.ParseSinceIn("2026-11-16 18:00:00", osLoc)
	if err != nil {
		t.Fatal(err)
	}
	// 绝对时刻 = 10:00 UTC
	if got := since.UTC().Format("2006-01-02 15:04:05"); got != "2026-11-16 10:00:00" {
		t.Errorf("since UTC = %s, want 2026-11-16 10:00:00", got)
	}
	// 关键断言：timestamp/date 传的是换算到 DB 时区的墙上时间 10:00，不是输入的 18:00。
	if got := SinceArgForCol(since, dbLoc, "timestamp"); got != "2026-11-16 10:00:00" {
		t.Errorf("timestamp arg = %v, want 2026-11-16 10:00:00 (DB 时区换算后)", got)
	}
	if got := SinceArgForCol(since, dbLoc, "date"); got != "2026-11-16" {
		t.Errorf("date arg = %v, want 2026-11-16", got)
	}
	// timestamptz 传绝对时刻，DB 按自身时区比较；In 只改显示时区不改绝对时刻。
	arg := SinceArgForCol(since, dbLoc, "timestamptz")
	tt, ok := arg.(time.Time)
	if !ok {
		t.Fatalf("timestamptz arg type = %T, want time.Time", arg)
	}
	if got := tt.UTC().Format("2006-01-02 15:04:05"); got != "2026-11-16 10:00:00" {
		t.Errorf("timestamptz arg UTC = %s, want 2026-11-16 10:00:00", got)
	}
	// 边界：OS 与 DB 同时区(+08)，换算后墙上时间应等于输入 18:00。
	since2, _ := tz.ParseSinceIn("2026-11-16 18:00:00", osLoc)
	if got := SinceArgForCol(since2, osLoc, "timestamp"); got != "2026-11-16 18:00:00" {
		t.Errorf("same-tz timestamp arg = %v, want 2026-11-16 18:00:00", got)
	}
}

func TestPickTimeColumn(t *testing.T) {
	// 候选名单命中优先
	got, err := PickTimeColumn([]TimeCol{
		{Name: "id", Kind: ""},
		{Name: "update_time", Kind: "timestamptz"},
		{Name: "create_time", Kind: "timestamp"},
	}, "")
	if err != nil || got.Name != "update_time" {
		t.Errorf("候选命中应返回 update_time, got %+v err=%v", got, err)
	}
	// 扩展候选名命中
	got, err = PickTimeColumn([]TimeCol{{Name: "modified_at", Kind: "timestamptz"}}, "")
	if err != nil || got.Name != "modified_at" {
		t.Errorf("modified_at 应在候选名单里, got %+v err=%v", got, err)
	}
	// 名单未命中但唯一时间列 -> 兜底（金仓 dt 场景）
	got, err = PickTimeColumn([]TimeCol{{Name: "dt", Kind: "date"}}, "")
	if err != nil || got.Name != "dt" || got.Kind != "date" {
		t.Errorf("唯一列兜底应返回 dt/date, got %+v err=%v", got, err)
	}
	// 名单未命中且多个时间列 -> 不猜，但要给明确提示（不能静默当"无时间字段"）
	got, err = PickTimeColumn([]TimeCol{
		{Name: "start_day", Kind: "date"},
		{Name: "end_day", Kind: "date"},
	}, "")
	if err == nil || !strings.Contains(err.Error(), "-time-col") || got.Name != "" {
		t.Errorf("多列应报错并提示 -time-col, got %+v err=%v", got, err)
	}
	// forced 精确匹配（大小写不敏感）
	got, err = PickTimeColumn([]TimeCol{{Name: "BizDay", Kind: "date"}}, "bizday")
	if err != nil || got.Name != "BizDay" {
		t.Errorf("forced 应匹配 BizDay, got %+v err=%v", got, err)
	}
	// forced 找不到 -> 报错
	if _, err := PickTimeColumn([]TimeCol{{Name: "dt", Kind: "date"}}, "nope"); err == nil {
		t.Error("forced 不存在应报错")
	}
}

func TestSkipNoteNoTimeCol(t *testing.T) {
	// 没有时间字段导致的 SKIP 必须说明前因，否则读者不知道为什么要去探 xmin。
	s := SkipNoteNoTimeCol("xmin 提交时间不可用：pq: could not get commit timestamp")
	if !contains(s, "表内无时间类型字段") || !contains(s, "xmin 提交时间不可用") {
		t.Errorf("SKIP 文案应带前因和具体原因, got %q", s)
	}
	// 表根本不存在时不能套"无时间字段"——那是表名/模式写错，方向不同。
	s = SkipNoteNoTimeCol("表不存在（检查 -tables/-schema 拼写）")
	if contains(s, "表内无时间类型字段") {
		t.Errorf("表不存在时不该写无时间字段, got %q", s)
	}
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}
