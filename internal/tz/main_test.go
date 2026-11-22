package tz

import (
	"strings"
	"testing"
	"time"
)

func TestParseSince(t *testing.T) {
	cases := []string{
		"2026-11-16 18:00:00",
		"2026-11-16T18:00:00",
		"2026-11-16 18:00",
		"2026-11-16",
	}
	for _, s := range cases {
		if _, err := ParseSince(s); err != nil {
			t.Errorf("ParseSince(%q) unexpected error: %v", s, err)
		}
	}
	if _, err := ParseSince("not-a-time"); err == nil {
		t.Error("ParseSince accepted invalid input")
	}
}

func TestLoadLocation(t *testing.T) {
	// 空 / Local -> 系统时区
	for _, s := range []string{"", "Local", " local "} {
		loc, err := LoadLocation(s)
		if err != nil || loc == nil {
			t.Errorf("LoadLocation(%q) failed: %v", s, err)
		}
	}
	// UTC
	loc, err := LoadLocation("UTC")
	if err != nil || loc != time.UTC {
		t.Errorf("LoadLocation(UTC) = %v, %v", loc, err)
	}
	// 固定偏移 +08:00 -> +28800
	loc, err = LoadLocation("+08:00")
	if err != nil {
		t.Fatalf("LoadLocation(+08:00) error: %v", err)
	}
	_, off := time.Now().In(loc).Zone()
	if off != 8*3600 {
		t.Errorf("+08:00 offset = %d, want %d", off, 8*3600)
	}
	// 负偏移 -05:00 -> -18000
	loc, err = LoadLocation("-05:00")
	if err != nil {
		t.Fatalf("LoadLocation(-05:00) error: %v", err)
	}
	_, off = time.Now().In(loc).Zone()
	if off != -5*3600 {
		t.Errorf("-05:00 offset = %d, want %d", off, -5*3600)
	}
	// IANA 时区名
	loc, err = LoadLocation("Asia/Shanghai")
	if err != nil || loc == nil {
		t.Errorf("LoadLocation(Asia/Shanghai) failed: %v", err)
	}
	// 非法偏移越界
	if _, err := LoadLocation("+15:00"); err == nil {
		t.Error("LoadLocation(+15:00) should fail")
	}
	// 非法时区名
	if _, err := LoadLocation("Mars/Olympus"); err == nil {
		t.Error("LoadLocation(Mars/Olympus) should fail")
	}
}

func TestNormalizeDateText(t *testing.T) {
	cases := map[string]string{
		"2026-11-17":                "2026-11-17",
		"2026-11-17 10:00:00":       "2026-11-17",
		"2026-11-17T10:00:00Z":      "2026-11-17",
		"2026-11-17T10:00:00+08:00": "2026-11-17",
		" 2026-11-17 ":              "2026-11-17",
		"not-a-date":                "not-a-date",
	}
	for in, want := range cases {
		if got := NormalizeDateText(in); got != want {
			t.Errorf("NormalizeDateText(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeTimestampText(t *testing.T) {
	cases := map[string]string{
		"2026-11-17 10:00:00":          "2026-11-17 10:00:00",
		"2026-11-17 10:00:00.948863":   "2026-11-17 10:00:00",
		"2026-11-17T10:00:00":          "2026-11-17 10:00:00",
		"2026-11-17T10:00:00.12345678": "2026-11-17 10:00:00",
		"weird":                        "weird",
	}
	for in, want := range cases {
		if got := NormalizeTimestampText(in); got != want {
			t.Errorf("NormalizeTimestampText(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLocationFromDBZone(t *testing.T) {
	// 名字能被 Go 加载 -> 用名字（Asia/Shanghai）
	loc := LocationFromDBZone("Asia/Shanghai", 0, false)
	_, off := time.Now().In(loc).Zone()
	if off != 8*3600 {
		t.Errorf("Asia/Shanghai offset = %d, want %d", off, 8*3600)
	}
	// 名字不认识但有偏移 -> 固定时区（金仓自定义名兜底）
	loc = LocationFromDBZone("KB_ZONE_CUSTOM", 8*3600, true)
	_, off = time.Now().In(loc).Zone()
	if off != 8*3600 {
		t.Errorf("fixed zone offset = %d, want %d", off, 8*3600)
	}
	if !contains(loc.String(), "KB_ZONE_CUSTOM") {
		t.Errorf("fixed zone name missing DB tag: %s", loc.String())
	}
	// 名字不认识且无偏移 -> 兜底 time.Local
	loc = LocationFromDBZone("WEIRD", 0, false)
	if loc != time.Local {
		t.Errorf("fallback should be time.Local, got %v", loc)
	}
	// UTC 名字
	loc = LocationFromDBZone("UTC", 0, true)
	if loc != time.UTC {
		t.Errorf("UTC name should map to time.UTC, got %v", loc)
	}
}

func TestTZCompare(t *testing.T) {
	// 一致
	s := TZCompare("PRC", 8*3600, true, 8*3600, "Local(+08:00)")
	if !contains(s, "一致") || !contains(s, "+08:00") {
		t.Errorf("一致分支输出异常: %s", s)
	}
	// 不一致：DB UTC vs OS +08
	s = TZCompare("UTC", 0, true, 8*3600, "Local(+08:00)")
	if !contains(s, "不一致") || !contains(s, "-08:00") {
		t.Errorf("不一致分支输出异常: %s", s)
	}
	// 失败降级
	s = TZCompare("unknown", 0, false, 8*3600, "Local(+08:00)")
	if !contains(s, "无法") {
		t.Errorf("降级分支输出异常: %s", s)
	}
}

func TestOSOffsetAndText(t *testing.T) {
	loc, _ := LoadLocation("+08:00")
	since, _ := ParseSinceIn("2026-11-16 18:00:00", loc)
	if got := OSTZOffset(since, loc); got != 8*3600 {
		t.Errorf("OSTZOffset = %d", got)
	}
	if got := OffsetText(8*3600, true); got != "+08:00" {
		t.Errorf("OffsetText(ok) = %s", got)
	}
	if got := OffsetText(0, false); got != "unavailable" {
		t.Errorf("OffsetText(fail) = %s", got)
	}
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}
