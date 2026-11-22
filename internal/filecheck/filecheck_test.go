package filecheck

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNodeFilePrefixMatches(t *testing.T) {
	cases := []struct {
		file string
		node string
		want bool
	}{
		{"16384", "16384", true},
		{"16384.1", "16384", true},
		{"16384_fsm", "16384", true},
		{"16384_vm", "16384", true},
		{"16384_init", "16384", true},
		{"163840", "16384", false}, // 关键：不能把 163840 当成 16384 的分片
		{"16384x1", "16384", false},
		{"12345", "16384", false},
		{"16384", "0", false},
		{"16384", "", false},
	}
	for _, c := range cases {
		if got := NodeFilePrefixMatches(c.file, c.node); got != c.want {
			t.Errorf("NodeFilePrefixMatches(%q, %q) = %v, want %v", c.file, c.node, got, c.want)
		}
	}
}

func TestScanNodeMTimeMap(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	files := map[string]time.Time{
		"16384":     base.Add(1 * time.Hour),
		"16384.1":   base.Add(3 * time.Hour),
		"16384_fsm": base.Add(2 * time.Hour),
		"163840":    base.Add(9 * time.Hour), // 不能误算到 16384
	}
	for name, mt := range files {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mt, mt); err != nil {
			t.Fatal(err)
		}
	}
	got, err := ScanNodeMTimeMap(dir, map[string]bool{"16384": true})
	if err != nil {
		t.Fatal(err)
	}
	if want := base.Add(3 * time.Hour); !got["16384"].Equal(want) {
		t.Errorf("16384 mtime = %v, want %v", got["16384"], want)
	}
}

func TestFileChangedTables(t *testing.T) {
	r := FileCheckReport{Results: []FileResult{
		{Table: "public.a", Changed: true},
		{Table: "public.b", Changed: false},
		{Table: "public.c", Changed: true},
	}}
	got := FileChangedTables(r)
	if len(got) != 2 || got[0] != "public.a" || got[1] != "public.c" {
		t.Errorf("FileChangedTables = %v", got)
	}
}

func TestMTimeAfterSince(t *testing.T) {
	since := time.Date(2026, 11, 16, 18, 0, 0, 0, time.Local)
	before := since.Add(-time.Hour)
	after := since.Add(time.Hour)
	if !after.After(since) {
		t.Error("after should be counted")
	}
	if before.After(since) {
		t.Error("before should not be counted")
	}
}
