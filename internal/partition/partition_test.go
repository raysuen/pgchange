package partition

import (
	"kbchange/internal/report"
	"kbchange/internal/table"
	"strings"
	"testing"
)

func buildTree(pairs ...string) *RelTree {
	t := &RelTree{children: map[string][]string{}}
	for _, p := range pairs {
		parts := strings.SplitN(p, ">", 2)
		t.children[parts[0]] = append(t.children[parts[0]], parts[1])
	}
	return t
}

func TestExpandSingleParent(t *testing.T) {
	tree := buildTree("ray.p>ray.c1", "ray.p>ray.c2", "ray.c1>ray.c11")
	owner, expanded := tree.Expand([]table.TableRef{{Schema: "ray", Name: "p"}})
	want := map[string]string{
		"ray.p": "ray.p", "ray.c1": "ray.p", "ray.c2": "ray.p", "ray.c11": "ray.p",
	}
	for k, v := range want {
		if owner[k] != v {
			t.Errorf("owner[%s] = %q, want %q", k, owner[k], v)
		}
	}
	if len(expanded["ray.p"]) != 3 {
		t.Errorf("expanded[ray.p] = %v, want 3 个子表", expanded["ray.p"])
	}
}

// 子分区被 -tables 单独点名时按自己成行，父表只认领没被点名的那些。

func TestExpandChildAlsoChecked(t *testing.T) {
	tree := buildTree("ray.p>ray.c1", "ray.p>ray.c2", "ray.c1>ray.c11")
	owner, expanded := tree.Expand([]table.TableRef{{Schema: "ray", Name: "p"}, {Schema: "ray", Name: "c1"}})
	if owner["ray.c1"] != "ray.c1" {
		t.Errorf("ray.c1 应归自己，实际 %q", owner["ray.c1"])
	}
	if owner["ray.c2"] != "ray.p" {
		t.Errorf("ray.c2 应归父表，实际 %q", owner["ray.c2"])
	}
	// c11 只在 c1 之下：最近的祖先是 c1，不能被更远的 p 抢走。
	if owner["ray.c11"] != "ray.c1" {
		t.Errorf("ray.c11 应归最近的祖先 ray.c1，实际 %q", owner["ray.c11"])
	}
	if len(expanded["ray.p"]) != 1 || expanded["ray.p"][0] != "ray.c2" {
		t.Errorf("expanded[ray.p] = %v, want [ray.c2]", expanded["ray.p"])
	}
}

// 分区关系查不到时退化为"自己归自己"，行为与改动前一致。

func TestExpandNilTree(t *testing.T) {
	var tree *RelTree
	owner, expanded := tree.Expand([]table.TableRef{{Schema: "ray", Name: "a"}, {Schema: "ray", Name: "b"}})
	if owner["ray.a"] != "ray.a" || owner["ray.b"] != "ray.b" {
		t.Errorf("nil tree 应自己归自己，实际 %v", owner)
	}
	if len(expanded) != 0 {
		t.Errorf("nil tree 不应有展开，实际 %v", expanded)
	}
}

func TestPartitionExpandNote(t *testing.T) {
	if got := PartitionExpandNote(map[string][]string{}, nil); got != "" {
		t.Errorf("无展开应返回空串，实际 %q", got)
	}
	note := PartitionExpandNote(map[string][]string{
		"ray.p": {"ray.c1", "ray.c2", "ray.c3", "ray.c4"},
	}, nil)
	if !strings.Contains(note, "ray.p 展开 4 个子表") {
		t.Errorf("说明缺少展开数量：%s", note)
	}
	if !strings.Contains(note, "等") {
		t.Errorf("超过 3 个子分区应截断加'等'：%s", note)
	}
}

// 分区父表在 pg_stat_user_tables 里没有自己的行，计数只能来自子分区汇总。

func TestAggregatePath3RowsPartition(t *testing.T) {
	owner := map[string]string{"ray.p": "ray.p", "ray.c1": "ray.p", "ray.c2": "ray.p"}
	rows := []report.Path3Raw{
		{Schema: "ray", Name: "c1", Ins: 1},
		{Schema: "ray", Name: "c2", Ins: 2, Del: 3},
		{Schema: "ray", Name: "other", Ins: 9},
	}
	out := report.AggregatePath3Rows([]table.TableRef{{Schema: "ray", Name: "p"}}, owner, rows)
	if len(out) != 1 {
		t.Fatalf("want 1 行，实际 %d: %+v", len(out), out)
	}
	got := out[0]
	if got.Ins != 3 || got.Del != 3 {
		t.Errorf("父表应汇总子分区计数，实际 增=%d 删=%d", got.Ins, got.Del)
	}
	if !got.FromPartition {
		t.Error("分区汇总结果应标 FromPartition")
	}
	if !got.Changed {
		t.Error("计数>0 应判 Changed")
	}
}

// 子分区自己被检查时，计数只记在子表名下，不重复并进父表。

func TestAggregatePath3RowsNoDoubleCount(t *testing.T) {
	owner := map[string]string{"ray.p": "ray.p", "ray.c1": "ray.c1"}
	rows := []report.Path3Raw{{Schema: "ray", Name: "c1", Upd: 5}}
	out := report.AggregatePath3Rows([]table.TableRef{{Schema: "ray", Name: "p"}, {Schema: "ray", Name: "c1"}}, owner, rows)
	if len(out) != 1 || out[0].Table != "ray.c1" || out[0].Upd != 5 {
		t.Fatalf("只应出现在子表名下，实际 %+v", out)
	}
	if out[0].FromPartition {
		t.Error("子表自己的计数不该标 FromPartition")
	}
}

// 父子同查时要说清口径：子表自己成行，父表那行不含它。

func TestOverlappingParents(t *testing.T) {
	tree := buildTree("ray.p>ray.c1", "ray.p>ray.c2")
	got := tree.OverlappingParents([]table.TableRef{{Schema: "ray", Name: "p"}, {Schema: "ray", Name: "c1"}})
	if len(got) != 1 || got[0] != "ray.p" {
		t.Fatalf("want [ray.p]，实际 %v", got)
	}
	note := PartitionExpandNote(nil, got)
	if !strings.Contains(note, "父表那行不再重复计入") {
		t.Errorf("提示文案不对：%s", note)
	}
	if got := tree.OverlappingParents([]table.TableRef{{Schema: "ray", Name: "p"}}); len(got) != 0 {
		t.Errorf("只查父表不该提示重复，实际 %v", got)
	}
}
