package table

import "testing"

func TestParseTables(t *testing.T) {
	refs := ParseTables("public.orders, items, sales.detail", "public")
	if len(refs) != 3 {
		t.Fatalf("expected 3 refs, got %d", len(refs))
	}
	want := []TableRef{
		{Schema: "public", Name: "orders"},
		{Schema: "public", Name: "items"},
		{Schema: "sales", Name: "detail"},
	}
	for i := range want {
		if refs[i] != want[i] {
			t.Errorf("ref[%d] = %+v, want %+v", i, refs[i], want[i])
		}
	}
	if got := refs[0].String(); got != "public.orders" {
		t.Errorf("String() = %q", got)
	}
}

func TestUnionSchemas(t *testing.T) {
	got := UnionSchemas([]string{"public"}, []TableRef{
		{Schema: "public", Name: "a"},
		{Schema: "ray", Name: "test01"},
		{Schema: "ray", Name: "test02"},
	})
	want := []string{"public", "ray"}
	if len(got) != len(want) {
		t.Fatalf("UnionSchemas = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("UnionSchemas[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestLimitRefsPerSchema(t *testing.T) {
	// 多模式按名字排序后全局 LIMIT 会把靠后的模式整张截掉；改成按模式分组限额。
	all := []TableRef{
		{Schema: "information_schema", Name: "tables"},
		{Schema: "information_schema", Name: "columns"},
		{Schema: "information_schema", Name: "views"},
		{Schema: "ray", Name: "test01"},
		{Schema: "ray", Name: "test02"},
	}
	refs, warns := LimitRefsPerSchema(all, 2)
	// 每个模式各留 2 张：靠后的 ray 不能整张漏掉。
	got := map[string]bool{}
	for _, r := range refs {
		got[r.String()] = true
	}
	if !got["information_schema.columns"] || !got["information_schema.tables"] {
		t.Errorf("information_schema 应保留排序前2张 columns/tables, got %v", got)
	}
	if !got["ray.test01"] || !got["ray.test02"] {
		t.Errorf("ray 模式不应被全局 LIMIT 整张截掉, got %v", got)
	}
	// information_schema 3 张截到 2 张给告警；ray 恰好 2 张不截断，不给告警。
	if len(warns) != 1 || !contains(warns[0], "information_schema") {
		t.Errorf("应有1条 information_schema 截断告警, got %v", warns)
	}

	// limit<=0 不静默返回空，要给告警。
	refs, warns = LimitRefsPerSchema(all, 0)
	if len(refs) != 0 || len(warns) == 0 {
		t.Errorf("limit<=0 应返回空表+告警, got refs=%v warns=%v", refs, warns)
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
