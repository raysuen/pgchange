package table

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/lib/pq"
)

type TableRef struct {
	Schema string
	Name   string
}

func (r TableRef) String() string { return r.Schema + "." + r.Name }

func SplitList(s string) []string {
	var out []string
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

// UnionSchemas 把 -schema 列表与 -tables 里实际出现的模式合并去重。
// 不加这一步，-tables 跨模式指定表时 path3 按 -schema 过滤会漏表。

func UnionSchemas(schemas []string, refs []TableRef) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(schemas)+len(refs))
	for _, s := range schemas {
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for _, r := range refs {
		if r.Schema != "" && !seen[r.Schema] {
			seen[r.Schema] = true
			out = append(out, r.Schema)
		}
	}
	sort.Strings(out)
	return out
}

func ParseTables(s, defSchema string) []TableRef {
	var out []TableRef
	seen := map[string]bool{}
	for _, item := range SplitList(s) {
		parts := strings.Split(item, ".")
		r := TableRef{Schema: defSchema, Name: parts[len(parts)-1]}
		if len(parts) >= 2 {
			r.Schema = parts[len(parts)-2]
		}
		key := r.String()
		if !seen[key] {
			seen[key] = true
			out = append(out, r)
		}
	}
	return out
}

// tz.DBTimeZone 只读查询数据库会话时区。

func LimitRefsPerSchema(refs []TableRef, limit int) ([]TableRef, []string) {
	var warns []string
	if limit <= 0 {
		return nil, []string{fmt.Sprintf("-limit=%d，未发现任何表", limit)}
	}
	bySchema := map[string][]string{}
	for _, r := range refs {
		bySchema[r.Schema] = append(bySchema[r.Schema], r.Name)
	}
	schemas := make([]string, 0, len(bySchema))
	for s := range bySchema {
		schemas = append(schemas, s)
		sort.Strings(bySchema[s])
	}
	sort.Strings(schemas)
	out := make([]TableRef, 0, len(refs))
	for _, s := range schemas {
		names := bySchema[s]
		keep := len(names)
		if keep > limit {
			keep = limit
			warns = append(warns, fmt.Sprintf("模式 %s 共 %d 张表，仅检查按名称排序的前 %d 张，其余 %d 张未检查（可调大 -limit）", s, len(names), keep, len(names)-keep))
		}
		for i := 0; i < keep; i++ {
			out = append(out, TableRef{Schema: s, Name: names[i]})
		}
	}
	return out, warns
}

// DiscoverTables 发现模式下所有基表，再按模式分组限额。
// 注意这里不在 SQL 侧做全局 LIMIT——那会让靠后的模式整张漏掉。

func DiscoverTables(db *sql.DB, schemas []string, limit int) ([]TableRef, []string, error) {
	rows, err := db.Query(`
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE' AND table_schema = ANY($1::text[])
		ORDER BY table_schema, table_name`, pq.Array(schemas))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var all []TableRef
	for rows.Next() {
		var s, n string
		if err := rows.Scan(&s, &n); err != nil {
			return nil, nil, err
		}
		all = append(all, TableRef{Schema: s, Name: n})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	refs, warns := LimitRefsPerSchema(all, limit)
	return refs, warns, nil
}

// 时间类型 -> 语义：带时区的按绝对时刻比较，不带时区的按墙上时间比较。

func SplitTableKey(key string) (string, string) {
	if i := strings.Index(key, "."); i >= 0 {
		return key[:i], key[i+1:]
	}
	return "", key
}
