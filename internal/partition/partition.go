package partition

import (
	"database/sql"
	"kbchange/internal/table"
	"sort"
	"strconv"
	"strings"
)

type RelTree struct {
	children map[string][]string
}

// LoadRelTree 一次只读查询把全库分区关系取回。
// pg_inherits 单库只有几百行，一次取回比按表递归查询省往返，也不会漏多层分区。

func LoadRelTree(db *sql.DB) (*RelTree, error) {
	rows, err := db.Query(`
		SELECT pn.nspname, p.relname, cn.nspname, c.relname
		FROM pg_inherits i
		JOIN pg_class p ON p.oid = i.inhparent
		JOIN pg_namespace pn ON pn.oid = p.relnamespace
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_namespace cn ON cn.oid = c.relnamespace`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	t := &RelTree{children: map[string][]string{}}
	for rows.Next() {
		var pSchema, pName, cSchema, cName string
		if err := rows.Scan(&pSchema, &pName, &cSchema, &cName); err != nil {
			return nil, err
		}
		parent := pSchema + "." + pName
		t.children[parent] = append(t.children[parent], cSchema+"."+cName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for k := range t.children {
		sort.Strings(t.children[k])
	}
	return t, nil
}

// descendantsWithDepth 返回所有层级的子表及层级深度（直接子表=1）。
// 带 visited 防环：分区关系理论上无环，但被人为改坏时不能死循环。

func (t *RelTree) descendantsWithDepth(key string) map[string]int {
	out := map[string]int{}
	if t == nil {
		return out
	}
	type item struct {
		key   string
		depth int
	}
	visited := map[string]bool{key: true}
	queue := []item{{key: key, depth: 0}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, ch := range t.children[cur.key] {
			if visited[ch] {
				continue
			}
			visited[ch] = true
			out[ch] = cur.depth + 1
			queue = append(queue, item{key: ch, depth: cur.depth + 1})
		}
	}
	return out
}

// Expand 计算"表 key -> 归属的报告表 key"，并返回每个父表实际展开进来的子分区。
//
//   - 每个 ref 归属自己；
//   - ref 的子分区若没被单独 -tables 点名，归属到最近的 ref 祖先（层级最小者胜）；
//   - 被单独点名的子分区归属自己，不被父表重复计入。
//
// owner 为 nil 时退化为"自己归自己"（分区关系查不到时的兜底，行为与改动前一致）。

func (t *RelTree) Expand(refs []table.TableRef) (owner map[string]string, expanded map[string][]string) {
	owner = map[string]string{}
	expanded = map[string][]string{}
	want := map[string]bool{}
	for _, r := range refs {
		want[r.String()] = true
		owner[r.String()] = r.String()
	}
	if t == nil {
		return owner, expanded
	}
	dist := map[string]int{}
	for _, r := range refs {
		self := r.String()
		for ch, d := range t.descendantsWithDepth(self) {
			if want[ch] {
				// 子分区自己被检查，按它自己的名字报，不并进父表。
				continue
			}
			if cur, ok := dist[ch]; ok && cur <= d {
				// 已经有更近的祖先认领（多层分区时取最近一层）。
				continue
			}
			owner[ch] = self
			dist[ch] = d
		}
	}
	// 展开清单从最终的 owner 反推，避免"先被远祖先占住、后被近祖先改判"时留下脏条目。
	for child, parent := range owner {
		if child == parent {
			continue
		}
		expanded[parent] = append(expanded[parent], child)
	}
	for k := range expanded {
		sort.Strings(expanded[k])
	}
	return owner, expanded
}

// OverlappingParents 返回"子分区同时又被单独检查"的父表清单。
// 这种父子同查的场合，被点名的子表按自己成行，父表那行不再重复计入它——
// 同一份数据只出现一次，但父表合计会少掉这块，不说明会被读成"父表没变"。

func (t *RelTree) OverlappingParents(refs []table.TableRef) []string {
	if t == nil {
		return nil
	}
	want := map[string]bool{}
	for _, r := range refs {
		want[r.String()] = true
	}
	var out []string
	for _, r := range refs {
		self := r.String()
		for ch := range t.descendantsWithDepth(self) {
			if want[ch] {
				out = append(out, self)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

// PartitionExpandNote 把展开结果写成一句可读说明，供报告"分区："行使用。
// overlaps 是"父子同时被检查"的父表清单，只做提示、不参与判定。

func PartitionExpandNote(expanded map[string][]string, overlaps []string) string {
	var parts []string
	if len(expanded) > 0 {
		parents := make([]string, 0, len(expanded))
		for p := range expanded {
			parents = append(parents, p)
		}
		sort.Strings(parents)
		for _, p := range parents {
			kids := expanded[p]
			shown := kids
			extra := ""
			if len(shown) > 3 {
				shown = shown[:3]
				extra = " 等"
			}
			parts = append(parts, "父表 "+p+" 展开 "+strconv.Itoa(len(kids))+" 个子表（"+
				strings.Join(shown, "、")+extra+"）")
		}
		parts = append(parts, "分区/INHERITS 继承的子表，其 DML、统计计数、数据文件 mtime 挂在子表名下，已归到父表；通路1 查父表本身即覆盖全部子表（子表被 -tables 单独点名时按子表单独计）")
	}
	if len(overlaps) > 0 {
		parts = append(parts, "父子表同时被检查（"+strings.Join(overlaps, "、")+" 的子表也在检查范围）：被 -tables 单独点名的子表按自己成行，父表那行不再重复计入它们的数据（父表合计只含未被点名的子表），合并数不会把同一份数据算两次")
	}
	return strings.Join(parts, "；")
}
