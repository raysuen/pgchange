package filecheck

import (
	"database/sql"
	"fmt"
	"kbchange/internal/table"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
)

type FileResult struct {
	Table       string `json:"table"`
	Relfilenode string `json:"relfilenode"`
	LatestMTime string `json:"latest_mtime"`
	Changed     bool   `json:"changed"`
	Status      string `json:"status"` // OK / SKIP / ERROR
	Note        string `json:"note,omitempty"`
}

type FileCheckReport struct {
	Available    bool         `json:"available"`
	DataDir      string       `json:"data_dir"`
	DBOID        string       `json:"db_oid"`
	ChangedCount int          `json:"changed_count"`
	Note         string       `json:"note,omitempty"`
	Results      []FileResult `json:"results"`
}

type TableNodes struct {
	Main  string
	Toast string
}

// GetDataDirAndOID 只读获取当前库的数据目录和 oid。
// pgdataOverride 非空时用它覆盖（本机目录与 SHOW 不一致的场景）。

func GetDataDirAndOID(db *sql.DB, pgdataOverride string) (string, string, error) {
	dataDir := strings.TrimSpace(pgdataOverride)
	if dataDir == "" {
		if err := db.QueryRow("SHOW data_directory").Scan(&dataDir); err != nil {
			return "", "", fmt.Errorf("SHOW data_directory 失败: %w", err)
		}
	}
	var oid string
	if err := db.QueryRow(`SELECT oid::text FROM pg_database WHERE datname = current_database()`).Scan(&oid); err != nil {
		return "", "", fmt.Errorf("查询当前库 oid 失败: %w", err)
	}
	return strings.TrimSpace(dataDir), oid, nil
}

// GetTableNodes 只读查每张表的主 relfilenode 和 TOAST relfilenode。
// keys 是 "schema.name" 列表（含父表 + 归到它们名下的子表）。
// relfilenode=0 表示映射关系由系统管理，无独立数据文件，标记跳过。

func GetTableNodes(db *sql.DB, keys []string) (map[string]TableNodes, error) {
	out := map[string]TableNodes{}
	if len(keys) == 0 {
		return out, nil
	}
	schemas := make([]string, 0, len(keys))
	names := make([]string, 0, len(keys))
	seenSchema := map[string]bool{}
	for _, key := range keys {
		schema, name := table.SplitTableKey(key)
		if !seenSchema[schema] {
			seenSchema[schema] = true
			schemas = append(schemas, schema)
		}
		names = append(names, name)
	}
	rows, err := db.Query(`
		SELECT n.nspname, c.relname,
		       c.relfilenode::text, COALESCE(tc.relfilenode::text, '0'),
		       c.reltablespace
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_class tc ON tc.oid = c.reltoastrelid
		WHERE n.nspname = ANY($1::text[]) AND c.relname = ANY($2::text[])
		  AND c.relkind IN ('r','p','m')`, pq.Array(schemas), pq.Array(names))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var schema, name, mainNode, toastNode string
		var tablespace uint32
		if err := rows.Scan(&schema, &name, &mainNode, &toastNode, &tablespace); err != nil {
			return nil, err
		}
		if tablespace != 0 {
			// 非默认表空间不在 base/<oid>/ 下，本通路跳过，注明。
			out[schema+"."+name] = TableNodes{Main: "", Toast: ""}
			continue
		}
		out[schema+"."+name] = TableNodes{Main: mainNode, Toast: toastNode}
	}
	return out, rows.Err()
}

// NodeFilePrefixMatches 判断文件名是否属于该 relfilenode。
// 精确前缀，避免 123 误匹配 1234：<node>、<node>.N、<node>_fsm/_vm/_init。

func NodeFilePrefixMatches(filename, node string) bool {
	if node == "" || node == "0" {
		return false
	}
	if filename == node {
		return true
	}
	if strings.HasPrefix(filename, node+".") {
		return true
	}
	if strings.HasPrefix(filename, node+"_") {
		return true
	}
	return false
}

// ScanNodeMTimeMap 只读扫一次 base/<oid>/，返回每个 relfilenode 的最新 mtime。
// 原来的实现每张表每个 node 都 ReadDir 一次，500 张表就是 500 次目录遍历，白烧 IO。

func ScanNodeMTimeMap(baseDir string, nodes map[string]bool) (map[string]time.Time, error) {
	out := map[string]time.Time{}
	if len(nodes) == 0 {
		return out, nil
	}
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		stem := name
		if i := strings.IndexAny(name, "._"); i > 0 {
			stem = name[:i]
		}
		if !nodes[stem] || !NodeFilePrefixMatches(name, stem) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if mt := info.ModTime(); mt.After(out[stem]) {
			out[stem] = mt
		}
	}
	return out, nil
}

// CheckFiles 通路4 主入口。只读，不改库、不碰文件。
// owner 是分区/继承归属映射（子表 key -> 父表 key）：写入落在子表文件上，
// 父表自己的文件不会因此变化（实测 INHERITS 父表 3 万行写入后父文件 mtime 不动，
// 子文件已刷盘）——不把子表文件纳入，父表的 mtime 就是结构性漏报，连"仅供参考"都算不上。

func CheckFiles(db *sql.DB, refs []table.TableRef, owner map[string]string, pgdataOverride string, since time.Time, sinceLoc *time.Location) FileCheckReport {
	r := FileCheckReport{Results: []FileResult{}}
	dataDir, oid, err := GetDataDirAndOID(db, pgdataOverride)
	if err != nil {
		r.Available = false
		r.Note = err.Error()
		return r
	}
	r.DataDir = dataDir
	r.DBOID = oid

	baseDir := filepath.Join(dataDir, "base", oid)
	if st, err := os.Stat(baseDir); err != nil || !st.IsDir() {
		r.Available = false
		r.Note = fmt.Sprintf("数据目录不可达（远程库或无读权限）：%s。通路4跳过。", baseDir)
		return r
	}
	r.Available = true

	// 查询范围 = 被检查的表 + 归到它们名下的子表（写入实际落在子表文件上）。
	queryKeys := make([]string, 0, len(refs))
	for _, ref := range refs {
		queryKeys = append(queryKeys, ref.String())
	}
	aggregated := map[string][]string{} // 父表 key -> 归入它的子表 key
	for child, parent := range owner {
		if child == parent {
			continue
		}
		if _, isRef := NodeKeys(refs)[parent]; !isRef {
			continue
		}
		if _, isRef := NodeKeys(refs)[child]; isRef {
			// 子表被 -tables 单独点名时按自己成行，不并入父表。
			continue
		}
		aggregated[parent] = append(aggregated[parent], child)
		queryKeys = append(queryKeys, child)
	}
	nodes, err := GetTableNodes(db, queryKeys)
	if err != nil {
		r.Available = false
		r.Note = fmt.Sprintf("查询 relfilenode 失败：%v", err)
		return r
	}

	nodeSet := map[string]bool{}
	for _, n := range nodes {
		for _, node := range []string{n.Main, n.Toast} {
			if node != "" && node != "0" {
				nodeSet[node] = true
			}
		}
	}
	latestByNode, err := ScanNodeMTimeMap(baseDir, nodeSet)
	if err != nil {
		r.Available = false
		r.Note = fmt.Sprintf("扫描数据目录失败：%v", err)
		return r
	}

	for _, ref := range refs {
		n, found := nodes[ref.String()]
		res := FileResult{Table: ref.String(), Relfilenode: n.Main, Status: "OK", LatestMTime: "-"}
		if !found {
			// 查不到不等于"非默认表空间"：也可能是表名打错、或不是普通表/分区表/物化视图。
			// 原来统一写"非默认表空间或映射关系"，会让打错表名的人往表空间方向查。
			res.Status = "SKIP"
			res.Note = "表不存在，或不是普通表/分区表/物化视图（检查 -tables/-schema 拼写）"
			r.Results = append(r.Results, res)
			continue
		}
		// 聚合节点 = 自己的 + 归入它的子表的（写入在子表文件上，父文件不会动）。
		ownNodes := []string{n.Main, n.Toast}
		childCount := 0
		var latest time.Time
		lookAt := func(nodes []string) {
			for _, node := range nodes {
				if node == "" || node == "0" {
					continue
				}
				if mt := latestByNode[node]; mt.After(latest) {
					latest = mt
				}
			}
		}
		lookAt(ownNodes)
		for _, child := range aggregated[ref.String()] {
			cn, ok := nodes[child]
			if !ok {
				continue
			}
			childCount++
			lookAt([]string{cn.Main, cn.Toast})
		}
		if latest.IsZero() {
			if n.Main == "" {
				res.Status = "SKIP"
				res.Note = "非默认表空间或映射关系，无独立数据文件"
			} else if n.Main == "0" && childCount == 0 {
				res.Status = "SKIP"
				res.Note = "relfilenode=0 且无子表数据文件（如分区父表无子分区）"
			} else {
				res.Status = "SKIP"
				res.Note = "未找到匹配的数据文件"
			}
			r.Results = append(r.Results, res)
			continue
		}
		if childCount > 0 {
			res.Note = fmt.Sprintf("mtime 含 %d 个未单独检查的子表文件（写入在子表上，父表文件不会动）", childCount)
		}
		// mtime 是文件系统记录的绝对时刻，按用户指定时区显示；比较用绝对时刻，不受时区影响。
		res.LatestMTime = latest.In(sinceLoc).Format("2006-01-02 15:04:05")
		res.Changed = latest.After(since)
		if res.Changed {
			r.ChangedCount++
		}
		r.Results = append(r.Results, res)
	}

	sort.Slice(r.Results, func(i, j int) bool { return r.Results[i].Table < r.Results[j].Table })
	return r
}

// NodeKeys 把 refs 转成 key 集合。

func NodeKeys(refs []table.TableRef) map[string]bool {
	m := map[string]bool{}
	for _, r := range refs {
		m[r.String()] = true
	}
	return m
}

// FileChangedTables 返回通路4 判定变化的表名，供 verbose 打印。

func FileChangedTables(r FileCheckReport) []string {
	out := []string{}
	for _, t := range r.Results {
		if t.Changed {
			out = append(out, t.Table)
		}
	}
	return out
}
