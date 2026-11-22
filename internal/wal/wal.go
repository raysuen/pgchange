package wal

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"kbchange/internal/table"
	"kbchange/internal/tz"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type WalCoverage string

const (
	WalFull    WalCoverage = "full"
	WalPartial WalCoverage = "partial"
	WalNone    WalCoverage = "none"
)

// WalTableDML 一张表的 DML 汇总。

type WalTableDML struct {
	Table   string `json:"table"`
	Ins     int64  `json:"ins"`
	Upd     int64  `json:"upd"`
	Del     int64  `json:"del"`
	LastLSN string `json:"last_lsn"`
}

// WalCheckReport 通路2 的完整结果。

type WalCheckReport struct {
	Enabled      bool          `json:"enabled"`
	Available    bool          `json:"available"`
	Coverage     WalCoverage   `json:"coverage,omitempty"`
	WALDir       string        `json:"wal_dir"`
	Waldump      string        `json:"waldump"`
	RangeStart   string        `json:"range_start"`  // WAL 最早提交时间
	RangeEnd     string        `json:"range_end"`    // WAL 最晚提交时间
	CompareFrom  string        `json:"compare_from"` // 实际解析起点时间
	Warning      string        `json:"warning"`      // 告警（覆盖不足/不可达）
	Note         string        `json:"note"`
	Changed      []WalTableDML `json:"changed_tables"`
	ChangedCount int           `json:"changed_count"`
	Scanned      int           `json:"scanned_records"`
	// OrphanTx/OrphanDML：有目标表 DML、但扫描窗口内找不到对应 COMMIT 的事务/行数。
	// 既可能是子事务（用子 xid，COMMIT 记在顶层 xid）、回滚事务、或 COMMIT 段已被回收。
	// 不能算进变化（会把回滚误报），也不能静默丢弃（会漏子事务）——暴露成告警。
	OrphanTx  int `json:"orphan_tx"`
	OrphanDML int `json:"orphan_dml"`
	// SubxactMerged：按 COMMIT 行 subxacts 列表归并到父事务的子事务个数。
	// 这些子事务的 DML 已按父事务的提交时间判定，不再算孤儿。
	SubxactMerged int `json:"subxact_merged"`
	// TLI：检测到的时间线切换（脑裂/恢复后 promote 会产生新时间线）。
	// 旧时间线的 COMMIT 记录无法区分"真实写入"与"切换时被撤回的事务"，
	// 按口径整段忽略：只扫新时间线，切换信息暴露给告警。
	TLI *TliFork `json:"tli_fork,omitempty"`
}

// TliFork 一次时间线切换的检测结果。ForkLSN 来自新时间线的 .history 文件，
// 段被回收时 history 可能缺失，只留空不阻塞。

type TliFork struct {
	From    string `json:"from"`               // 切换来源的旧 TLI（8 位 hex）
	To      string `json:"to"`                 // 当前扫描的新 TLI
	ForkLSN string `json:"fork_lsn,omitempty"` // 切换点 LSN（history 缺失时为空）
}

// waldump 行解析用的正则。
// 示例：rmgr: Heap  len (rec/tot): 54/1950, tx: 890, lsn: 0/06000120, ... desc: INSERT off 8 ..., blkref #0: rel 1664/0/1262 blk 0 FPW

var (
	reWALRecord = regexp.MustCompile(`^rmgr:\s+(\S+)\s+.*?\btx:\s*(\d+),\s*lsn:\s*([0-9A-Fa-f]+/[0-9A-Fa-f]+)`)
	// rel 三段：tablespace/dboid/relfilenode
	reWALRel = regexp.MustCompile(`\brel\s+(\d+)/(\d+)/(\d+)`)
	// desc 里的操作类型：
	//   COPY 走 MULTI_INSERT；Kingbase V009 的堆内原地更新打印 INPLACE；
	//   LOCK 也列进来是为了让 op 保持空、显式跳过（它不是 DML）。
	reHeapOp = regexp.MustCompile(`desc:\s*(MULTI_INSERT|INSERT|UPDATE|DELETE|HOT_UPDATE|INPLACE|LOCK)`)
	// MULTI_INSERT off 0 num 3 —— 一条 WAL 记录塞多行，按 num 计行数。
	reMultiInsertNum = regexp.MustCompile(`\bnum\s+(\d+)`)
	// COMMIT 行两种写法都要认：
	//   金仓：desc: COMMIT 2026-11-17 08:45:31.473489 UTC; inval...
	//   PG：  desc: COMMIT 2026-11-17 08:45:31.473489+08
	// 时区段允许紧贴时间，也允许空格隔开。
	reCommit = regexp.MustCompile(`desc:\s*COMMIT\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s*([A-Za-z][A-Za-z0-9_/+-]*|[+-]\d{2}(?::?\d{2})?)?`)
	// COMMIT 行尾会列出"随本事务一起提交的子事务"：desc: COMMIT ...; subxacts: 973 974
	// 子事务的 DML 记的是子事务 xid，不把这批 xid 归到父事务，就会被当成孤儿丢掉。
	// 实测 BEGIN; SAVEPOINT s1; INSERT; RELEASE s1; COMMIT 的行确实入库，
	// 但通路2 报 0 张变化 + 孤儿告警（假阴性）。
	reSubxacts = regexp.MustCompile(`subxacts:\s*([0-9][0-9,\s]*)`)
	// 回滚记录：desc: ABORT 2026-11-17 13:53:59.026899 UTC
	// 回滚掉的 DML 本来就不算变化，当成"找不到 COMMIT 的孤儿"报"可能低估"是误报。
	reAbort = regexp.MustCompile(`desc:\s*ABORT\b`)
)

// LocateWALDir 定位 WAL 目录：
//   - override 非空直接用；
//   - 否则按 dataDir 拼 sys_wal（金仓）/ pg_wal（PG），探测哪个存在。

func LocateWALDir(dataDir, override string) (string, error) {
	if override != "" {
		info, err := os.Stat(override)
		if err != nil {
			return "", fmt.Errorf("指定 WAL 目录不可访问：%w", err)
		}
		if !info.IsDir() {
			return "", fmt.Errorf("指定 WAL 路径不是目录：%s", override)
		}
		return override, nil
	}
	if dataDir == "" {
		return "", fmt.Errorf("无数据目录，无法自动定位 WAL（可用 -wal 指定）")
	}
	// 金仓用 sys_wal，PG 用 pg_wal，两个都试。
	for _, name := range []string{"sys_wal", "pg_wal"} {
		p := filepath.Join(dataDir, name)
		if info, err := os.Stat(p); err == nil && info.IsDir() {
			return p, nil
		}
	}
	return "", fmt.Errorf("数据目录 %s 下未找到 sys_wal 或 pg_wal（可用 -wal 指定）", dataDir)
}

// ResolveWALOverride 把 -wal 参数换成宿主机能 stat 的路径。
// 相对名按数据目录找；容器里的绝对路径加 loaderRoot 前缀；已经是 root 前缀的不动。

func ResolveWALOverride(override, loaderRoot, dataDir string) string {
	if override == "" || loaderRoot == "" {
		return override
	}
	if filepath.IsAbs(override) {
		if strings.HasPrefix(override, loaderRoot) {
			return override
		}
		return filepath.Join(loaderRoot, filepath.FromSlash(override))
	}
	base := dataDir
	if base == "" {
		base = loaderRoot
	}
	return filepath.Join(base, filepath.FromSlash(override))
}

// WalSegmentNames 列 WAL 目录下的段文件名（24 位十六进制），按名字排序。

func WalSegmentNames(walDir string) ([]string, error) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return nil, err
	}
	segRe := regexp.MustCompile(`^[0-9A-F]{24}$`)
	var out []string
	for _, e := range entries {
		if !e.IsDir() && segRe.MatchString(e.Name()) {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

// SegStartNum 把段文件名换成起始 LSN 的数值。
// 文件名 = TLI(8) + log(8) + seg(8)；每个 log id 下有 0x100000000/segSize 个段。

func SegStartNum(name string, segSize uint64) (uint64, error) {
	if len(name) != 24 {
		return 0, fmt.Errorf("非法段名 %q", name)
	}
	logID, err := strconv.ParseUint(name[8:16], 16, 32)
	if err != nil {
		return 0, err
	}
	segNo, err := strconv.ParseUint(name[16:24], 16, 32)
	if err != nil {
		return 0, err
	}
	if segSize == 0 {
		segSize = 16 * 1024 * 1024
	}
	perLog := uint64(0x100000000) / segSize
	return (logID*perLog + segNo) * segSize, nil
}

// SegNameToStartLSN 把段文件名换成 "高32位/低32位" 的 LSN 文本。

func SegNameToStartLSN(name string, segSize uint64) (string, error) {
	lsn, err := SegStartNum(name, segSize)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%X/%X", lsn>>32, lsn&0xFFFFFFFF), nil
}

// WalSegmentGaps 检查同一时间线内的段号是否连续，返回缺口描述。
// 段被回收/手工删除时会在这里暴露，避免把"少了一段"误当成"那段时间没有数据"。

func WalSegmentGaps(segs []string, segSize uint64) []string {
	var gaps []string
	var prev uint64
	var prevTLI string
	for i, name := range segs {
		n, err := SegStartNum(name, segSize)
		if err != nil || len(name) < 8 {
			continue
		}
		tli := name[:8]
		if i > 0 && tli == prevTLI && n > prev && n-prev > segSize {
			gaps = append(gaps, fmt.Sprintf("%s 与 %s 之间缺 %d 个段", segs[i-1], name, (n-prev)/segSize-1))
		}
		prev, prevTLI = n, tli
	}
	return gaps
}

// TliSplit 按段名前 8 位 TLI 检查时间线。
// 全部同一 TLI：原样返回。
// 出现更高 TLI 的段：说明实例发生过时间线切换（脑裂/恢复后 promote）。
// 旧时间线上已 COMMIT 的事务可能已被切换撤回（数据不存在），也可能仍保留（fork 点前的写入），
// 只凭 WAL COMMIT 记录无法区分——按口径整段忽略，只返回新 TLI 的段用于扫描，并给出切换信息。
// history 文件（<TLI>.history）记录切换点 LSN；段被回收时可能缺失，缺失不阻塞、ForkLSN 留空。

func TliSplit(walDir string, segs []string) ([]string, *TliFork) {
	if len(segs) == 0 {
		return segs, nil
	}
	minTLI, maxTLI := segs[0][:8], segs[0][:8]
	for _, s := range segs {
		t := s[:8]
		if t < minTLI {
			minTLI = t
		}
		if t > maxTLI {
			maxTLI = t
		}
	}
	if minTLI == maxTLI {
		return segs, nil
	}
	var kept []string
	for _, s := range segs {
		if s[:8] == maxTLI {
			kept = append(kept, s)
		}
	}
	if len(kept) == 0 {
		// 理论上到不了（maxTLI 取自段名本身），防御一下：拿不准时不做任何剔除。
		return segs, nil
	}
	return kept, &TliFork{From: minTLI, To: maxTLI, ForkLSN: ReadTLIHistory(walDir, maxTLI)}
}

// ReadTLIHistory 读新时间线的 history 文件，返回进入该时间线的切换点 LSN。
// 行格式："parentTLI<TAB>forkLSN<TAB>reason"。多级切换链（1→2→3）时 history 会有多行，
// 取 parent 数值最大的行——那是进入当前 TLI 的最近一次切换；文件缺失/解析不出返回空串。

func ReadTLIHistory(walDir, tli string) string {
	b, err := os.ReadFile(filepath.Join(walDir, tli+".history"))
	if err != nil {
		return ""
	}
	bestParent := uint64(0)
	lsn := ""
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		p, err := strconv.ParseUint(fields[0], 16, 32)
		if err != nil {
			continue
		}
		if lsn == "" || p >= bestParent {
			bestParent = p
			lsn = fields[1]
		}
	}
	return lsn
}

// ParseSizeText 解析 SHOW wal_segment_size 的返回值。
// 金仓实测返回 "16MB"，PG 有的版本返回字节数；两种都要认，否则段起始 LSN 会算错。

func ParseSizeText(s string) (uint64, error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	re := regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGTP]?B?)?$`)
	m := re.FindStringSubmatch(t)
	if m == nil {
		return 0, fmt.Errorf("无法解析大小 %q", s)
	}
	var num float64
	if _, err := fmt.Sscanf(m[1], "%f", &num); err != nil {
		return 0, err
	}
	mult := uint64(1)
	switch strings.TrimSuffix(m[2], "B") {
	case "":
	case "K":
		mult = 1 << 10
	case "M":
		mult = 1 << 20
	case "G":
		mult = 1 << 30
	case "T":
		mult = 1 << 40
	case "P":
		mult = 1 << 50
	default:
		return 0, fmt.Errorf("无法解析大小单位 %q", s)
	}
	return uint64(num * float64(mult)), nil
}

// waldumpCmd 构造只读调用命令。
// 容器场景：sys_waldump 的动态库在容器内，宿主机直接跑会缺库，需要用容器内的 loader + library-path。
// 参数 loaderRoot 形如 /proc/<pid>/root；为空表示工具在宿主 PATH 里可直接执行。

type WaldumpSpec struct {
	Bin     string // waldump 可执行文件路径
	Loader  string // 容器内动态链接器路径（可空）
	LibPath string // 容器内库路径，冒号分隔（可空）
}

func (s WaldumpSpec) build(args ...string) *exec.Cmd {
	if s.Loader != "" {
		full := []string{"--library-path", s.LibPath, s.Bin}
		full = append(full, args...)
		return exec.Command(s.Loader, full...)
	}
	return exec.Command(s.Bin, args...)
}

// ParseCommitTime 解析 waldump COMMIT 行里的时间。waldump 按自身 TZ 输出（如 CST/UTC），
// 这里用调用方传入的 loc 解释——调用方应把 waldump 的 TZ 设成 DB 会话时区，跟 since 同口径。

func ParseCommitTime(ts, zone string, loc *time.Location) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
	}
	// PG 会输出紧贴时间的 +08 / +00:00；按这个偏移解释，别硬塞给 loc。
	z := strings.TrimRight(strings.TrimSpace(zone), ";,")
	if strings.HasPrefix(z, "+") || strings.HasPrefix(z, "-") {
		if zloc, err := tz.LoadLocation(z); err == nil {
			for _, l := range layouts {
				if t, err := time.ParseInLocation(l, ts, zloc); err == nil {
					return t, nil
				}
			}
		}
	}
	var t time.Time
	var err error
	for _, l := range layouts {
		t, err = time.ParseInLocation(l, ts, loc)
		if err == nil {
			return t, nil
		}
	}
	// 名字对不上时（如自定义时区名），退一步：按 UTC 解释字符串，保证能比较。
	t, err = time.ParseInLocation(layouts[0], ts, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("无法解析 COMMIT 时间 %q %q: %w", ts, zone, err)
	}
	return t, nil
}

// WalDMLKey 事务内一张表的一次 DML。
// TS 是表空间段：relfilenode 在"表空间+库"内才唯一（OID 回绕后同库可能撞号），
// 不带表空间就可能把 A 表的 DML 记到 B 表头上——键必须用完整三段。

type WalDMLKey struct {
	DBOID   uint32
	TS      uint32
	RelFile uint32
	Op      string
}

// relKey 返回 "表空间/relfilenode" 键，与 RelfilenodeMap 的键对应。

func (k WalDMLKey) relKey() string {
	return fmt.Sprintf("%d/%d", k.TS, k.RelFile)
}

// WalScanResult 一次完整扫描的中间结果。

type WalScanResult struct {
	Commits   map[uint32]time.Time // tx -> 提交时间（含随父事务提交的子事务 xid）
	CommitMin time.Time
	CommitMax time.Time
	Aborted   map[uint32]bool                // tx -> 已回滚
	DMLByTx   map[uint32]map[WalDMLKey]int64 // tx -> (表+操作) -> 次数
	LastLSN   map[WalDMLKey]string           // 表+操作 -> 最后一条 LSN，便于回查
	Scanned   int
	ParseErrs int
	// SubxactMerged：从 COMMIT 行的 subxacts 列表归并进来的子事务个数。
	SubxactMerged int
}

// LsnValue 把 "0/1A2B3C4" 变成可比较的数值。

func LsnValue(s string) uint64 {
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return 0
	}
	hi, err1 := strconv.ParseUint(parts[0], 16, 32)
	lo, err2 := strconv.ParseUint(parts[1], 16, 32)
	if err1 != nil || err2 != nil {
		return 0
	}
	return hi<<32 | lo
}

// ScanWaldumpOutput 解析 waldump 文本输出，收集 COMMIT 与 DML。纯函数，便于单测。
// currentDBOID 非 0 时只保留该库的 DML（rel 中间段 = dboid）。
// allowedKeys 非 nil 时只保留这些 "表空间/relfilenode" 键，避免把系统表/别的表装进内存。

func ScanWaldumpOutput(r io.Reader, loc *time.Location, currentDBOID uint32, allowedKeys map[string]bool) WalScanResult {
	res := WalScanResult{
		Commits: map[uint32]time.Time{},
		Aborted: map[uint32]bool{},
		DMLByTx: map[uint32]map[WalDMLKey]int64{},
		LastLSN: map[WalDMLKey]string{},
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024) // 单行 inval 明细可能很长
	for sc.Scan() {
		line := sc.Text()
		m := reWALRecord.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		res.Scanned++
		tx, err := strconv.ParseUint(m[2], 10, 32)
		if err != nil {
			continue
		}
		rmgr := m[1]

		if rmgr == "Transaction" {
			if cm := reCommit.FindStringSubmatch(line); cm != nil {
				t, err := ParseCommitTime(cm[1], cm[2], loc)
				if err != nil {
					res.ParseErrs++
					continue
				}
				res.Commits[uint32(tx)] = t
				// 同一事务里开的子事务（SAVEPOINT）如果提交了，COMMIT 行会在 subxacts 里列出来。
				// 它们的 DML 记的是子事务 xid，不认这批 xid 就会全部落进"孤儿"、进而漏报。
				for _, sub := range ParseSubxactList(line) {
					if _, dup := res.Commits[sub]; dup {
						continue
					}
					res.Commits[sub] = t
					res.SubxactMerged++
				}
				if res.CommitMin.IsZero() || t.Before(res.CommitMin) {
					res.CommitMin = t
				}
				if t.After(res.CommitMax) {
					res.CommitMax = t
				}
				continue
			}
			if reAbort.MatchString(line) {
				res.Aborted[uint32(tx)] = true
			}
			continue
		}

		if rmgr != "Heap" && rmgr != "Heap2" {
			continue
		}
		op := ""
		rawOp := ""
		if om := reHeapOp.FindStringSubmatch(line); om != nil {
			rawOp = om[1]
			switch om[1] {
			case "INSERT", "MULTI_INSERT":
				op = "ins"
			case "UPDATE", "HOT_UPDATE", "INPLACE":
				// INPLACE 是 Kingbase 的原地 UPDATE（不产生新 tuple 版本），按改计数。
				op = "upd"
			case "DELETE":
				op = "del"
			}
		}
		if op == "" {
			continue
		}
		rel := reWALRel.FindStringSubmatch(line)
		if rel == nil {
			continue
		}
		dbOID, _ := strconv.ParseUint(rel[2], 10, 32)
		tsOID, _ := strconv.ParseUint(rel[1], 10, 32)
		relFile, _ := strconv.ParseUint(rel[3], 10, 32)
		if currentDBOID != 0 && uint32(dbOID) != currentDBOID {
			continue
		}
		key := WalDMLKey{DBOID: uint32(dbOID), TS: uint32(tsOID), RelFile: uint32(relFile), Op: op}
		if allowedKeys != nil && !allowedKeys[key.relKey()] {
			continue
		}
		cnt := int64(1)
		if rawOp == "MULTI_INSERT" {
			if nm := reMultiInsertNum.FindStringSubmatch(line); nm != nil {
				if n, err := strconv.ParseInt(nm[1], 10, 64); err == nil && n > 0 {
					cnt = n
				}
			}
		}
		if res.DMLByTx[uint32(tx)] == nil {
			res.DMLByTx[uint32(tx)] = map[WalDMLKey]int64{}
		}
		res.DMLByTx[uint32(tx)][key] += cnt
		if old := res.LastLSN[key]; old == "" || LsnValue(m[3]) > LsnValue(old) {
			res.LastLSN[key] = m[3]
		}
	}
	if err := sc.Err(); err != nil {
		res.ParseErrs++
	}
	return res
}

// RelfilenodeMap 只读查 pg_class，把 relfilenode 映射回 schema.table。
// 注意：金仓里 `nspname || '.' || relname` 的 || 可能被当成逻辑或（返回 false），
// 所以分开取两列，在 Go 侧拼接，避免踩方言差异。
// 返回 map 键为 "表空间oid/relfilenode"。relfilenode 只在表空间+库内唯一，
// 同库跨表空间可能撞号（当前库实测无重复；OID 回绕后理论上会撞），键必须带表空间段。

func RelfilenodeMap(db *sql.DB) (map[string]string, error) {
	// 注意：不能用 SQL 侧 || 拼键——金仓把 || 当逻辑或（实测返回 false），
	// 必须分开取两列，在 Go 侧拼 "表空间/relfilenode"。上一版在 SQL 里拼键，
	// 矩阵一把就抓到（所有映射行变 false，allowedKeys 全空，通路2 报"目标表没有匹配到数据文件"）。
	// 另一个坑：pg_class.reltablespace=0 表示"默认表空间"（不是字面 0），
	// WAL 里默认表空间打的是 pg_default 的实际 OID（1663）。0 必须解析成默认表空间 OID，
	// 否则键对不上，所有普通表都被 allowedKeys 过滤掉（第二轮矩阵抓到）。
	var defTS uint64
	if err := db.QueryRow(`SELECT oid FROM pg_tablespace WHERE spcname IN ('pg_default','sys_default') LIMIT 1`).Scan(&defTS); err != nil || defTS == 0 {
		defTS = 1663 // 查不到按 PG 惯例兜底
	}
	rows, err := db.Query(`
		SELECT c.reltablespace::text, c.relfilenode::text, n.nspname, c.relname
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relfilenode <> 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[string]string{}
	for rows.Next() {
		var tsStr, rfStr, schema, name string
		if err := rows.Scan(&tsStr, &rfStr, &schema, &name); err != nil {
			continue
		}
		var ts, rf uint64
		if _, err := fmt.Sscanf(tsStr, "%d", &ts); err != nil {
			continue
		}
		if ts == 0 {
			ts = defTS
		}
		if _, err := fmt.Sscanf(rfStr, "%d", &rf); err != nil || rf == 0 {
			continue
		}
		m[fmt.Sprintf("%d/%d", ts, rf)] = schema + "." + name
	}
	return m, rows.Err()
}

// DecideWALCoverage 按可读 WAL 里的提交时间窗判覆盖。
// 注意方向：
//   - since < commitMin：WAL 起点在指定时点之后，[since, commitMin) 这段不在可读 WAL 内 → partial；
//   - commitMin <= since <= commitMax：WAL 跨过指定时点，[since, 末尾] 可读 → full；
//   - since > commitMax：可读 WAL 里时点后没有任何提交 → none。
//
// 返回 coverage、实际比对起点文本、告警文本。

func DecideWALCoverage(since time.Time, sinceLoc *time.Location, commitMin, commitMax time.Time) (WalCoverage, string, string) {
	sinceText := since.In(sinceLoc).Format("2006-01-02 15:04:05")
	if commitMax.IsZero() || !since.Before(commitMax) {
		return WalNone, commitMax.In(sinceLoc).Format("2006-01-02 15:04:05"),
			fmt.Sprintf("可读 WAL 最新提交 %s（DB时区）早于或等于指定时点 %s，时点后无已提交事务（若段有缺口，不能排除缺口内的变化）",
				commitMax.In(sinceLoc).Format("2006-01-02 15:04:05"), sinceText)
	}
	if since.Before(commitMin) {
		minText := commitMin.In(sinceLoc).Format("2006-01-02 15:04:05")
		return WalPartial, minText,
			fmt.Sprintf("WAL 最早提交 %s（DB时区）晚于指定时点 %s，[%s, %s) 这段不在可读 WAL 内，仅解析已存在的部分（若该段有写入无法判定）",
				minText, sinceText, sinceText, minText)
	}
	return WalFull, sinceText, ""
}

// ParseSubxactList 取 COMMIT 行里 subxacts 后面的子事务 xid 列表。
// 实测两种分隔写法都有："subxacts: 973, 974" / "subxacts: 973 974"。
// 解析不出来就返回 nil——比乱猜一个 xid 安全。

func ParseSubxactList(line string) []uint32 {
	m := reSubxacts.FindStringSubmatch(line)
	if m == nil {
		return nil
	}
	var out []uint32
	for _, tok := range strings.FieldsFunc(m[1], func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	}) {
		n, err := strconv.ParseUint(tok, 10, 32)
		if err != nil || n == 0 {
			continue
		}
		out = append(out, uint32(n))
	}
	return out
}

// CountOrphans 统计"有目标表 DML、但既没提交也没回滚"的事务及其 DML 条数。
// 已回滚的事务不算孤儿：它的 DML 本来就不该计入变化，报"可能低估"是误报
// （实测 BEGIN; INSERT; ROLLBACK 会让告警里多出一条不存在的变化）。
// 抽成纯函数：覆盖判定的 none 分支（一条 COMMIT 都没有）也要计数，
// 否则"有 DML 但全无 COMMIT"这个最关键的信号会被 none 的提前 return 吞掉。

func CountOrphans(dmlByTx map[uint32]map[WalDMLKey]int64, commits map[uint32]time.Time, aborted map[uint32]bool) (tx int, dml int) {
	for t, dmls := range dmlByTx {
		if _, ok := commits[t]; ok {
			continue
		}
		if aborted[t] {
			continue
		}
		tx++
		for _, cnt := range dmls {
			dml += int(cnt)
		}
	}
	return tx, dml
}

// JoinWarn 把多条非空告警拼成一条，中文分号分隔；全空返回空串。
// 用它统一拼告警，避免"原值等于空串时前缀多出一个分号"这类拼接副作用。

func JoinWarn(parts ...string) string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, "；")
}

// OrphanWarningText 把孤儿计数写成告警文案。
// 已回滚的事务在计数前就被排除了，所以这里列的原因里不再有"回滚"。

func OrphanWarningText(tx, dml int) string {
	if tx <= 0 {
		return ""
	}
	return fmt.Sprintf("有 %d 个事务（%d 条目标表 DML）在可读 WAL 内既找不到 COMMIT 也找不到 ABORT（事务仍在运行、PREPARE 后未 COMMIT、或 COMMIT 记录随段被回收），这些 DML 未计入变化表；可能低估", tx, dml)
}

// WalStderrFilter 过滤 waldump 的 stderr 噪音行，返回可解析的有意义内容。
// "could not identify current directory" 是容器内二进制借宿主机 loader 运行时 getcwd 失败的产物，
// 与 WAL 内容无关、每次必现，留着会把真正的错误淹掉。

func WalStderrFilter(s string) string {
	var keep []string
	for _, ln := range strings.Split(s, "\n") {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		if strings.Contains(strings.ToLower(ln), "could not identify current directory") {
			continue
		}
		keep = append(keep, ln)
	}
	return strings.Join(keep, "；")
}

// IsWALTailEnd 判断 waldump 的报错是不是"读到 WAL 末尾未写满区段"的正常收尾。
// 实测 sys_waldump 扫到活动库 WAL 尾部时报 `error in WAL tmpRec at 0/612F338:
// invalid record length at 0/612F368: wanted 24, got 0` 并 exit 1——之前的记录都已正常输出，
// 这不是故障。一律写成"执行失败"会让每次正常巡检都报警。

func IsWALTailEnd(stderr string) bool {
	l := strings.ToLower(stderr)
	if !strings.Contains(l, "invalid record length") {
		return false
	}
	return strings.Contains(l, "got 0")
}

// WaldumpExitText 把 waldump 非零退出翻译成分诊后的告警，benign 表示属正常收尾。

func WaldumpExitText(waitErr error, stderr string, scanned int) (msg string, benign bool) {
	if waitErr == nil {
		return "", false
	}
	clean := WalStderrFilter(stderr)
	if IsWALTailEnd(clean) {
		return fmt.Sprintf("waldump 读到 WAL 末尾未写满的区段后退出（%v，属正常收尾，已解析 %d 条记录；不是故障）：%s",
			waitErr, scanned, clean), true
	}
	if clean == "" {
		clean = "stderr 无输出"
	}
	return fmt.Sprintf("waldump 执行失败（可能只解析了部分 WAL）：%v / %s", waitErr, clean), false
}

// CheckPath2 通路2 主入口：探覆盖 → 判告警 → 解析 DML → 映射表名 → 按 since 过滤。
// sinceLoc 传 DB 会话时区；waldump 进程 TZ 也设成它，保证 COMMIT 时间与 since 同口径。
// owner 是分区归属映射（表 key -> 报告表 key，子分区归到父表名下）；nil 表示按表名一一对应。

func CheckPath2(db *sql.DB, walOverride, waldumpBin string, loaderRoot string, since time.Time, sinceLoc *time.Location, refs []table.TableRef, owner map[string]string) WalCheckReport {
	// Coverage 不预置 none：none 的定义是"已读到 WAL、时点后无已提交事务"。
	// 启用但中途失败（目录不可达/段不可读）时没有读到内容，JSON 里不该出现 "none"，
	// 否则跟"没启用"一样会被读成"查过且没变化"。
	r := WalCheckReport{
		Enabled: true, Available: false,
		Note: "只读解析 WAL：sys_waldump/pg_waldump 读段文件，COMMIT 时间定位事务，DML 行取 relfilenode 映射表名",
	}
	r.Changed = []WalTableDML{}

	// 1. 数据目录 + WAL 目录定位（只读）
	var dataDir string
	_ = db.QueryRow(`SHOW data_directory`).Scan(&dataDir)
	// 容器场景：DB 返回的是容器内路径，宿主机上要加 root 前缀（/proc/<pid>/root）才能 stat 到。
	if loaderRoot != "" && dataDir != "" && !strings.HasPrefix(dataDir, loaderRoot) {
		dataDir = filepath.Join(loaderRoot, filepath.FromSlash(dataDir))
	}
	walOverride = ResolveWALOverride(walOverride, loaderRoot, dataDir)
	walDir, err := LocateWALDir(dataDir, walOverride)
	if err != nil {
		r.Warning = "WAL 目录不可达：" + err.Error()
		r.Note += "；" + r.Warning
		return r
	}
	r.WALDir = walDir

	// 2. waldump 命令定位
	spec := WaldumpSpec{}
	if waldumpBin != "" {
		spec.Bin = waldumpBin
	} else {
		spec.Bin = "sys_waldump"
		if _, err := exec.LookPath(spec.Bin); err != nil {
			spec.Bin = "pg_waldump"
		}
	}
	r.Waldump = spec.Bin
	if loaderRoot != "" {
		// 容器场景：用容器内 loader 启动，绕开动态库缺失。
		for _, ld := range []string{"lib64/ld-linux-aarch64.so.1", "lib/ld-linux-aarch64.so.1",
			"lib64/ld-linux-x86-64.so.2", "lib/ld-linux-x86-64.so.2"} {
			p := filepath.Join(loaderRoot, ld)
			if _, err := os.Stat(p); err == nil {
				spec.Loader = p
				break
			}
		}
		if spec.Loader != "" {
			// 相对 loaderRoot 找 bin 与 lib
			if waldumpBin == "" {
				spec.Bin = FindWaldumpInRoot(loaderRoot)
				if _, err := os.Stat(spec.Bin); err != nil {
					r.Warning = "容器内未找到 sys_waldump/pg_waldump（可用 -wal-bin 指定）"
					r.Note += "；" + r.Warning
					return r
				}
			}
			var libs []string
			for _, d := range []string{"lib", "lib64", "usr/lib", "usr/lib64", "home/kingbase/install/kingbase/lib"} {
				p := filepath.Join(loaderRoot, d)
				if _, err := os.Stat(p); err == nil {
					libs = append(libs, p)
				}
			}
			spec.LibPath = strings.Join(libs, ":")
			r.Waldump = spec.Bin
		}
	}

	// 3. 列段名 → 起始 LSN
	segs, err := WalSegmentNames(walDir)
	if err != nil || len(segs) == 0 {
		r.Warning = fmt.Sprintf("WAL 段不可读或无段文件：%v", err)
		r.Note += "；" + r.Warning
		return r
	}
	// 时间线检查：出现更高 TLI 的段说明发生过切换（脑裂/恢复后 promote）。
	// 旧时间线的 COMMIT 无法区分真实写入与切换时被撤回的事务，整段忽略、只扫新时间线。
	kept, tliForkInfo := TliSplit(walDir, segs)
	r.TLI = tliForkInfo
	if tliForkInfo != nil {
		segs = kept
	}
	var segSize uint64 = 16 * 1024 * 1024
	var ss string
	segSizeFallback := false
	if err := db.QueryRow(`SHOW wal_segment_size`).Scan(&ss); err == nil {
		if n, e := ParseSizeText(ss); e == nil && n > 0 {
			segSize = n
		} else {
			segSizeFallback = true
		}
	} else {
		segSizeFallback = true
		ss = "查询失败"
	}
	startLSN, err := SegNameToStartLSN(segs[0], segSize)
	if err != nil {
		r.Warning = "段名解析失败：" + err.Error()
		r.Note += "；" + r.Warning
		return r
	}
	segGaps := WalSegmentGaps(segs, segSize)

	// 3.1 先确定库 oid 与目标 relfilenode，扫描时只留这些表，避免系统表把内存撑大。
	var currentDBOID uint32
	var oidStr string
	if db.QueryRow(`SELECT oid FROM pg_database WHERE datname = current_database()`).Scan(&oidStr) == nil {
		if n, e := strconv.ParseUint(oidStr, 10, 32); e == nil {
			currentDBOID = uint32(n)
		}
	}
	rfMap, err := RelfilenodeMap(db)
	if err != nil {
		r.Warning = "relfilenode 映射失败：" + err.Error()
		r.Note += "；" + r.Warning
		return r
	}
	want := map[string]bool{}
	for _, ref := range refs {
		want[ref.String()] = true
	}
	// relfilenode -> 报告里显示的表名。
	// 分区父表的 relfilenode 是 0、子分区各有自己的数据文件，WAL 里的 rel 记的是子分区。
	// 不把子分区映射到父表名下，这些 DML 会被 allowedRF 整段过滤掉，
	// 结果就是"父表 0 张 + 覆盖 full"——实测 ray.px 的假阴性正是这么来的。
	rfOwner := map[string]string{}
	for key, name := range rfMap {
		rep := name
		if owner != nil {
			if v, ok := owner[name]; ok {
				rep = v
			}
		}
		if want[rep] {
			rfOwner[key] = rep
		}
	}
	allowedKeys := map[string]bool{}
	for key := range rfOwner {
		allowedKeys[key] = true
	}
	if len(allowedKeys) == 0 {
		// 一张表都没匹配到数据文件：表名打错、或全是 relfilenode=0 的对象。
		// 这时通路2 根本没扫到任何目标表，不能给出"覆盖=full、0 张"这种像确认过的结论。
		r.Warning = "目标表没有匹配到数据文件（表不存在 / relfilenode=0 且无子表），通路2 未扫描任何目标表"
		r.Note += "；" + r.Warning
		return r
	}
	// 不设 -e：让 waldump 一直读到当前末尾，避免用末段起始 LSN 当终点时漏掉末段内的数据。

	// 4. 调 waldump：只读扫全程（Transaction + Heap/Heap2），流式解析 stdout。
	// waldump 默认按 timeline 1（或 STARTSEG 推断）找段。真实切换环境里 TLI1 的段可能
	// 已被回收（只剩 .partial）或与新 TLI 段号重叠，不显式指定 timeline 会直接找不到段
	// （214 实测：sys_wal 里只有 TLI1 的 03/04/05 + 06.partial、TLI2 的 06，waldump 报
	// "could not find file 000000010000000000000006"）。检出切换时必须显式 -t 新 TLI。
	cmdArgs := []string{"-p", walDir, "-s", startLSN}
	if tliForkInfo != nil {
		if n, e := strconv.ParseUint(tliForkInfo.To, 16, 32); e == nil {
			cmdArgs = append(cmdArgs, "-t", strconv.FormatUint(n, 10))
		}
	}
	cmd := spec.build(cmdArgs...)
	// waldump 用进程 TZ 打印 COMMIT 时间；强制 UTC 输出，避免时区名（如 CST/PRC）在
	// 解析端与 DB 会话时区对不上。解析端也按 UTC，再统一换算到 sinceLoc 显示与比较。
	// 注意：exec 对重复环境变量取第一个，必须先滤掉 os.Environ() 里已有的 TZ 再追加，
	// 否则宿主机 TZ（如 Asia/Shanghai）会盖掉这里的 UTC。
	var cleanEnv []string
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "TZ=") {
			cleanEnv = append(cleanEnv, e)
		}
	}
	cmd.Env = append(cleanEnv, "TZ=UTC")

	// 时间线切换告警：切换前数据已按口径忽略，必须显式说清楚，不能静默缩小扫描范围。
	// 提前构造——waldump 全失败的提前 return 也要带上它，切换信息不能因为解析挂了就消失。
	var tliWarn string
	if tliForkInfo != nil {
		at := "切换点 LSN 未知（history 缺失）"
		if tliForkInfo.ForkLSN != "" {
			at = "切换点 LSN " + tliForkInfo.ForkLSN
		}
		tliWarn = fmt.Sprintf("检测到时间线切换（TLI %s→%s，%s）：切换前的 WAL 已整段忽略不计入——旧时间线的 COMMIT 无法区分真实写入与切换时被撤回的事务，'有变化/无变化'仅基于切换后（TLI %s）数据，切换前的窗口需人工核对",
			tliForkInfo.From, tliForkInfo.To, at, tliForkInfo.To)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		r.Warning = "waldump 管道失败：" + err.Error()
		r.Note += "；" + r.Warning
		return r
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		r.Warning = "waldump 启动失败：" + err.Error()
		r.Note += "；" + r.Warning
		return r
	}
	scan := ScanWaldumpOutput(stdout, time.UTC, currentDBOID, allowedKeys)
	waitErr := cmd.Wait()
	r.Scanned = scan.Scanned
	// 有部分输出时 waldump 仍可能非零退出。这里要分诊两种：
	//   - 读到 WAL 末尾未写满区段的正常收尾（活动库必然发生）：保留结果，告警里说明不是故障；
	//   - 真失败（读不到文件/权限/loader 报错）：保留已解析结果，但必须明确"只解析了部分"。
	// 两者都不能直接写 r.Warning——下面 DecideWALCoverage 会把 r.Warning 整个覆盖，
	// 结果就是"只解析了一半，却给出一份看起来完整的结论"。
	waldumpErr, benignExit := WaldumpExitText(waitErr, stderr.String(), r.Scanned)
	if waldumpErr != "" && r.Scanned == 0 && !benignExit {
		// 全失败也要带上时间线告警：切换信息不能因为 waldump 挂了就消失。
		r.Warning = JoinWarn(tliWarn, waldumpErr)
		r.Note += "；" + r.Warning
		return r
	}
	r.Available = true

	// 4. 覆盖判定
	if len(scan.Commits) == 0 {
		r.Coverage = WalNone
		// 一条 COMMIT 都没有时，目标表 DML 全是孤儿，必须在这里就计数——
		// 否则第五轮加的孤儿告警被这个提前 return 吞掉，最该报的场景反而静默。
		r.OrphanTx, r.OrphanDML = CountOrphans(scan.DMLByTx, scan.Commits, scan.Aborted)
		w := "WAL 中没有可解析的 COMMIT 记录，无法判定指定时点后的变化（段可能被回收或无提交）"
		if r.Scanned > 0 {
			w += fmt.Sprintf("（已扫 %d 条记录，解析失败 %d 条）", r.Scanned, scan.ParseErrs)
		}
		r.Warning = JoinWarn(tliWarn, waldumpErr, w, OrphanWarningText(r.OrphanTx, r.OrphanDML))
		r.Note += "；" + r.Warning
		return r
	}
	r.RangeStart = scan.CommitMin.In(sinceLoc).Format("2006-01-02 15:04:05")
	r.RangeEnd = scan.CommitMax.In(sinceLoc).Format("2006-01-02 15:04:05")
	var covWarn string
	r.Coverage, r.CompareFrom, covWarn = DecideWALCoverage(since, sinceLoc, scan.CommitMin, scan.CommitMax)
	var gapWarn string
	if len(segGaps) > 0 {
		gapWarn = "WAL 段不连续：" + strings.Join(segGaps, "；")
	}
	var segWarn string
	if segSizeFallback {
		// 段大小靠兜底值时提示，避免静默算错起始 LSN。
		segWarn = fmt.Sprintf("wal_segment_size=%q 解析失败，段起始 LSN 按 16MB 估算", ss)
	}
	var parseWarn string
	if scan.ParseErrs > 0 {
		parseWarn = fmt.Sprintf("waldump 行解析失败 %d 条", scan.ParseErrs)
	}
	r.Warning = JoinWarn(tliWarn, waldumpErr, covWarn, gapWarn, segWarn, parseWarn)
	if r.Warning != "" {
		r.Note += "；" + r.Warning
	}

	// 5. 汇总：事务提交时间 > since 的 DML 才算变化。
	// 匹配不到 COMMIT 的事务单独计数并告警，不静默丢弃——
	// 子事务（COMMIT 在顶层 xid）、COMMIT 段被回收都会落到这里，静默=漏报。
	agg := map[string]*WalTableDML{}
	r.OrphanTx, r.OrphanDML = CountOrphans(scan.DMLByTx, scan.Commits, scan.Aborted)
	r.SubxactMerged = scan.SubxactMerged
	for tx, dmls := range scan.DMLByTx {
		ct, hasCommit := scan.Commits[tx]
		if !hasCommit {
			continue
		}
		if !ct.After(since) {
			continue
		}
		for k, cnt := range dmls {
			name, ok := rfOwner[k.relKey()]
			if !ok {
				continue
			}
			a := agg[name]
			if a == nil {
				a = &WalTableDML{Table: name}
				agg[name] = a
			}
			switch k.Op {
			case "ins":
				a.Ins += cnt
			case "upd":
				a.Upd += cnt
			case "del":
				a.Del += cnt
			}
			if lsn := scan.LastLSN[k]; a.LastLSN == "" || LsnValue(lsn) > LsnValue(a.LastLSN) {
				a.LastLSN = lsn
			}
		}
	}
	for _, a := range agg {
		r.Changed = append(r.Changed, *a)
	}
	sort.Slice(r.Changed, func(i, j int) bool { return r.Changed[i].Table < r.Changed[j].Table })
	r.ChangedCount = len(r.Changed)
	if w := OrphanWarningText(r.OrphanTx, r.OrphanDML); w != "" {
		if r.Warning != "" {
			r.Warning += "；" + w
		} else {
			r.Warning = w
		}
		if !strings.Contains(r.Note, w) {
			r.Note += "；" + w
		}
	}
	return r
}

// FindWaldumpInRoot 在容器 root 里找 sys_waldump / pg_waldump。

func FindWaldumpInRoot(root string) string {
	candidates := []string{
		filepath.Join(root, "home/kingbase/install/kingbase/bin/sys_waldump"),
	}
	// PG 小版本路径不固定（12/13/15/16...），别写死某一个。
	if hits, err := filepath.Glob(filepath.Join(root, "usr/lib/postgresql/*/bin/pg_waldump")); err == nil {
		candidates = append(candidates, hits...)
	}
	candidates = append(candidates, filepath.Join(root, "usr/bin/pg_waldump"))
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return "sys_waldump"
}

func CoverageText(c WalCoverage) string {
	switch c {
	case WalFull:
		return "full（WAL 完整覆盖指定时点之后）"
	case WalPartial:
		return "partial（WAL 起点晚于指定时点，只覆盖部分区间）"
	case WalNone:
		return "none（已读到 WAL、时点后无已提交事务；与'WAL 不可达未解析'不同，后者在告警里单列）"
	default:
		return "unknown"
	}
}
