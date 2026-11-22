// kbchange: 单库只读检查指定模式下所有表在指定时间后是否有数据变化。
// 通路1：表内时间字段 / xmin 提交时间（纯 SELECT）
// 通路2：WAL 解析（只读调用 sys_waldump/pg_waldump，先探覆盖再解析 DML）
// 通路3：pg_stat_user_tables 统计计数（纯 SELECT，补 DELETE 盲点）
// 通路4：数据文件 mtime（只读 stat，辅助线索，不并入合并数）
// 全程只读：不建对象、不改配置、不 reset 统计、不消费复制槽。

package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"kbchange/internal/filecheck"
	"kbchange/internal/partition"
	"kbchange/internal/path1"
	"kbchange/internal/report"
	"kbchange/internal/table"
	"kbchange/internal/tz"
	"kbchange/internal/wal"
	"os"
	"strings"
	"time"
)

func main() {
	dsn := flag.String("dsn", "", `当前服务器金仓/PG 库连接串，如 "host=127.0.0.1 port=54321 user=system password=xxx dbname=kingbase sslmode=disable"`)
	sinceStr := flag.String("since", "", `起始时间点，如 "2026-11-16 18:00:00"（查该时点之后的变化）；按操作系统时区输入，程序自动换算成数据库时区比对`)
	schemaStr := flag.String("schema", "public", `要检查的模式，可逗号分隔多个`)
	tablesStr := flag.String("tables", "", `只检查这些表，逗号分隔 schema.table；留空=扫描模式下所有表`)
	timeColFlag := flag.String("time-col", "", `通路1强制指定时间字段名；留空=自动判断表内是否存在时间类型字段`)
	pgdata := flag.String("pgdata", "", `通路4数据目录覆盖；留空=用 SHOW data_directory（需与库同机且有读权限）`)
	walEnable := flag.Bool("wal-enable", false, `启用通路2 WAL 解析（只读调用 sys_waldump/pg_waldump）`)
	walDir := flag.String("wal", "", `通路2 WAL 目录；留空=在数据目录内自动找 sys_wal(金仓)/pg_wal(PG)`)
	walBin := flag.String("wal-bin", "", `通路2 waldump 可执行文件路径；留空=PATH 里找 sys_waldump/pg_waldump`)
	walRoot := flag.String("wal-root", "", `通路2 容器 root 路径（如 /proc/<pid>/root）；用于容器内 waldump 缺库时借 loader 启动`)
	limit := flag.Int("limit", 500, "自动发现时最多检查多少张表")
	verbose := flag.Bool("verbose", false, "打印通路1、通路3、通路4判定有变化的表名")
	jsonOut := flag.Bool("json", false, "以 JSON 输出")
	flag.Parse()

	if *dsn == "" || *sinceStr == "" {
		fmt.Fprintln(os.Stderr, "必须提供 -dsn 和 -since（只连当前服务器的一个库，不对比多库）")
		flag.Usage()
		os.Exit(2)
	}
	schemas := table.SplitList(*schemaStr)
	if len(schemas) == 0 {
		fmt.Fprintln(os.Stderr, "-schema 不能为空")
		os.Exit(2)
	}

	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "连接失败:", err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	if err := db.Ping(); err != nil {
		fmt.Fprintln(os.Stderr, "连接失败:", err)
		os.Exit(1)
	}

	// 时区程序自己判断，全程只读：
	//  1. since 按操作系统时区（命令输入的默认口径）解析成绝对时刻；
	//  2. 读 DB 会话时区，用 since 时刻的偏移构造 dbLoc（夏令时下不能用 now 的偏移），
	//     再把 since 换算成 DB 时区后比对。
	since, err := tz.ParseSinceIn(*sinceStr, time.Local)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	dbName := tz.DBTimeZone(db)
	dbOff, dbOK := tz.DBTZOffset(db, since)
	dbLoc := tz.LocationFromDBZone(dbName, dbOff, dbOK)

	refs := table.ParseTables(*tablesStr, schemas[0])
	discoveryNote := ""
	scanScope := ""
	if len(refs) == 0 {
		var truncWarnings []string
		refs, truncWarnings, err = table.DiscoverTables(db, schemas, *limit)
		if err != nil {
			fmt.Fprintln(os.Stderr, "发现表失败:", err)
			os.Exit(1)
		}
		if len(truncWarnings) > 0 {
			discoveryNote = strings.Join(truncWarnings, "；")
			fmt.Fprintln(os.Stderr, "表发现告警:", discoveryNote)
		}
		// information_schema.tables 里没有物化视图（实测金仓/标准 PG 都一样），
		// 自动发现只能覆盖普通表/分区表。不写出来就是静默漏表，用户会当成"这些对象没变化"。
		scanScope = "自动发现只含普通表/分区表（relkind r,p）；物化视图/视图/外部表不在自动发现范围，" +
			"需要时用 -tables 指定（物化视图指定后通路1/3/4 可查；通路2 能检出最近一次 REFRESH 写进去的行，" +
			"但更早的 REFRESH/TRUNCATE 记录因 relfilenode 已换掉、映射不回表名）"
	}
	// -tables 可以跨 -schema 指定表；所有按模式过滤的通路都要用并集，
	// 否则 path3 会漏掉这些表，报告里的模式也不准。
	querySchemas := table.UnionSchemas(schemas, refs)

	// 分区父表：数据文件的 relfilenode 和统计行都挂在子分区名下，父表本身两者都没有。
	// 先算出"子分区 -> 父表"的归属映射，通路2/3 才能把子分区的变化算到父表头上。
	owner := map[string]string{}
	for _, r := range refs {
		owner[r.String()] = r.String()
	}
	partitionNote := ""
	if tree, terr := partition.LoadRelTree(db); terr != nil {
		fmt.Fprintln(os.Stderr, "分区关系查询失败:", terr)
		partitionNote = "分区/继承关系查询失败（" + terr.Error() + "），父表的子表未纳入通路2/3，可能低估"
	} else {
		o, exp := tree.Expand(refs)
		owner = o
		partitionNote = partition.PartitionExpandNote(exp, tree.OverlappingParents(refs))
	}

	// 三个通路统一传 dbLoc：since 的绝对时刻会换算成 DB 时区墙上时间再比对。
	p1 := path1.CheckPath1(db, refs, since, dbLoc, *timeColFlag)
	p3, statsReset, baseline, err := report.CheckPath3(db, refs, owner, since, dbLoc)
	path3Failed := false
	if err != nil {
		path3Failed = true
		fmt.Fprintln(os.Stderr, "通路3失败:", err)
		p3 = []report.Path3Result{}
		baseline = "unavailable"
	}
	p4 := filecheck.CheckFiles(db, refs, owner, *pgdata, since, dbLoc)
	// 通路2：默认不启用；启用时先探覆盖，再只读解析 WAL。
	// 未启用时 Coverage 留空：none 的定义是"已读到 WAL、时点后无已提交事务"，
	// 没跑却给 none，JSON 读起来就像"查过且没变化"。
	p2 := wal.WalCheckReport{Enabled: false, Changed: []wal.WalTableDML{}}
	if *walEnable {
		p2 = wal.CheckPath2(db, *walDir, *walBin, *walRoot, since, dbLoc, refs, owner)
	}

	p1n, p1err, p1skip, p1amb, p3n, p3Reliable, p3Candidate := 0, 0, 0, 0, 0, 0, 0
	changed := map[string]bool{}
	for _, t := range p1 {
		if t.Changed {
			p1n++
			changed[t.Table] = true
		}
		switch t.Status {
		case "ERROR":
			p1err++
		case "SKIP":
			p1skip++
		case "AMBIG":
			// date 精度定位不了时点：不算变化（可能高估），也绝不能当"无变化"（可能低估）。
			p1amb++
		}
	}
	for _, t := range p3 {
		if !t.Changed {
			continue
		}
		p3n++
		// 只有 reliable 的通路3 结果才并入"确定有变化"的集合。
		if t.Reliable {
			p3Reliable++
			changed[t.Table] = true
		} else {
			p3Candidate++
		}
	}
	// 通路2 的 DML 是直接证据，命中即并入合并集合。
	for _, t := range p2.Changed {
		changed[t.Table] = true
	}

	tzNote := "since 按操作系统时区输入，程序自动换算成 DB 会话时区后比对；timestamptz/xmin 按绝对时刻比较并换算到 DB 时区显示；timestamp/date 按 DB 时区墙上时间比较"
	path1Note := "按表内时间字段/xmin 提交时间查指定时点后的行，属直接证据；只覆盖时间字段随写入更新的场景，不更新时间字段的 UPDATE 查不到；查不了 DELETE/TRUNCATE；date 列精度只到天——当天写入无法定位到时点，单列'未判定'、不当无变化；时间字段允许 NULL 且确实存在 NULL 行时，这些行同样定位不到时点，也列'未判定'；无时间字段且 xmin 不可用时 SKIP"
	path3Note := "pg_stat_user_tables 累计计数，只报窗口内增改删总数、无时间点；" + report.BaselineText(baseline) + "；单向可信——stale 只可信'无'、partial 只可信'有'，不可信的一半仅列候选不并入合并数；父表的计数为未被单独检查的子表合计（含分区与 INHERITS）"
	if path3Failed {
		path3Note += "；本轮查询失败，未采信任何通路3结果"
	}
	path4Note := "数据文件 mtime，刷盘时间会被 checkpoint/vacuum 污染，仅供参考，不并入合并数；远程库/无读权限时不可达"
	path2Note := "只读解析 WAL：COMMIT 时间定位事务，DML 行取 relfilenode 映射表名（分区/INHERITS 父表把子表 DML 归到父表名下），覆盖 Heap 增删改（含 Kingbase INPLACE 原地改、COPY 的 MULTI_INSERT）；SAVEPOINT 子事务按父事务 COMMIT 行的 subxacts 归并、回滚事务(ABORT)不计入；先探覆盖区间，段被回收/无提交/孤儿事务(有 DML 但既无 COMMIT 也无 ABORT)会告警；TRUNCATE/DROP 等改 relfilenode 的 DDL 不在覆盖范围"
	// dbOff/dbOK 已经是 since 时刻的值，夏令时下与比对口径一致。
	osOff := tz.OSTZOffset(since, time.Local)
	r := report.Report{
		Schema: strings.Join(querySchemas, ","),
		Since:  since.In(dbLoc).Format("2006-01-02 15:04:05"),
		TZ: tz.TZInfo{
			OSLocal:    tz.TZName(time.Local),
			SinceTZ:    tz.TZName(time.Local) + "(自动=OS)",
			SinceInput: *sinceStr,
			DBTimeZone: dbName,
			DBOffset:   tz.OffsetText(dbOff, dbOK),
			OSOffset:   tz.FormatOffset(osOff),
			Compare:    tz.TZCompare(dbName, dbOff, dbOK, osOff, tz.TZName(time.Local)),
			SinceUTC:   since.UTC().Format("2006-01-02 15:04:05"),
			Note:       tzNote,
		},
		TableCount:     len(refs),
		DiscoveryNote:  discoveryNote,
		ScanScope:      scanScope,
		PartitionNote:  partitionNote,
		StatsReset:     statsReset,
		StatsBaseline:  baseline,
		Path1Count:     p1n,
		Path1Error:     p1err,
		Path1Skip:      p1skip,
		Path1Ambiguous: p1amb,
		Path3Count:     p3n,
		Path3Reliable:  p3Reliable,
		Path3Candidate: p3Candidate,
		CombinedCount:  len(changed),
		Path1:          p1,
		Path3:          p3,
		Path4:          p4,
		Path2:          p2,
		Path1Note:      path1Note,
		Path3Note:      path3Note,
		Path4Note:      path4Note,
		Path2Note:      path2Note,
	}
	r.Conclusion = report.BuildConclusion(r)

	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(r); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	report.PrintReport(r, *verbose)
}
