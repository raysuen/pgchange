package path1

import (
	"database/sql"
	"fmt"
	"kbchange/internal/table"
	"kbchange/internal/tz"
	"strings"
	"time"

	"github.com/lib/pq"
)

type Path1Result struct {
	Table     string `json:"table"`
	Basis     string `json:"basis"`
	ColKind   string `json:"col_kind,omitempty"` // timestamptz / timestamp / date / xmin
	RowsAfter int64  `json:"rows_after"`
	// DayHit 仅 date 列：值等于 since 当天的行数。date 只有天精度，
	// 这些行可能写在时点前也可能在时点后，无法定位——不能当"无变化"，单列出来。
	DayHit int64 `json:"day_hit,omitempty"`
	// NullTime 仅当时间字段允许 NULL 时统计：这些行的写入时点通路1 定位不到。
	NullTime int64  `json:"null_time,omitempty"`
	MaxTime  string `json:"max_time"`
	Status   string `json:"status"` // OK / AMBIG(date精度或时间字段为NULL) / SKIP / ERROR
	Note     string `json:"note,omitempty"`
	Changed  bool   `json:"changed"`
}

// tz.TZInfo 记录三方时区，便于核对换算是否有偏差。

type TimeCol struct {
	Name     string
	Kind     string
	Type     string
	Nullable bool
}

// report.Path3Result 里 Changed 是"计数>0"的原始信号，Reliable 才是能否据此判定
// "指定时点后有变化"。两者分开：stale 下计数>0 可能是时点前的写入，列出来但不采信。

var TimeColumnCandidates = []string{
	"update_time", "updated_at", "updated_date", "gmt_modified", "modify_time", "modified_time",
	"modified_at", "last_modified", "upt_time", "last_update_time", "change_time",
	"create_time", "created_at", "create_date", "insert_date", "gmt_create", "crt_time", "insert_time",
}

// tz.NormalizeDateText 把驱动可能返回的各种日期串统一成 YYYY-MM-DD 显示。
// 金仓 date 的 max() 是 []byte，PG 有时给 time.Time 字符串，别把 RFC3339 原样塞进报告。

func TimeColKind(dataType string) string {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "timestamp with time zone", "timestamptz":
		return "timestamptz"
	case "timestamp without time zone", "timestamp":
		return "timestamp"
	case "date":
		return "date"
	}
	return ""
}

// PickTimeColumn 从表的时间类型列里挑一个：forced 精确匹配 > 候选名单 > 唯一列兜底。
// 抽成纯函数便于单测，不依赖数据库。
// 有多个时间列、名字都不在候选名单时返回 err，避免静默走 xmin 或误当"无时间字段"。

func PickTimeColumn(cols []TimeCol, forced string) (TimeCol, error) {
	if forced != "" {
		for _, c := range cols {
			if strings.EqualFold(c.Name, forced) {
				return c, nil
			}
		}
		return TimeCol{}, fmt.Errorf("指定时间字段 %q 不在表中", forced)
	}
	lower := map[string]TimeCol{}
	for _, c := range cols {
		lower[strings.ToLower(c.Name)] = c
	}
	for _, cand := range TimeColumnCandidates {
		if c, ok := lower[cand]; ok {
			return c, nil
		}
	}
	if len(cols) == 1 {
		return cols[0], nil
	}
	if len(cols) > 1 {
		names := make([]string, 0, len(cols))
		for _, c := range cols {
			names = append(names, c.Name+"("+c.Kind+")")
		}
		return TimeCol{}, fmt.Errorf("表内有 %d 个时间字段但都不在候选名单：%s，无法自动选；请用 -time-col 指定", len(cols), strings.Join(names, ", "))
	}
	return TimeCol{}, nil
}

// TableColumnsInfoSchema 取表的所有列（名字 + data_type），时间类型在 Go 侧筛。
// 不在 SQL 里过滤时间类型：过滤后返回 0 行时，"表里没有时间列"和"信息模式根本没有这张表的列"
// 两种情况长得一样，没法决定要不要走系统目录兜底。
// 信息模式的好处是域类型（domain over timestamp）会报底层类型，系统目录只报域名。
// data_type 兜底 udt_name：金仓集群版 V8R6（鲲鹏虚机实测 server_version 12.1）对 timestamp
// 返回 data_type="USER-DEFINED"、udt_name="timestamp"——只看 data_type 会把时间列判没，
// 通路1 全部 SKIP 成"无时间字段"。标准 PG/容器版 data_type 是全名，udt_name 是基名，两边都认。

func TableColumnsInfoSchema(db *sql.DB, r table.TableRef) ([]TimeCol, error) {
	const q4 = `
		SELECT column_name, data_type, is_nullable, udt_name FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`
	const q3 = `
		SELECT column_name, data_type, is_nullable FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`
	rows, err := db.Query(q4, r.Schema, r.Name)
	if err != nil {
		// 旧版本 information_schema.columns 没有 udt_name 列（实测金仓 V009 容器版就报
		// column "udt_name" does not exist）。这是"信息模式版本旧"，不是"表没列"，
		// 必须降级成不带 udt_name 的查询，把错误往上传会把通路1 整体打成 ERROR。
		rows, err = db.Query(q3, r.Schema, r.Name)
		if err != nil {
			return nil, err
		}
	}
	defer rows.Close()
	// 按实际返回列数分支扫描：带 udt_name 的版本（金仓集群版 V8R6）用 udt_name 兜底
	// 时间类型判定；不带的版本只认 data_type。
	colNames, _ := rows.Columns()
	hasUDT := len(colNames) >= 4
	var cols []TimeCol
	for rows.Next() {
		var c, dt, nullable, udt string
		if hasUDT {
			if err := rows.Scan(&c, &dt, &nullable, &udt); err != nil {
				return nil, err
			}
		} else if err := rows.Scan(&c, &dt, &nullable); err != nil {
			return nil, err
		}
		kind := TimeColKind(dt)
		if kind == "" && hasUDT {
			kind = TimeColKind(udt)
		}
		cols = append(cols, TimeCol{
			Name: c, Kind: kind, Type: dt,
			Nullable: strings.EqualFold(strings.TrimSpace(nullable), "YES"),
		})
	}
	return cols, rows.Err()
}

// TableColumnsCatalog 用系统目录取列（typname：timestamp/timestamptz/date）。
// 为什么需要它：物化视图在 information_schema.columns 里一条列都没有（实测金仓 V009：
// ray.mvtest 有 update_time，信息模式查出来 0 行）。只看信息模式会把"有时间列"错判成
// "无时间列"，接着回退 xmin、再报"xmin 提交时间不可用"，把排障方向带偏。

func TableColumnsCatalog(db *sql.DB, r table.TableRef) ([]TimeCol, error) {
	rows, err := db.Query(`
		SELECT a.attname, t.typname, a.attnotnull
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
		JOIN pg_type t ON t.oid = a.atttypid
		WHERE n.nspname = $1 AND c.relname = $2
		ORDER BY a.attnum`, r.Schema, r.Name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []TimeCol
	for rows.Next() {
		var c, typ string
		var notNull bool
		if err := rows.Scan(&c, &typ, &notNull); err != nil {
			return nil, err
		}
		cols = append(cols, TimeCol{Name: c, Kind: TimeColKind(typ), Type: typ, Nullable: !notNull})
	}
	return cols, rows.Err()
}

// 通路1前置：判断表内是否存在时间类型字段，返回列名 + 时间语义。

func FindTimeColumn(db *sql.DB, r table.TableRef, forced string) (TimeCol, error) {
	cols, err := TableColumnsInfoSchema(db, r)
	if err != nil {
		return TimeCol{}, err
	}
	if len(cols) == 0 {
		// 信息模式没有列信息（物化视图等）→ 系统目录兜底。
		cols, err = TableColumnsCatalog(db, r)
		if err != nil {
			return TimeCol{}, err
		}
	}
	var timeCols []TimeCol
	for _, c := range cols {
		if c.Kind != "" {
			timeCols = append(timeCols, c)
		}
	}
	c, err := PickTimeColumn(timeCols, forced)
	if err != nil {
		// forced 未命中时先分诊：列根本不存在，还是列在但不是时间类型？
		// 两种情况报同一个"不在表中"会误导排障方向。上面已经把所有列取回来了，直接查。
		if forced != "" {
			for _, col := range cols {
				if strings.EqualFold(col.Name, forced) {
					typ := col.Type
					if typ == "" {
						typ = "unknown"
					}
					return TimeCol{}, fmt.Errorf("指定时间字段 %q 类型为 %q，不是时间类型，通路1 用不了（表 %s）", forced, typ, r.String())
				}
			}
		}
		return TimeCol{}, fmt.Errorf("%w（表 %s）", err, r.String())
	}
	return c, nil
}

// SinceArgForCol 按字段时间语义生成比较参数（sinceLoc 传的是 DB 会话时区）：
// timestamptz 传绝对时刻（Go time.Time 带时区），DB 按自身时区比较；
// timestamp/date 把 since 的绝对时刻换算成 DB 时区墙上时间字符串再比，
// 例如 OS +08 输入 18:00 → 绝对时刻 10:00 UTC → 传 "2026-11-16 10:00:00"。

func SinceArgForCol(since time.Time, sinceLoc *time.Location, kind string) interface{} {
	if kind == "timestamptz" {
		return since.In(sinceLoc)
	}
	if kind == "date" {
		return since.In(sinceLoc).Format("2006-01-02")
	}
	return since.In(sinceLoc).Format("2006-01-02 15:04:05")
}

// CommitTSOK 只读探测 xmin 提交时间是否可用，返回可用性与具体原因。
// 原实现把所有失败（表不存在、track off、权限不足）都吞成一个 false，
// 上层只能笼统写"无时间字段且 xmin 不可用"——表不存在时会误导排障方向。
// 空表时 LIMIT 1 返回零行（ErrNoRows），跟 track off 无法区分，回退用 max() 探测（空表代价为 0）。

func CommitTSOK(db *sql.DB, r table.TableRef) (bool, string) {
	// 先看对象存不存在、是什么种类。视图/外部表被 PG 报的是
	// column "xmin" does not exist，靠错误文案猜会一律猜成"表不存在"，
	// 把"这是个视图，去查基表"这种有效信息丢掉（矩阵 C02 实测到的误导）。
	relkind, exists := TableRelkind(db, r)
	if !exists {
		return false, "表不存在（检查 -tables/-schema 拼写）"
	}
	if relkind == "v" {
		return false, "这是视图，没有 xmin/数据文件，通路1 查不了；要查数据请查它的基表"
	}
	if relkind == "f" {
		return false, "这是外部表，没有 xmin 提交时间"
	}

	qualified := pq.QuoteIdentifier(r.Schema) + "." + pq.QuoteIdentifier(r.Name)
	q := fmt.Sprintf("SELECT pg_xact_commit_timestamp(xmin) FROM %s LIMIT 1", qualified)
	var ts sql.NullTime
	err := db.QueryRow(q).Scan(&ts)
	if err == nil {
		return true, ""
	}
	if err == sql.ErrNoRows {
		// 空表：换 max() 探测，空表 max() 无代价且总返回一行（NULL 也算可用——track on 就行）。
		q2 := fmt.Sprintf("SELECT max(pg_xact_commit_timestamp(xmin)) FROM %s", qualified)
		var ts2 sql.NullTime
		err2 := db.QueryRow(q2).Scan(&ts2)
		if err2 == nil {
			return true, ""
		}
		err = err2
	}
	msg := err.Error()
	return false, "xmin 提交时间不可用：" + msg
}

// TableRelkind 返回对象的 relkind（r 普通表 / p 分区父表 / m 物化视图 / v 视图 / f 外部表）。
// 第二个返回值为 false 表示对象不存在。

func TableRelkind(db *sql.DB, r table.TableRef) (string, bool) {
	var kind string
	err := db.QueryRow(`SELECT c.relkind FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2`, r.Schema, r.Name).Scan(&kind)
	if err != nil {
		return "", false
	}
	return kind, true
}

// SkipNoteNoTimeCol 给"没有时间字段导致跳过"补上前因。
// 原来的 SKIP 文案只有"xmin 提交时间不可用：pq: could not get commit timestamp"，
// 看报告的人不知道这条为什么去探 xmin（因为表里没有时间类型的列）。

func SkipNoteNoTimeCol(xminWhy string) string {
	if strings.Contains(xminWhy, "表不存在") {
		return xminWhy
	}
	return "表内无时间类型字段；" + xminWhy
}

// CountNullTimeRows 统计时间字段为 NULL 的行数。只在"这张表暂被判成没有变化 + 该列允许 NULL"时调用：
// NOT NULL 的列不可能有 NULL，已经有变化的表也不需要这个补充判定。
// 为什么必须查：时间字段为 NULL 的行，通路1 的 count(tcol > since) 根本数不到它，
// 整张表会被报成 status=OK、0 行变化——看着像"查过且没变"，实际这类行完全没被覆盖
// （实测 ray.n1：3 分钟前插入一行 update_time=NULL，通路1 报 0 张、未判定 0，结论"0 张"）。

func CountNullTimeRows(db *sql.DB, qualified, expr string) (int64, error) {
	q := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s IS NULL", qualified, expr)
	var n int64
	err := db.QueryRow(q).Scan(&n)
	return n, err
}

// MarkNullTimeRows 把 NULL 行数写进结果：有 NULL 就降级成 AMBIG，不能算"无变化"。
// 查询失败不改状态——这条是补充判定，不该因为它失败就把表判成 ERROR（主查询已经成功了）。

func MarkNullTimeRows(res *Path1Result, n int64) {
	res.NullTime = n
	if n <= 0 {
		return
	}
	res.Status = "AMBIG"
	res.Note = fmt.Sprintf("时间字段有 %d 行为 NULL（写入时没带时间值）：通路1 定位不到这些行的写入时点，本表不能当成'无变化'，可用通路2/3 核对", n)
}

// 通路1：判断时间字段 -> 有则查业务时间，无则回退 xmin 提交时间，再无则跳过。
// sinceLoc 传 DB 会话时区：since 换算到该时区比对，timestamptz 的 max 值也按该时区显示。

func CheckPath1(db *sql.DB, refs []table.TableRef, since time.Time, sinceLoc *time.Location, forced string) []Path1Result {
	out := make([]Path1Result, 0, len(refs))
	for _, r := range refs {
		qualified := pq.QuoteIdentifier(r.Schema) + "." + pq.QuoteIdentifier(r.Name)
		res := Path1Result{Table: r.String(), Status: "OK", MaxTime: "-"}

		col, err := FindTimeColumn(db, r, forced)
		if err != nil {
			res.Status = "ERROR"
			res.Note = err.Error()
			out = append(out, res)
			continue
		}

		expr := ""
		kind := ""
		if col.Name != "" {
			expr = pq.QuoteIdentifier(col.Name)
			res.Basis = col.Name
			kind = col.Kind
			res.ColKind = col.Kind
		} else {
			// 只有"没有业务时间字段"时才需要探测 xmin 提交时间。
			// 原来无条件先探一次：有 time 列的表也会白打一条必然失败的查询（track off 时），
			// 500 张表就是 500 次无谓往返。
			xminOK, xminWhy := CommitTSOK(db, r)
			if !xminOK {
				res.Status = "SKIP"
				res.Basis = "none"
				// 具体原因透传：表不存在、track off、权限不足各说各的，别笼统混成一条。
				res.Note = SkipNoteNoTimeCol(xminWhy)
				out = append(out, res)
				continue
			}
			expr = "pg_xact_commit_timestamp(xmin)"
			res.Basis = "xmin commit ts"
			kind = "timestamptz"
			res.ColKind = "timestamptz"
		}

		arg := SinceArgForCol(since, sinceLoc, kind)
		var cnt int64
		if kind == "date" {
			// date 只有天精度，拆两档（对齐通路3 单向可信口径）：
			//   dt > 当天  = 确定变化（写入必然在 since 之后）；
			//   dt = 当天  = 无法定位时点（时点前/后写入都可能），单列 DayHit、归"未判定"，
			//               不能并进"无变化"——原来用 dt > 当天会把当天写入全漏掉（假阴性）。
			q := fmt.Sprintf(`SELECT count(CASE WHEN %s > $1 THEN 1 END),
			       count(CASE WHEN %s = $1 THEN 1 END), max(%s) FROM %s WHERE %s >= $1`,
				expr, expr, expr, qualified, expr)
			var dayHit int64
			var mx sql.NullString
			if err := db.QueryRow(q, arg).Scan(&cnt, &dayHit, &mx); err != nil {
				res.Status = "ERROR"
				res.Note = err.Error()
				out = append(out, res)
				continue
			}
			res.RowsAfter = cnt
			res.DayHit = dayHit
			res.Changed = cnt > 0
			if mx.Valid {
				res.MaxTime = tz.NormalizeDateText(mx.String)
			}
			if !res.Changed && dayHit > 0 {
				res.Status = "AMBIG"
				res.Note = fmt.Sprintf("date 精度仅到天：当天 %d 行无法定位到时点（可能含时点前写入），不能当'无变化'；可用通路2/3 或 -time-col 换精确时间列", dayHit)
			} else if !res.Changed && col.Nullable {
				if n, nerr := CountNullTimeRows(db, qualified, expr); nerr == nil {
					MarkNullTimeRows(&res, n)
				}
			}
			out = append(out, res)
			continue
		}
		if kind == "timestamp" {
			// 金仓 timestamp 的 max() 也当 []uint8 返回，Scan 进 time.Time 会报错；
			// 按字符串接收再统一显示格式。timestamptz 才是 time.Time。
			q := fmt.Sprintf("SELECT count(*), max(%s) FROM %s WHERE %s > $1", expr, qualified, expr)
			var mx sql.NullString
			if err := db.QueryRow(q, arg).Scan(&cnt, &mx); err != nil {
				res.Status = "ERROR"
				res.Note = err.Error()
				out = append(out, res)
				continue
			}
			res.RowsAfter = cnt
			res.Changed = cnt > 0
			if mx.Valid {
				res.MaxTime = tz.NormalizeTimestampText(mx.String)
			}
			if !res.Changed && col.Nullable {
				if n, nerr := CountNullTimeRows(db, qualified, expr); nerr == nil {
					MarkNullTimeRows(&res, n)
				}
			}
			out = append(out, res)
			continue
		}
		q := fmt.Sprintf("SELECT count(*), max(%s) FROM %s WHERE %s > $1", expr, qualified, expr)
		var mx sql.NullTime
		if err := db.QueryRow(q, arg).Scan(&cnt, &mx); err != nil {
			res.Status = "ERROR"
			res.Note = err.Error()
			out = append(out, res)
			continue
		}
		res.RowsAfter = cnt
		res.Changed = cnt > 0
		if mx.Valid {
			if kind == "timestamptz" {
				// 带时区字段：把绝对时刻换算到用户时区显示，与 since 同口径。
				res.MaxTime = mx.Time.In(sinceLoc).Format("2006-01-02 15:04:05")
			} else {
				// 墙上时间：驱动按会话时区解析出来的墙上值，按库里原值显示，不再二次换算。
				res.MaxTime = mx.Time.Format("2006-01-02 15:04:05")
			}
		}
		// xmin 依据没有 NULL 问题（每行都有 xmin），只有真实时间列才补查。
		if !res.Changed && col.Nullable && col.Name != "" {
			if n, nerr := CountNullTimeRows(db, qualified, expr); nerr == nil {
				MarkNullTimeRows(&res, n)
			}
		}
		out = append(out, res)
	}
	return out
}

// report.Path3Reliability 按"统计起点 R 与比对时点 T 的关系"给出通路3 的可信度。
// R<T=stale：窗口 [R,now] 包住 [T,now]，N>0 可能全是 T 前的写入 → 不可采信；N=0 可反证 T 后无变化。
// R>=T=partial：窗口 [R,now] 是 [T,now] 子集，N>0 必是 T 后的写入 → 可采信；N=0 不能排除 [T,R) 漏报。
// 结论：通路3 单向可信——stale 只信"无"，partial 只信"有"，都不是双向证据。
