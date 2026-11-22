package tz

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type TZInfo struct {
	OSLocal    string `json:"os_local"`    // 运行 kbchange 的机器时区（time.Local）
	SinceTZ    string `json:"since_tz"`    // since 的输入时区（=操作系统时区）
	SinceInput string `json:"since_input"` // 用户填的原始时间字符串（OS 时区）
	DBTimeZone string `json:"db_timezone"` // 数据库会话时区名，只读查询
	DBOffset   string `json:"db_offset"`   // DB 会话时区在 since 时刻相对 UTC 的偏移
	OSOffset   string `json:"os_offset"`   // 操作系统时区在 since 时刻相对 UTC 的偏移
	Compare    string `json:"compare"`     // DB 与 OS 时区的自动对比结论
	SinceUTC   string `json:"since_utc"`   // since 的绝对时刻（UTC）
	Note       string `json:"note"`        // 换算口径说明
}

// path1.TimeCol 是通路1选中的时间字段及其时间语义。
// Kind: timestamptz（绝对时刻）/ timestamp（墙上时间）/ date（墙上日期）。
// Type 是原始类型名，只用于报错文案（"指定字段类型为 X，不是时间类型"）。
// Nullable 表示该列允许 NULL——只有允许时，才需要在"没查到变化"时补查一次 NULL 行数：
// 时间字段为 NULL 的行，通路1 根本定位不到写入时点，不能说成"无变化"。

func NormalizeDateText(s string) string {
	t := strings.TrimSpace(s)
	for _, layout := range []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		time.RFC3339,
	} {
		if v, err := time.Parse(layout, t); err == nil {
			return v.Format("2006-01-02")
		}
	}
	return t
}

// NormalizeTimestampText 处理金仓把 timestamp 当 []uint8 返回的情况。
// 墙上时间原样显示，不做时区换算，只统一格式。

func NormalizeTimestampText(s string) string {
	t := strings.TrimSpace(s)
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02T15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	} {
		if v, err := time.Parse(layout, t); err == nil {
			return v.Format("2006-01-02 15:04:05")
		}
	}
	return t
}

// LoadLocation 解析时区输入：空或 Local=操作系统时区；UTC；IANA 名（Asia/Shanghai）；
// 固定偏移（+08:00 / +0800 / -05:00 / Z）。

func LoadLocation(spec string) (*time.Location, error) {
	s := strings.TrimSpace(spec)
	low := strings.ToLower(s)
	if s == "" || low == "local" {
		return time.Local, nil
	}
	if low == "utc" || low == "gmt" || s == "Z" {
		return time.UTC, nil
	}
	re := regexp.MustCompile(`^[+-]\d{1,2}(:\d{2}|\d{2})?$`)
	if re.MatchString(s) {
		sign := 1
		if s[0] == '-' {
			sign = -1
		}
		digits := strings.ReplaceAll(s[1:], ":", "")
		var hh, mm int
		switch len(digits) {
		case 1, 2:
			hh, _ = strconv.Atoi(digits)
		case 4:
			hh, _ = strconv.Atoi(digits[:2])
			mm, _ = strconv.Atoi(digits[2:])
		default:
			return nil, fmt.Errorf("无法解析时区偏移 %q", s)
		}
		off := sign * (hh*3600 + mm*60)
		if off > 14*3600 || off < -12*3600 {
			return nil, fmt.Errorf("时区偏移超出范围 %q", s)
		}
		absOff := off
		prefix := "+"
		if off < 0 {
			prefix = "-"
			absOff = -off
		}
		name := fmt.Sprintf("UTC%s%02d:%02d", prefix, absOff/3600, (absOff%3600)/60)
		return time.FixedZone(name, off), nil
	}
	loc, err := time.LoadLocation(s)
	if err != nil {
		return nil, fmt.Errorf("未知时区 %q（可用 Local / UTC / Asia/Shanghai / +08:00）: %w", s, err)
	}
	return loc, nil
}

func ParseSinceIn(s string, loc *time.Location) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
	}
	for _, l := range layouts {
		if t, err := time.ParseInLocation(l, s, loc); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("无法解析时间 %q，支持 YYYY-MM-DD HH:MM:SS / YYYY-MM-DDTHH:MM:SS / YYYY-MM-DD HH:MM / YYYY-MM-DD", s)
}

// ParseSince 保留默认用操作系统时区的包装，供旧调用与单测使用。

func ParseSince(s string) (time.Time, error) {
	return ParseSinceIn(s, time.Local)
}

// TZName 给时区一个可读标识。

func TZName(loc *time.Location) string {
	if loc == nil {
		return "unknown"
	}
	if loc == time.Local {
		_, off := time.Now().In(loc).Zone()
		return fmt.Sprintf("Local(%s)", FormatOffset(off))
	}
	return loc.String()
}

func FormatOffset(off int) string {
	prefix := "+"
	if off < 0 {
		prefix = "-"
		off = -off
	}
	return fmt.Sprintf("%s%02d:%02d", prefix, off/3600, (off%3600)/60)
}

func OffsetText(off int, ok bool) string {
	if !ok {
		return "unavailable"
	}
	return FormatOffset(off)
}

// LocationFromDBZone 把数据库会话时区转成程序用来解释 since 的 location。
// 程序自己判断，不看用户传参：
//  1. 时区名能被 Go 加载（Asia/Shanghai、PRC、UTC、Etc/GMT-8 等）→ 用它，自带夏令时规则；
//  2. 名字不认识 → 用查到的偏移秒构造固定时区（金仓自定义时区名兜底）；
//  3. 偏移也查不到 → 最后兜底用操作系统时区。

func LocationFromDBZone(name string, off int, offOK bool) *time.Location {
	if loc, err := LoadLocation(name); err == nil {
		return loc
	}
	if offOK {
		return time.FixedZone("DB("+name+")", off)
	}
	return time.Local
}

func DBTimeZone(db *sql.DB) string {
	var tz string
	if err := db.QueryRow(`SELECT current_setting('TimeZone')`).Scan(&tz); err != nil {
		return "unknown(" + err.Error() + ")"
	}
	return strings.TrimSpace(tz)
}

// DBTZOffset 只读查询数据库会话时区在 at 时刻相对 UTC 的偏移秒。
// 用 extract(timezone from timestamptz)，PG/金仓通用；失败返回 ok=false 让上层降级。
// 用 at 时刻而非 now()：带夏令时时，按 since 时刻的偏移比才和查询口径一致。

func DBTZOffset(db *sql.DB, at time.Time) (int, bool) {
	var sec int
	err := db.QueryRow(`SELECT extract(timezone from $1::timestamptz)::int`, at).Scan(&sec)
	if err != nil {
		return 0, false
	}
	return sec, true
}

// OSTZOffset 取时区 loc 在 at 时刻相对 UTC 的偏移秒。

func OSTZOffset(at time.Time, loc *time.Location) int {
	_, off := at.In(loc).Zone()
	return off
}

// TZCompare 给出 DB 时区与 OS 时区的对比结论。

func TZCompare(dbName string, dbOff int, dbOK bool, osOff int, osName string) string {
	if !dbOK {
		return "DB 时区偏移读取失败，无法与操作系统时区自动对比（函数名按版本核对）"
	}
	if dbOff == osOff {
		return fmt.Sprintf("一致：DB 会话时区 %s 与操作系统时区 %s 都是 %s", dbName, osName, FormatOffset(dbOff))
	}
	d := dbOff - osOff
	return fmt.Sprintf("不一致：DB 会话时区 %s=%s，操作系统时区 %s=%s，相差 %s（以 since 时刻计）",
		dbName, FormatOffset(dbOff), osName, FormatOffset(osOff), FormatOffset(d))
}

// table.LimitRefsPerSchema 按模式分组、每组最多保留 limit 张，返回保留表和被截断的告警。
// 不能全局 ORDER BY + LIMIT：多模式时按名字排序靠后的模式会被整张截掉，用户传了却没查，
// 报告还不提示。按模式分组限额，每个指定模式都有机会被检查，截断也说得清。
// 抽成纯函数便于单测，不依赖数据库。
