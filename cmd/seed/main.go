package main

// seed: 给 kbchange 造测试数据的辅助工具（仅测试用，不是 kbchange 的一部分）。
// 在 public 下建 5 张表，覆盖 timestamptz / timestamp / date / 无时间字段 / 仅删除 五种场景。
// 用法: seed -dsn "..."

import (
	"database/sql"
	"flag"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	dsn := flag.String("dsn", "", "金仓/PG 连接串")
	flag.Parse()
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "必须提供 -dsn")
		os.Exit(2)
	}
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "连接失败:", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		fmt.Fprintln(os.Stderr, "Ping 失败:", err)
		os.Exit(1)
	}

	var tz, commitTS string
	_ = db.QueryRow(`SELECT current_setting('TimeZone')`).Scan(&tz)
	_ = db.QueryRow(`SHOW track_commit_timestamp`).Scan(&commitTS)
	fmt.Printf("DB TimeZone=%s  track_commit_timestamp=%s\n", tz, commitTS)

	stmts := []string{
		`DROP TABLE IF EXISTS public.kbtest_tstz, public.kbtest_ts, public.kbtest_date, public.kbtest_notime, public.kbtest_del CASCADE`,

		`CREATE TABLE public.kbtest_tstz (id int primary key, val text, update_time timestamptz)`,
		// T=2026-11-16 18:00:00（DB 是 UTC，按 UTC 解释）。一行前、一行后。
		`INSERT INTO public.kbtest_tstz VALUES (1,'before','2026-11-16 17:00:00+00'),(2,'after','2026-11-16 20:00:00+00')`,

		`CREATE TABLE public.kbtest_ts (id int primary key, val text, update_time timestamp without time zone)`,
		// 墙上时间：一行前、一行后。
		`INSERT INTO public.kbtest_ts VALUES (1,'before','2026-11-16 17:00:00'),(2,'after','2026-11-16 20:00:00')`,

		`CREATE TABLE public.kbtest_date (id int primary key, val text, dt date)`,
		`INSERT INTO public.kbtest_date VALUES (1,'before','2026-11-15'),(2,'after','2026-11-17')`,

		// 无时间字段：靠 xmin 提交时间，track_commit_timestamp 未开则应 SKIP。
		`CREATE TABLE public.kbtest_notime (id int primary key, val text)`,
		`INSERT INTO public.kbtest_notime VALUES (1,'x'),(2,'y')`,

		// 仅删除：update_time 都在 T 前，通路1 查不到；靠通路3 的 n_tup_del 补。
		`CREATE TABLE public.kbtest_del (id int primary key, val text, update_time timestamptz)`,
		`INSERT INTO public.kbtest_del VALUES (1,'a','2026-11-16 10:00:00+00'),(2,'b','2026-11-16 10:00:00+00'),(3,'c','2026-11-16 10:00:00+00')`,
		`DELETE FROM public.kbtest_del WHERE id = 3`,
	}
	for _, q := range stmts {
		if _, err := db.Exec(q); err != nil {
			fmt.Fprintf(os.Stderr, "执行失败 [%s]: %v\n", q, err)
			os.Exit(1)
		}
	}
	fmt.Println("测试数据已就绪：kbtest_tstz / kbtest_ts / kbtest_date / kbtest_notime / kbtest_del")
}
