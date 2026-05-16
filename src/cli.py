"""统一命令行入口。

用法::

    python -m amazingdata_fetcher fetch stock_info
    python -m amazingdata_fetcher fetch finance --statement balance_sheet
    python -m amazingdata_fetcher fetch finance --test-codes 600519.SH 000001.SZ
    python -m amazingdata_fetcher fetch kline --type stock --date 20240102
    python -m amazingdata_fetcher fetch equity
    python -m amazingdata_fetcher cleanup --month 202603
    python -m amazingdata_fetcher sync
    python -m amazingdata_fetcher sync --file info_stock_basic.parquet
    python -m amazingdata_fetcher sync --s3-only
    python -m amazingdata_fetcher daily
    python -m amazingdata_fetcher daily --backfill-only
    python -m amazingdata_fetcher compare-schema <file_a> <file_b>
    python -m amazingdata_fetcher verify-finance
"""

from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv
from loguru import logger

from amazingdata_fetcher.fetcher import (
    EquityFetcher,
    FinanceFetcher,
    IndexInfoFetcher,
    IndustryInfoFetcher,
    KlineFetcher,
    MarginFetcher,
    MonthlyCleanup,
    StockInfoFetcher,
)

_FETCHERS = {
    "stock_info":    StockInfoFetcher,
    "finance":       FinanceFetcher,
    "kline":         KlineFetcher,
    "equity":        EquityFetcher,
    "margin":        MarginFetcher,
    "index_info":    IndexInfoFetcher,
    "industry_info": IndustryInfoFetcher,
}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="amazingdata_fetcher",
        description="AmazingData 数据采集工具",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # -- fetch 子命令 --
    fetch_p = sub.add_parser("fetch", help="拉取数据")
    fetch_p.add_argument("target", choices=_FETCHERS.keys(), help="数据类型")
    fetch_p.add_argument("--statement", default=None,
                         help="[finance] 指定报表: balance_sheet / cash_flow / income")
    fetch_p.add_argument("--type", dest="ktype", default="all",
                         help="[kline] 拉取类型: stock / index / etf / all")
    fetch_p.add_argument("--date", default=None,
                         help="[kline] 交易日期 YYYYMMDD，默认今天")
    fetch_p.add_argument("--start-date", default=None,
                         help="[kline] 批量起始日期 YYYYMMDD（含）")
    fetch_p.add_argument("--end-date", default=None,
                         help="[kline] 批量结束日期 YYYYMMDD（含），默认今天")
    fetch_p.add_argument("--test-codes", nargs="+", default=None,
                         help="[finance] 测试模式：只拉取指定股票代码")

    # -- cleanup 子命令 --
    cleanup_p = sub.add_parser("cleanup", help="月度 K 线文件合并清理")
    cleanup_p.add_argument("--month", default=None,
                           help="要处理的年月 YYYYMM，默认上个月")

    # -- sync 子命令 --
    sync_p = sub.add_parser("sync", help="同步本地 Parquet 到 RustFS / Iceberg")
    sync_p.add_argument("--file", default=None,
                        help="只同步指定文件名（如 info_stock_basic.parquet）")
    sync_p.add_argument("--s3-only", action="store_true",
                        help="仅上传到 S3，不写 Iceberg 表")

    # -- daily 子命令 --
    daily_p = sub.add_parser("daily", help="每日定时采集（缺口补全 + 全量 fetcher + 同步）")
    daily_p.add_argument("--backfill-only", action="store_true",
                         help="只补 kline 缺口，不跑其他 fetcher 和 sync")
    daily_p.add_argument("--lookback", type=int, default=30,
                         help="kline 回溯检查的交易日数（默认 30）")
    daily_p.add_argument("--skip-trading-day-check", action="store_true",
                         help="跳过交易日判断，强制执行（用于手动补数据）")

    # -- compare-schema 子命令 --
    cs_p = sub.add_parser("compare-schema", help="比较两个 Parquet 文件的 schema 和行数")
    cs_p.add_argument("file_a", help="Parquet 文件 A 的路径")
    cs_p.add_argument("file_b", help="Parquet 文件 B 的路径")

    # -- verify-finance 子命令 --
    sub.add_parser("verify-finance", help="验证财务报表增量拉取逻辑")

    return parser


def _handle_sync(output_dir: str, args: argparse.Namespace) -> None:
    import os

    import pandas as pd

    from amazingdata_fetcher.storage import (
        filename_to_table_name,
        infer_sync_mode,
        sync_directory_to_s3,
        sync_to_iceberg,
        upload_to_s3,
    )

    if args.file:
        filepath = os.path.join(output_dir, args.file)
        if not os.path.exists(filepath):
            logger.error(f"文件不存在: {filepath}")
            return
        upload_to_s3(filepath)
        if not args.s3_only:
            df = pd.read_parquet(filepath)
            table_name = filename_to_table_name(args.file)
            mode = infer_sync_mode(args.file)
            sync_to_iceberg(df, table_name, mode=mode)
    else:
        sync_directory_to_s3(output_dir)
        if not args.s3_only:
            for fname in sorted(os.listdir(output_dir)):
                if not fname.endswith(".parquet"):
                    continue
                filepath = os.path.join(output_dir, fname)
                df = pd.read_parquet(filepath)
                table_name = filename_to_table_name(fname)
                mode = infer_sync_mode(fname)
                sync_to_iceberg(df, table_name, mode=mode)

    logger.info("同步完成")


def main() -> None:
    load_dotenv()
    args = _build_parser().parse_args()

    output_dir = os.environ.get("OUTPUT_DIR", "./data")
    sdk_cache_dir = os.environ.get("SDK_CACHE_DIR", "./sdk_cache")

    if args.command == "cleanup":
        MonthlyCleanup(output_dir).run(month=args.month)
        return

    if args.command == "sync":
        _handle_sync(output_dir, args)
        return

    if args.command == "daily":
        from amazingdata_fetcher.commands.daily import run as daily_run
        daily_run(args)
        return

    if args.command == "compare-schema":
        from amazingdata_fetcher.commands.compare_schema import run as cs_run
        cs_run(args)
        return

    if args.command == "verify-finance":
        from amazingdata_fetcher.commands.verify_finance import run as vf_run
        vf_run(args)
        return

    # -- fetch --
    fetcher_cls = _FETCHERS[args.target]
    fetcher = fetcher_cls(output_dir=output_dir, sdk_cache_dir=sdk_cache_dir)

    # 构建 fetch() 的关键字参数
    fetch_kwargs: dict = {}
    if args.target == "finance":
        if args.statement:
            fetch_kwargs["statement"] = args.statement
        if args.test_codes:
            fetch_kwargs["test_codes"] = args.test_codes
    elif args.target == "kline":
        fetch_kwargs["ktype"] = args.ktype
        if args.date:
            fetch_kwargs["trade_date"] = args.date
        if args.start_date:
            fetch_kwargs["start_date"] = args.start_date
        if args.end_date:
            fetch_kwargs["end_date"] = args.end_date

    force_exit = not (args.target == "kline" and args.start_date)
    logger.info(f"启动 {fetcher_cls.__name__}...")
    fetcher.run(force_exit=force_exit, **fetch_kwargs)


if __name__ == "__main__":
    main()
