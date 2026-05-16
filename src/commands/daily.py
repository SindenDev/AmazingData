"""daily — 每日数据采集编排命令。

三阶段执行：
  1. kline 缺口检测与补拉
  2. 全量 fetcher 执行（各 fetcher 内部增量逻辑自动补差值）
  3. 同步到已配置目标（RustFS / Iceberg / ClickHouse）

各 fetcher 通过 subprocess 在独立进程中运行，隔离 SDK segfault 风险。

用法::

    python -m amazingdata_fetcher daily
    python -m amazingdata_fetcher daily --backfill-only
    python -m amazingdata_fetcher daily --lookback 60
    python -m amazingdata_fetcher daily --skip-trading-day-check
"""

from __future__ import annotations

import argparse
import glob
import os
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from loguru import logger

# ── 常量 ────────────────────────────────────────────────────────────
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
KLINE_TYPES = ("stock", "index", "etf")
DEFAULT_LOOKBACK = 30  # 回溯交易日数


# ── SDK 交易日历 ────────────────────────────────────────────────────

def _init_sdk() -> list[str]:
    """登录 SDK 并返回交易日历（YYYYMMDD 字符串列表）。"""
    import AmazingData as ad
    import pandas as pd

    from amazingdata_fetcher.client import get_client

    get_client()
    bdo = ad.BaseData()
    calendar = bdo.get_calendar()

    if isinstance(calendar, pd.DataFrame):
        dates = calendar.iloc[:, 0].astype(str).tolist()
    else:
        dates = [str(d) for d in calendar]

    return sorted(dates)


def _is_trading_day(calendar: list[str], date_str: str) -> bool:
    return date_str in calendar


def _recent_trading_days(calendar: list[str], today: str, lookback: int) -> list[str]:
    """返回 today 及之前 lookback 个交易日（含 today，如果它是交易日）。"""
    up_to = [d for d in calendar if d <= today]
    return up_to[-lookback:] if len(up_to) >= lookback else up_to


# ── Kline 缺口检测 ─────────────────────────────────────────────────

def _find_kline_gaps(
    output_dir: str,
    trading_days: list[str],
) -> dict[str, list[str]]:
    """扫描 output_dir 中缺失的 kline 日文件，按类型返回缺失日期。"""
    gaps: dict[str, list[str]] = {}
    for ktype in KLINE_TYPES:
        existing = set()
        for f in glob.glob(os.path.join(output_dir, f"extra_{ktype}_????????.parquet")):
            fname = os.path.basename(f)
            date_part = fname.replace(f"extra_{ktype}_", "").replace(".parquet", "")
            if len(date_part) == 8 and date_part.isdigit():
                existing.add(date_part)

        # 也检查已合并到 history 的日期（避免误报已 cleanup 的月份）
        history_path = os.path.join(output_dir, f"extra_{ktype}_history.parquet")
        if os.path.exists(history_path):
            import pandas as pd

            try:
                df_hist = pd.read_parquet(history_path, columns=["kline_time"])
                if pd.api.types.is_datetime64_any_dtype(df_hist["kline_time"]):
                    hist_dates = set(df_hist["kline_time"].dt.strftime("%Y%m%d").unique())
                else:
                    hist_dates = set(
                        pd.to_datetime(df_hist["kline_time"]).dt.strftime("%Y%m%d").unique()
                    )
                existing |= hist_dates
            except Exception:
                pass  # history 文件损坏或格式不一致，忽略

        missing = sorted(d for d in trading_days if d not in existing)
        if missing:
            gaps[ktype] = missing

    return gaps


# ── 子命令执行 ──────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now(SHANGHAI_TZ).strftime("%H:%M:%S")


def _run(args: list[str], timeout: int = 7200) -> bool:
    """执行 amazingdata_fetcher 子命令，返回是否成功。"""
    cmd = [sys.executable, "-m", "amazingdata_fetcher"] + args
    label = " ".join(args)
    logger.info(f"开始: {label}")

    try:
        result = subprocess.run(cmd, timeout=timeout)
        if result.returncode == 0:
            logger.info(f"OK {label}")
            return True
        else:
            logger.error(f"FAIL {label} (exit={result.returncode})")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"TIMEOUT {label}")
        return False
    except Exception as e:
        logger.error(f"ERROR {label}: {e}")
        return False


# ── 补全逻辑 ────────────────────────────────────────────────────────

def _backfill_kline_gaps(gaps: dict[str, list[str]]) -> list[str]:
    """对每种 kline 类型的缺失日期批量补拉，返回失败列表。"""
    errors: list[str] = []
    for ktype, missing_dates in gaps.items():
        if not missing_dates:
            continue
        start = missing_dates[0]
        end = missing_dates[-1]
        logger.info(f"补拉 kline/{ktype}: {len(missing_dates)} 天缺失 ({start} ~ {end})")
        ok = _run([
            "fetch", "kline",
            "--type", ktype,
            "--start-date", start,
            "--end-date", end,
        ])
        if not ok:
            errors.append(f"backfill_kline_{ktype}")
    return errors


# ── 主流程 ──────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> None:
    """每日采集主入口。"""
    now = datetime.now(SHANGHAI_TZ)
    today_str = now.strftime("%Y%m%d")
    output_dir = os.environ.get("OUTPUT_DIR", "./data")

    logger.info(f"AmazingData 每日数据采集")
    logger.info(f"日期: {today_str}  时间: {now:%H:%M:%S} (Asia/Shanghai)")
    logger.info(f"数据目录: {os.path.abspath(output_dir)}")
    logger.info(f"回溯天数: {args.lookback}")

    # 初始化 SDK、获取交易日历
    calendar = _init_sdk()
    logger.info(f"交易日历: {calendar[0]} ~ {calendar[-1]}，共 {len(calendar)} 天")

    # 交易日判断
    if not args.skip_trading_day_check and not _is_trading_day(calendar, today_str):
        logger.info(f"{today_str} 非交易日，跳过。")
        return

    errors: list[str] = []

    # ── 阶段 1: 检测并补全 kline 缺口 ──────────────────────────────
    recent_days = _recent_trading_days(calendar, today_str, args.lookback)
    gaps = _find_kline_gaps(output_dir, recent_days)

    if gaps:
        logger.info(f"发现 kline 缺口:")
        for ktype, dates in gaps.items():
            logger.info(f"  {ktype}: {len(dates)} 天缺失 — {dates[0]} ~ {dates[-1]}")
        errors.extend(_backfill_kline_gaps(gaps))
    else:
        logger.info(f"kline 近 {args.lookback} 个交易日无缺口")

    if args.backfill_only:
        _print_summary(errors)
        return

    # ── 阶段 2: 执行各 fetcher（内部增量逻辑自动补差值）─────────────
    fetchers = [
        ["fetch", "stock_info"],
        ["fetch", "finance"],
        ["fetch", "equity"],
        ["fetch", "index_info"],
        ["fetch", "industry_info"],
        ["fetch", "margin"],
        ["fetch", "kline"],
    ]
    for cmd_args in fetchers:
        ok = _run(cmd_args)
        if not ok:
            errors.append(" ".join(cmd_args))

    # ── 阶段 3: 同步 ───────────────────────────────────────────────
    ok = _run(["sync"])
    if not ok:
        errors.append("sync")

    _print_summary(errors)


def _print_summary(errors: list[str]) -> None:
    if errors:
        logger.warning(f"执行完成，失败任务 ({len(errors)}):")
        for e in errors:
            logger.warning(f"  - {e}")
        sys.exit(1)
    else:
        logger.info("全部成功")
