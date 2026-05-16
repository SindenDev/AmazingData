"""verify-finance — 验证财务报表增量拉取逻辑。

测试步骤：
  1. 用 3 只股票拉取全量 balance_sheet（无 begin_date）
  2. 记录最大 REPORTING_PERIOD
  3. 用同样 3 只股票拉取增量（begin_date = 下一季度初）
  4. 验证增量数据与全量数据的关系是否正确

用法::

    python -m amazingdata_fetcher verify-finance
"""

from __future__ import annotations

import argparse
import datetime
from pathlib import Path

import AmazingData as ad
import pandas as pd
from loguru import logger

from amazingdata_fetcher.client import get_client

TEST_CODES = ["600519.SH", "000001.SZ", "601318.SH"]  # 茅台、平安银行、中国平安


def _next_quarter_start(date_str: str) -> int | None:
    if not date_str:
        return None
    d = datetime.date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    next_quarter_first_month = {3: 4, 6: 7, 9: 10, 12: 1}
    next_month = next_quarter_first_month.get(d.month, d.month + 1)
    next_year = d.year + 1 if d.month == 12 else d.year
    return int(f"{next_year}{next_month:02d}01")


def _dict_to_df(d: dict) -> pd.DataFrame:
    non_empty = [v for v in d.values() if v is not None and not v.empty]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True)


def run(args: argparse.Namespace) -> None:
    """验证财务增量逻辑主入口。"""
    logger.info("登录 AmazingData...")
    get_client()
    ido = ad.InfoData()

    tmp = "/tmp/verify_finance/"
    Path(tmp).mkdir(parents=True, exist_ok=True)

    # ── Step 1: 全量拉取 ──────────────────────────────────────────
    logger.info(f"Step 1: 全量拉取 balance_sheet，代码: {TEST_CODES}")
    result_full = ido.get_balance_sheet(TEST_CODES, local_path=tmp, is_local=False)
    if result_full is None:
        logger.error("全量拉取返回 None，退出")
        return
    df_full = _dict_to_df(result_full) if isinstance(result_full, dict) else result_full
    logger.info(f"全量返回 {len(df_full):,} 行，列: {list(df_full.columns)}")

    # 标准化日期列
    col = df_full["REPORTING_PERIOD"]
    if pd.api.types.is_datetime64_any_dtype(col):
        df_full["_rp_str"] = col.dt.strftime("%Y%m%d")
    else:
        df_full["_rp_str"] = col.astype(str).str[:8]

    max_dt_overall = df_full["_rp_str"].max()
    logger.info(f"全量最大 REPORTING_PERIOD: {max_dt_overall}")

    per_code_max = df_full.groupby("MARKET_CODE")["_rp_str"].max()
    logger.info("各代码最大 REPORTING_PERIOD:")
    for code, dt in per_code_max.items():
        logger.info(f"  {code}: {dt}")

    min_of_max = per_code_max.min()
    logger.info(f"各代码最大日期中的最小值（min_max）: {min_of_max}")

    # ── Step 2: 增量拉取 ──────────────────────────────────────────
    begin_date = _next_quarter_start(min_of_max)
    logger.info(f"Step 2: 增量拉取 balance_sheet，begin_date={begin_date}")

    if begin_date is None:
        logger.warning("begin_date 为 None，跳过增量测试")
        return

    result_incr = ido.get_balance_sheet(
        TEST_CODES, local_path=tmp, is_local=False, begin_date=begin_date,
    )
    if result_incr is None:
        logger.info("增量拉取返回 None（无新数据，符合预期）")
        df_incr = pd.DataFrame()
    else:
        df_incr = _dict_to_df(result_incr) if isinstance(result_incr, dict) else result_incr

    logger.info(f"增量返回 {len(df_incr):,} 行")

    if not df_incr.empty:
        col2 = df_incr["REPORTING_PERIOD"]
        if pd.api.types.is_datetime64_any_dtype(col2):
            df_incr["_rp_str"] = col2.dt.strftime("%Y%m%d")
        else:
            df_incr["_rp_str"] = col2.astype(str).str[:8]
        logger.info(
            f"增量数据 REPORTING_PERIOD 范围: "
            f"{df_incr['_rp_str'].min()} ~ {df_incr['_rp_str'].max()}"
        )
        bad = df_incr[df_incr["_rp_str"] < str(begin_date)]
        if bad.empty:
            logger.info("验证通过：增量数据全部 >= begin_date")
        else:
            logger.error(f"验证失败：{len(bad)} 行数据早于 begin_date")

    # ── Step 3: 合并去重检查 ──────────────────────────────────────
    logger.info("Step 3: 模拟 concat（全量 + 增量），检查重复")
    df_full_clean = df_full.drop(columns=["_rp_str"])
    if not df_incr.empty:
        df_incr_clean = df_incr.drop(columns=["_rp_str"])
        df_combined = pd.concat([df_full_clean, df_incr_clean], ignore_index=True)
    else:
        df_combined = df_full_clean

    logger.info(f"合并后总行数: {len(df_combined):,}")

    dup_cols = ["MARKET_CODE", "REPORTING_PERIOD"]
    if all(c in df_combined.columns for c in dup_cols):
        dups = df_combined.duplicated(subset=dup_cols, keep=False)
        if dups.any():
            logger.warning(f"发现 {dups.sum()} 行重复（MARKET_CODE + REPORTING_PERIOD）")
        else:
            logger.info("无重复行（MARKET_CODE + REPORTING_PERIOD 唯一）")

    logger.info("验证完成")
