"""MonthlyCleanup — 每月初合并上月增量 K 线文件。

将上月所有每日增量文件合并为历史汇总文件，然后删除日文件：
  - ``extra_stock_history.parquet``
  - ``extra_index_history.parquet``
  - ``extra_etf_history.parquet``
"""

from __future__ import annotations

import glob
import os
from datetime import date, timedelta

import pandas as pd
from loguru import logger

from amazingdata_fetcher.writer import write_parquet


def _get_last_month(ref_date: date) -> str:
    """返回上个月的 YYYYMM 字符串。"""
    first_of_month = ref_date.replace(day=1)
    last_month = first_of_month - timedelta(days=1)
    return last_month.strftime("%Y%m")


def _merge_and_cleanup(data_type: str, month_str: str, output_dir: str) -> None:
    """合并指定月份的增量文件为历史汇总文件，然后删除日文件。"""
    pattern = os.path.join(output_dir, f"extra_{data_type}_{month_str}??.parquet")
    daily_files = sorted(glob.glob(pattern))

    if not daily_files:
        logger.warning(f"未找到 {pattern} 匹配文件，跳过")
        return

    logger.info(f"找到 {len(daily_files)} 个 extra_{data_type}_{month_str}?? 文件，开始合并...")

    history_file = os.path.join(output_dir, f"extra_{data_type}_history.parquet")
    df_new = pd.concat([pd.read_parquet(f) for f in daily_files], ignore_index=True)
    logger.info(f"当月新增数据：{len(df_new)} 行")

    if os.path.exists(history_file):
        df_history = pd.read_parquet(history_file)
        logger.info(f"历史文件已有 {len(df_history)} 行，合并中...")
        df_combined = pd.concat([df_history, df_new], ignore_index=True)
    else:
        df_combined = df_new

    write_parquet(df_combined, output_dir, f"extra_{data_type}_history.parquet")
    logger.info(f"历史文件已写入（共 {len(df_combined)} 行）")

    for f in daily_files:
        os.remove(f)
        logger.info(f"已删除：{f}")


class MonthlyCleanup:
    """月度 K 线文件合并清理。"""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    def run(self, month: str | None = None) -> None:
        month_str = month or _get_last_month(date.today())
        logger.info(f"处理月份：{month_str}，数据目录：{self.output_dir}")
        for data_type in ("stock", "index", "etf"):
            _merge_and_cleanup(data_type, month_str, self.output_dir)
        logger.info("MonthlyCleanup 完成")
