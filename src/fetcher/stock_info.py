"""StockInfoFetcher — 拉取股票基础信息与复权因子。

输出文件：
  - ``info_stock_basic.parquet``  ← ``ido.get_stock_basic``（增量：仅新上市代码）
  - ``info_stock_factor.parquet`` ← ``bdo.get_backward_factor``（全量覆写，宽表→长表）
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
from loguru import logger

from amazingdata_fetcher.incremental import load_existing, new_codes
from amazingdata_fetcher.sdk_helpers import ensure_dataframe
from amazingdata_fetcher.writer import write_parquet

from .base import BaseFetcher


class StockInfoFetcher(BaseFetcher):

    def _fetch_stock_basic(self) -> None:
        logger.info("开始拉取 info_stock_basic（增量：仅新上市代码）")
        out_path = str(Path(self.output_dir) / "info_stock_basic.parquet")
        existing = load_existing(out_path)

        all_codes = self.get_code_list()
        logger.info(f"全量代码数: {len(all_codes)}")

        delta_codes = new_codes(existing, all_codes, code_col="MARKET_CODE")
        if not delta_codes:
            logger.info("无新增代码，跳过 get_stock_basic")
            return

        logger.info(f"新增代码数: {len(delta_codes)}，开始拉取...")
        df_new = ensure_dataframe(self.ido.get_stock_basic(delta_codes))
        if df_new is None:
            logger.warning("get_stock_basic 返回空数据")
            return

        df_new = df_new.reset_index(drop=True)
        logger.info(f"拉取到 {len(df_new)} 行新数据")

        result = pd.concat([existing, df_new], ignore_index=True) if existing is not None else df_new
        write_parquet(result, self.output_dir, "info_stock_basic.parquet")
        logger.info("info_stock_basic 写入完成")

    def _fetch_stock_factor(self) -> None:
        logger.info("开始拉取 info_stock_factor（全量覆写，get_backward_factor）")
        out_path = Path(self.output_dir) / "info_stock_factor.parquet"

        if out_path.exists():
            mtime = dt.datetime.fromtimestamp(out_path.stat().st_mtime)
            market_close_today = dt.datetime.combine(dt.date.today(), dt.time(15, 30))
            if mtime >= market_close_today:
                logger.info(f"info_stock_factor 已在今日 {mtime:%H:%M} 收盘后写入，跳过")
                return

        code_list = self.get_code_list("EXTRA_STOCK_A")
        logger.info(f"股票代码数: {len(code_list)}，开始下载复权因子...")

        cache = self.cache_path("stock_factor")
        df_factor = self.bdo.get_backward_factor(code_list, local_path=cache, is_local=False)
        if df_factor is None or df_factor.empty:
            logger.error("get_backward_factor 返回空数据")
            return

        logger.info(f"原始宽表: {df_factor.shape}，开始 unstack...")
        df_factor = df_factor.unstack().reset_index()
        df_factor.columns = ["instrument", "datetime", "backward_factor"]
        logger.info(f"长表行数: {len(df_factor)}，日期范围: {df_factor['datetime'].min()} ~ {df_factor['datetime'].max()}")

        calendar = self.bdo.get_calendar()
        today_str = str(calendar[-1])
        max_dt_str = pd.to_datetime(df_factor["datetime"].max()).strftime("%Y%m%d")
        if max_dt_str != today_str:
            logger.warning(f"复权因子最新日期 {max_dt_str} != 交易日历最新 {today_str}")
        else:
            logger.info(f"复权因子日期校验通过: {max_dt_str}")

        write_parquet(df_factor, self.output_dir, "info_stock_factor.parquet")
        logger.info("info_stock_factor 写入完成")

    def fetch(self, **kwargs) -> list[str]:
        errors: list[str] = []
        for name, fn in [("stock_basic", self._fetch_stock_basic),
                          ("stock_factor", self._fetch_stock_factor)]:
            try:
                fn()
            except Exception as e:
                logger.error(f"fetch_{name} 失败: {type(e).__name__}: {e}")
                errors.append(name)
        return errors
