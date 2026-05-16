"""MarginFetcher — 拉取融资融券数据（增量模式）。

输出文件：
  - ``margin_summary_history.parquet`` ← ``get_margin_summary``（追加新 TRADE_DATE 行）
  - ``margin_detail_history.parquet``  ← ``get_margin_detail``（追加新 TRADE_DATE 行）

增量策略：SDK 不支持日期过滤，每次全量下载后客户端过滤。
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from loguru import logger

from amazingdata_fetcher.incremental import load_existing, max_date_str, append_new_rows
from amazingdata_fetcher.sdk_helpers import ensure_dataframe, sdk_fetch
from amazingdata_fetcher.writer import write_parquet

from .base import BaseFetcher


class MarginFetcher(BaseFetcher):

    def _load_exclusion_list(self) -> set[str]:
        csv_path = Path(self.output_dir) / "no_margin_stock_list.csv"
        if not csv_path.exists():
            return set()
        df = pd.read_csv(csv_path, dtype=str)
        if "stock_code" not in df.columns:
            logger.warning(f"排除清单缺少 stock_code 列: {csv_path}")
            return set()
        codes = set(df["stock_code"].str.strip())
        logger.info(f"排除清单: {len(codes)} 只股票")
        return codes

    def _fetch_summary(self) -> None:
        logger.info("开始拉取 margin_summary_history（增量：追加新 TRADE_DATE 行）")
        out_path = str(Path(self.output_dir) / "margin_summary_history.parquet")
        existing = load_existing(out_path)
        max_dt = max_date_str(existing, "TRADE_DATE") if existing is not None else None

        cache = self.cache_path("margin")
        df = ensure_dataframe(sdk_fetch(self.ido.get_margin_summary, cache, True))
        if df is None:
            logger.warning("get_margin_summary 返回空数据，跳过")
            return

        result = append_new_rows(existing, df, "TRADE_DATE", max_dt)
        if result is existing:
            logger.info("无新增行，跳过写入")
            return
        write_parquet(result, self.output_dir, "margin_summary_history.parquet")
        logger.info("margin_summary_history 写入完成")

    def _fetch_detail(self, code_list: list) -> None:
        logger.info("开始拉取 margin_detail_history（增量：追加新 TRADE_DATE 行）")
        out_path = Path(self.output_dir) / "margin_detail_history.parquet"

        existing_max_dt = None
        if out_path.exists() and out_path.stat().st_size > 10_000:
            existing_max_dt = max_date_str(
                pd.read_parquet(out_path, columns=["TRADE_DATE"]), "TRADE_DATE"
            )
            logger.info(f"已有文件最大 TRADE_DATE: {existing_max_dt}")
        else:
            logger.info("无已有文件，全量写入")

        cache = self.cache_path("margin")
        df = ensure_dataframe(sdk_fetch(self.ido.get_margin_detail, code_list, cache, True))
        if df is None:
            logger.warning("get_margin_detail 返回空数据，跳过")
            return
        logger.info(f"SDK 返回 {len(df):,} 行")

        if existing_max_dt is None:
            result = df.reset_index(drop=True)
        else:
            col = df["TRADE_DATE"]
            if pd.api.types.is_datetime64_any_dtype(col):
                mask = col.dt.strftime("%Y%m%d") > existing_max_dt
            else:
                mask = col.astype(str).str[:8] > existing_max_dt
            new_rows = df[mask]
            del df
            logger.info(f"增量行数: {len(new_rows):,}")
            if new_rows.empty:
                logger.info("无新增行，跳过写入")
                return
            existing = pd.read_parquet(out_path)
            result = pd.concat([existing, new_rows], ignore_index=True)
            del existing, new_rows

        write_parquet(result, str(out_path.parent), out_path.name)
        logger.info("margin_detail_history 写入完成")

    def fetch(self, **kwargs) -> list[str]:
        code_list = self.get_code_list()
        logger.info(f"获取到 {len(code_list)} 个股票代码")

        exclusion = self._load_exclusion_list()
        if exclusion:
            code_list = [c for c in code_list if c not in exclusion]
            logger.info(f"排除后剩余 {len(code_list)} 只")

        errors: list[str] = []
        for name, fn in [("margin_summary", self._fetch_summary),
                          ("margin_detail", lambda: self._fetch_detail(code_list))]:
            try:
                fn()
            except Exception as e:
                logger.error(f"fetch_{name} 失败: {type(e).__name__}: {e}")
                errors.append(name)
        return errors
