"""IndustryInfoFetcher — 拉取行业基础信息与成分数据。

输出文件：
  - ``info_industry_basic_history.parquet``  ← ``get_industry_base_info``（全量刷新）
  - ``info_industry_detail_history.parquet`` ← ``get_industry_constituent``（增量：追加新 INDATE 行）

行业代码来源：从 ``info_industry_basic_history.parquet`` 的 ``INDEX_CODE`` 列读取。
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from loguru import logger

from amazingdata_fetcher.incremental import load_existing, max_date_str, append_new_rows
from amazingdata_fetcher.sdk_helpers import ensure_dataframe
from amazingdata_fetcher.writer import write_parquet

from .base import BaseFetcher


class IndustryInfoFetcher(BaseFetcher):

    def _fetch_basic(self) -> None:
        logger.info("开始拉取 info_industry_basic_history（全量刷新）")
        cache = self.cache_path("industry")
        df = ensure_dataframe(
            self.ido.get_industry_base_info(local_path=cache, is_local=False)
        )
        if df is None:
            logger.error("get_industry_base_info 返回空数据")
            return
        df = df.reset_index(drop=True)
        logger.info(f"拉取到 {len(df)} 行行业基础数据")
        write_parquet(df, self.output_dir, "info_industry_basic_history.parquet")
        logger.info("info_industry_basic_history 写入完成")

    def _fetch_detail(self, industry_codes: list) -> None:
        logger.info(f"开始拉取 info_industry_detail_history（增量），共 {len(industry_codes)} 个行业代码")
        out_path = str(Path(self.output_dir) / "info_industry_detail_history.parquet")
        existing = load_existing(out_path)
        max_dt = max_date_str(existing, "INDATE") if existing is not None else None
        logger.info(f"已有最大 INDATE: {max_dt}")

        cache = self.cache_path("industry")
        df = ensure_dataframe(
            self.ido.get_industry_constituent(industry_codes, local_path=cache, is_local=False)
        )
        if df is None:
            logger.error("get_industry_constituent 返回空数据")
            return
        logger.info(f"SDK 返回 {len(df)} 行")

        result = append_new_rows(existing, df, "INDATE", max_dt)
        write_parquet(result, self.output_dir, "info_industry_detail_history.parquet")
        logger.info("info_industry_detail_history 写入完成")

    def fetch(self, **kwargs) -> list[str]:
        errors: list[str] = []

        try:
            self._fetch_basic()
        except Exception as e:
            logger.error(f"fetch_industry_basic 失败: {type(e).__name__}: {e}")
            errors.append("industry_basic")

        # 从刚写入的文件读取行业代码
        basic_path = Path(self.output_dir) / "info_industry_basic_history.parquet"
        if not basic_path.exists():
            logger.error("info_industry_basic_history.parquet 不存在，跳过行业成分拉取")
            errors.append("industry_detail")
            return errors

        df_ind = pd.read_parquet(basic_path)
        industry_codes = df_ind["INDEX_CODE"].dropna().unique().tolist()
        logger.info(f"获取到 {len(industry_codes)} 个行业代码")

        try:
            self._fetch_detail(industry_codes)
        except Exception as e:
            logger.error(f"fetch_industry_detail 失败: {type(e).__name__}: {e}")
            errors.append("industry_detail")

        return errors
