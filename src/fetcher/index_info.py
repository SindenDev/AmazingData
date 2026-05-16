"""IndexInfoFetcher — 拉取交易所指数成分与权重数据。

输出文件：
  - ``info_index_detail_history.parquet`` ← ``get_index_constituent``（增量：追加新 INDATE 行）
  - ``info_index_weight_history.parquet`` ← ``get_index_weight``（增量：追加新 TRADE_DATE 行）

指数代码来源：``bdo.get_code_list('EXTRA_INDEX_A_SH_SZ')``（约 624 个）。
权重数据仅对 8 个主流宽基指数有效。
"""

from __future__ import annotations

import pandas as pd
from loguru import logger

from amazingdata_fetcher.incremental import load_existing, max_date_str, append_new_rows
from amazingdata_fetcher.sdk_helpers import ensure_dataframe
from amazingdata_fetcher.writer import write_parquet

from .base import BaseFetcher

_MAJOR_INDEX_CODES = [
    "000016.SH", "000300.SH", "000905.SH", "000906.SH",
    "000852.SH", "000985.SH", "000688.SH", "399006.SZ",
]


class IndexInfoFetcher(BaseFetcher):

    def _fetch_detail(self, index_codes: list) -> None:
        logger.info(f"开始拉取 info_index_detail_history（增量），共 {len(index_codes)} 个指数")
        out_path = str(self.output_dir + "/info_index_detail_history.parquet")
        existing = load_existing(out_path)
        max_dt = max_date_str(existing, "INDATE") if existing is not None else None
        logger.info(f"已有最大 INDATE: {max_dt}")

        cache = self.cache_path("index")
        df = ensure_dataframe(
            self.ido.get_index_constituent(index_codes, local_path=cache, is_local=False)
        )
        if df is None:
            logger.error("get_index_constituent 返回空数据")
            return
        logger.info(f"SDK 返回 {len(df)} 行")

        result = append_new_rows(existing, df, "INDATE", max_dt)
        write_parquet(result, self.output_dir, "info_index_detail_history.parquet")
        logger.info("info_index_detail_history 写入完成")

    def _fetch_weight(self, index_codes: list) -> None:
        logger.info(f"开始拉取 info_index_weight_history（增量），共 {len(index_codes)} 个指数")
        out_path = str(self.output_dir + "/info_index_weight_history.parquet")
        existing = load_existing(out_path)
        max_dt = max_date_str(existing, "TRADE_DATE") if existing is not None else None
        logger.info(f"已有最大 TRADE_DATE: {max_dt}")

        # 逐个代码调用，避免 SDK 批量调用崩溃
        cache = self.cache_path("index")
        dfs: list[pd.DataFrame] = []
        for code in index_codes:
            try:
                result = self.ido.get_index_weight([code], local_path=cache, is_local=False)
            except Exception as e:
                logger.warning(f"get_index_weight({code}) 异常: {e}，跳过")
                continue
            if result is None:
                continue
            if isinstance(result, dict):
                dfs.extend(v for v in result.values() if v is not None and not v.empty)
            elif isinstance(result, pd.DataFrame) and not result.empty:
                dfs.append(result)

        if not dfs:
            logger.error("get_index_weight 所有代码均失败")
            return

        df = pd.concat(dfs, ignore_index=True)
        logger.info(f"SDK 合并后 {len(df)} 行")

        result = append_new_rows(existing, df, "TRADE_DATE", max_dt)
        write_parquet(result, self.output_dir, "info_index_weight_history.parquet")
        logger.info("info_index_weight_history 写入完成")

    def fetch(self, **kwargs) -> list[str]:
        index_codes = self.get_code_list("EXTRA_INDEX_A_SH_SZ")
        logger.info(f"获取到 {len(index_codes)} 个指数代码")

        errors: list[str] = []
        for name, fn in [
            ("index_detail", lambda: self._fetch_detail(index_codes)),
            ("index_weight", lambda: self._fetch_weight(_MAJOR_INDEX_CODES)),
        ]:
            try:
                fn()
            except Exception as e:
                logger.error(f"fetch_{name} 失败: {type(e).__name__}: {e}")
                errors.append(name)
        return errors
