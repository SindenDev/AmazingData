"""EquityFetcher — 拉取股本结构与分红数据（全量覆写）。

输出文件：
  - ``equity_structure_history.parquet`` ← ``get_equity_structure``
  - ``equity_dividend_history.parquet``  ← ``get_dividend``
"""

from __future__ import annotations

from loguru import logger

from amazingdata_fetcher.sdk_helpers import ensure_dataframe, sdk_fetch
from amazingdata_fetcher.writer import write_parquet

from .base import BaseFetcher

_DATASETS: dict[str, tuple[str, str]] = {
    "equity_structure": ("equity_structure_history.parquet", "get_equity_structure"),
    "equity_dividend":  ("equity_dividend_history.parquet",  "get_dividend"),
}


class EquityFetcher(BaseFetcher):

    def fetch(self, **kwargs) -> list[str]:
        code_list = self.get_code_list()
        logger.info(f"获取到 {len(code_list)} 个股票代码")
        cache = self.cache_path("equity")

        errors: list[str] = []
        for name, (filename, method_name) in _DATASETS.items():
            try:
                logger.info(f"开始拉取 {filename}（全量覆写）")
                df = ensure_dataframe(
                    sdk_fetch(getattr(self.ido, method_name), code_list, cache, False)
                )
                if df is None:
                    logger.warning(f"{method_name} 返回空数据，跳过")
                    continue
                logger.info(f"SDK 返回 {len(df):,} 行")
                write_parquet(df, self.output_dir, filename)
                logger.info(f"{filename} 写入完成")
            except Exception as e:
                logger.error(f"fetch_{name} 失败: {type(e).__name__}: {e}")
                errors.append(name)
        return errors
