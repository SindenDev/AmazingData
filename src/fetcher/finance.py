"""FinanceFetcher — 拉取三张财务报表（增量模式）。

输出文件：
  - ``finance_balance_sheet_history.parquet`` ← ``get_balance_sheet``
  - ``finance_cash_flow_history.parquet``     ← ``get_cash_flow``
  - ``finance_income_history.parquet``        ← ``get_income``

增量策略：SDK 不支持 begin_date 过滤，每次全量下载后客户端过滤。
过滤基准：各公司在已有文件中的最大 REPORTING_PERIOD 的最小值。
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from loguru import logger

from amazingdata_fetcher.incremental import load_existing, max_date_str, append_new_rows
from amazingdata_fetcher.sdk_helpers import ensure_dataframe, sdk_fetch
from amazingdata_fetcher.writer import write_parquet

from .base import BaseFetcher

# (文件名, SDK 方法名)
_STATEMENTS: dict[str, tuple[str, str]] = {
    "balance_sheet": ("finance_balance_sheet_history.parquet", "get_balance_sheet"),
    "cash_flow":     ("finance_cash_flow_history.parquet",     "get_cash_flow"),
    "income":        ("finance_income_history.parquet",        "get_income"),
}


def _min_max_date_per_code(
    existing: pd.DataFrame | None,
    date_col: str,
    code_col: str = "MARKET_CODE",
) -> str | None:
    """各公司最大报告期的最小值，确保不遗漏尚未提交最新季报的公司。"""
    if existing is None:
        return None
    if code_col not in existing.columns:
        return max_date_str(existing, date_col)
    per_code = existing.groupby(code_col)[date_col].max()
    val = per_code.min()
    if hasattr(val, "strftime"):
        return val.strftime("%Y%m%d")
    return str(int(val))[:8]


class FinanceFetcher(BaseFetcher):

    def _fetch_statement(self, name: str, code_list: list) -> None:
        filename, method_name = _STATEMENTS[name]
        out_path = str(Path(self.output_dir) / filename)
        existing = load_existing(out_path)
        cutoff_dt = _min_max_date_per_code(existing, "REPORTING_PERIOD")
        logger.info(f"[{name}] 增量基准日期: {cutoff_dt}")

        cache = self.cache_path("finance")
        df = ensure_dataframe(sdk_fetch(getattr(self.ido, method_name), code_list, cache, True))
        if df is None:
            logger.warning(f"{method_name} 返回空数据，跳过")
            return
        logger.info(f"SDK 返回 {len(df):,} 行")

        result = append_new_rows(existing, df, "REPORTING_PERIOD", cutoff_dt)
        if result is existing:
            logger.info("无新增行，跳过写入")
            return
        write_parquet(result, self.output_dir, filename)
        logger.info(f"{filename} 写入完成 ({len(result):,} 行)")

    def fetch(
        self,
        *,
        statement: str | None = None,
        test_codes: list[str] | None = None,
        **kwargs,
    ) -> list[str]:
        code_list = test_codes or self.get_code_list()
        if test_codes:
            logger.info(f"测试模式，使用指定代码: {code_list}")
        else:
            logger.info(f"获取到 {len(code_list)} 个股票代码")

        targets = [statement] if statement else list(_STATEMENTS)
        errors: list[str] = []
        for name in targets:
            try:
                self._fetch_statement(name, code_list)
            except Exception as e:
                logger.error(f"fetch_{name} 失败: {type(e).__name__}: {e}")
                errors.append(name)
        return errors
