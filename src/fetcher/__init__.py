"""Fetcher 子包 — 导出所有数据抓取器。"""

from .base import BaseFetcher
from .cleanup import MonthlyCleanup
from .equity import EquityFetcher
from .finance import FinanceFetcher
from .index_info import IndexInfoFetcher
from .industry_info import IndustryInfoFetcher
from .kline import KlineFetcher
from .margin import MarginFetcher
from .stock_info import StockInfoFetcher

__all__ = [
    "BaseFetcher",
    "EquityFetcher",
    "FinanceFetcher",
    "IndexInfoFetcher",
    "IndustryInfoFetcher",
    "KlineFetcher",
    "MarginFetcher",
    "MonthlyCleanup",
    "StockInfoFetcher",
]
