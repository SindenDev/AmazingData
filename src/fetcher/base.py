"""BaseFetcher — 所有数据抓取器的抽象基类。

统一处理：SDK 实例化、缓存路径、错误收集、安全退出。
子类只需实现 :meth:`fetch` 定义具体的数据抓取逻辑。
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod

import AmazingData as ad
from loguru import logger

from amazingdata_fetcher.cache import normalize_sdk_cache_dir
from amazingdata_fetcher.client import get_client
from amazingdata_fetcher.sdk_helpers import dict_to_df, ensure_dataframe, sdk_fetch
from amazingdata_fetcher.writer import write_parquet


class BaseFetcher(ABC):
    """数据抓取器基类。

    Parameters
    ----------
    output_dir : str
        Parquet 输出目录。
    sdk_cache_dir : str
        SDK 本地缓存根目录。
    """

    def __init__(self, output_dir: str, sdk_cache_dir: str):
        self.output_dir = output_dir
        self.sdk_cache_dir = sdk_cache_dir
        self._bdo: ad.BaseData | None = None
        self._ido: ad.InfoData | None = None
        self._mdo: ad.MarketData | None = None

    # -- SDK 实例（懒加载） --------------------------------------------------

    @property
    def bdo(self) -> ad.BaseData:
        if self._bdo is None:
            self._bdo = ad.BaseData()
        return self._bdo

    @property
    def ido(self) -> ad.InfoData:
        if self._ido is None:
            self._ido = ad.InfoData()
        return self._ido

    @property
    def mdo(self) -> ad.MarketData:
        if self._mdo is None:
            calendar = self.bdo.get_calendar()
            self._mdo = ad.MarketData(calendar)
        return self._mdo

    # -- 工具方法 ------------------------------------------------------------

    def cache_path(self, subdirectory: str) -> str:
        """获取规范化的 SDK 缓存子目录路径。"""
        return normalize_sdk_cache_dir(self.sdk_cache_dir, subdirectory)

    def get_code_list(self, category: str | None = None) -> list:
        """获取代码列表。"""
        return self.bdo.get_code_list(category) if category else self.bdo.get_code_list()

    # -- 抽象接口 ------------------------------------------------------------

    @abstractmethod
    def fetch(self, **kwargs) -> list[str]:
        """执行数据抓取，返回失败任务名称列表（空列表表示全部成功）。"""
        ...

    # -- 统一入口 ------------------------------------------------------------

    def run(self, *, force_exit: bool = True, **kwargs) -> None:
        """登录 → fetch → 安全退出。"""
        get_client()
        errors = self.fetch(**kwargs)
        if errors:
            raise RuntimeError(f"以下数据拉取失败: {errors}")
        logger.info(f"{self.__class__.__name__} 全部完成")
        if force_exit:
            os._exit(0)
