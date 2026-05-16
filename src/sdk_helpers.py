"""AmazingData SDK 返回值处理工具。

消除各脚本中重复的 ``dict_to_df`` / ``_sdk_fetch`` 等样板代码。
"""

from __future__ import annotations

import pandas as pd
from loguru import logger


def dict_to_df(d: dict) -> pd.DataFrame:
    """将 SDK 返回的 ``dict[code, DataFrame]`` 合并为单个 DataFrame。"""
    non_empty = [
        v for v in d.values()
        if v is not None and not (isinstance(v, pd.DataFrame) and v.empty)
    ]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True)


def sdk_fetch(fn, *args):
    """安全调用 SDK 方法，异常时返回 ``None``。"""
    try:
        return fn(*args)
    except Exception as e:
        logger.warning(f"SDK 调用异常（已跳过）: {type(e).__name__}: {e}")
        return None


def ensure_dataframe(result) -> pd.DataFrame | None:
    """统一处理 SDK 返回值。

    - ``None`` / 空 → ``None``
    - ``dict`` → ``dict_to_df`` 后返回
    - ``DataFrame`` → 直接返回（空则 ``None``）
    """
    if result is None:
        return None
    if isinstance(result, dict):
        df = dict_to_df(result)
        return df if not df.empty else None
    if isinstance(result, pd.DataFrame):
        return result if not result.empty else None
    return None
