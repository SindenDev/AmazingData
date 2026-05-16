"""
incremental.py — 增量合并工具函数

核心逻辑：从已有 parquet 文件读取最大日期/代码，
SDK 返回全量数据后只保留新增部分，concat 后写回。
"""
from pathlib import Path
import pandas as pd
from loguru import logger


def load_existing(path: str) -> pd.DataFrame | None:
    """读取已有 parquet，不存在则返回 None"""
    p = Path(path)
    if p.exists() and p.stat().st_size > 10_000:  # 至少 10KB，避免误用空壳文件
        df = pd.read_parquet(p)
        logger.info(f"读取已有文件: {path} ({len(df):,} 行)")
        return df
    return None


def max_date_str(df: pd.DataFrame, col: str) -> str | None:
    """返回日期列的最大值（字符串形式 YYYYMMDD），支持 string/int/timestamp 类型"""
    if df is None or col not in df.columns:
        return None
    series = df[col].dropna()
    if series.empty:
        return None
    val = series.max()
    # timestamp → YYYYMMDD string
    if hasattr(val, 'strftime'):
        return val.strftime("%Y%m%d")
    return str(int(val))


def append_new_rows(existing: pd.DataFrame | None, new_df: pd.DataFrame,
                    date_col: str, date_val: str | None = None) -> pd.DataFrame:
    """
    从 new_df 中过滤出比 existing 更新的行，append 到 existing。
    date_val: existing 中的最大日期（YYYYMMDD string）；若为 None 则全部保留。
    """
    if existing is None or date_val is None:
        return new_df.reset_index(drop=True)

    # 统一比较：转为字符串 YYYYMMDD
    col = new_df[date_col]
    if pd.api.types.is_datetime64_any_dtype(col):
        new_df = new_df.copy()
        new_df["_date_str"] = col.dt.strftime("%Y%m%d")
        mask = new_df["_date_str"] > date_val
        new_rows = new_df[mask].drop(columns=["_date_str"])
    else:
        mask = col.astype(str).str[:8] > date_val
        new_rows = new_df[mask]

    logger.info(f"增量行数: {len(new_rows):,}（已有最大日期: {date_val}）")
    if new_rows.empty:
        return existing

    result = pd.concat([existing, new_rows], ignore_index=True)
    return result


def new_codes(existing: pd.DataFrame | None, all_codes: list, code_col: str = "MARKET_CODE") -> list:
    """返回 all_codes 中在 existing 里没有的新代码"""
    if existing is None:
        return all_codes
    existing_codes = set(existing[code_col].dropna().unique())
    new = [c for c in all_codes if c not in existing_codes]
    logger.info(f"新增代码数: {len(new)}（全量: {len(all_codes)}，已有: {len(existing_codes)}）")
    return new
