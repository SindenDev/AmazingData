import os
import pandas as pd
from pathlib import Path
from loguru import logger


def write_parquet(df: pd.DataFrame, output_dir: str, filename: str) -> str:
    """将 DataFrame 写为 Parquet 文件，返回完整文件路径"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    df.to_parquet(filepath, index=False, compression="zstd")
    logger.info(f"写入完成: {filepath} ({len(df)} 行)")

    if os.environ.get("SYNC_ENABLED", "false").lower() == "true":
        _auto_sync(df, filepath, filename)

    return filepath


def _auto_sync(df: pd.DataFrame, filepath: str, filename: str) -> None:
    """写完后自动同步到已配置的存储目标。"""
    try:
        from amazingdata_fetcher.storage import sync_file_to_targets

        sync_file_to_targets(df, filepath, filename)
    except Exception:
        logger.exception(f"自动同步失败: {filename}（本地文件已写入，不影响数据采集）")
