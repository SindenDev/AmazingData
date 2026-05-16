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
    """写完后自动同步到 S3 + Iceberg。"""
    try:
        from amazingdata_fetcher.storage import (
            filename_to_table_name,
            infer_sync_mode,
            sync_to_iceberg,
            upload_to_s3,
        )

        upload_to_s3(filepath)
        table_name = filename_to_table_name(filename)
        mode = infer_sync_mode(filename)
        sync_to_iceberg(df, table_name, mode=mode)
    except Exception:
        logger.exception(f"自动同步失败: {filename}（本地文件已写入，不影响数据采集）")
