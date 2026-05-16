"""storage.py — S3 上传 + Iceberg 表管理。

提供两个核心功能：
1. 将本地 Parquet 文件上传到 RustFS (S3 兼容存储)
2. 将 DataFrame 写入 Apache Iceberg 表
"""

from __future__ import annotations

import os
import re
from functools import lru_cache

import boto3
import pandas as pd
import pyarrow as pa
from loguru import logger
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError


# ── S3 配置 ─────────────────────────────────────────────────────

def _s3_config() -> dict:
    return {
        "endpoint_url": os.environ.get("RUSTFS_ENDPOINT", "http://192.168.112.170:9000"),
        "aws_access_key_id": os.environ["RUSTFS_ACCESS_KEY"],
        "aws_secret_access_key": os.environ["RUSTFS_SECRET_KEY"],
    }


def _s3_bucket() -> str:
    return os.environ.get("RUSTFS_BUCKET", "amazingdata")


@lru_cache(maxsize=1)
def _s3_client():
    cfg = _s3_config()
    return boto3.client(
        "s3",
        endpoint_url=cfg["endpoint_url"],
        aws_access_key_id=cfg["aws_access_key_id"],
        aws_secret_access_key=cfg["aws_secret_access_key"],
    )


# ── S3 上传 ─────────────────────────────────────────────────────

def upload_to_s3(local_path: str, s3_key: str | None = None) -> str:
    """上传单个文件到 S3，返回 S3 路径。"""
    bucket = _s3_bucket()
    if s3_key is None:
        filename = os.path.basename(local_path)
        s3_key = f"parquet/{filename}"

    client = _s3_client()
    client.upload_file(local_path, bucket, s3_key)
    s3_path = f"s3://{bucket}/{s3_key}"
    logger.info(f"已上传到 S3: {s3_path}")
    return s3_path


def sync_directory_to_s3(directory: str) -> list[str]:
    """将目录下所有 .parquet 文件上传到 S3，返回已上传的 S3 路径列表。"""
    uploaded = []
    for fname in sorted(os.listdir(directory)):
        if not fname.endswith(".parquet"):
            continue
        local_path = os.path.join(directory, fname)
        s3_path = upload_to_s3(local_path)
        uploaded.append(s3_path)
    return uploaded


# ── Iceberg Catalog ─────────────────────────────────────────────

_NAMESPACE = "amazingdata"
_WAREHOUSE_PATH = "iceberg/warehouse"


@lru_cache(maxsize=1)
def get_iceberg_catalog() -> SqlCatalog:
    """获取 Iceberg SQL catalog（SQLite + S3 warehouse）。"""
    cfg = _s3_config()
    bucket = _s3_bucket()
    output_dir = os.environ.get("OUTPUT_DIR", "./data")
    catalog_db = os.path.join(output_dir, ".iceberg_catalog.db")

    catalog = SqlCatalog(
        "amazingdata",
        **{
            "uri": f"sqlite:///{catalog_db}",
            "warehouse": f"s3://{bucket}/{_WAREHOUSE_PATH}",
            "s3.endpoint": cfg["endpoint_url"],
            "s3.access-key-id": cfg["aws_access_key_id"],
            "s3.secret-access-key": cfg["aws_secret_access_key"],
            "s3.path-style-access": "true",
        },
    )

    try:
        catalog.create_namespace(_NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass

    return catalog


# ── Iceberg 写入 ────────────────────────────────────────────────

def sync_to_iceberg(
    df: pd.DataFrame,
    table_name: str,
    mode: str = "overwrite",
) -> None:
    """将 DataFrame 写入 Iceberg 表。

    Parameters
    ----------
    df : pd.DataFrame
        要写入的数据。
    table_name : str
        完整表名，如 ``amazingdata.info_stock_basic``。
    mode : str
        ``"overwrite"`` 全量覆写 | ``"append"`` 追加。
    """
    catalog = get_iceberg_catalog()

    # PyIceberg 不支持 timestamp[ns]，需降精度到 us
    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].astype("datetime64[us]")

    arrow_table = pa.Table.from_pandas(df, preserve_index=False)

    try:
        iceberg_table = catalog.load_table(table_name)
    except NoSuchTableError:
        iceberg_table = catalog.create_table(table_name, schema=arrow_table.schema)
        logger.info(f"Iceberg 表已创建: {table_name}")

    if mode == "overwrite":
        iceberg_table.overwrite(arrow_table)
    else:
        iceberg_table.append(arrow_table)

    logger.info(f"Iceberg 写入完成: {table_name} ({mode}, {len(df)} 行)")


# ── 文件名 → 表名 / 写入模式 映射 ──────────────────────────────

_DAILY_PATTERN = re.compile(r"^extra_(stock|index|etf)_(\d{8})\.parquet$")


def filename_to_table_name(filename: str) -> str:
    """将 Parquet 文件名映射为 Iceberg 表名。

    日 K 文件 ``extra_stock_20240102.parquet`` → ``amazingdata.extra_stock_daily``
    其他文件 ``info_stock_basic.parquet``      → ``amazingdata.info_stock_basic``
    """
    m = _DAILY_PATTERN.match(filename)
    if m:
        return f"{_NAMESPACE}.extra_{m.group(1)}_daily"
    stem = filename.removesuffix(".parquet")
    return f"{_NAMESPACE}.{stem}"


def infer_sync_mode(filename: str) -> str:
    """推断写入模式：日 K 文件用 append，其余用 overwrite。"""
    if _DAILY_PATTERN.match(filename):
        return "append"
    return "overwrite"
