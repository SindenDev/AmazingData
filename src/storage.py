"""storage.py — RustFS / Iceberg / ClickHouse 同步管理。

提供三个核心功能：
1. 将本地 Parquet 文件上传到 RustFS (S3 兼容存储)
2. 将 DataFrame 写入 Apache Iceberg 表
3. 将 DataFrame 写入 ClickHouse 表
"""

from __future__ import annotations

import base64
import json
import os
import re
from functools import lru_cache
from urllib import parse, request

import boto3
import pandas as pd
import pyarrow as pa
from loguru import logger
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_unsigned_integer_dtype,
)
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError


# ── S3 配置 ─────────────────────────────────────────────────────

def rustfs_sync_configured() -> bool:
    return bool(os.environ.get("RUSTFS_ACCESS_KEY") and os.environ.get("RUSTFS_SECRET_KEY"))


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


# ── ClickHouse 配置 / 写入 ──────────────────────────────────────

def clickhouse_sync_configured() -> bool:
    return bool(os.environ.get("CLICKHOUSE_HOST") and os.environ.get("CLICKHOUSE_DB"))


def _clickhouse_config() -> dict[str, str | int]:
    return {
        "host": os.environ["CLICKHOUSE_HOST"],
        "port": int(os.environ.get("CLICKHOUSE_PORT", "9000")),
        "user": os.environ.get("CLICKHOUSE_USER", "default"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "database": os.environ["CLICKHOUSE_DB"],
    }


@lru_cache(maxsize=1)
def _clickhouse_client():
    from clickhouse_driver import Client

    cfg = _clickhouse_config()
    return Client(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
    )


def _clickhouse_http_port() -> int:
    if os.environ.get("CLICKHOUSE_HTTP_PORT"):
        return int(os.environ["CLICKHOUSE_HTTP_PORT"])
    if str(_clickhouse_config()["port"]) == "9000":
        return 8123
    return int(_clickhouse_config()["port"])


def _clickhouse_http_base_url() -> str:
    cfg = _clickhouse_config()
    scheme = os.environ.get("CLICKHOUSE_SCHEME", "http")
    return f"{scheme}://{cfg['host']}:{_clickhouse_http_port()}"


def _quote_clickhouse_ident(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def _clickhouse_qualified_table(table_name: str, database: str | None = None) -> str:
    database_name = database or str(_clickhouse_config()["database"])
    return f"{_quote_clickhouse_ident(database_name)}.{_quote_clickhouse_ident(table_name)}"


def _pandas_dtype_to_clickhouse(dtype) -> str:
    if is_datetime64_any_dtype(dtype):
        return "Nullable(DateTime64(6))"
    if is_bool_dtype(dtype):
        return "Nullable(UInt8)"
    if is_unsigned_integer_dtype(dtype):
        return "Nullable(UInt64)"
    if is_integer_dtype(dtype):
        return "Nullable(Int64)"
    if is_float_dtype(dtype):
        return "Nullable(Float64)"
    return "Nullable(String)"


def _is_missing_clickhouse_value(value: object) -> bool:
    if value is None:
        return True

    try:
        result = pd.isna(value)
    except TypeError:
        return False

    if isinstance(result, bool):
        return result

    try:
        return bool(result)
    except (TypeError, ValueError):
        return False


def _prepare_clickhouse_column(series: pd.Series, *, json_safe: bool) -> list[object | None]:
    if is_datetime64_any_dtype(series.dtype):
        return [
            None if _is_missing_clickhouse_value(value) else (
                value.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S.%f")
                if json_safe else value.to_pydatetime()
            )
            for value in series
        ]

    if is_bool_dtype(series.dtype):
        return [
            None if _is_missing_clickhouse_value(value) else int(bool(value))
            for value in series
        ]

    if is_unsigned_integer_dtype(series.dtype) or is_integer_dtype(series.dtype):
        return [
            None if _is_missing_clickhouse_value(value) else int(value)
            for value in series
        ]

    if is_float_dtype(series.dtype):
        return [
            None if _is_missing_clickhouse_value(value) else float(value)
            for value in series
        ]

    return [
        None if _is_missing_clickhouse_value(value) else str(value)
        for value in series
    ]


def _prepare_clickhouse_rows(df: pd.DataFrame) -> list[tuple]:
    columnar: list[list[object | None]] = []
    for col in df.columns:
        columnar.append(_prepare_clickhouse_column(df[col], json_safe=False))

    return list(zip(*columnar, strict=False))


def _prepare_clickhouse_json_payload(df: pd.DataFrame) -> bytes:
    columnar = {
        col: _prepare_clickhouse_column(df[col], json_safe=True)
        for col in df.columns
    }
    lines = [
        json.dumps(
            dict(zip(df.columns, row, strict=False)),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        for row in zip(*columnar.values(), strict=False)
    ]
    return "\n".join(lines).encode("utf-8")


def _execute_clickhouse_http(
    query: str,
    data: bytes | None = None,
    *,
    database: str | None = None,
) -> bytes:
    cfg = _clickhouse_config()
    target_db = database or str(cfg["database"])
    url = f"{_clickhouse_http_base_url()}/?database={parse.quote(target_db)}"
    auth = base64.b64encode(
        f"{cfg['user']}:{cfg['password']}".encode("utf-8")
    ).decode("ascii")
    payload = query.encode("utf-8") if data is None else query.encode("utf-8") + b"\n" + data
    req = request.Request(
        url,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "text/plain; charset=utf-8",
        },
    )
    with request.urlopen(req) as resp:
        return resp.read()


def _sync_to_clickhouse_http(
    df: pd.DataFrame,
    table_name: str,
    mode: str,
    *,
    qualified_table: str,
    columns_sql: str,
    column_list: str,
) -> None:
    database = str(_clickhouse_config()["database"])
    _execute_clickhouse_http(
        f"CREATE DATABASE IF NOT EXISTS {_quote_clickhouse_ident(database)}",
        database="default",
    )

    if mode == "overwrite":
        _execute_clickhouse_http(f"DROP TABLE IF EXISTS {qualified_table}")

    _execute_clickhouse_http(
        f"CREATE TABLE IF NOT EXISTS {qualified_table} "
        f"({columns_sql}) ENGINE = MergeTree ORDER BY tuple()"
    )

    if df.empty:
        logger.info(f"ClickHouse 写入完成: {qualified_table} ({mode}, 0 行)")
        return

    chunk_size = int(os.environ.get("CLICKHOUSE_INSERT_CHUNK_SIZE", "50000"))
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        payload = _prepare_clickhouse_json_payload(chunk)
        _execute_clickhouse_http(
            f"INSERT INTO {qualified_table} ({column_list}) FORMAT JSONEachRow",
            data=payload,
        )

    logger.info(f"ClickHouse 写入完成: {qualified_table} ({mode}, {len(df)} 行)")


def filename_to_clickhouse_table_name(filename: str) -> str:
    return filename_to_table_stem(filename)


def sync_to_clickhouse(
    df: pd.DataFrame,
    table_name: str,
    mode: str = "overwrite",
) -> None:
    from clickhouse_driver.errors import NetworkError

    client = _clickhouse_client()
    cfg = _clickhouse_config()
    database = str(cfg["database"])
    qualified_table = _clickhouse_qualified_table(table_name, database=database)
    columns_sql = ", ".join(
        f"{_quote_clickhouse_ident(col)} {_pandas_dtype_to_clickhouse(dtype)}"
        for col, dtype in df.dtypes.items()
    )
    column_list = ", ".join(_quote_clickhouse_ident(col) for col in df.columns)
    try:
        client.execute(f"CREATE DATABASE IF NOT EXISTS {_quote_clickhouse_ident(database)}")

        if mode == "overwrite":
            client.execute(f"DROP TABLE IF EXISTS {qualified_table}")

        client.execute(
            f"CREATE TABLE IF NOT EXISTS {qualified_table} "
            f"({columns_sql}) ENGINE = MergeTree ORDER BY tuple()"
        )

        if df.empty:
            logger.info(f"ClickHouse 写入完成: {qualified_table} ({mode}, 0 行)")
            return

        chunk_size = int(os.environ.get("CLICKHOUSE_INSERT_CHUNK_SIZE", "50000"))
        insert_sql = f"INSERT INTO {qualified_table} ({column_list}) VALUES"

        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start:start + chunk_size]
            client.execute(insert_sql, _prepare_clickhouse_rows(chunk))

        logger.info(f"ClickHouse 写入完成: {qualified_table} ({mode}, {len(df)} 行)")
    except NetworkError as exc:
        logger.warning(f"ClickHouse Native 连接失败，回退 HTTP 接口: {exc}")
        _sync_to_clickhouse_http(
            df,
            table_name,
            mode,
            qualified_table=qualified_table,
            columns_sql=columns_sql,
            column_list=column_list,
        )


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


def sync_file_to_targets(
    df: pd.DataFrame,
    filepath: str,
    filename: str,
    *,
    s3_only: bool = False,
) -> list[str]:
    synced_targets: list[str] = []
    mode = infer_sync_mode(filename)

    if rustfs_sync_configured():
        upload_to_s3(filepath)
        synced_targets.append("s3")
        if not s3_only:
            iceberg_table = filename_to_table_name(filename)
            sync_to_iceberg(df, iceberg_table, mode=mode)
            synced_targets.append("iceberg")
    elif s3_only:
        raise RuntimeError("未配置 RustFS 凭证，无法执行 --s3-only")

    if not s3_only and clickhouse_sync_configured():
        clickhouse_table = filename_to_clickhouse_table_name(filename)
        sync_to_clickhouse(df, clickhouse_table, mode=mode)
        synced_targets.append("clickhouse")

    if not synced_targets:
        raise RuntimeError("未检测到可用同步目标，请配置 RustFS_* 或 CLICKHOUSE_* 环境变量")

    return synced_targets


# ── 文件名 → 表名 / 写入模式 映射 ──────────────────────────────

_DAILY_PATTERN = re.compile(r"^extra_(stock|index|etf)_(\d{8})\.parquet$")


def filename_to_table_stem(filename: str) -> str:
    m = _DAILY_PATTERN.match(filename)
    if m:
        return f"extra_{m.group(1)}_daily"
    return filename.removesuffix(".parquet")


def filename_to_table_name(filename: str) -> str:
    """将 Parquet 文件名映射为 Iceberg 表名。

    日 K 文件 ``extra_stock_20240102.parquet`` → ``amazingdata.extra_stock_daily``
    其他文件 ``info_stock_basic.parquet``      → ``amazingdata.info_stock_basic``
    """
    return f"{_NAMESPACE}.{filename_to_table_stem(filename)}"


def infer_sync_mode(filename: str) -> str:
    """推断写入模式：日 K 文件用 append，其余用 overwrite。"""
    if _DAILY_PATTERN.match(filename):
        return "append"
    return "overwrite"
