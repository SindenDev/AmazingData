"""compare-schema — 比较两个 Parquet 文件的 schema 和行数。

用法::

    python -m amazingdata_fetcher compare-schema <file_a> <file_b>
"""

from __future__ import annotations

import argparse

import pyarrow.parquet as pq

IGNORE_COLS = {"__index_level_0__"}


def _read_schema(path: str) -> tuple[dict[str, str], int]:
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    cols = {field.name: str(field.type) for field in schema if field.name not in IGNORE_COLS}
    rows = pf.metadata.num_rows
    return cols, rows


def run(args: argparse.Namespace) -> None:
    """比较两个 Parquet 文件的 schema 和行数。"""
    path_a = args.file_a
    path_b = args.file_b

    cols_a, rows_a = _read_schema(path_a)
    cols_b, rows_b = _read_schema(path_b)

    print(f"\n{'=' * 60}")
    print(f"A: {path_a}  ({rows_a:,} rows)")
    print(f"B: {path_b}  ({rows_b:,} rows)")
    print(f"{'=' * 60}")

    # 行数对比
    if rows_a != rows_b:
        diff_pct = abs(rows_a - rows_b) / max(rows_b, 1) * 100
        print(f"行数差异: A={rows_a:,}, B={rows_b:,}  ({diff_pct:.1f}%)")
    else:
        print(f"行数一致: {rows_a:,}")

    # 列名只在 A 中
    only_a = set(cols_a) - set(cols_b)
    if only_a:
        print(f"\n仅在 A 中的列: {sorted(only_a)}")

    # 列名只在 B 中
    only_b = set(cols_b) - set(cols_a)
    if only_b:
        print(f"仅在 B 中的列: {sorted(only_b)}")

    # dtype 不一致
    dtype_diff = []
    for col in sorted(set(cols_a) & set(cols_b)):
        if cols_a[col] != cols_b[col]:
            dtype_diff.append((col, cols_a[col], cols_b[col]))
    if dtype_diff:
        print(f"\ndtype 差异（共 {len(dtype_diff)} 列）:")
        for col, ta, tb in dtype_diff:
            print(f"  {col}: A={ta}, B={tb}")

    if not only_a and not only_b and not dtype_diff:
        print("Schema 完全一致（忽略 __index_level_0__）")
