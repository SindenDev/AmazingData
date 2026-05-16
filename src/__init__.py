"""amazingdata_fetcher — AmazingData 数据采集框架。"""

from amazingdata_fetcher.cache import normalize_sdk_cache_dir
from amazingdata_fetcher.client import get_client
from amazingdata_fetcher.incremental import append_new_rows, load_existing, max_date_str, new_codes
from amazingdata_fetcher.sdk_helpers import dict_to_df, ensure_dataframe, sdk_fetch
from amazingdata_fetcher.writer import write_parquet

__all__ = [
    "append_new_rows",
    "dict_to_df",
    "ensure_dataframe",
    "get_client",
    "load_existing",
    "max_date_str",
    "new_codes",
    "normalize_sdk_cache_dir",
    "sdk_fetch",
    "write_parquet",
]
