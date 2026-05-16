"""SDK 缓存路径规范化。

AmazingData SDK 内部对 ``local_path`` 参数做字符串拼接（而非 ``os.path.join``），
例如传入 ``./sdk_cache`` 会生成 ``./sdk_cacheinfodata`` 而非 ``./sdk_cache/infodata/``。
本模块确保传给 SDK 的路径始终以 ``/`` 结尾并自动创建子目录。
"""

import os
from pathlib import Path


def normalize_sdk_cache_dir(base_dir: str, subdirectory: str = "") -> str:
    """返回规范化的 SDK 缓存路径（保证尾部 ``/``）。

    Parameters
    ----------
    base_dir : str
        缓存根目录，通常来自环境变量 ``SDK_CACHE_DIR``。
    subdirectory : str, optional
        子目录名称，如 ``"finance"``、``"equity"`` 等。

    Returns
    -------
    str
        以 ``/`` 结尾的绝对或相对路径，目录已创建。
    """
    path = os.path.join(base_dir, subdirectory) if subdirectory else base_dir
    if not path.endswith(os.sep):
        path += os.sep
    Path(path).mkdir(parents=True, exist_ok=True)
    return path
