# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概要

通过 AmazingData SDK（中国银河证券星耀数智）拉取 A 股市场数据，输出为 Parquet 文件，可选同步到 RustFS (S3) + Apache Iceberg。  
输出文件名和 schema 属于外部接口，修改前必须考虑下游兼容性。

## 常用命令

```bash
# 安装/同步依赖（使用 uv，镜像源为阿里云）
uv sync

# 拉取数据（统一 CLI 入口）
python -m amazingdata_fetcher fetch stock_info
python -m amazingdata_fetcher fetch finance --statement balance_sheet
python -m amazingdata_fetcher fetch finance --test-codes 600519.SH 000001.SZ
python -m amazingdata_fetcher fetch kline --type stock --date 20240102
python -m amazingdata_fetcher fetch kline --start-date 20130101 --end-date 20241231
python -m amazingdata_fetcher fetch equity
python -m amazingdata_fetcher fetch margin
python -m amazingdata_fetcher fetch index_info
python -m amazingdata_fetcher fetch industry_info

# 月度 K 线合并
python -m amazingdata_fetcher cleanup --month 202603

# 同步到 RustFS / Iceberg
python -m amazingdata_fetcher sync
python -m amazingdata_fetcher sync --file info_stock_basic.parquet
python -m amazingdata_fetcher sync --s3-only

# 每日定时采集（含 kline 缺口补全 + 全量 fetcher + 同步）
python -m amazingdata_fetcher daily
python -m amazingdata_fetcher daily --backfill-only
python -m amazingdata_fetcher daily --skip-trading-day-check  # 非交易日强制执行

# 辅助工具
python -m amazingdata_fetcher compare-schema /path/to/a.parquet /path/to/b.parquet
python -m amazingdata_fetcher verify-finance
```

环境变量配置：复制 `.env.template` 为 `.env`。必要变量：`AD_HOST`、`AD_PORT`、`AD_USERNAME`、`AD_PASSWORD`、`OUTPUT_DIR`、`SDK_CACHE_DIR`。同步需要额外配置 `RUSTFS_*` 和 `SYNC_ENABLED=true`。

## 架构要点

### 包映射

物理目录为 `src/`，通过 `pyproject.toml` 的 `[tool.setuptools.package-dir]` 映射为 `amazingdata_fetcher` 包。import 时用 `from amazingdata_fetcher.xxx import yyy`，不用 `from src.xxx`。

### 数据写入管道

`write_parquet()` 是唯一合法的 Parquet 输出入口（zstd 压缩）。当 `SYNC_ENABLED=true` 时，写完后自动触发 S3 上传 + Iceberg 写入。**不要直接 `df.to_parquet()`。**

### Fetcher 继承体系

所有数据抓取器继承 `BaseFetcher`（在 `src/fetcher/base.py`），子类只需实现 `fetch(**kwargs) -> list[str]`（返回失败任务列表）。

关键机制：
- **SDK 实例懒加载** — `self.bdo`（BaseData）、`self.ido`（InfoData）、`self.mdo`（MarketData）按需创建，子类直接使用属性
- **缓存路径** — 必须通过 `self.cache_path(subdirectory)` 获取，底层 `normalize_sdk_cache_dir()` 确保尾部 `/`，因为 SDK 内部做字符串拼接而非 `os.path.join`
- **安全退出** — `BaseFetcher.run()` 默认以 `os._exit(0)` 结束进程，规避 SDK teardown 阶段的 segfault
- **SDK 返回值** — 统一通过 `ensure_dataframe()` 处理（可能是 `DataFrame`、`dict[code, DataFrame]` 或 `None`）

新增 Fetcher 步骤：(1) 在 `src/fetcher/` 创建文件继承 `BaseFetcher`，实现 `fetch()` (2) 在 `src/fetcher/__init__.py` 导出 (3) 在 `cli.py` 的 `_FETCHERS` 字典注册

### CLI 命令体系

所有功能统一通过 `python -m amazingdata_fetcher <command>` 调用：
- `fetch <target>` — 数据拉取（stock_info / finance / kline / equity / margin / index_info / industry_info）
- `cleanup` — 月度 K 线合并
- `sync` — 同步到 RustFS / Iceberg
- `daily` — 每日定时采集编排（subprocess 隔离各 fetcher 进程，防 SDK segfault）
- `compare-schema` — Parquet schema 对比工具
- `verify-finance` — 财务增量逻辑验证

非 fetcher 命令的实现在 `src/commands/` 模块中。

### 增量策略（各 Fetcher 不同，不要强行统一）

| Fetcher | 策略 |
| --- | --- |
| `StockInfoFetcher` | 基础信息按新代码增量；复权因子全量覆写（宽表→长表） |
| `FinanceFetcher` | SDK 全量返回 + 客户端按各股票最大 `REPORTING_PERIOD` 的最小值过滤 |
| `MarginFetcher` | SDK 全量返回 + 客户端按 `TRADE_DATE` 过滤 |
| `KlineFetcher` | 每日写独立文件 `extra_{type}_{date}.parquet`；由 `MonthlyCleanup` 合并到 `_history.parquet` |
| `IndexInfoFetcher` | 按日期增量；权重按单个指数逐个请求（避免批量触发 SDK 异常） |
| `IndustryInfoFetcher` | 基础全量；成分按日期增量（用 `INDEX_CODE` 非股票代码） |
| `EquityFetcher` | 全量覆写 |

### daily 命令执行模型

`daily` 命令通过 `subprocess.run()` 调用各 CLI 子命令（不是直接调 Python 函数），每个 fetcher 运行在独立进程中。三阶段：kline 缺口补拉 → 全量 fetcher → 同步。通过 SDK 交易日历判断是否为交易日。

### Airflow DAG

`src/dags/` 中的 DAG 通过 Docker 容器执行 `python3 -m amazingdata_fetcher fetch <target>`。容器镜像 `amazingdata-fetcher:latest`，挂载 `data/` 和 `sdk_cache/` 目录。

### 依赖说明

- AmazingData SDK 和 tgw 是私有包，wheel 放在 `docs/deps/`，通过 `[tool.uv] find-links` 引用
- Python 版本固定为 `3.12.*`
- `pyiceberg` 要求 `>=0.7.0`（PyPI 上 0.0.2 是占位包）

## 日期与时区

运行环境可能是 UTC。凡涉及"今天""上个月"的判断，如果行为必须以上海交易日为准，应用 `Asia/Shanghai` 明确取日期。

## 重要约束

- 始终通过 `write_parquet()` 写 Parquet
- 不要把不同 Fetcher 的增量逻辑强行抽象为同一套规则
- 不要在仓库中写入凭证、密码等敏感内容
- `KlineFetcher` 会将 `kline_time` 统一为 `datetime64[ns]`，`amount` 转为 `int64`
- 指数成分使用 `EXTRA_INDEX_A_SH_SZ` 代码列表
