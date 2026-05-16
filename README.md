# AmazingData 数据采集项目

通过中国银河证券星耀数智（AmazingData）SDK 拉取 A 股市场数据，并将结果写为 Parquet 文件。

## 项目目标

- 产出与下游 ETL 兼容的 Parquet 文件
- 保持既有文件名和 schema 稳定
- 按数据类型分别采用全量覆写或增量追加策略

## 仓库结构

```text
amazingdata/
├── README.md
├── CLAUDE.md
├── .env.template
├── pyproject.toml
├── docs/
│   ├── AmazingData开发手册.pdf
│   ├── Amazing Data 接口文件列表.md
│   ├── crontab.example            # crontab 配置参考
│   ├── extract_ad_stock.ipynb     # SDK 数据探索 notebook
│   └── deps/                      # 本地 wheel 包
└── src/                           # Python 包根目录（映射为 amazingdata_fetcher 包）
    ├── __init__.py                # 包入口
    ├── __main__.py                # python -m 支持
    ├── cli.py                     # 统一 CLI 入口
    ├── client.py                  # SDK 登录封装
    ├── writer.py                  # Parquet 写入（zstd）+ 自动同步
    ├── storage.py                 # S3 上传 + Iceberg 表管理
    ├── incremental.py             # 增量逻辑
    ├── cache.py                   # SDK 缓存路径规范化
    ├── sdk_helpers.py             # SDK 返回值处理工具
    ├── fetcher/                   # OOP 数据抓取器
    │   ├── base.py                # BaseFetcher 抽象基类
    │   ├── stock_info.py          # StockInfoFetcher
    │   ├── finance.py             # FinanceFetcher
    │   ├── kline.py               # KlineFetcher
    │   ├── equity.py              # EquityFetcher
    │   ├── margin.py              # MarginFetcher
    │   ├── index_info.py          # IndexInfoFetcher
    │   ├── industry_info.py       # IndustryInfoFetcher
    │   └── cleanup.py             # MonthlyCleanup
    ├── commands/                   # 非 fetcher 命令实现
    │   ├── daily.py               # 每日采集编排
    │   ├── compare_schema.py      # Parquet schema 对比
    │   └── verify_finance.py      # 财务增量验证
    └── dags/                       # Airflow DAG 定义
        ├── fetch_stock_info.py
        ├── fetch_finance.py
        ├── fetch_kline.py
        ├── fetch_equity.py
        ├── fetch_margin.py
        ├── fetch_index_info.py
        ├── fetch_industry_info.py
        └── monthly_cleanup.py
```

## 核心模块

### [`cli.py`](src/cli.py)

- 统一命令行入口，所有功能通过 `python -m amazingdata_fetcher <command>` 调用
- 子命令：`fetch`（数据拉取）、`cleanup`（月度合并）、`sync`（同步）、`daily`（每日采集）、`compare-schema`（schema 对比）、`verify-finance`（增量验证）
- 通过 `pyproject.toml` 注册为 `amazingdata-fetcher` console_scripts

### [`fetcher/base.py`](src/fetcher/base.py)

- `BaseFetcher` 抽象基类，所有数据抓取器的父类
- 统一处理：SDK 实例懒加载（`bdo`/`ido`/`mdo`）、缓存路径规范化、安全退出
- 子类只需实现 `fetch()` 方法

### [`client.py`](src/client.py)

- 从环境变量读取 AmazingData 登录信息
- 封装 `ad.login(...)`

### [`writer.py`](src/writer.py)

- 统一 Parquet 输出入口
- 使用 `zstd` 压缩
- 自动创建输出目录并记录日志
- 当 `SYNC_ENABLED=true` 时，写完后自动同步到 RustFS + Iceberg

### [`incremental.py`](src/incremental.py)

- 读取已有 Parquet
- 获取已有数据中的最大日期
- 追加新日期行或新代码

### [`cache.py`](src/cache.py)

- 修复 SDK 内部 `local_path + "infodata"` 字符串拼接导致的路径错误
- 确保传给 SDK 的路径始终以 `/` 结尾并自动创建子目录

### [`sdk_helpers.py`](src/sdk_helpers.py)

- `dict_to_df()` — 合并 SDK 返回的 `dict[code, DataFrame]`
- `sdk_fetch()` — 安全调用 SDK 方法，异常时返回 `None`
- `ensure_dataframe()` — 统一处理 SDK 返回值（None/dict/DataFrame）

### [`storage.py`](src/storage.py)

- S3 上传（boto3）+ Iceberg 表管理（PyIceberg SqlCatalog）
- `upload_to_s3()` — 上传单个文件到 S3
- `sync_to_iceberg()` — 将 DataFrame 写入 Iceberg 表（append 或 overwrite）
- `filename_to_table_name()` — 文件名映射为 Iceberg 表名
- `infer_sync_mode()` — 推断写入模式（日 K 文件用 append，其余用 overwrite）

### [`commands/`](src/commands/)

- `daily.py` — 每日采集编排（交易日判断 + kline 缺口补全 + 全量 fetcher + 同步），各 fetcher 通过 subprocess 在独立进程中运行
- `compare_schema.py` — 比较两个 Parquet 文件的 schema 和行数
- `verify_finance.py` — 验证财务报表增量拉取逻辑

## 环境变量

复制 `.env.template` 为 `.env` 后再运行。

| 变量 | 说明 |
| --- | --- |
| `AD_HOST` | AmazingData 服务地址 |
| `AD_PORT` | AmazingData 服务端口 |
| `AD_USERNAME` | AmazingData 用户名 |
| `AD_PASSWORD` | AmazingData 密码 |
| `OUTPUT_DIR` | Parquet 输出目录 |
| `SDK_CACHE_DIR` | SDK 本地缓存目录 |
| `RUSTFS_ENDPOINT` | RustFS (S3) 服务地址 |
| `RUSTFS_ACCESS_KEY` | RustFS Access Key |
| `RUSTFS_SECRET_KEY` | RustFS Secret Key |
| `RUSTFS_BUCKET` | RustFS Bucket 名称 |
| `SYNC_ENABLED` | 是否在 fetch 后自动同步（true/false） |
| `LOG_LEVEL` | 日志级别 |
| `LOG_DIR` | 日志目录 |

## 常用运行方式

所有功能通过统一 CLI 入口运行：

```bash
# 股票基础信息 + 复权因子
python -m amazingdata_fetcher fetch stock_info

# 交易所指数
python -m amazingdata_fetcher fetch index_info

# 行业指数
python -m amazingdata_fetcher fetch industry_info

# 股本结构 + 分红
python -m amazingdata_fetcher fetch equity

# 融资融券
python -m amazingdata_fetcher fetch margin
```

按单张财务报表运行：

```bash
python -m amazingdata_fetcher fetch finance --statement balance_sheet
python -m amazingdata_fetcher fetch finance --statement cash_flow
python -m amazingdata_fetcher fetch finance --statement income
```

财务数据测试指定股票：

```bash
python -m amazingdata_fetcher fetch finance --statement balance_sheet --test-codes 600519.SH 000001.SZ
```

拉取指定日期的日 K 线：

```bash
python -m amazingdata_fetcher fetch kline --type stock --date 20240102
python -m amazingdata_fetcher fetch kline --type index --date 20240102
python -m amazingdata_fetcher fetch kline --type etf --date 20240102
```

批量拉取历史日 K 线（自动跳过已有文件）：

```bash
python -m amazingdata_fetcher fetch kline --start-date 20130101
python -m amazingdata_fetcher fetch kline --type stock --start-date 20130101 --end-date 20241231
```

合并月度日 K 文件：

```bash
python -m amazingdata_fetcher cleanup --month 202603
```

同步数据到 RustFS / Iceberg：

```bash
# 同步所有本地 Parquet
python -m amazingdata_fetcher sync

# 同步指定文件
python -m amazingdata_fetcher sync --file info_stock_basic.parquet

# 仅上传到 S3（不写 Iceberg）
python -m amazingdata_fetcher sync --s3-only
```

每日定时采集（含缺口补全）：

```bash
# 完整流程：交易日判断 → kline 缺口补拉 → 全量 fetcher → 同步
python -m amazingdata_fetcher daily

# 只补 kline 缺口，不跑其他 fetcher
python -m amazingdata_fetcher daily --backfill-only

# 自定义回溯天数（默认 30 个交易日）
python -m amazingdata_fetcher daily --lookback 60

# 跳过交易日判断，强制执行（手动补数据用）
python -m amazingdata_fetcher daily --skip-trading-day-check
```

辅助工具：

```bash
# 比较两个 Parquet 文件的 schema 和行数
python -m amazingdata_fetcher compare-schema /path/to/file_a.parquet /path/to/file_b.parquet

# 验证财务数据增量逻辑
python -m amazingdata_fetcher verify-finance
```

## 数据抓取器

| Fetcher 类 | 主要输出 | 策略 |
| --- | --- | --- |
| `StockInfoFetcher` | `info_stock_basic.parquet`, `info_stock_factor.parquet` | 基础信息增量；复权因子全量覆写 |
| `IndexInfoFetcher` | `info_index_detail_history.parquet`, `info_index_weight_history.parquet` | 按日期增量追加 |
| `IndustryInfoFetcher` | `info_industry_basic_history.parquet`, `info_industry_detail_history.parquet` | 基础信息全量；成分按日期增量 |
| `EquityFetcher` | `equity_structure_history.parquet`, `equity_dividend_history.parquet` | 全量覆写 |
| `FinanceFetcher` | `finance_balance_sheet_history.parquet`, `finance_cash_flow_history.parquet`, `finance_income_history.parquet` | 客户端增量过滤 |
| `KlineFetcher` | `extra_stock_{date}.parquet`, `extra_index_{date}.parquet`, `extra_etf_{date}.parquet` | 每日独立文件 |
| `MarginFetcher` | `margin_summary_history.parquet`, `margin_detail_history.parquet` | 客户端增量过滤 |
| `MonthlyCleanup` | `extra_{type}_history.parquet` | 合并月内日文件 |

## 关键约定

- 下游依赖现有文件名和 schema，修改输出结构前要先确认兼容性
- 所有 Parquet 输出统一走 `write_parquet(...)`
- AmazingData SDK 返回值统一通过 `ensure_dataframe()` 处理
- 复权因子使用宽表转长表后全量覆写，不能按普通增量逻辑处理
- 行业成分使用行业 `INDEX_CODE`，不是股票代码
- 指数成分使用 `EXTRA_INDEX_A_SH_SZ` 代码列表
- 指数权重按单个指数逐个请求，避免批量调用触发 SDK 异常
- `KlineFetcher` 会将 `kline_time` 统一为 `datetime64[ns]`，并将 `amount` 转为 `int64`
- 日 K 按日写文件，历史汇总由 `MonthlyCleanup` 合并
- 新增 Fetcher 应继承 `BaseFetcher` 并实现 `fetch()` 方法，然后在 `cli.py` 的 `_FETCHERS` 字典中注册

## Airflow DAG

`dags/` 目录中的 DAG 与各 Fetcher 类一一对应，通过 Docker 容器执行 `python -m amazingdata_fetcher fetch <target>` 命令，按计划调度各类数据任务。
