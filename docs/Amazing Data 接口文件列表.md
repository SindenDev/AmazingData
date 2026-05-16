# Amazing Data 接口文件列表

> 基于 AmazingData SDK V1.0.24 开发手册整理，涵盖 SDK 全部数据接口。

---

## 接口覆盖率

| 类别 | 总数 | 已接入 | 覆盖率 |
| --- | ---: | ---: | ---: |
| 基础接口（登录/登出） | 3 | — | 基础设施 |
| 批量数据接口（3.5.2–3.5.15） | 54 | 15 | 28% |
| 实时行情订阅（3.5.3） | 9 | 0 | 0% |
| 金融算子（3.6） | 4 类 | — | 工具库 |

已接入的 15 个数据接口覆盖了 A 股研究最核心的维度：基础信息、财务三表、K 线行情、股本分红、融资融券、交易所指数、行业指数。

未接入的 39 个批量接口主要分布在：可转债（11）、期权（3）、ETF（4）、股东数据（2）、交易异动（2）、业绩快报/预告（2）等。9 个实时行情订阅接口需要流式架构支持，暂不在批量采集范围内。

---

## 本项目已接入的数据接口

以下表格列出本项目当前通过 `amazingdata_fetcher` 采集的数据，包含文件命名、获取策略、SDK 接口、Iceberg 表映射及同步模式。

数据获取后保存在 `$OUTPUT_DIR`（默认 `./data`）目录。当 `SYNC_ENABLED=true` 时，写入本地后自动上传到 `s3://amazingdata/parquet/{filename}` 并注册为 Iceberg 表。

| 文件名称 | 文件内容 | 策略 | AD 接口 | SDK 类 | Fetcher 类 | Iceberg 表 | 同步模式 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `info_stock_basic.parquet` | 证券基础信息 | 增量（新代码） | `get_stock_basic` | `InfoData` | `StockInfoFetcher` | `amazingdata.info_stock_basic` | overwrite |
| `info_stock_factor.parquet` | 后复权因子 | 全量覆写 | `get_backward_factor` | `BaseData` | `StockInfoFetcher` | `amazingdata.info_stock_factor` | overwrite |
| `info_index_detail_history.parquet` | 交易所指数成分股 | 按日期增量 | `get_index_constituent` | `InfoData` | `IndexInfoFetcher` | `amazingdata.info_index_detail_history` | overwrite |
| `info_index_weight_history.parquet` | 交易所指数权重 | 按日期增量 | `get_index_weight` | `InfoData` | `IndexInfoFetcher` | `amazingdata.info_index_weight_history` | overwrite |
| `info_industry_basic_history.parquet` | 行业指数基本信息 | 全量覆写 | `get_industry_base_info` | `InfoData` | `IndustryInfoFetcher` | `amazingdata.info_industry_basic_history` | overwrite |
| `info_industry_detail_history.parquet` | 行业指数成分股 | 按日期增量 | `get_industry_constituent` | `InfoData` | `IndustryInfoFetcher` | `amazingdata.info_industry_detail_history` | overwrite |
| `equity_structure_history.parquet` | 股本结构 | 全量覆写 | `get_equity_structure` | `InfoData` | `EquityFetcher` | `amazingdata.equity_structure_history` | overwrite |
| `equity_dividend_history.parquet` | 分红数据 | 全量覆写 | `get_dividend` | `InfoData` | `EquityFetcher` | `amazingdata.equity_dividend_history` | overwrite |
| `finance_balance_sheet_history.parquet` | 资产负债表 | 客户端增量 | `get_balance_sheet` | `InfoData` | `FinanceFetcher` | `amazingdata.finance_balance_sheet_history` | overwrite |
| `finance_cash_flow_history.parquet` | 现金流量表 | 客户端增量 | `get_cash_flow` | `InfoData` | `FinanceFetcher` | `amazingdata.finance_cash_flow_history` | overwrite |
| `finance_income_history.parquet` | 利润表 | 客户端增量 | `get_income` | `InfoData` | `FinanceFetcher` | `amazingdata.finance_income_history` | overwrite |
| `margin_summary_history.parquet` | 融资融券汇总 | 客户端增量 | `get_margin_summary` | `InfoData` | `MarginFetcher` | `amazingdata.margin_summary_history` | overwrite |
| `margin_detail_history.parquet` | 融资融券明细 | 客户端增量 | `get_margin_detail` | `InfoData` | `MarginFetcher` | `amazingdata.margin_detail_history` | overwrite |
| `extra_stock_{date}.parquet` | 股票日 K 线 | 每日独立文件 | `query_kline` | `MarketData` | `KlineFetcher` | `amazingdata.extra_stock_daily` | append |
| `extra_index_{date}.parquet` | 指数日 K 线 | 每日独立文件 | `query_kline` | `MarketData` | `KlineFetcher` | `amazingdata.extra_index_daily` | append |
| `extra_etf_{date}.parquet` | ETF 日 K 线 | 每日独立文件 | `query_kline` | `MarketData` | `KlineFetcher` | `amazingdata.extra_etf_daily` | append |

### 月度数据清理

股票价格、指数、ETF 数据为每日增量获取。每月 2 日由 `MonthlyCleanup` 将上月日文件合并为历史文件（`extra_stock_history.parquet`、`extra_index_history.parquet`、`extra_etf_history.parquet`），并删除已合并的日文件。合并后的历史文件对应 Iceberg 表 `amazingdata.extra_{type}_history`，同步模式为 overwrite。

### 基础设施接口

以下接口不直接产出数据文件，但在 Fetcher 内部被使用：

| 接口名称 | SDK 类 | 用途 |
| --- | --- | --- |
| `get_code_list` | `BaseData` | 所有 Fetcher 通过 `BaseFetcher.get_code_list()` 获取证券代码列表 |
| `get_calendar` | `BaseData` | `MarketData` 初始化依赖；`StockInfoFetcher` 用于验证复权因子日期 |
| `get_backward_factor` | `BaseData` | `KlineFetcher` 拉取股票 K 线时关联后复权因子 |

---

## AmazingData SDK 完整接口列表

以下为 AmazingData SDK V1.0.24 提供的全部数据接口，按开发手册章节分类。

状态标记：✅ 已接入 · ⚙️ 内部使用 · ❌ 未接入

### 3.5.1 基础接口

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ⚙️ | `login` | 登录认证 | — |
| ⚙️ | `logout` | 登出 | — |
| ❌ | `update_password` | 修改密码 | — |

### 3.5.2 基础数据

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ❌ | `get_code_info` | 获取证券代码信息 | `BaseData` |
| ⚙️ | `get_code_list` | 获取证券代码列表 | `BaseData` |
| ❌ | `get_future_code_list` | 获取期货代码列表 | `BaseData` |
| ❌ | `get_option_code_list` | 获取期权代码列表 | `BaseData` |
| ✅ | `get_backward_factor` | 获取后复权因子 | `BaseData` |
| ❌ | `get_adj_factor` | 获取复权因子 | `BaseData` |
| ❌ | `get_hist_code_list` | 获取历史证券代码列表 | `BaseData` |
| ⚙️ | `get_calendar` | 获取交易日历 | `BaseData` |
| ✅ | `get_stock_basic` | 获取证券基础信息 | `BaseData` |
| ❌ | `get_history_stock_status` | 获取历史证券状态 | `BaseData` |
| ❌ | `get_bj_code_mapping` | 获取北交所代码映射 | `BaseData` |

### 3.5.3 实时行情数据

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ❌ | `onSnapshotindex` | 订阅指数实时快照 | `SubscribeData` |
| ❌ | `onSnapshot` | 订阅股票实时快照 | `SubscribeData` |
| ❌ | `onSnapshotglra` | 订阅港股通实时快照 | `SubscribeData` |
| ❌ | `onSnapshotfuture` | 订阅期货实时快照 | `SubscribeData` |
| ❌ | `onSnapshotetf` | 订阅 ETF 实时快照 | `SubscribeData` |
| ❌ | `onSnapshotkzz` | 订阅可转债实时快照 | `SubscribeData` |
| ❌ | `onSnapshothkt` | 订阅沪港通实时快照 | `SubscribeData` |
| ❌ | `onSnapshotoption` | 订阅期权实时快照 | `SubscribeData` |
| ❌ | `OnKLine` | 订阅实时 K 线 | `SubscribeData` |

> 实时行情订阅接口需要流式架构支持，暂不在批量采集范围内。

### 3.5.4 历史行情数据

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ❌ | `query_snapshot` | 查询历史快照 | `MarketData` |
| ✅ | `query_kline` | 查询历史 K 线 | `MarketData` |

### 3.5.5 财务数据

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ✅ | `get_balance_sheet` | 获取资产负债表 | `InfoData` |
| ✅ | `get_cash_flow` | 获取现金流量表 | `InfoData` |
| ✅ | `get_income` | 获取利润表 | `InfoData` |
| ❌ | `get_profit_express` | 获取业绩快报 | `InfoData` |
| ❌ | `get_profit_notice` | 获取业绩预告 | `InfoData` |

### 3.5.6 股东股本数据

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ❌ | `get_share_holder` | 获取十大股东 | `InfoData` |
| ❌ | `get_holder_num` | 获取股东户数 | `InfoData` |
| ✅ | `get_equity_structure` | 获取股本结构 | `InfoData` |
| ❌ | `get_equity_pledge_freeze` | 获取股权质押冻结 | `InfoData` |
| ❌ | `get_equity_restricted` | 获取限售解禁 | `InfoData` |

### 3.5.7 股东权益数据

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ✅ | `get_dividend` | 获取分红送转 | `InfoData` |
| ❌ | `get_right_issue` | 获取配股数据 | `InfoData` |

### 3.5.8 融资融券数据

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ✅ | `get_margin_summary` | 获取融资融券汇总 | `InfoData` |
| ✅ | `get_margin_detail` | 获取融资融券明细 | `InfoData` |

### 3.5.9 交易异动数据

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ❌ | `get_long_hu_bang` | 获取龙虎榜数据 | `InfoData` |
| ❌ | `get_block_trading` | 获取大宗交易数据 | `InfoData` |

### 3.5.10 期权数据

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ❌ | `get_option_basic_info` | 获取期权基本信息 | `InfoData` |
| ❌ | `get_option_std_ctr_specs` | 获取期权标准合约规格 | `InfoData` |
| ❌ | `get_option_mon_ctr_specs` | 获取期权月合约规格 | `InfoData` |

### 3.5.11 ETF 数据

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ❌ | `get_etf_pcf` | 获取 ETF 申赎清单（PCF） | `InfoData` |
| ❌ | `get_fund_share` | 获取基金份额 | `InfoData` |
| ❌ | `get_fund_nav` | 获取基金净值 | `InfoData` |
| ❌ | `get_fund_iopv` | 获取基金 IOPV | `InfoData` |

### 3.5.12 交易所指数数据

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ✅ | `get_index_constituent` | 获取指数成分股 | `InfoData` |
| ✅ | `get_index_weight` | 获取指数成分股权重 | `InfoData` |

### 3.5.13 行业指数数据

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ✅ | `get_industry_base_info` | 获取行业基本信息 | `InfoData` |
| ✅ | `get_industry_constituent` | 获取行业成分股 | `InfoData` |
| ❌ | `get_industry_weight` | 获取行业成分股权重 | `InfoData` |
| ❌ | `get_industry_daily` | 获取行业日行情 | `InfoData` |

### 3.5.14 可转债数据

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ❌ | `get_kzz_issuance` | 获取可转债发行信息 | `InfoData` |
| ❌ | `get_kzz_share` | 获取可转债股份变动 | `InfoData` |
| ❌ | `get_kzz_conv` | 获取可转债转股信息 | `InfoData` |
| ❌ | `get_kzz_conv_change` | 获取可转债转股价变动 | `InfoData` |
| ❌ | `get_kzz_corr` | 获取可转债修正信息 | `InfoData` |
| ❌ | `get_kzz_call` | 获取可转债赎回信息 | `InfoData` |
| ❌ | `get_kzz_put` | 获取可转债回售信息 | `InfoData` |
| ❌ | `get_kzz_put_call_item` | 获取可转债回售赎回条款 | `InfoData` |
| ❌ | `get_kzz_put_explanation` | 获取可转债回售说明 | `InfoData` |
| ❌ | `get_kzz_call_explanation` | 获取可转债赎回说明 | `InfoData` |
| ❌ | `get_kzz_suspend` | 获取可转债停牌信息 | `InfoData` |

### 3.5.15 国债收益率数据

| 状态 | 接口名称 | 说明 | SDK 类 |
| :---: | --- | --- | --- |
| ❌ | `get_treasury_yield` | 获取国债收益率 | `InfoData` |

### 3.6 金融算子

SDK 还提供四类金融算子，用于量化计算（非数据拉取接口）：

| 算子类 | 说明 | 示例函数 |
| --- | --- | --- |
| `MathFunction` | 数学函数 | `abs`, `log`, `max`, `min`, `round`, `sign`, `sqrt` 等 |
| `StatisticsFunction` | 统计函数 | `mean`, `std`, `var`, `corr`, `cov`, `skew`, `kurt` 等 |
| `TimeSeriesFunction` | 时间序列函数 | `ts_mean`, `ts_std`, `ts_max`, `ts_min`, `ts_rank`, `ts_delay`, `ts_delta`, `ts_sum`, `ts_corr`, `ts_cov` 等 |
| `CrossSectionFunction` | 截面函数 | `cs_rank`, `cs_zscore`, `cs_demean`, `cs_winsorize` 等 |

---

## 数据同步架构

本地 Parquet 文件通过三条路径同步到远端存储：

1. **S3 上传** — 文件原样上传到 `s3://amazingdata/parquet/{filename}`（via boto3）
2. **Iceberg 表注册** — DataFrame 写入 Apache Iceberg 表（via PyIceberg SqlCatalog）
3. **ClickHouse 写入** — DataFrame 写入 ClickHouse MergeTree 表（via clickhouse-driver）

同步触发方式：
- **自动同步**：`SYNC_ENABLED=true` 时，`write_parquet()` 写完本地文件后自动触发所有已配置目标的同步
- **手动同步**：`python -m amazingdata_fetcher sync` 命令批量或按文件同步

Iceberg Catalog 配置：
- Catalog 类型：`SqlCatalog`（SQLite 存储在 `$OUTPUT_DIR/.iceberg_catalog.db`）
- Warehouse：`s3://amazingdata/iceberg/warehouse/`
- Namespace：`amazingdata`

同步模式规则：
- 日 K 文件（`extra_{type}_{date}.parquet`）→ **append**（追加到对应的 `_daily` 表）
- 其他所有文件 → **overwrite**（全量覆写对应表）
