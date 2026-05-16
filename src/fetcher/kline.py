"""KlineFetcher — 拉取每日行情数据（股票/指数/ETF）。

输出文件：
  - ``extra_stock_{date}.parquet`` ← ``query_kline(EXTRA_STOCK_A)`` + backward_factor
  - ``extra_index_{date}.parquet`` ← ``query_kline(EXTRA_INDEX_A_SH_SZ)``
  - ``extra_etf_{date}.parquet``   ← ``query_kline(EXTRA_ETF)``

每天写独立文件，``MonthlyCleanup`` 负责月末合并。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import AmazingData as ad
import pandas as pd
from loguru import logger

from amazingdata_fetcher.sdk_helpers import dict_to_df
from amazingdata_fetcher.writer import write_parquet

from .base import BaseFetcher


def _normalize_kline_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """统一 kline 类型：kline_time → ns，amount → int64。"""
    if pd.api.types.is_datetime64_any_dtype(df["kline_time"]):
        df["kline_time"] = df["kline_time"].astype("datetime64[ns]")
    df["amount"] = df["amount"].astype("int64")
    return df


class KlineFetcher(BaseFetcher):

    _factor_cache_wide: pd.DataFrame | None = None

    def _load_factor_wide(self, code_list: list) -> pd.DataFrame | None:
        """下载复权因子（宽表）并缓存，不做 unstack 避免内存爆炸。"""
        if self._factor_cache_wide is not None:
            return self._factor_cache_wide

        cache = self.cache_path("kline")
        h5_path = Path(cache) / "basedata" / "backward_factor" / "backward_factor.h5"
        is_local = h5_path.exists()
        logger.info(f"加载复权因子（宽表），is_local={is_local}...")
        df_factor = self.bdo.get_backward_factor(code_list, local_path=cache, is_local=is_local)
        if df_factor is None or df_factor.empty:
            self._factor_cache_wide = pd.DataFrame()
            return self._factor_cache_wide

        self._factor_cache_wide = df_factor
        logger.info(f"复权因子缓存完成，宽表 shape: {df_factor.shape}")
        return self._factor_cache_wide

    def _get_factor_for_date(self, code_list: list, trade_date: str) -> pd.DataFrame | None:
        """获取某天的复权因子（长表），从宽表缓存中按行过滤后 melt。"""
        wide = self._load_factor_wide(code_list)
        if wide is None or wide.empty:
            return None

        date_str = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
        ts = pd.Timestamp(date_str)

        if ts not in wide.index:
            return None

        # 取当天一行，melt 为长表
        row = wide.loc[[ts]]  # 保持 DataFrame 形状 (1, N)
        df_long = row.melt(ignore_index=False, var_name="code", value_name="backward_factor")
        df_long = df_long.reset_index()
        df_long.columns = ["kline_time", "code", "backward_factor"]
        df_long["kline_time"] = df_long["kline_time"].astype("datetime64[ns]")
        df_long = df_long.dropna(subset=["backward_factor"])
        return df_long

    def _query_kline(self, code_list: list, begin_date: str, end_date: str | None = None) -> pd.DataFrame | None:
        """调用 query_kline 并返回合并后的 DataFrame。"""
        end_date = end_date or begin_date
        result = self.mdo.query_kline(
            code_list,
            begin_date=int(begin_date),
            end_date=int(end_date),
            period=ad.constant.Period.day.value,
        )
        if not result:
            return None
        return dict_to_df(result) if isinstance(result, dict) else result

    def _fetch_stock_batch(self, begin_date: str, end_date: str, trading_days: list[str]) -> None:
        """按年批量拉取股票 K 线，一次请求获取整段数据后按日拆分写文件。"""
        # 过滤掉已存在的日期
        missing_days = [d for d in trading_days if not (Path(self.output_dir) / f"extra_stock_{d}.parquet").exists()]
        if not missing_days:
            logger.info(f"extra_stock {begin_date}~{end_date} 全部已存在，跳过")
            return

        code_list = self.get_code_list("EXTRA_STOCK_A")
        logger.info(f"批量拉取 extra_stock {begin_date}~{end_date}，缺失 {len(missing_days)} 天，股票数: {len(code_list)}")

        df = self._query_kline(code_list, begin_date, end_date)
        if df is None:
            logger.warning(f"query_kline EXTRA_STOCK_A {begin_date}~{end_date} 返回空")
            return
        logger.info(f"query_kline 返回 {len(df):,} 行")

        # 确保 kline_time 为 datetime 以便按日分组
        if not pd.api.types.is_datetime64_any_dtype(df["kline_time"]):
            df["kline_time"] = pd.to_datetime(df["kline_time"])

        # 按日期分组写文件
        df["_date_str"] = df["kline_time"].dt.strftime("%Y%m%d")
        for trade_date, day_df in df.groupby("_date_str"):
            out_path = Path(self.output_dir) / f"extra_stock_{trade_date}.parquet"
            if out_path.exists():
                continue

            day_df = day_df.drop(columns=["_date_str"])

            # 复权因子
            df_factor = self._get_factor_for_date(code_list, trade_date)
            if df_factor is not None and not df_factor.empty:
                day_df = pd.merge(day_df, df_factor, on=["kline_time", "code"], how="left")
            else:
                day_df = day_df.copy()
                day_df["backward_factor"] = float("nan")

            day_df = _normalize_kline_dtypes(day_df.reset_index(drop=True))
            write_parquet(day_df, self.output_dir, f"extra_stock_{trade_date}.parquet")

        logger.info(f"extra_stock {begin_date}~{end_date} 批量写入完成")

    def _fetch_simple_batch(self, category: str, label: str, begin_date: str, end_date: str) -> None:
        """批量拉取指数或 ETF K 线（无复权因子），一次请求后按日拆分写文件。"""
        code_list = self.get_code_list(category)
        logger.info(f"批量拉取 extra_{label} {begin_date}~{end_date}，代码数: {len(code_list)}")

        df = self._query_kline(code_list, begin_date, end_date)
        if df is None:
            logger.warning(f"query_kline {category} {begin_date}~{end_date} 返回空")
            return
        logger.info(f"query_kline 返回 {len(df):,} 行")

        if not pd.api.types.is_datetime64_any_dtype(df["kline_time"]):
            df["kline_time"] = pd.to_datetime(df["kline_time"])

        df["_date_str"] = df["kline_time"].dt.strftime("%Y%m%d")
        for trade_date, day_df in df.groupby("_date_str"):
            filename = f"extra_{label}_{trade_date}.parquet"
            if (Path(self.output_dir) / filename).exists():
                continue
            day_df = day_df.drop(columns=["_date_str"])
            day_df = _normalize_kline_dtypes(day_df.reset_index(drop=True))
            write_parquet(day_df, self.output_dir, filename)

        logger.info(f"extra_{label} {begin_date}~{end_date} 批量写入完成")

    def _get_trading_days(self, start_date: str, end_date: str) -> list[str]:
        """从交易日历中过滤出 [start_date, end_date] 范围内的交易日。"""
        calendar = self.bdo.get_calendar()
        if isinstance(calendar, pd.DataFrame):
            dates = pd.to_datetime(calendar.iloc[:, 0].astype(str), format="%Y%m%d")
        else:
            dates = pd.to_datetime(pd.Series(calendar).astype(str), format="%Y%m%d")
        mask = (dates >= start_date) & (dates <= end_date)
        return [d.strftime("%Y%m%d") for d in sorted(dates[mask])]

    def _split_into_years(self, start_date: str, end_date: str) -> list[tuple[str, str]]:
        """将日期范围按年拆分为 [(year_start, year_end), ...]。"""
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        segments = []
        for year in range(start_year, end_year + 1):
            seg_start = start_date if year == start_year else f"{year}0101"
            seg_end = end_date if year == end_year else f"{year}1231"
            segments.append((seg_start, seg_end))
        return segments

    def fetch(
        self,
        *,
        ktype: str = "all",
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs,
    ) -> list[str]:
        if start_date:
            end_date = end_date or date.today().strftime("%Y%m%d")
            trading_days = self._get_trading_days(start_date, end_date)
            logger.info(f"批量模式: {start_date} ~ {end_date}，共 {len(trading_days)} 个交易日")
        else:
            trading_days = [trade_date or date.today().strftime("%Y%m%d")]

        targets = list({"stock", "index", "etf"}) if ktype == "all" else [ktype]

        errors: list[str] = []

        # 批量模式：按年分段请求
        if start_date:
            year_segments = self._split_into_years(start_date, end_date)
            for i, (seg_start, seg_end) in enumerate(year_segments, 1):
                logger.info(f"[{i}/{len(year_segments)}] 年段: {seg_start} ~ {seg_end}")
                seg_days = [d for d in trading_days if seg_start <= d <= seg_end]
                for name in targets:
                    try:
                        if name == "stock":
                            self._fetch_stock_batch(seg_start, seg_end, seg_days)
                        elif name == "index":
                            self._fetch_simple_batch("EXTRA_INDEX_A_SH_SZ", "index", seg_start, seg_end)
                        elif name == "etf":
                            self._fetch_simple_batch("EXTRA_ETF", "etf", seg_start, seg_end)
                    except Exception as e:
                        logger.error(f"fetch_kline_{name} {seg_start}~{seg_end} 失败: {type(e).__name__}: {e}")
                        errors.append(f"{name}_{seg_start}_{seg_end}")
        else:
            # 单日模式
            td = trading_days[0]
            for name in targets:
                try:
                    if name == "stock":
                        self._fetch_stock_batch(td, td, [td])
                    elif name == "index":
                        self._fetch_simple_batch("EXTRA_INDEX_A_SH_SZ", "index", td, td)
                    elif name == "etf":
                        self._fetch_simple_batch("EXTRA_ETF", "etf", td, td)
                except Exception as e:
                    logger.error(f"fetch_kline_{name} {td} 失败: {type(e).__name__}: {e}")
                    errors.append(f"{name}_{td}")
        return errors
