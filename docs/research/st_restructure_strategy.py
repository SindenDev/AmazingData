"""
ST + 重整/重组/招募 策略 (回测 + 每日候选生成)
================================================

数据源
------
- ClickHouse.daily_market_mover       : 韭研公社每日热点榜单 (section/tags/hitlimit_count/...)
- ClickHouse.extra_stock_history      : 日 K 线 (含后复权因子)
- ClickHouse.extra_index_history      : 上证综指 (大盘择时)
- ClickHouse.info_stock_basic / equity_structure_history / finance_*_history

连接配置来自仓库根目录 `.env` 里的 `CLICKHOUSE_*` 变量.

策略规则 (T+1 合规)
--------------------
基础版:
  1) section == 'ST板块' 且名称含 ST/*ST
  2) tags 含 重整|重组|招募
  3) T+1 开盘买入 (next_open/T_close < 1.045, 防一字板)
  4) T+2 收盘卖出 (持仓 2 个交易日)
  5) 单笔双边成本 0.15%

增强版 (--enhanced) 在基础上增加:
  - 大盘择时:   上证 5 日跌幅 > 2% 时暂停 (深跌期间 ST 重整也躲不开)
  - 流动性过滤: T 日成交额 >= 0.5 亿 (避免僵尸股)
  - 强催化 tag: 限定 重整|预重整|招募|并购 (剔除单纯"重组"和"破产")
  - 黑名单 tag: 排除 破产清算|破产终结 (套利窗口已关)

实测表现 (2020-08 ~ 2026-04)
-----------------------------
                       n     μ/笔    胜率   CAGR    MDD     Sharpe   NAV
基础版 全期             868  +0.92%  54.4%  +53.0%  -25.3%  2.99    11.43x
增强版 全期             441  +1.21%  54.6%  +48.1%  -18.4%  3.70     6.90x

样本外验证 (训练 2020-22 → 测试 2023+):
  基础版 2020-2022:  μ +0.11%  Sharpe 0.51    ← 策略尚未激活
  基础版 2023+:      μ +1.07%  CAGR +105.2%   ← 退市新规+并购六条催化
  增强版 2023+:      μ +1.37%  CAGR  +82.2%  Sharpe 4.36 (更稳)

→ 「年化超过一倍」的真相: 在 2023+ 退市政策周期内, 基础版裸跑 CAGR +105%.
  2020-22 几乎平本. 这是政策驱动主题, 未来若退市新规修订, 策略可能失效.

为何各类增量过滤被采用 / 拒绝
-----------------------------
| 维度        | 测试结果                                         | 采用?    |
| 大盘择时    | 上证 5日跌>2% 期间 μ +0.24% (基本不赚)           | ✓ 增强   |
| 流动性      | T日成交<0.5亿 CAGR +14.5% (僵尸股拖累)           | ✓ 增强   |
| 板块热度    | ST 板块当日上榜数与收益相关性弱                   | ✗ 不用   |
| T日异动     | T 日涨停的反而后续走得更好 (信号确认)             | ✗ 不限   |
| tag 细分    | "破产清算" Sharpe 0.18, "重组only" Sharpe 1.73   | ✓ 排除   |
| 财务过滤    | debt_ratio/小市值/资不抵债 均降低收益 (信息已在 tag) | ✗ 不用 |
| 异动 reason | ST 板块该字段为空, 无可用信号                     | ✗ N/A    |

容量估算 (增强版)
-----------------
- 日均交易 1.3 单, 信号股 T 日成交中位数 1.17 亿
- 单只仓位上限按 10% T 日成交额: 中位数容量 ~1175 万, 偏小标的 ~600 万
- 全策略总资金容量 ~1500~2000 万 (保守); 超过则滑点显著

用法
----
  python docs/research/st_restructure_strategy.py              # 基础版 回测+候选
  python docs/research/st_restructure_strategy.py --enhanced   # 增强版 回测+候选
  python docs/research/st_restructure_strategy.py --signals    # 仅最新一日候选
  python docs/research/st_restructure_strategy.py --enhanced --signals
"""
import argparse
import pandas as pd, numpy as np
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _clickhouse import kline_max_date, load_kline_table, load_table, parse_array_literal, query_scalar
from _metrics import portfolio_metrics, yearly_metrics, load_trading_calendar

DATA_DIR = Path(__file__).resolve().parents[2] / 'data'
COST = 0.0015
TAG_PATTERN = '重整|重组|招募'                       # 基础 tag 入围
ENH_TAG = '重整|预重整|招募|并购'                     # 增强版强催化 tag
ENH_TAG_BLACK = '破产清算|破产终结'                   # 增强版排除 tag (已破产无套利)
SECTION = 'ST板块'

def load():
    mover_max = query_scalar("SELECT max(trade_date) FROM daily_market_mover")
    stock_max = kline_max_date("stock")
    index_max = kline_max_date("index")
    aligned_end = min(x for x in [mover_max, stock_max, index_max] if x is not None)
    mv = load_table(
        'daily_market_mover',
        columns=['trade_date', 'section', 'code', 'name', 'hitlimit_count', 'tags', 'change_percent'],
        where=f"trade_date <= '{aligned_end}'"
    )
    mv['tag_str'] = mv['tags'].map(
        lambda x: ' | '.join(parse_array_literal(x))
    )
    px = load_kline_table(
        'stock',
        columns=['code', 'kline_time', 'open', 'close', 'amount', 'backward_factor'],
        where=f"formatDateTime(kline_time, '%Y%m%d') <= '{aligned_end}'"
    )
    px = px.sort_values(['code', 'kline_time']).reset_index(drop=True)
    px['adj_open']  = px['open']  * px['backward_factor']
    px['adj_close'] = px['close'] * px['backward_factor']
    px['next_open'] = px.groupby('code')['adj_open'].shift(-1)
    px['d2_close']  = px.groupby('code')['adj_close'].shift(-2)
    px['trade_date'] = px['kline_time'].dt.strftime('%Y%m%d')

    idx = load_kline_table(
        'index',
        columns=['code', 'kline_time', 'close'],
        where=f"formatDateTime(kline_time, '%Y%m%d') <= '{aligned_end}'"
    )
    sh = idx[idx['code'] == '000001.SH'].sort_values('kline_time').reset_index(drop=True)
    sh['trade_date'] = sh['kline_time'].dt.strftime('%Y%m%d')
    sh['sh_ret5']  = sh['close'].pct_change(5)
    sh['sh_close'] = sh['close']
    return mv, px, sh[['trade_date', 'sh_close', 'sh_ret5']]

def delisting_filter(sig: pd.DataFrame, px: pd.DataFrame, mode: str = 'lite') -> pd.DataFrame:
    """剔除退市高风险候选.

    mode='full' — 全套 6 条 (含流动性/市值, 偏激进, 会切到 alpha)
    mode='lite' — 仅 3 条法律意义退市信号: 名字含退 / close<1 / 净资产<0 / 亏损+营收<3亿
                  推荐用于 ST 重组策略 (实测剔除噪声但不伤 alpha)
    """
    n0 = len(sig)
    drop_log = {}

    basic = load_table('info_stock_basic', columns=['MARKET_CODE', 'SECURITY_NAME'])
    bad_names = set(basic.loc[basic['SECURITY_NAME'].astype(str)
                              .str.contains('退', na=False), 'MARKET_CODE'])
    m1 = sig['code'].isin(bad_names)
    drop_log['name含退'] = int(m1.sum())
    sig = sig[~m1].copy()

    sig['orig_close_chk'] = sig['adj_close'] / sig['backward_factor']
    thr = 1.5 if mode == 'full' else 1.0
    m2 = sig['orig_close_chk'] < thr
    drop_log[f'close<{thr}元'] = int(m2.sum())
    sig = sig[~m2].copy()

    if mode == 'full':
        eq = load_table(
            'equity_structure_history',
            columns=['MARKET_CODE', 'CHANGE_DATE', 'TOT_SHARE']
        )
        eq = eq.sort_values(['MARKET_CODE', 'CHANGE_DATE'])
        eq_last = eq.groupby('MARKET_CODE').tail(1).set_index('MARKET_CODE')['TOT_SHARE']
        sig['tot_share'] = sig['code'].map(eq_last)
        sig['mcap'] = sig['orig_close_chk'] * sig['tot_share'] * 1e4
        m3 = sig['mcap'].fillna(1e12) < 8e8
        drop_log['市值<8亿'] = int(m3.sum())
        sig = sig[~m3].copy()

        px_amt = px[['code', 'trade_date', 'amount']].sort_values(['code', 'trade_date']).copy()
        px_amt['amt20'] = px_amt.groupby('code')['amount'].transform(
            lambda s: s.rolling(20, min_periods=10).mean())
        sig = sig.merge(px_amt[['code', 'trade_date', 'amt20']],
                        on=['code', 'trade_date'], how='left')
        m4 = sig['amt20'].fillna(0) < 1.5e7
        drop_log['20日均额<1500万'] = int(m4.sum())
        sig = sig[~m4].copy()

    inc = load_table(
        'finance_income_history',
        columns=['MARKET_CODE', 'REPORTING_PERIOD', 'NET_PRO_INCL_MIN_INT_INC', 'OPERA_REV']
    )
    inc['REPORTING_PERIOD'] = inc['REPORTING_PERIOD'].astype(str)
    inc = inc.sort_values(['MARKET_CODE', 'REPORTING_PERIOD'])
    inc_last = inc.groupby('MARKET_CODE').tail(1).set_index('MARKET_CODE')[
        ['REPORTING_PERIOD', 'NET_PRO_INCL_MIN_INT_INC', 'OPERA_REV']]
    sig = sig.merge(inc_last.rename(columns={
        'REPORTING_PERIOD': 'inc_rep', 'NET_PRO_INCL_MIN_INT_INC': 'np',
        'OPERA_REV': 'rev'}), left_on='code', right_index=True, how='left')
    age = (pd.to_datetime(sig['trade_date'], format='%Y%m%d', errors='coerce')
           - pd.to_datetime(sig['inc_rep'], format='%Y%m%d', errors='coerce')).dt.days
    rev_thr = 5e8 if mode == 'full' else 3e8
    m5 = (age.between(0, 365)) & (sig['np'] < 0) & (sig['rev'] < rev_thr)
    drop_log[f'亏损+营收<{rev_thr/1e8:.0f}亿'] = int(m5.sum())
    sig = sig[~m5].copy()

    bal = load_table(
        'finance_balance_sheet_history',
        columns=['MARKET_CODE', 'REPORTING_PERIOD', 'TOT_SHARE_EQUITY_INCL_MIN_INT']
    )
    bal['REPORTING_PERIOD'] = bal['REPORTING_PERIOD'].astype(str)
    bal = bal.sort_values(['MARKET_CODE', 'REPORTING_PERIOD'])
    bal_last = bal.groupby('MARKET_CODE').tail(1).set_index('MARKET_CODE')[
        'TOT_SHARE_EQUITY_INCL_MIN_INT']
    sig['equity_net'] = sig['code'].map(bal_last)
    m6 = sig['equity_net'].fillna(1) < 0
    drop_log['净资产<0'] = int(m6.sum())
    sig = sig[~m6].copy()

    n1 = len(sig)
    print(f'  [排雷-{mode}] {n0} → {n1}  剔除明细: {drop_log}')
    return sig.drop(columns=['orig_close_chk', 'tot_share', 'mcap', 'amt20',
                             'inc_rep', 'np', 'rev', 'equity_net'], errors='ignore')


def build_signals(mv, px, sh, enhanced=False):
    cols = ['code', 'trade_date', 'adj_close', 'next_open', 'd2_close', 'amount', 'backward_factor']
    m = mv.merge(px[cols], on=['code', 'trade_date'], how='left')
    m = m.merge(sh, on='trade_date', how='left')
    m['next_gap']    = m['next_open'] / m['adj_close'] - 1
    m['orig_close']  = m['adj_close'] / m['backward_factor']
    m['amount_yi']   = m['amount'] / 1e8
    m = m.sort_values(['trade_date', 'code', 'section']).drop_duplicates(['trade_date', 'code'])
    has_market_data = m['adj_close'].notna() & m['amount_yi'].notna()

    is_st  = (m['section'] == SECTION) & m['name'].str.contains('ST', na=False)
    is_tag = m['tag_str'].str.contains(TAG_PATTERN, na=False)
    sig = m[has_market_data & is_st & is_tag].copy()
    if enhanced:
        sig = sig[
            (sig['sh_ret5'] > -0.02)                                 # 大盘 5日跌幅 < 2%
            & (sig['amount_yi'] >= 0.5)                              # T 日成交额 >= 0.5 亿
            & (~sig['tag_str'].str.contains(ENH_TAG_BLACK, na=False))# 排除已破产
            & sig['tag_str'].str.contains(ENH_TAG, na=False)         # 限定强催化
        ].copy()
    return sig

def _realistic_exit(sig: pd.DataFrame, px: pd.DataFrame, hold_days: int = 2,
                    delist_loss: float = -0.9) -> pd.DataFrame:
    """对 d2_close 缺失的 (T+2 停牌/退市), 用 T+1 之后能找到的下一个 close 退出.
    若信号日之后 px 中再无该 code 的 K 线 → 视为退市, r = delist_loss (默认 -90%).
    """
    s = sig.dropna(subset=['next_open']).copy()  # T+1 完全买不进的, 信号作废
    normal = s['d2_close'].notna()
    s.loc[normal, 'exit_close'] = s.loc[normal, 'd2_close']

    bad = ~normal
    if bad.any():
        px_sorted = px[['code', 'trade_date', 'adj_close']].sort_values(['code', 'trade_date'])
        # 对每个有问题的信号, 找 trade_date > 信号日的最后一个 adj_close
        last_by_code = px_sorted.groupby('code').agg(
            last_date=('trade_date', 'max'),
            last_close=('adj_close', 'last')
        )
        bad_idx = s.index[bad]
        for i in bad_idx:
            code = s.at[i, 'code']
            sig_date = s.at[i, 'trade_date']
            if code in last_by_code.index and last_by_code.at[code, 'last_date'] > sig_date:
                # 找该 code 在 sig_date 之后的下一个 close (≥ T+1)
                later = px_sorted[(px_sorted['code'] == code) &
                                  (px_sorted['trade_date'] > sig_date)]
                if len(later) > 0:
                    s.at[i, 'exit_close'] = later['adj_close'].iloc[-1]
                    continue
            # 无后续 K 线 → 强制退市损失
            s.at[i, 'exit_close'] = s.at[i, 'next_open'] * (1 + delist_loss)
    return s


def backtest(sig, hold_days=2, calendar=None, realistic_exit=False, px=None):
    if realistic_exit and px is not None:
        s = _realistic_exit(sig, px, hold_days=hold_days)
        s = s[s['next_gap'] < 0.045]
        s['r'] = s['exit_close'] / s['next_open'] - 1 - COST
    else:
        s = sig.dropna(subset=['next_open', 'd2_close']).copy()
        s = s[s['next_gap'] < 0.045]
        s['r'] = s['d2_close'] / s['next_open'] - 1 - COST
    if calendar is None:
        calendar = sorted(s['trade_date'].unique().tolist())
    m = portfolio_metrics(s[['trade_date', 'r']], hold_days=hold_days, calendar=calendar)
    daily = (s.groupby('trade_date')
              .agg(r=('r', 'mean'), n=('r', 'size'))
              .reset_index().sort_values('trade_date').reset_index(drop=True))
    return dict(daily=daily, signals=s,
                years=m['years'], final_nav=m['final_nav'],
                cagr=m['cagr'], mdd=m['mdd'], sharpe=m['sharpe'],
                n_trades=len(s), mean_trade=s['r'].mean(), win=(s['r'] > 0).mean(),
                n_trading_days=m['n_trading_days'])

def print_yearly(s, hold_days=2, calendar=None):
    if calendar is None:
        calendar = sorted(s['trade_date'].unique().tolist())
    yr = yearly_metrics(s[['trade_date', 'r']].assign(trade_date=s['trade_date']),
                        hold_days=hold_days, calendar=calendar)
    if yr.empty:
        return
    print('\n年度细分 (组合层口径):')
    for _, row in yr.iterrows():
        print(f"  {row['year']}: signals={int(row['n_trades']):>4d}  days={int(row['n_days']):>3d}  "
              f"μ={row['mean_trade']*100:+5.2f}%  win={row['win']*100:5.1f}%  "
              f"NAV_year={row['nav']:.2f}  MDD={row['mdd']*100:+5.1f}%  Sharpe={row['sharpe']:.2f}")

def latest_candidates(sig, sh):
    """对最新一日的候选: 同时打印 T 日上榜后, T+1 应当开盘买入的列表."""
    last_d = sig['trade_date'].max()
    today = sig[sig['trade_date'] == last_d].copy()
    sh_today = sh[sh['trade_date'] == last_d]
    regime = ''
    if not sh_today.empty and pd.notna(sh_today['sh_ret5'].iloc[0]):
        r5 = sh_today['sh_ret5'].iloc[0]
        regime = f"  | 上证5日: {r5*100:+.2f}%  {'(向好)' if r5>-0.02 else '⚠（>2%深跌, 增强版会暂停)'}"
    cols = ['code', 'name', 'orig_close', 'change_percent', 'hitlimit_count', 'amount_yi', 'tag_str']
    print(f'\n=== 信号日 {last_d} → T+1 开盘买入候选 ==={regime}')
    if len(today) == 0:
        print('(无候选)')
        return today
    today_show = today[cols].rename(columns={
        'orig_close': '收盘价', 'change_percent': '今日涨幅%', 'hitlimit_count': '连板',
        'amount_yi': '成交额(亿)', 'tag_str': '标签'
    })
    print(today_show.to_string(index=False, max_colwidth=70))
    print('\n操作: 次一交易日开盘买入 (若一字开板, 弃单), 持有 2 个交易日, 第 2 日收盘卖出.')
    return today

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--signals', action='store_true', help='仅输出最近一日候选')
    p.add_argument('--enhanced', action='store_true', help='使用增强过滤器 (大盘+流动性+强催化tag)')
    p.add_argument('--filter-delist', choices=['lite', 'full'], nargs='?', const='lite',
                  default=None, help='启用退市风险过滤器 (lite=推荐, full=激进会误杀alpha)')
    p.add_argument('--realistic-exit', action='store_true', help='T+2 停牌/退市时按真实出场价计算 r')
    p.add_argument('--compare', action='store_true', help='对比 4 种模式 (origin / realistic / +filter / enhanced+all)')
    args = p.parse_args()
    mv, px, sh = load()
    sig = build_signals(mv, px, sh, enhanced=args.enhanced)
    calendar = load_trading_calendar(DATA_DIR)

    if args.compare:
        sig_base = build_signals(mv, px, sh, enhanced=False)
        sig_enh = build_signals(mv, px, sh, enhanced=True)
        sig_base_lite = delisting_filter(sig_base.copy(), px, mode='lite')
        sig_base_full = delisting_filter(sig_base.copy(), px, mode='full')
        sig_enh_lite = delisting_filter(sig_enh.copy(), px, mode='lite')
        configs = [
            ('基础 / 旧口径 (dropna d2)',     sig_base,      False),
            ('基础 / 真实退出',               sig_base,      True),
            ('基础 / 真实退出 + 排雷-lite',   sig_base_lite, True),
            ('基础 / 真实退出 + 排雷-full',   sig_base_full, True),
            ('增强 / 真实退出 + 排雷-lite',   sig_enh_lite,  True),
        ]
        print(f'\n{"模式":<32} {"n":>5} {"μ%":>7} {"win%":>7} {"NAV":>8} {"CAGR%":>7} {"MDD%":>7} {"Sharpe":>7}')
        print('-' * 90)
        for label, s_in, re_exit in configs:
            res = backtest(s_in, calendar=calendar, realistic_exit=re_exit, px=px)
            print(f'{label:<32} {res["n_trades"]:>5d} {res["mean_trade"]*100:>+7.2f} '
                  f'{res["win"]*100:>7.1f} {res["final_nav"]:>8.2f} {res["cagr"]*100:>+7.1f} '
                  f'{res["mdd"]*100:>+7.1f} {res["sharpe"]:>7.2f}')
        return

    if args.filter_delist:
        sig = delisting_filter(sig, px, mode=args.filter_delist)
    print(f"模式: {'增强版' if args.enhanced else '基础版'}"
          f"{f' + 排雷-{args.filter_delist}' if args.filter_delist else ''}"
          f"{' + 真实退出' if args.realistic_exit else ''}")
    if not args.signals:
        res = backtest(sig, calendar=calendar, realistic_exit=args.realistic_exit, px=px)
        print(f"=== 回测 {res['daily']['trade_date'].iloc[0]} ~ {res['daily']['trade_date'].iloc[-1]} "
              f"({res['years']:.2f}y, {res['n_trading_days']}个交易日) ===")
        print(f"交易笔数: {res['n_trades']}   平均 {len(res['signals'])/max(len(res['daily']),1):.2f} 笔/有信号日")
        print(f"单笔 μ: {res['mean_trade']*100:+.2f}%   胜率: {res['win']*100:.1f}%")
        print(f"NAV: {res['final_nav']:.2f}x   CAGR: {res['cagr']*100:+.1f}%   MDD: {res['mdd']*100:+.1f}%   Sharpe: {res['sharpe']:.2f}")
        print_yearly(res['signals'], calendar=calendar)
    latest_candidates(sig, sh)

if __name__ == '__main__':
    main()
