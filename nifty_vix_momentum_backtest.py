
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Tuple, Optional, List, Dict

# ==============================================
# Momentum Intraday Backtest for NIFTY + VIX
# - Timeframes: 5m / 15m / 60m
# - Momentum rule: EMA(20) > EMA(50) + positive slope + ROC(5) filter (LONG)
#                  EMA(20) < EMA(50) + negative slope + ROC(5) filter (SHORT)
# - Risk management: SL = 1.0 * ATR(14); TP = 2.0 * ATR(14)  (=> 1:2 RR)
# - Sequential trades only (max 3/day, one at a time)
# - Flat by 15:25 IST
# - VIX bucketed output: <12, 12-15, >15 and ALL
# - Outputs mirrored to user’s requested summary format
# ==============================================

# ---------------- Config ----------------
#Give me Option 1 (confirmation + intrabar trigger) is the cleanest balance:

#Make minimal code change only, dont touch entire code structure etc

SESSION_START = "09:15"
SESSION_END   = "15:30"
FORCE_FLAT_AT = "15:25"
MAX_TRADES_PER_DAY = 3
ATR_PERIOD = 14
EMA_FAST = 20 # 20/50 EMA results are little better in terms of DD
EMA_SLOW = 50
ROC_PERIOD = 5
RISK_MULT = 1.0            # SL = RISK_MULT * ATR
TARGET_R_MULT = 2.0        # TP = TARGET_R_MULT * Risk  (1:2 RR)
SKIP_IF_VIX_LT_12 = False   # trade filter for very low vol
BASE_CAPITAL = 100000.0
# ---------------------------------------

def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    return df

def in_session(df: pd.DataFrame) -> pd.DataFrame:
    s_h, s_m = map(int, SESSION_START.split(':'))
    e_h, e_m = map(int, SESSION_END.split(':'))
    m = ((df['date'].dt.hour*60 + df['date'].dt.minute) >= s_h*60+s_m) &         ((df['date'].dt.hour*60 + df['date'].dt.minute) <= e_h*60+e_m)
    return df.loc[m].copy()

def resample_timeframe(min1: pd.DataFrame, tf_min: int) -> pd.DataFrame:
    """Resample 1-min to tf_min bars with OHLC."""
    g = min1.set_index('date').resample(f"{tf_min}min").agg({
        'open':'first','high':'max','low':'min','close':'last'
    }).dropna().reset_index()
    # Keep only bars inside the trading session
    g = in_session(g)
    return g

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    hl = df['high'] - df['low']
    hc = (df['high'] - df['close'].shift()).abs()
    lc = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def roc(series: pd.Series, period: int) -> pd.Series:
    return series.pct_change(periods=period)

def vix_bucket(v: float) -> str:
    if v < 12: return "<12"
    if v <= 15: return "12-15"
    return ">15"

def calc_max_dd_with_period(equity: pd.Series) -> Tuple[float, Optional[str], Optional[str]]:
    """Return (max_dd_points, start_date, end_date) using equity in points (cumulative P&L)."""
    if equity.empty:
        return 0.0, None, None
    peak = equity.cummax()
    dd = equity - peak
    min_dd = dd.min()
    if pd.isna(min_dd) or min_dd == 0:
        return 0.0, None, None
    dd_end = dd.idxmin()
    dd_start = equity.loc[:dd_end].idxmax()
    fmt = "%m/%d/%Y"
    return float(-min_dd), dd_start.date().strftime(fmt), dd_end.date().strftime(fmt)

def calc_streaks(trade_pnls: List[float]) -> Tuple[int, int]:
    max_win = max_loss = cur_win = cur_loss = 0
    for p in trade_pnls:
        if p > 0:
            cur_win += 1; cur_loss = 0
        else:
            cur_loss += 1; cur_win = 0
        max_win = max(max_win, cur_win)
        max_loss = max(max_loss, cur_loss)
    return max_win, max_loss

def daily_return_summary(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty:
        return pd.DataFrame(columns=['date','pnl_pts','ret_pct_of_base'])
    daily = trades_df.groupby('date')['pnl_pts'].sum().reset_index()
    daily['ret_pct_of_base'] = daily['pnl_pts'] / BASE_CAPITAL
    return daily

def cagr_and_sharpe(daily_returns: pd.Series) -> Tuple[float, float]:
    if daily_returns.empty:
        return 0.0, 0.0
    cum = (1 + daily_returns.fillna(0)).prod()
    years = len(daily_returns) / 252.0
    cagr = (cum ** (1/years) - 1) * 100 if years > 0 else 0.0
    sharpe_like = daily_returns.mean() / (daily_returns.std(ddof=1)+1e-12) * np.sqrt(252) if len(daily_returns)>1 else 0.0
    return float(cagr), float(sharpe_like)

def build_signals(df: pd.DataFrame) -> pd.DataFrame:
    """Add momentum indicators & directions on the timeframe df."""
    df = df.copy()
    df['ema_fast'] = ema(df['close'], EMA_FAST)
    df['ema_slow'] = ema(df['close'], EMA_SLOW)
    df['ema_fast_slope'] = df['ema_fast'] - df['ema_fast'].shift(1)
    df['roc'] = roc(df['close'], ROC_PERIOD)
    df['atr'] = atr(df, ATR_PERIOD)

    # Directional signals
    df['long_signal']  = (df['close'] > df['ema_fast']) & (df['ema_fast'] > df['ema_slow']) &                          (df['ema_fast_slope'] > 0) & (df['roc'] > 0)
    df['short_signal'] = (df['close'] < df['ema_fast']) & (df['ema_fast'] < df['ema_slow']) &                          (df['ema_fast_slope'] < 0) & (df['roc'] < 0)
    return df

def merge_vix_on_time(tf_df: pd.DataFrame, vix_min1: pd.DataFrame) -> pd.DataFrame:
    """Merge nearest VIX minute to each timeframe bar, then forward fill within the day."""
    v = vix_min1[['date','close']].rename(columns={'close':'vix_close'}).copy()
    out = pd.merge_asof(tf_df.sort_values('date'), v.sort_values('date'), on='date')
    # forward fill gaps but reset at day boundary
    out['date_only'] = out['date'].dt.date
    out['vix_close'] = out.groupby('date_only')['vix_close'].ffill()
    out.drop(columns=['date_only'], inplace=True)
    return out



def backtest_momentum(nifty_min1: pd.DataFrame, vix_min1: pd.DataFrame, tf_min: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Build timeframe bars with indicators
    tf = resample_timeframe(nifty_min1, tf_min)
    tf = build_signals(tf)
    tf = merge_vix_on_time(tf, vix_min1)

    tf['date_only'] = tf['date'].dt.date
    trades: List[Dict] = []
    nifty_min1['date_only'] = nifty_min1['date'].dt.normalize()
    if tf_min == 15:
        dbg = tf[tf['date'].dt.date == pd.to_datetime("2025-07-24").date()]
        print("DEBUG 15-min bars on 2025-07-24:")
        print(dbg[['date','open','high','low','close']].head(20))

    # Iterate by day
    for day, day_df in tf.groupby('date_only'):
        day_df = day_df.reset_index(drop=True)
        if day_df.empty: 
            continue
        # also slice the same day's 1-min data
        day_min1 = nifty_min1[nifty_min1['date_only'] == pd.to_datetime(day)].reset_index(drop=True)

        trades_taken = 0
        i = 2
        while i < len(day_df) and trades_taken < MAX_TRADES_PER_DAY:
            row = day_df.iloc[i]
            t = row['date']
            if t.time().strftime("%H:%M") >= FORCE_FLAT_AT:
                break

            vix_val = row.get('vix_close', np.nan)
            if SKIP_IF_VIX_LT_12 and pd.notna(vix_val) and vix_val < 12:
                i += 1; continue

            trade_taken = False  # <- flag to ensure we always advance i

            # ---- LONG ----
            if day_df.loc[i-1,'long_signal']:
                trigger = day_df.loc[i-1,'high']
                start = day_df.loc[i-1,'date']
                end   = start + pd.Timedelta(minutes=tf_min)
                #end   = row['date']
                sub = day_min1[(day_min1['date'] > start) & (day_min1['date'] <= end)]
                hit = sub[sub['high'] > trigger]
                if not hit.empty:
                    entry_time = hit.iloc[0]['date']
                    entry = trigger
                    risk = RISK_MULT * max(day_df.loc[i-1,'atr'], 1e-9)
                    sl = entry - risk
                    tp = entry + TARGET_R_MULT * risk

                    exit_px, exit_reason, exit_time = None, None, None
                    j = i
                    while j < len(day_df):
                        r2 = day_df.iloc[j]
                        if r2['low'] <= sl:
                            exit_px, exit_reason, exit_time = sl, 'SL', r2['date']; break
                        if r2['high'] >= tp:
                            exit_px, exit_reason, exit_time = tp, 'TP', r2['date']; break
                        if r2['date'].time().strftime("%H:%M") >= FORCE_FLAT_AT:
                            exit_px, exit_reason, exit_time = r2['close'], 'EOD', r2['date']; break
                        j += 1
                    if exit_px is None:
                        rlast = day_df.iloc[-1]
                        exit_px, exit_reason, exit_time = rlast['close'], 'SESSION_END', rlast['date']
                        j = len(day_df)

                    pnl = exit_px - entry
                    trades.append(dict(date=pd.to_datetime(day), timeframe=tf_min, side='LONG',
                                       entry_time=entry_time, entry=entry,
                                       exit_time=exit_time, exit=exit_px, exit_reason=exit_reason,
                                       pnl_pts=pnl, vix_at_entry=float(vix_val) if pd.notna(vix_val) else np.nan,
                                       vix_bucket=vix_bucket(float(vix_val)) if pd.notna(vix_val) else 'NA'))
                    trades_taken += 1
                    i = j + 1
                    trade_taken = True
                    continue

            # ---- SHORT ----
            if not trade_taken and day_df.loc[i-1,'short_signal']:
                trigger = day_df.loc[i-1,'low']
                start = day_df.loc[i-1,'date']
                end   = start + pd.Timedelta(minutes=tf_min)
                #end   = row['date']
                sub = day_min1[(day_min1['date'] > start) & (day_min1['date'] <= end)]
                hit = sub[sub['low'] < trigger]
                if not hit.empty:
                    entry_time = hit.iloc[0]['date']
                    entry = trigger
                    risk = RISK_MULT * max(day_df.loc[i-1,'atr'], 1e-9)
                    sl = entry + risk
                    tp = entry - TARGET_R_MULT * risk

                    exit_px, exit_reason, exit_time = None, None, None
                    j = i
                    while j < len(day_df):
                        r2 = day_df.iloc[j]
                        if r2['high'] >= sl:
                            exit_px, exit_reason, exit_time = sl, 'SL', r2['date']; break
                        if r2['low'] <= tp:
                            exit_px, exit_reason, exit_time = tp, 'TP', r2['date']; break
                        if r2['date'].time().strftime("%H:%M") >= FORCE_FLAT_AT:
                            exit_px, exit_reason, exit_time = r2['close'], 'EOD', r2['date']; break
                        j += 1
                    if exit_px is None:
                        rlast = day_df.iloc[-1]
                        exit_px, exit_reason, exit_time = rlast['close'], 'SESSION_END', rlast['date']
                        j = len(day_df)

                    pnl = entry - exit_px
                    trades.append(dict(date=pd.to_datetime(day), timeframe=tf_min, side='SHORT',
                                       entry_time=entry_time, entry=entry,
                                       exit_time=exit_time, exit=exit_px, exit_reason=exit_reason,
                                       pnl_pts=pnl, vix_at_entry=float(vix_val) if pd.notna(vix_val) else np.nan,
                                       vix_bucket=vix_bucket(float(vix_val)) if pd.notna(vix_val) else 'NA'))
                    trades_taken += 1
                    i = j + 1
                    trade_taken = True
                    continue

            # if no signal at all
            if not trade_taken:
                i += 1

    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        return trades_df, pd.DataFrame(columns=['date','pnl_pts','ret_pct_of_base'])
    # Daily summary from trades
    daily = trades_df.groupby('date')['pnl_pts'].sum().reset_index()
    daily['ret_pct_of_base'] = daily['pnl_pts'] / BASE_CAPITAL
    return trades_df, daily


def backtest_momentum_orig(nifty_min1: pd.DataFrame, vix_min1: pd.DataFrame, tf_min: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Build timeframe bars with indicators
    tf = resample_timeframe(nifty_min1, tf_min)
    tf = build_signals(tf)
    tf = merge_vix_on_time(tf, vix_min1)

    tf['date_only'] = tf['date'].dt.date
    trades: List[Dict] = []

    # Iterate by day
    for day, day_df in tf.groupby('date_only'):
        day_df = day_df.reset_index(drop=True)
        if day_df.empty: 
            continue
        trades_taken = 0
        i = 2
        while i < len(day_df) and trades_taken < MAX_TRADES_PER_DAY:
            row = day_df.iloc[i]
            t = row['date']
            if t.time().strftime("%H:%M") >= FORCE_FLAT_AT:
                break

            vix_val = row.get('vix_close', np.nan)
            if SKIP_IF_VIX_LT_12 and pd.notna(vix_val) and vix_val < 12:
                i += 1; continue
            # Long
            #if row['long_signal'] and row['high'] > day_df.loc[i-1,'high']:
            if day_df.loc[i-1,'long_signal'] and row['high'] > day_df.loc[i-1,'high']:
                entry = max(day_df.loc[i-1,'high'], row['open']) if row['open'] > day_df.loc[i-1,'high'] else day_df.loc[i-1,'high']
                #risk = RISK_MULT * max(row['atr'], 1e-9)
                risk = RISK_MULT * max(day_df.loc[i-1,'atr'], 1e-9)
                sl = entry - risk
                tp = entry + TARGET_R_MULT * risk
                exit_px, exit_reason, exit_time = None, None, None
                j = i
                while j < len(day_df):
                    r2 = day_df.iloc[j]
                    if r2['low'] <= sl:
                        exit_px, exit_reason, exit_time = sl, 'SL', r2['date']; break
                    if r2['high'] >= tp:
                        exit_px, exit_reason, exit_time = tp, 'TP', r2['date']; break
                    if r2['date'].time().strftime("%H:%M") >= FORCE_FLAT_AT:
                        exit_px, exit_reason, exit_time = r2['close'], 'EOD', r2['date']; break
                    j += 1
                if exit_px is None:
                    rlast = day_df.iloc[-1]
                    exit_px, exit_reason, exit_time = rlast['close'], 'SESSION_END', rlast['date']
                    j = len(day_df)
                pnl = exit_px - entry
                trades.append(dict(date=pd.to_datetime(day), timeframe=tf_min, side='LONG',
                                   entry_time=row['date'], entry=entry,
                                   exit_time=exit_time, exit=exit_px, exit_reason=exit_reason,
                                   pnl_pts=pnl, vix_at_entry=float(vix_val) if pd.notna(vix_val) else np.nan,
                                   vix_bucket=vix_bucket(float(vix_val)) if pd.notna(vix_val) else 'NA'))
                trades_taken += 1
                i = j + 1
                continue
            # Short
            #if row['short_signal'] and row['low'] < day_df.loc[i-1,'low']:
            if day_df.loc[i-1,'short_signal'] and row['low'] < day_df.loc[i-1,'low']:
                entry = min(day_df.loc[i-1,'low'], row['open']) if row['open'] < day_df.loc[i-1,'low'] else day_df.loc[i-1,'low']
                #risk = RISK_MULT * max(row['atr'], 1e-9)
                risk = RISK_MULT * max(day_df.loc[i-1,'atr'], 1e-9)
                sl = entry + risk
                tp = entry - TARGET_R_MULT * risk
                exit_px, exit_reason, exit_time = None, None, None
                j = i
                while j < len(day_df):
                    r2 = day_df.iloc[j]
                    if r2['high'] >= sl:
                        exit_px, exit_reason, exit_time = sl, 'SL', r2['date']; break
                    if r2['low'] <= tp:
                        exit_px, exit_reason, exit_time = tp, 'TP', r2['date']; break
                    if r2['date'].time().strftime("%H:%M") >= FORCE_FLAT_AT:
                        exit_px, exit_reason, exit_time = r2['close'], 'EOD', r2['date']; break
                    j += 1
                if exit_px is None:
                    rlast = day_df.iloc[-1]
                    exit_px, exit_reason, exit_time = rlast['close'], 'SESSION_END', rlast['date']
                    j = len(day_df)
                pnl = entry - exit_px
                trades.append(dict(date=pd.to_datetime(day), timeframe=tf_min, side='SHORT',
                                   entry_time=row['date'], entry=entry,
                                   exit_time=exit_time, exit=exit_px, exit_reason=exit_reason,
                                   pnl_pts=pnl, vix_at_entry=float(vix_val) if pd.notna(vix_val) else np.nan,
                                   vix_bucket=vix_bucket(float(vix_val)) if pd.notna(vix_val) else 'NA'))
                trades_taken += 1
                i = j + 1
                continue
            i += 1

    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        return trades_df, pd.DataFrame(columns=['date','pnl_pts','ret_pct_of_base'])
    # Daily summary from trades
    daily = trades_df.groupby('date')['pnl_pts'].sum().reset_index()
    daily['ret_pct_of_base'] = daily['pnl_pts'] / BASE_CAPITAL
    return trades_df, daily

def cagr_and_sharpe(daily_returns: pd.Series):
    if daily_returns.empty:
        return 0.0, 0.0
    cum = (1 + daily_returns.fillna(0)).prod()
    years = len(daily_returns) / 252.0
    cagr = (cum ** (1/years) - 1) * 100 if years > 0 else 0.0
    sharpe_like = daily_returns.mean() / (daily_returns.std(ddof=1)+1e-12) * np.sqrt(252) if len(daily_returns)>1 else 0.0
    return float(cagr), float(sharpe_like)

def summarize(trades_df: pd.DataFrame, daily_df: pd.DataFrame, tf_min: int, bucket: Optional[str]) -> dict:
    df = trades_df.copy()
    if bucket and bucket != "ALL":
        df = df[df['vix_bucket'] == bucket]
    # trades, win rate
    trades = len(df)
    if trades == 0:
        return dict(timeframe=tf_min, vix_bucket=bucket or "ALL", trades=0, win_rate=0.0,
                    avg_pnl_pts=0.0, total_pnl_pts=0.0, profit_factor=0.0,
                    max_dd_pts=0.0, dd_start=None, dd_end=None,
                    cagr_pct=0.0, sharpe_like=0.0, max_win_streak=0, max_loss_streak=0)
    wins = (df['pnl_pts'] > 0).sum()
    win_rate = wins / trades * 100.0
    avg_pnl = df['pnl_pts'].mean()
    total_pnl = df['pnl_pts'].sum()
    gross_profit = df.loc[df['pnl_pts'] > 0, 'pnl_pts'].sum()
    gross_loss = -df.loc[df['pnl_pts'] <= 0, 'pnl_pts'].sum()
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else np.inf

    # Equity curve (points) for DD and period
    eq = df.groupby('date')['pnl_pts'].sum().cumsum()
    max_dd_pts, dd_start, dd_end = calc_max_dd_with_period(eq)

    # CAGR & Sharpe-like based on daily returns (subset by bucket's dates)
    if not daily_df.empty:
        keep_days = df['date'].dt.date.unique()
        daily_sub = daily_df[daily_df['date'].dt.date.isin(keep_days)]
        cagr, sharpe_like = cagr_and_sharpe(daily_sub['ret_pct_of_base'])
    else:
        cagr, sharpe_like = 0.0, 0.0

    # Streaks
    max_win_streak, max_loss_streak = calc_streaks(df['pnl_pts'].tolist())

    return dict(timeframe=tf_min, vix_bucket=bucket or "ALL", trades=trades, win_rate=win_rate,
                avg_pnl_pts=avg_pnl, total_pnl_pts=total_pnl, profit_factor=profit_factor,
                max_dd_pts=max_dd_pts, dd_start=dd_start, dd_end=dd_end,
                cagr_pct=cagr, sharpe_like=sharpe_like,
                max_win_streak=max_win_streak, max_loss_streak=max_loss_streak)

def run(nifty_csv: str, vix_csv: str, out_prefix: str = "momo"):
    nifty = in_session(load_csv(nifty_csv))
    vix   = in_session(load_csv(vix_csv))

    timeframes = [5, 15, 60]
    all_summaries = []

    for tf in timeframes:
        trades_df, daily_df = backtest_momentum(nifty, vix, tf)

        # Save per-tf trades & daily files
        trades_path = f"trades_{out_prefix}_{tf}m.csv"
        daily_path  = f"daily_{out_prefix}_{tf}m.csv"
        trades_df.to_csv(trades_path, index=False)
        daily_df.to_csv(daily_path, index=False)

        # Summaries for ALL and each bucket
        buckets = ["ALL", "<12", "12-15", ">15"]
        for b in buckets:
            summary = summarize(trades_df, daily_df, tf_min=tf, bucket=None if b=="ALL" else b)
            all_summaries.append(summary)

    summary_df = pd.DataFrame(all_summaries, columns=[
        "timeframe", "vix_bucket", "trades", "win_rate", "avg_pnl_pts",
        "total_pnl_pts", "profit_factor", "max_dd_pts", "dd_start", "dd_end",
        "cagr_pct", "sharpe_like", "max_win_streak", "max_loss_streak"
    ])
    summary_path = f"summary_by_vix_and_tf_{out_prefix}.csv"
    summary_df.to_csv(summary_path, index=False)

    # Leaderboard by ALL
    leaderboard = summary_df[summary_df['vix_bucket']=="ALL"].sort_values(
        by=['profit_factor','total_pnl_pts'], ascending=[False, False]
    )
    leaderboard.to_csv(f"leaderboard_{out_prefix}.csv", index=False)
    print("Saved:")
    print(" -", summary_path)
    print(" -", f"leaderboard_{out_prefix}.csv")
    for tf in [5,15,60]:
        print(" -", f"trades_{out_prefix}_{tf}m.csv")
        print(" -", f"daily_{out_prefix}_{tf}m.csv")

if __name__ == "__main__":
    # Simple CLI
    import argparse
    p = argparse.ArgumentParser(description="Momentum Intraday Backtest for NIFTY + VIX (Sequential trades, max 3/day)")
    p.add_argument("--nifty_csv", type=str, default="NIFTY_50_minute.csv")
    p.add_argument("--vix_csv", type=str, default="INDIA_VIX_minute.csv")
    p.add_argument("--out_prefix", type=str, default="momo")
    args = p.parse_args()
    run(args.nifty_csv, args.vix_csv, args.out_prefix)
