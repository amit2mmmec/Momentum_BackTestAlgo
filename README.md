
python nifty_vix_momentum_backtest.py --nifty_csv NIFTY_50_minute.csv --vix_csv INDIA_VIX_minute.csv --out_prefix momo


python nifty_vix_momentum_backtest_updated.py --nifty_csv NIFTY_50_minute_with_EMA_conditions.csv --vix_csv INDIA_VIX_minute.csv --out_prefix momo

# NIFTY Intraday Momentum Backtest (5m / 15m / 60m)

A simple, fast momentum strategy designed to keep **1:2 risk–reward**, take **max 3 trades per day**, and produce a **summary identical to your ORB format** (including DD period & streaks).

## Inputs
- `NIFTY_50_minute.csv` with minute OHLC
- `INDIA_VIX_minute.csv` with minute OHLC (volume ignored).

## Core Idea (per timeframe)
- Build timeframe bars (5/15/60 minutes).
- **LONG** when: `close > EMA20 > EMA50`, `EMA20 slope > 0`, `ROC(5) > 0`, and **breaks above prior bar’s high**.
- **SHORT** when: `close < EMA20 < EMA50`, `EMA20 slope < 0`, `ROC(5) < 0`, and **breaks below prior bar’s low**.
- **Risk/Exit**: `SL = 1.0 × ATR(14)`, `TP = 2.0 × ATR(14)` (=> 1:2 RR). Flat by **15:25 IST**.
- **Max 3 trades per day.**
- By default, skip trades when **VIX < 12** (`SKIP_IF_VIX_LT_12 = True`), configurable inside script.

> This is a momentum template intended to be robust; I can tune filters/parameters with your data to maximize profitability per regime.

## Running
```bash
python nifty_vix_momentum_backtest.py --nifty_csv NIFTY_50_minute.csv --vix_csv INDIA_VIX_minute.csv --out_prefix momo
```

## Outputs
- `trades_momo_{5|15|60}m.csv` — detailed trades (entry/exit/reason, P&L, VIX bucket).
- `daily_momo_{5|15|60}m.csv` — daily P&L and returns vs base capital.
- `summary_by_vix_and_tf_momo.csv` — summary in your requested format:
```
timeframe vix_bucket trades win_rate avg_pnl_pts total_pnl_pts profit_factor max_dd_pts dd_start dd_end cagr_pct sharpe_like max_win_streak max_loss_streak
```
- `leaderboard_momo.csv` — ranked (ALL buckets) by Profit Factor & Total PnL.

## Notes
- All numbers are **index points**, not ₹. If trading derivatives, convert to instrument P&L (lot-size changes over years!).
- No brokerage/slippage is included; add fixed costs if needed.
- You can tweak constants near the top (EMA/ATR/ROC periods, RR, VIX filter, etc.).
