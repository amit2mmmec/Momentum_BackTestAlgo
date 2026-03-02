# realtime_adapter_test_harness.py
import time, os, logging
import pandas as pd
from realtime_adapter import (
    append_tick_to_min_buffer,
    update_indicators_on_minute_close,
    check_pending_triggers_and_take_entry,
    check_exit_conditions,
)

# --- Simulated order functions (no broker calls) ---
def sim_place_order(kite, direction, qty=50):
    return {"order_id": f"SIM-{direction}-{int(time.time())}", "symbol": "SIMFUT"}

def sim_exit_order(kite, direction, qty=50):
    return {"order_id": f"SIMEXIT-{direction}-{int(time.time())}", "symbol": "SIMFUT"}


# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- Load CSVs ---
nifty_min_csv = "NIFTY_50_minute.csv"
vix_min_csv = "INDIA_VIX_minute.csv"

n_df = pd.read_csv(nifty_min_csv, parse_dates=["date"])
n_df = n_df.sort_values("date").drop_duplicates("date").reset_index(drop=True)

# Keep only the first 1000 entries
n_df = n_df.head(2000)

if os.path.exists(vix_min_csv):
    v_df = pd.read_csv(vix_min_csv, parse_dates=["date"])
    v_df = v_df.sort_values("date").drop_duplicates("date").reset_index(drop=True)
else:
    v_df = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])


# --- Replay loop ---
for idx, row in n_df.iterrows():
    tick = {"timestamp": row["date"].to_pydatetime(), "last_price": float(row["close"])}
    flushed = append_tick_to_min_buffer(tick, symbol="NIFTY")

    if flushed:
        logging.info("Flushed minute: %s", flushed["date"])
        # update indicators
        update_indicators_on_minute_close(flushed["date"], state_file="state_test.json")
        # check entries
        check_pending_triggers_and_take_entry(flushed, state_file="state_test.json",
                                              kite=None, place_order_fn=sim_place_order)
        # check exits
        check_exit_conditions(flushed, state_file="state_test.json",
                              kite=None, exit_order_fn=sim_exit_order)

print("Simulation complete. Inspect live_trades.csv and state_test.json")
