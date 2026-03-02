from kiteconnect import KiteConnect, KiteTicker

import argparse
import logging
import datetime
import time
import threading
import os
import signal
import sys
import json
import csv
import configparser
import tkinter as tk
from tkinter import messagebox
from datetime import time as dt_time

from telegram_bot import send_telegram_message
from state_manager_momo import save_state, load_state, clear_state
logging.basicConfig(
    filename="momo_algo.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tools.order_fetcher import read_latest_orders
from tools.utils import check_exit_or_stop_flag

config = configparser.ConfigParser()
config.read("config_momo.ini")
current_algo_name = config["GENERAL"]["CURRENT_ALGO_NAME"] 
API_KEY = config["GENERAL"]["API_KEY"]
TRADING_SYMBOL = config["GENERAL"]["TRADING_SYMBOL"]
EXCHANGE = config["GENERAL"]["EXCHANGE"]
STRIKE_DIFF = int(config["GENERAL"]["STRIKE_DIFF"])
SL_PERCENT = int(config["GENERAL"]["SL_PERCENT"])

# Convert "09:20" to datetime.time object
ENTRY_TIME = dt_time.fromisoformat(config["GENERAL"]["ENTRY_TIME"])
EXIT_TIME = dt_time.fromisoformat(config["GENERAL"]["EXIT_TIME"])

ORDER_TAG = config["GENERAL"]["ORDER_TAG"]
CSV_LOG_FILE = config["GENERAL"]["CSV_LOG_FILE"]
QUANTITY = int(config["GENERAL"]["QUANTITY"])
is_test_mode = int(config["GENERAL"]["IS_TEST_MODE"])
default_state_file = config.get('STATE', 'STATE_FILE')
ticker_started = False
last_tick_time = datetime.datetime.now()
momo_already_placed = False

total_volume = 0
total_premium_volume = 0
run_ticks = True

last_spot_fetch = datetime.datetime.now() - datetime.timedelta(seconds=31)
spot_price = None
vix_value = None

last_ce_tick = None
last_pe_tick = None
sl_hit_timestamps = {}  # key: leg, value: datetime when SL was first breached

SL_HOLD_TIME_SEC = 0  # time buffer to hold above SL before triggering exit

api_key=API_KEY

orders = {
        "CE": {"order_id": None, "symbol": None, "entry_price": None, "filled_quantity": None, "exit_order_id": None, "exit_price": None, "exit_qty": None},
        "PE": {"order_id": None, "symbol": None, "entry_price": None, "filled_quantity": None, "exit_order_id": None, "exit_price": None, "exit_qty": None}
}
sl_triggered = {"CE": False, "PE": False}
sl_exit_price = {"CE": None, "PE": None}
ltp = {"CE": None, "PE": None}
mtm = {"CE": None, "PE": None}
slippage = {"CE": None, "PE": None}
market_snapshot = {
        "timestamp": None,
        "nifty_spot": None,
        "india_vix": None,
        "iv": {"CE": None, "PE": None}
        }
token_to_symbol = {}
remaining_qty = {"CE": None, "PE": None}

kws = None  # Global for KiteTicker
symbol_token_map = {}  # Global dictionary to hold symbol-token mapping
TOKEN_CACHE_FILE = "symbol_token_map.json"


def stop_algo(kws):
    tokens = []
    
    for leg in ["CE", "PE"]:
        symbol = orders.get(leg, {}).get("symbol")
        
        if symbol:
            token = symbol_token_map.get(symbol)
            if token:
                tokens.append(token)
                
            else:
                logging.warning(f"No instrument token found in symbol_token_map for: {symbol}")
        else:
            logging.warning(f"No symbol found in orders for leg: {leg}")

        if tokens:
            kws.unsubscribe(tokens)
            logging.info(f"Unsubscribed to tokens: {tokens}")
        else:
            logging.error("No valid tokens to unsubscribe to")

    logging.info("🛑 Algo stopped")
    try:
        if kws:
            kws.close()  # This will stop receiving ticks
            logging.info("✅ WebSocket closed.")
    except Exception as e:
        logging.error(f"⚠️ Error closing WebSocket: {e}")

def load_access_token(file_path="../access_token.txt"):
    try:
        with open(file_path, "r") as f:
            token = f.read().strip()
            #logging.info(f"Access Token: {token}")
            return token
    except FileNotFoundError:
        logging.info("access_token.txt not found.")
        exit(1)

def build_symbol_token_map(kite):
    global symbol_token_map
    symbol_token_map.clear()  # Clear old mappings
    instruments = None
    try:
        instruments = kite.instruments("NFO")
        with open(TOKEN_CACHE_FILE, "w") as f:
            json.dump(instruments, f, default=str)
        logging.info("Fetched live instruments and updated cache.")
    except Exception as e:
        logging.error("Live instrument fetch failed, trying cached file...", exc_info=True)
        if os.path.exists(TOKEN_CACHE_FILE):
            try:
                with open(TOKEN_CACHE_FILE, "r") as f:
                    instruments = json.load(f)
                logging.info("Loaded instruments from cached file.")
            except json.JSONDecodeError as je:
                logging.error("Cached instrument file is corrupted.", exc_info=True)
                raise RuntimeError("Cached instrument file is invalid.")
        else:
            raise RuntimeError("No instruments available (live or cached)")

    if not isinstance(instruments, list):
        raise RuntimeError("Instruments data is not a list.")

    for inst in instruments:
        symbol_token_map[inst["tradingsymbol"]] = inst["instrument_token"]

    logging.info(f"Built symbol_token_map with {len(symbol_token_map)} entries.")




def prompt_resume():
    try:
        # If stdin is not connected to a terminal, skip prompt and return True (resume)
        if not sys.stdin.isatty():
            logging.info("No interactive input available; auto-resuming from saved state.")
            return True

        while True:
            choice = input("Found existing saved state. Resume from last session? (y/n): ").strip().lower()
            if choice in ['y', 'n']:
                return choice == 'y'
            logging.info("Please enter 'y' or 'n'.")
    except EOFError:
        # No input possible (e.g., GUI launch), assume resume
        logging.info("No input detected, auto-resuming from saved state.")
        return True

def is_interactive():
    return sys.stdin.isatty()

def log_trade_to_csv(leg, order_type, symbol, price, timestamp):
    write_header = not os.path.exists(CSV_LOG_FILE)
    with open(CSV_LOG_FILE, mode='a', newline='') as file:
        writer = csv.writer(file)
        if write_header:
            writer.writerow(["Time", "Leg", "Order Type", "Symbol", "Price"])
        writer.writerow([timestamp, leg, order_type, symbol, price])


def get_next_expiry():
    today = datetime.date.today()
    weekday = today.weekday()
    # Calculate days to next Tuesday (weekday=1)
    days_to_tuesday = (1 - weekday) % 7
    expiry_date = today + datetime.timedelta(days=days_to_tuesday)
    # Return in YYYY-MM-DD format (same as in instruments CSV)
    return expiry_date.strftime("%Y-%m-%d")


def load_instruments_csv(filepath="nfo_instruments.csv"):
    instruments = []
    token_to_symbol = {}
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert expiry string to datetime.date for comparison later
            row["expiry"] = datetime.datetime.strptime(row["expiry"], "%Y-%m-%d").date()
            # Convert strike to float for numeric comparison
            row["strike"] = float(row["strike"])
            row["instrument_token"] = int(row["instrument_token"].strip())
            row["tradingsymbol"] = row["tradingsymbol"].strip()
            instruments.append(row)
            token_to_symbol[row["instrument_token"]] = row["tradingsymbol"]
    return instruments, token_to_symbol

def get_atm_option_tokens(instruments, expiry_str, atm_strike):
    expiry_date = datetime.datetime.strptime(expiry_str, "%Y-%m-%d").date()
    ce_token = None
    pe_token = None
    for inst in instruments:
        if (
            inst["name"] == "NIFTY"
            and inst["expiry"] == expiry_date
            and inst["strike"] == atm_strike
        ):
            if inst["instrument_type"] == "CE":
                ce_token = inst["instrument_token"]
            elif inst["instrument_type"] == "PE":
                pe_token = inst["instrument_token"]

    logging.info(f"NIFTY ce pe : {ce_token, pe_token}")    
    return ce_token, pe_token

def get_symbols_from_tokens(instruments, ce_token, pe_token):
    ce_symbol = None
    pe_symbol = None

    for inst in instruments:
        if inst["instrument_token"] == ce_token:
            ce_symbol = inst["tradingsymbol"]
        elif inst["instrument_token"] == pe_token:
            pe_symbol = inst["tradingsymbol"]

        # Optimization: stop when both are found
        if ce_symbol and pe_symbol:
            break

    return ce_symbol, pe_symbol

def get_next_expiry_fut():
    today = datetime.date.today()
    weekday = today.weekday()
    days_to_tuesday = (1 - weekday) % 7
    expiry = today + datetime.timedelta(days=days_to_tuesday)
    return expiry

def find_fut_premium_options(kite):
  
    try:
        spot = kite.ltp("NSE:NIFTY 50")["NSE:NIFTY 50"]["last_price"]
        logging.info(f"NIFTY Spot Price: {spot}")

        ce_list = []
        pe_list = []
        token_map = {}
        expiry_date = get_next_expiry_fut()
        logging.info(f"Nearest Weekly Expiry: {expiry_date}")

        instruments = kite.instruments("NFO")

        for inst in instruments:
            if (
                inst["name"] == "NIFTY"
                and inst["expiry"] == expiry_date
                and inst["instrument_type"] in ["CE", "PE"]
            ):
                token_map[str(inst["instrument_token"])] = inst
                if inst["instrument_type"] == "CE":
                    ce_list.append(inst["instrument_token"])
                else:
                    pe_list.append(inst["instrument_token"])

        logging.info(f"Fetching LTP for {len(ce_list)} CE and {len(pe_list)} PE instruments...")

        if not ce_list or not pe_list:
            logging.info(f"No CE or PE instruments found for expiry {expiry_date}")
            return None, None

        # Combine and batch fetch
        all_tokens = ce_list + pe_list
        ltp_data = kite.ltp(all_tokens)

        ce_closest = None
        pe_closest = None
        ce_min_diff = float('inf')
        pe_min_diff = float('inf')

        for token in ce_list:
            ltp = ltp_data.get(str(token), {}).get("last_price")
            if ltp is not None and 20 <= ltp <= 30:
                diff = abs(ltp - 25)
                if diff < ce_min_diff:
                    ce_min_diff = diff
                    ce_closest = (token, ltp)

        for token in pe_list:
            ltp = ltp_data.get(str(token), {}).get("last_price")
            if ltp is not None and 20 <= ltp <= 30:
                diff = abs(ltp - 25)
                if diff < pe_min_diff:
                    pe_min_diff = diff
                    pe_closest = (token, ltp)

        if ce_closest and pe_closest:
            ce_token = ce_closest[0]
            pe_token = pe_closest[0]
            logging.info(f"CE Token: {ce_token} | LTP: ₹{ce_closest[1]}")
            logging.info(f"PE Token: {pe_token} | LTP: ₹{pe_closest[1]}")
            return ce_token, pe_token
        else:
            logging.info("Could not find CE/PE near ₹25.")
            return None, None

    except Exception as e:
        logging.info("Error:", e)
        return None, None



def place_momo_orders(args, retry_delay=2):
    global orders, sl_triggered

    access_token = load_access_token()
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    # Step 1: Get NIFTY Spot Price
    spot = kite.ltp("NSE:NIFTY 50")["NSE:NIFTY 50"]["last_price"]
    logging.info(f"NIFTY Spot Price: {spot}")

    # Step 2: Get weekly expiry
    expiry = get_next_expiry()
    logging.info(f"Weekly Expiry: {expiry}")

    # Step 3: Load instrument data
    instruments, token_to_symbol = load_instruments_csv()

    # Step 4: Find CE and PE strikes with premium ~25
    ce_token, pe_token = find_fut_premium_options(kite)
    if not ce_token or not pe_token:
        logging.info("Error: Could not find CE/PE token for the given expiry and strike.")
        return False

    # Step 6: Fetch LTP for CE and PE
    ltp_data = kite.ltp([str(ce_token), str(pe_token)])
    ce_ltp = ltp_data.get(str(ce_token), {}).get("last_price", 0)
    pe_ltp = ltp_data.get(str(pe_token), {}).get("last_price", 0)

    if ce_ltp == 0 or pe_ltp == 0:
        logging.info("❌ Failed to fetch LTPs for CE or PE. Aborting order placement.")
        return False

    logging.info(f"CE LTP: {ce_ltp}")
    logging.info(f"PE LTP: {pe_ltp}")
    # Convert tokens to tradingsymbols
    ce_symbol, pe_symbol = get_symbols_from_tokens(instruments, ce_token, pe_token)
    logging.info(f"CE Symbol: {ce_symbol}")
    logging.info(f"PE Symbol: {pe_symbol}")

    # Initialize state dicts
    orders = orders or {"CE": {}, "PE": {}}
    sl_triggered = sl_triggered or {"CE": False, "PE": False}

    # Update symbols
    orders["CE"].update({"symbol": ce_symbol})
    orders["PE"].update({"symbol": pe_symbol})

    state = load_state(args.state_file)
    state["orders"] = orders
    state["sl_triggered"] = sl_triggered
    save_state(state, args.state_file)
    
    def place_order_until_success(symbol, leg_name):
        while True:
            try:
                if is_test_mode:
                    fake_order_id = f"SIM-{leg_name}-ORDER-ID"
                    orders[leg_name].update({
                        "order_id": fake_order_id,
                        "entry_price": 0,  # or simulate realistic entry price if needed
                        "filled_quantity":QUANTITY
                    })
                    logging.info(f"[SIMULATION] Would place order for {symbol}")
                    return fake_order_id
                else:
                    order_id  = kite.place_order(
                            variety=kite.VARIETY_REGULAR,
                            exchange="NFO",
                            tradingsymbol=symbol,
                            transaction_type=kite.TRANSACTION_TYPE_SELL,
                            quantity=QUANTITY,
                            product=kite.PRODUCT_NRML,
                            order_type=kite.ORDER_TYPE_MARKET,
                            validity=kite.VALIDITY_DAY,
                            tag=ORDER_TAG)
                    logging.info(f"{leg_name} Order placed, ID: {order_id}")
                    return order_id
            except Exception as e:
                err_msg = str(e).split("\n")[0]  # Just the first line of the exception
                logging.info(f"{leg_name} Order failed: {e}. Retrying in {retry_delay}s...")
                logging.warning(f"{leg_name} Order failed : {err_msg}")
                logging.debug("Full exception info", exc_info=True)
                time.sleep(retry_delay)

    legs_placed = []

    # Step 8: Place CE order if missing
    if not orders["CE"].get("order_id"):
        ce_order_id = place_order_until_success(ce_symbol, "CE")
        orders["CE"].update({"order_id": ce_order_id, "symbol": ce_symbol, "entry_price": None, "filled_quantity": None, "exit_order_id": None, "exit_price": None, "exit_qty": None})
        legs_placed.append("CE")
    else:
        ce_order_id = orders["CE"]["order_id"]
        logging.info(f"CE order already present with ID: {ce_order_id}")

    # Step 9: Place PE order if missing
    if not orders["PE"].get("order_id"):
        pe_order_id = place_order_until_success(pe_symbol, "PE")
        orders["PE"].update({"order_id": pe_order_id, "symbol": pe_symbol, "entry_price": None, "filled_quantity": None, "exit_order_id": None, "exit_price": None, "exit_qty": None})
        legs_placed.append("PE")
    else:
        pe_order_id = orders["PE"]["order_id"]
        logging.info(f"PE order already present with ID: {pe_order_id}")
    
    sl_triggered = {"CE": False, "PE": False}
    state = load_state(args.state_file)
    state["orders"] = orders
    state["sl_triggered"] = sl_triggered
    save_state(state, args.state_file)

    logging.info(f"Placed CE Order: {ce_symbol}, ID: {ce_order_id}")
    logging.info(f"Placed PE Order: {pe_symbol}, ID: {pe_order_id}")

    # Log placed legs
    if legs_placed:
        logging.info(f"Placed orders: {', '.join(legs_placed)}")
        if not is_test_mode:
            time.sleep(2)
            update_entry_prices_qty(kite)
            state = load_state(args.state_file)
            state["orders"] = orders
            state["sl_triggered"] = sl_triggered
            save_state(state, args.state_file)

        send_telegram_message(
                f"momo orders placed: "
                f"{'CE(' + ce_symbol + ') Order_Id(' + ce_order_id + ') ' if 'CE' in legs_placed else ''}"
                f"{'PE(' + pe_symbol + ') Order_Id(' + pe_order_id + ')' if 'PE' in legs_placed else ''}"
                )
    else:
        logging.info("✅ Both CE and PE orders are already present. No new orders placed.")
        send_telegram_message("✅ momo already placed. Both CE & PE orders exist.")

    return True



def sync_orders_from_broker(kite, args):
    """
    Syncs existing orders with our local orders dict by checking orders
    from Zerodha with the specific tag (ORDER_TAG) for CE and PE symbols.

    If either leg is already placed or SL was triggered in previous session, skip re-placement.
    Checks if any momo order (CE and PE both) has already been placed today.
    If so, sets `momo_already_placed` to True to prevent re-entry.
    """
    global orders, sl_triggered, momo_already_placed
    ce_entry_done = False
    pe_entry_done = False
    
    ce_exit_done = False
    pe_exit_done = False

    try:
        all_orders = read_latest_orders()  # Fetch all orders for the day
        ce_found = sl_triggered.get("CE", False)
        pe_found = sl_triggered.get("PE", False)
        if ce_found:
            logging.info("CE SL was already triggered in previous session. Skipping placement.")
        if pe_found:
            logging.info("PE SL was already triggered in previous session. Skipping placement.")

        # Now check all orders with correct tag for ANY CE or PE symbol (any strike)
        for order in reversed(all_orders):
            if order.get("tag") != ORDER_TAG:
                continue
            if order.get("status") not in ["COMPLETE", "OPEN", "TRIGGER PENDING"]:
                continue

            symbol = order.get("tradingsymbol", "")
            txn_type = order.get("transaction_type", "").upper()
            status = order.get("status")
            order_type = order.get("order_type", "").upper()
            product = order.get("product", "").upper()
            trigger_price = order.get("trigger_price")
            filled = order.get("filled_quantity", 0)
            order_id = order["order_id"]
            leg = "CE" if symbol.endswith("CE") else "PE" if symbol.endswith("PE") else None
            if not leg:
                continue
            
            if leg not in orders:
                orders[leg] = {}

            if txn_type == "SELL" and filled > 0:
                orders[leg]["symbol"] = symbol
                orders[leg]["order_id"] = order_id
                orders[leg]["entry_price"] = order["average_price"]
                orders[leg]["filled_quantity"] = filled
                if leg == "CE":
                    ce_entry_done = True
                else:
                    pe_entry_done = True

            if txn_type == "BUY" and order["status"] == "COMPLETE":
                is_sl = (
                        "SL" in order_type or "SL" in product or
                        (trigger_price not in [None, 0])
                        )
                if is_sl or order_type == "MARKET":
                    sl_triggered[leg] = True
                    orders[leg]["exit_order_id"] = order_id
                    orders[leg]["exit_price"] = order["average_price"]
                    orders[leg]["exit_qty"] = filled
                    sl_exit_price[leg] = order["average_price"]
                    
                    if leg == "CE":
                        ce_exit_done = True
                    else:
                        pe_exit_done = True
                    logging.info(f"✅ SL BUY executed for {leg}: Price {order['average_price']}, ID {order_id}")
            if orders[leg]["entry_price"] is not None and orders[leg]["exit_price"] is not None:
                mtm[leg] = round((orders[leg]["entry_price"] - orders[leg]["exit_price"]) * filled, 2)

            if ce_entry_done and pe_entry_done and ce_exit_done and pe_exit_done:
                break

        if ce_entry_done and pe_entry_done:
            momo_already_placed = True
            logging.info("✅ momo already placed or handled earlier today. Will not re-enter.")
        # Build and save only if CE or PE leg was found

        try:
            state = load_state(args.state_file)  # <- This ensures state is defined
        except Exception as e:
            logging.warning(f"⚠️ Failed to load state file, using empty state: {e}")
            state = {}

        state["orders"] = orders
        state["sl_triggered"] = sl_triggered
        state["sl_exit_price"] = sl_exit_price
        state["mtm"] = mtm
        state.setdefault("ltp", {"CE": None, "PE": None})
        state.setdefault("slippage", {"CE": None, "PE": None})
        
        state.setdefault("market_snapshot", {
            "timestamp": None,
            "nifty_spot": None,
            "india_vix": None,
            "iv": {"CE": None, "PE": None}
            })
        
        # This line ensures timestamps don't get wiped
        state.setdefault("momo_fut", {}).setdefault("timestamps", [])
        state["momo_fut"].setdefault("combined_premiums", [])
        state["momo_fut"].setdefault("vwap", [])
        save_state(state, args.state_file)
    except Exception as e:
        logging.error(f"Error syncing orders from broker: {e}", exc_info=True)      
        send_telegram_message(f"⚠️ Error syncing orders: {str(e).splitlines()[0]}")

def update_entry_prices_qty(kite):
    global orders, remaining_qty
    for leg in ["CE", "PE"]:
        try:
            if leg not in orders or "order_id" not in orders[leg]:
                logging.warning(f"{leg} leg not in orders dict or missing order_id.")
                continue
            order_id = orders[leg].get("order_id")
            symbol = orders[leg].get("symbol", "UNKNOWN")

            history = kite.order_history(order_id)
            for item in reversed(history):
                if item.get("tag") == ORDER_TAG and item.get("status") == "COMPLETE":
                    entry_price = float(item["average_price"])
                    orders[leg]["entry_price"] = entry_price
                    filled_quantity = item.get("filled_quantity")
                    orders[leg]["filled_quantity"] = filled_quantity
                    remaining_qty[leg] = filled_quantity 
                    log_trade_to_csv(leg, "SELL", symbol, entry_price, datetime.datetime.now())
                    logging.info(f"Updated entry price for {leg} ({symbol}), Order ID {order_id}: {entry_price} {filled_quantity}")
                    break
            else:
                logging.warning(f"No completed order with tag {ORDER_TAG} found for {leg} (Symbol: {symbol}, Order ID: {order_id})")

        except Exception as e:
            logging.error(f"Error fetching entry price for {leg}", exc_info=True)

def on_connect(ws, response):
    tokens = []

    for leg in ["CE", "PE"]:
        symbol = orders.get(leg, {}).get("symbol")
        
        if symbol:
            token = symbol_token_map.get(symbol)
            if token:
                tokens.append(token)
            else:
                logging.warning(f"No instrument token found in symbol_token_map for: {symbol}")
        else:
            logging.warning(f"No symbol found in orders for leg: {leg}")

    if tokens:
        ws.subscribe(tokens)
        logging.info(f"Subscribed to tokens: {tokens}")
    else:
        logging.error("No valid tokens to subscribe to. Ticker connection skipped.")


def save_nfo_instruments_to_csv(kite):

    try:
        logging.info("Fetching NFO instrument dump...")
        instruments = kite.instruments("NFO")

        # Define the filename
        filename = "nfo_instruments.csv"

        # Get column headers from the first dictionary
        headers = instruments[0].keys()

        # Write to CSV
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(instruments)

        logging.info(f"Instrument dump saved to {filename}")

    except Exception as e:
        logging.info("Error fetching/saving instrument dump:", e)
		

def ticker_heartbeat():
    global last_tick_time, kws

    while True:
        if datetime.datetime.now() - last_tick_time > datetime.timedelta(seconds=30):
            logging.info("[⚠️ WARNING] Ticks not received in last 30 seconds!")
            try:
                kws.close()  # Close existing connection
            except Exception as e:
                logging.info(f"Error closing ticker: {e}")
            ticker_started = False
            time.sleep(2)
            start_ticker()  # Reconnect
            last_tick_time = datetime.datetime.now()
        time.sleep(10)

def on_close(ws, code, reason):
    logging.info(f"[Ticker closed] Code: {code}, Reason: {reason}")

def on_error(ws, code, reason):
    logging.info(f"[Ticker error] {reason}")

def on_reconnect(ws, attempts):
    logging.info(f"[Ticker reconnect attempt {attempts}]")

def load_token_to_symbol_map(filepath="symbol_token_map.json"):
    with open(filepath, "r") as f:
        data = json.load(f)

    token_to_symbol = {
        int(item["instrument_token"]): item["tradingsymbol"].strip()
        for item in data
    }

    return token_to_symbol

def on_ticks(ws, ticks):
    global orders, sl_triggered, sl_exit_price, ltp, last_tick_time, market_snapshot, run_ticks
    global last_ce_tick, last_pe_tick
    global total_volume, total_premium_volume
    global last_spot_fetch, spot_price, vix_value

    if not run_ticks:
        stop_algo(ws)
        return
    
    last_tick_time = datetime.datetime.now()
    #logging.info(f"[Tick received at {last_tick_time.strftime('%H:%M:%S')}]")
    now = datetime.datetime.now() 

    if (now - last_spot_fetch).total_seconds() > 30: # Fetch only every 30 seconds
        try:
            #spot_price = kite.ltp("NSE:NIFTY 50")["NSE:NIFTY 50"]["last_price"]
            #vix_value = kite.ltp("NSE:INDIA VIX")["NSE:INDIA VIX"]["last_price"]
            last_spot_fetch = now
        except Exception as e:
            logging.info("Failed to fetch VIX or NIFTY Spot:", e)

    try:
        sync_exit_status_from_broker(kite, args)
    except Exception as e:
        logging.error(f"[SYNC ERROR] Failed syncing exit status: {e}")

    ce_symbol = orders.get("CE", {}).get("symbol")
    pe_symbol = orders.get("PE", {}).get("symbol")
    last_ce_tick = None
    last_pe_tick = None

    # --- Update LTP for CE and PE legs, track last ticks ---
    try:
        for tick in ticks:
            token = int(tick.get("instrument_token", 0))
            symbol = token_to_symbol.get(token)

            if not symbol:
                logging.warning(f"Unknown token: {token}")
                continue

            if symbol == orders.get("CE", {}).get("symbol"):
                last_ce_tick = tick
            elif symbol == orders.get("PE", {}).get("symbol"):
                last_pe_tick = tick

    except Exception as e:
        logging.exception(f"[❌] Error in on_ticks: {e}")
    
    # --- Update VWAP and combined premium only if both ticks available --
    
    #if last_ce_tick:
     #   logging.warning("CE Tick data:\n" + json.dumps(last_ce_tick, indent=2))
    #if last_pe_tick:
     #   logging.warning("PE Tick data:\n" + json.dumps(last_pe_tick, indent=2))

    if last_ce_tick and last_pe_tick:
        ce_ltp = last_ce_tick.get("last_price")
        pe_ltp = last_pe_tick.get("last_price")
        ce_vol = last_ce_tick.get("volume_traded")
        pe_vol = last_pe_tick.get("volume_traded")

        combined_premium = ce_ltp + pe_ltp
        if ce_vol is None: ce_vol = 0
        if pe_vol is None: pe_vol = 0
        combined_volume = ce_vol + pe_vol


        #logging.warning(f"momo {ce_ltp} CEL {pe_ltp} PEL {ce_vol} CEV {pe_vol} PEV ")

        if combined_volume > 0:
            total_premium_volume += combined_premium * combined_volume
            total_volume += combined_volume
            vwap = total_premium_volume / total_volume
            vwap = round(vwap, 2)
            combined_premium = round(combined_premium, 2)

            state = load_state(args.state_file)

            # Append for history
            state["momo_fut"]["timestamps"].append(last_tick_time.strftime('%H:%M:%S'))
            state["momo_fut"]["combined_premiums"].append(combined_premium)
            state["momo_fut"]["vwap"].append(vwap)

            save_state(state, args.state_file)

    if sl_triggered["CE"] and sl_triggered["PE"]:
        logging.info("🚫 Both SLs triggered. Disabling tick processing.")
        run_ticks = False
        logging.info("🛑 Stopped on_ticks")

    for leg in ["CE", "PE"]:
        leg_data = orders.get(leg, {})

        symbol = leg_data.get("symbol")
        entry_price = leg_data.get("entry_price")
        exit_qty = remaining_qty[leg] # should be remaining_qty
        sl_hit = sl_triggered.get(leg, False)

        if symbol and entry_price and not sl_hit:
            try:
                ltp_data = kite.ltp(f"NFO:{symbol}")
                ltpkite = ltp_data.get(f"NFO:{symbol}", {}).get("last_price")
                time.sleep(1)
                #time.sleep(random.uniform(1.0, 1.5))
                

                if ltpkite is None:
                    logging.warning(f"Could not fetch LTP for {symbol}")
                    continue

                sl_price = entry_price * (1 + SL_PERCENT / 100)
                ltp[leg] = ltpkite
                state = load_state(args.state_file)
                state["ltp"] = ltp 
                state["mtm"][leg] = round((entry_price - ltpkite) * QUANTITY, 2)
                # Save Market Snapshot
                state["market_snapshot"] = {
                        "timestamp": last_tick_time.strftime('%Y-%m-%d %H:%M:%S'),
                        "nifty_spot": spot_price,
                        "india_vix": vix_value,
                        "iv": state.get("market_snapshot", {}).get("iv", {})
                        }
                # Attempt to get IVs (if plan allows greeks)
                #try:
                #    quote = kite.quote([f"NFO:{symbol}"])
                #    greeks = quote.get(f"NFO:{symbol}", {}).get("greeks", {})
                #    if greeks:
                #        state["market_snapshot"]["iv"][leg] = greeks.get("iv")
                #except:
                #    pass  # Silent fail if IV not available

                save_state(state, args.state_file)

                if check_sl_with_time_buffer(leg, ltpkite, sl_price):
                    logging.warning(f"momo {leg} SL hit. LTP: {ltpkite} >= SL: {sl_price}")
                    send_telegram_message(f"momo {leg} SL hit sustained for {SL_HOLD_TIME_SEC}s at {ltpkite}. Exiting leg.")

                    exit_price = exit_leg_with_retry(leg, exit_qty)
                    
                    state = load_state(args.state_file)
                    if exit_price is not None:
                        sl_exit_price[leg] = exit_price
                        state["slippage"][leg]  = round(abs(sl_price - exit_price) * exit_qty, 2)
                        state["mtm"][leg] = round((entry_price - exit_price) * exit_qty, 2)
                        sl_triggered[leg] = True
                        state["orders"] = orders
                        state["sl_triggered"] = sl_triggered
                        state["sl_exit_price"] = sl_exit_price
                        save_state(state, args.state_file)
                    else:
                        logging.warning(f"Exit price for {leg} was None")
            except Exception as e:
                logging.error(f"Error checking SL for {leg}", exc_info=True)
        else:
            logging.debug(f"Skipping SL check for {leg} - Symbol: {symbol}, Entry Price: {entry_price}, SL Triggered: {sl_hit}")


def get_executed_price(kite, order_id, retries=5, delay=2):
    for attempt in range(retries):
        try:
            order_history = kite.order_history(order_id)
            for event in reversed(order_history):
                if event['status'] == 'COMPLETE':
                    return float(event['average_price']), int(event['filled_quantity'])
        except Exception as e:
            logging.error(f"[Attempt {attempt + 1}] Error fetching executed price for order {order_id}: {e}")
        time.sleep(delay)
    return None, None



def sync_exit_status_from_broker(kite, args):
    global orders, sl_triggered, sl_exit_price, ltp

    try:
        all_orders = read_latest_orders()
        state = load_state(args.state_file)

        for leg in ["CE", "PE"]:
            if sl_triggered.get(leg):
                continue  # Already marked exited

            leg_data = orders.get(leg, {})
            symbol = leg_data.get("symbol")
            entry_order_id = leg_data.get("order_id")
            entry_price = leg_data.get("entry_price")

            if not symbol or not entry_price or not entry_order_id:
                continue  # Can't validate without full data

            sl_price = entry_price * (1 + SL_PERCENT / 100)
            exit_found = False
            exit_price = None
            remaining_qty[leg] = leg_data.get("filled_quantity")

            for order in reversed(all_orders):
                if (
                    order.get("tradingsymbol") == symbol
                    and order.get("transaction_type") == "BUY"  # Must be BUY for exit
                    and order.get("status") == "COMPLETE"
                    and order.get("order_id") != entry_order_id
                    and order.get("tag") == ORDER_TAG  # Must match strategy's tag
                ):
                    exit_price = order.get("average_price")
                    exit_found = True
                    remaining_qty[leg] = 0
                    logging.info(f"🔁 Already Exited detected for {leg} at {exit_price}")
                    break

            if exit_found and exit_price is not None:
                sl_triggered[leg] = True
                sl_exit_price[leg] = exit_price
                ltp[leg] = exit_price
                orders[leg]["exit_order_id"] = order.get("order_id")
                orders[leg]["exit_price"] = exit_price
                orders[leg]["exit_qty"] = order.get("filled_quantity")

                mtm = round((entry_price - exit_price) * QUANTITY, 2)

                state.setdefault("sl_triggered", {})[leg] = True
                state.setdefault("sl_exit_price", {})[leg] = exit_price
                #state.setdefault("slippage", {})[leg] = slippage
                state.setdefault("mtm", {})[leg] = mtm
                state.setdefault("ltp", {})[leg] = exit_price

                save_state(state, args.state_file)

                send_telegram_message(
                    f"🔁 Synced manual exit for {leg} at {exit_price}.\n"
                    f"💰 MTM: ₹{mtm}, 📉 Slippage not valid"
                )

    except Exception as e:
        logging.error("❌ Error syncing manual exits from broker", exc_info=True)
        send_telegram_message(f"⚠️ Error syncing manual exits: {str(e).splitlines()[0]}")


def check_sl_with_time_buffer(leg, ltp, sl_price):
    now = datetime.datetime.now()

    if ltp >= sl_price:
        #SL Breach logic has some delays, reverting to original
        return True

        #if leg not in sl_hit_timestamps:
        #    # SL just breached first time
        #    sl_hit_timestamps[leg] = now
        #    logging.info(f"{leg} SL breached at {now}. Waiting {SL_HOLD_TIME_SEC}s before exit.")
        #    return False  # don't exit yet

        #elapsed = (now - sl_hit_timestamps[leg]).total_seconds()
        #if elapsed >= SL_HOLD_TIME_SEC:
        #    logging.info(f"{leg} SL breach held for {elapsed}s. Trigger exit.")
        #    return True  # exit now
        #else:
            # Still in buffer waiting time
        #    return False
    else:
        return False
        # Price went back below SL, reset timer
        #if leg in sl_hit_timestamps:
        #    logging.info(f"{leg} price dropped below SL again, resetting timer.")
        #    del sl_hit_timestamps[leg]
        #return False

def exit_leg_with_retry(leg, exit_qty):
    global remaining_qty
    try:
        symbol = orders[leg].get("symbol")
        entry_price = orders[leg].get("entry_price")
        entry_order_id = orders[leg].get("order_id")

        if not symbol or not entry_price or not entry_order_id:
            logging.warning(f"{leg} missing symbol or entry data; skipping exit")
            return None

        if exit_qty is None:
            logging.warning(f"{remaining_qty[leg]} {exit_qty} is None, setting to QUANTITY")
            exit_qty = QUANTITY

        sl_price = entry_price * (1 + SL_PERCENT / 100)

        if is_test_mode:
            logging.info(f"[SIMULATION] Would place order for {symbol}")
            logging.info("Order Params:")
            logging.info({
                "symbol": symbol,
                "quantity": exit_qty,
                "transaction_type": "BUY"
                })
            return 0

        all_orders = read_latest_orders()
        
        # Check if exit already done by manual order (with tag)
        for o in reversed(all_orders):
            if (
                o.get("tradingsymbol") == symbol
                and o.get("transaction_type") == kite.TRANSACTION_TYPE_BUY
                and o.get("status") == "COMPLETE"
                and o.get("tag") == ORDER_TAG
                and o.get("order_id") != entry_order_id
				and int(o.get("filled_quantity", 0)) == exit_qty
            ):
                exit_price = o.get("average_price")
                remaining_qty[leg] = 0
                logging.info(f"{leg} Exit already detected at {exit_price}, {exit_qty} skipping order placement")
                return exit_price
                
        # Calculate how much qty already exited in exit orders tagged with ORDER_TAG
        filled_qty = sum(int(o.get("filled_quantity", 0)) for o in all_orders if o.get("tradingsymbol") == symbol and o.get("tag") == ORDER_TAG and o.get("order_id") != entry_order_id and o.get("transaction_type") == kite.TRANSACTION_TYPE_BUY and o.get("status") == "COMPLETE")
        remaining_qty[leg] = exit_qty - filled_qty

        if remaining_qty[leg] <= 0:
            logging.info(f"{leg} fully exited already, total filled vs remaining: {filled_qty}, remaining_qty[leg] ")
            exit_price = o.get("average_price")
            return exit_price

        if remaining_qty[leg] > 0 and remaining_qty[leg] <= QUANTITY:
            # Cancel unfilled part
            #kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=limit_order_id)
            #logging.info(f"{leg} cancelled unfilled limit qty {still_remaining}, placing MARKET order for remainder")

            # Place Market order for remainder
            try:
                market_order_id = kite.place_order(
                        variety=kite.VARIETY_REGULAR,
                        exchange="NFO",
                        tradingsymbol=symbol,
                        transaction_type=kite.TRANSACTION_TYPE_BUY,
                        quantity=remaining_qty[leg],
                        order_type=kite.ORDER_TYPE_MARKET,
                        product=kite.PRODUCT_NRML,
                        validity=kite.VALIDITY_DAY,
                        tag=ORDER_TAG
                        )
                logging.info(f"{leg} placed MARKET exit order qty={remaining_qty[leg]} id={market_order_id}")
                exit_price, filled = get_executed_price(kite, market_order_id)
                if exit_price is None or filled is None:
                    logging.warning(f"Failed to fetch executed price for {leg} exit order {market_order_id}. Setting values to 0.")
                    exit_price, filled = 0.0, remaining_qty[leg]

                orders[leg]["exit_order_id"] = market_order_id
                orders[leg]["exit_price"] = exit_price
                orders[leg]["exit_qty"] = filled
                remaining_qty[leg] = remaining_qty[leg] - filled
                logging.info(f"{leg} EXIT ORDER placed. ID: {market_order_id}")
                send_telegram_message(f"{leg} leg exited due to SL hit.")
                log_trade_to_csv(leg, "BUY (SL)", symbol, exit_price, datetime.datetime.now())
                return exit_price
            except Exception as e:
                logging.error(f"{leg} FAILED to place MARKET exit order for qty={remaining_qty[leg]}: {e}")
                return None
        else:
            logging.info(f"{leg} fully exited already, remaining: {remaining_qty[leg]}")
    except Exception as e:
        logging.error(f"Error in exit_leg_with_retry for {leg}: {e}", exc_info=True)
        send_telegram_message(f"⚠️ Error while exiting {leg}: {str(e).splitlines()[0]}")
        return None

def exit_leg(leg, exit_qty):
    global remaining_qty
    try:
        symbol = orders[leg]["symbol"]
        if not symbol:
            logging.warning(f"{leg} has no symbol. Skipping exit.")
            return None

        if exit_qty is None:
            logging.warning(f"{remaining_qty[leg]} {exit_qty} is None, setting to QUANTITY")
            exit_qty = QUANTITY
        
        if is_test_mode:
            logging.info(f"[SIMULATION] Would place order for {symbol}")
            logging.info("Order Params:")
            logging.info({
                "symbol": symbol,
                "quantity": exit_qty,
                "transaction_type": "BUY"
                })
            # Simulated LTP (optional: could use random or fixed value)
            exit_price = 0
        else:
            remaining_qty[leg] = exit_qty
            if remaining_qty[leg] > 0 and remaining_qty[leg] <= QUANTITY:
                order_id = kite.place_order(
                        variety=kite.VARIETY_REGULAR,
                        exchange="NFO",
                        tradingsymbol=orders[leg]["symbol"],
                        transaction_type=kite.TRANSACTION_TYPE_BUY,
                        quantity=exit_qty,
                        product=kite.PRODUCT_NRML,
                        order_type=kite.ORDER_TYPE_MARKET,
                        validity=kite.VALIDITY_DAY,
                        tag=ORDER_TAG
                        )
                exit_price, filled = get_executed_price(kite, order_id)
                if exit_price is None or filled is None:
                    logging.warning(f"Failed to fetch executed price for {leg} exit order {market_order_id}. Setting values to 0.")
                    exit_price, filled = 0.0, remaining_qty[leg]

                orders[leg]["exit_order_id"] = order_id
                orders[leg]["exit_price"] = exit_price
                orders[leg]["exit_qty"] = filled
                remaining_qty[leg] = remaining_qty[leg] - filled
                logging.info(f"{leg} SL EXIT ORDER placed. ID: {order_id}")
                send_telegram_message(f"{leg} leg exited due to SL hit.")
                log_trade_to_csv(leg, "BUY (SL)", symbol, exit_price, datetime.datetime.now())
                return exit_price
            else:
                logging.info(f"{leg} fully exited already, remaining: {remaining_qty[leg]}")
    except Exception as e:
        logging.error(f"Error exiting {leg}", exc_info=True)
        send_telegram_message(f"⚠️ Error while exiting {leg}: {str(e).splitlines()[0]}")    
        return None

def scheduled_exit(args):
    global orders, sl_triggered, sl_exit_price
    logging.info("Scheduled exit started.")
    send_telegram_message("⏰ Scheduled exit triggered. Closing all open legs.")

    try:
        state = load_state(args.state_file)
    except Exception as e:
        logging.error(f"Failed to load state file before exit: {e}", exc_info=True)
        send_telegram_message(f"⚠️ Failed to load state before exit: {str(e).splitlines()[0]}")
        return

    try:
        sync_exit_status_from_broker(kite, args)
    except Exception as e:
        logging.error(f"[SYNC ERROR inside scheduled exit] Failed syncing exit status: {e}")

    
    # Cache references to state parts for clarity
    orders_state = state.get("orders", {})
    sl_triggered_state = state.get("sl_triggered", {"CE": False, "PE": False})
    sl_exit_price_state = state.get("sl_exit_price", {"CE": None, "PE": None})

    for leg in ["CE", "PE"]:
        try:
            leg_data = orders.get(leg, {})
            symbol = leg_data.get("symbol")
            order_id = leg_data.get("order_id")
            exit_qty = remaining_qty[leg]
            sl_hit = sl_triggered.get(leg, False)
            entry_price = leg_data.get("entry_price")

            if symbol and order_id and not sl_hit:
                logging.info(f"Exiting {leg} leg with order_id {order_id}")

                if is_test_mode:
                    logging.info(f"[SIMULATION] Would exit {leg} - Symbol: {symbol}, Order ID: {order_id}")
                    logging.info(f"[SIMULATION] Exited {leg} leg.")
                    orders[leg]["order_id"] = None
                    orders[leg]["entry_price"] = None
                    sl_triggered[leg] = True
                else:
                    try:
                        exit_price = exit_leg(leg, exit_qty)
                        state = load_state(args.state_file)
                        if exit_price is not None:
                            sl_exit_price[leg] = exit_price
                            state["mtm"][leg] = round((entry_price - exit_price) * exit_qty, 2)
                            sl_triggered[leg] = True
                            state["orders"] = orders
                            state["sl_triggered"] = sl_triggered
                            state["sl_exit_price"] = sl_exit_price
                            save_state(state, args.state_file)
                        else:
                            logging.warning(f"Exit price for {leg} was None")
                        
                    except Exception as e:
                        logging.error(f"Error while exiting {leg} leg: {e}", exc_info=True)
                        send_telegram_message(f"⚠️ Error while exiting {leg} leg: {str(e).splitlines()[0]}")
                        continue
            else:
                logging.info(f"Skipping {leg} - Already exited, no order, or SL hit.")
        except Exception as e:
            logging.error(f"Error while exiting {leg} leg: {e}", exc_info=True)
            send_telegram_message(f"⚠️ Error while exiting {leg} leg: {str(e).splitlines()[0]}")
    state = load_state(args.state_file)
    state["orders"] = orders
    state["sl_triggered"] = sl_triggered
    save_state(state, args.state_file)

    logging.info("Scheduled exit completed for all legs.")
    send_telegram_message("✅ Scheduled exit completed. All legs closed.")

def run_strategy(strategy, args):
    global orders, sl_triggered, sl_exit_price,mtm, slippage, selected_strategy, market_snapshot, momo_already_placed
    selected_strategy = strategy
    logging.info(f"Running strategy: {selected_strategy}")

    sl_triggered = {"CE": False, "PE": False}
    sl_exit_price = {"CE": None, "PE": None}
    ltp = {"CE": None, "PE": None}
    mtm = {"CE": None, "PE": None}
    slippage = {"CE": None, "PE": None}
    market_snapshot = {
            "timestamp": None,
            "nifty_spot": None,
            "india_vix": None,
            "iv": {"CE": None, "PE": None}
            }
    momo_fut = {
            "timestamps": [],
            "combined_premiums": [],
            "vwap": []
            }

    # Create state file if missing
    if not os.path.exists(args.state_file):
        initial_state = {
                "orders": {
                    "CE": {"order_id": None, "symbol": None, "entry_price": None, "filled_quantity": None, "exit_order_id": None, "exit_price": None, "exit_qty": None},
                    "PE": {"order_id": None, "symbol": None, "entry_price": None, "filled_quantity": None, "exit_order_id": None, "exit_price": None, "exit_qty": None}
                    },
                "sl_triggered": {"CE": False, "PE": False},
                "ltp": {"CE": None, "PE": None},
                "sl_exit_price": {"CE": None, "PE": None},
                "mtm": {"CE": None, "PE": None},
                "slippage": {"CE": None, "PE": None},
                "market_snapshot": {
                    "timestamp": None,
                    "nifty_spot": None,
                    "india_vix": None,
                    "iv": {"CE": None, "PE": None}
                },
                "momo_fut": {
                    "timestamps": [],
                    "combined_premiums": [],
                    "vwap": []
                    }
                }
        save_state(initial_state, args.state_file)
        logging.info(f"Initial state file created: {args.state_file}")

    state = load_state(args.state_file)
    resumed = False
    if state:
        orders = state.get("orders", {})
        sl_triggered = state.get("sl_triggered", {"CE": False, "PE": False})
        sl_exit_price = state.get("sl_exit_price", {"CE": None, "PE": None})
        slippage = state.get("slippage", {"CE": None, "PE": None})
        mtm = state.get("mtm", {"CE": None, "PE": None})
        ltp = state.get("ltp", {"CE": None, "PE": None})
        market_snapshot = state.get("market_snapshot", {
        "timestamp": None,
        "nifty_spot": None,
        "india_vix": None,
        "iv": {"CE": None, "PE": None}
        })

        
        if orders and all(v.get("order_id") is not None for v in orders.values()):
            logging.info("Orders already placed from previous session. Resuming ticker...")
            resumed = True
            try:
                start_ticker()
                logging.info("📈 Ticker started in resume mode.")
            except Exception as e:
                logging.error("❌ Failed to start ticker on resume.", exc_info=True)
        else:
            logging.info("No active orders found in saved state, starting fresh.")
            orders = state.get("orders", {})
            sl_triggered = {"CE": False, "PE": False}
    else:
        logging.info("No saved state found, starting fresh.")

    entry_time = ENTRY_TIME
    exit_time = EXIT_TIME
    # Sync existing manual orders from broker by tag
    sync_orders_from_broker(kite, args)

    while True:
        now = datetime.datetime.now().time()
        #logging.info(f"Current time: {now}, entry_time: {entry_time}, exit_time: {exit_time}")

        if now >= exit_time:
            logging.info("Exit time reached, executing scheduled exit.")
            send_telegram_message("🛑 Exit time reached. Executing scheduled exit.")
            
            try:
                run_ticks = False
                logging.info("🛑 Stopped on_ticks before scheduled exit.")
                scheduled_exit(args)
                #clear_state(args.state_file)
                logging.info("Scheduled exit completed, state cleared. Exiting strategy loop.")
            except Exception as e:
                logging.error("Error during scheduled exit", exc_info=True)
                send_telegram_message(f"❌ Error during exit: {e}")
            break

        if now >= entry_time and (not orders or not all(v.get("order_id") is not None for v in orders.values())):
            logging.info("Entry time reached, placing momo orders.")
            send_telegram_message("📥 Entry time reached. Placing momo orders...")
            try:
                if momo_already_placed:
                    logging.info("✅ momo already placed today. Skipping order placement.")
                    success = True
                else:
                    success= place_momo_orders(args)
                    logging.info(f"place_momo_orders() returned: {success}")

                if success:
                    try:
                        start_ticker()
                        logging.info("📈 Ticker started post-entry.")
                        send_telegram_message("✅ Orders placed successfully. Ticker started.")
                    except Exception as e:
                        logging.error("❌ Failed to start ticker after order placement.", exc_info=True)
                        send_telegram_message("❌ Orders placed but ticker failed to start.")
                else:
                    logging.warning("Failed to place orders, will retry.")
            except Exception as e:
                logging.error("Error in strategy start", exc_info=True)
                send_telegram_message(f"❌ Error placing orders: {e}")
        time.sleep(5)


def start_ticker():
    global kws, ticker_started

    if ticker_started:
        logging.debug("Ticker already started. Skipping duplicate start.")
        return

    access_token = load_access_token()
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    kws = KiteTicker(api_key, access_token)
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error
    kws.on_reconnect = on_reconnect
    kws.connect(threaded=True)
    # 🧠 Start heartbeat monitor thread
    threading.Thread(target=ticker_heartbeat, daemon=True).start()
    ticker_started = True
    logging.info("Ticker started and heartbeat monitor running.")
    


def exit_gracefully(signum, frame):
    flag = check_exit_or_stop_flag(current_algo_name)

    if flag == "STOP" and signum == signal.SIGBREAK:  # From Stop button (Windows only)
        logging.warning("SIGBREAK STOP received. Closing WebSocket only.")
        if kws:
            try:
                kws.close()
                logging.info("WebSocket closed.")
            except Exception as e:
                logging.error("Error closing WebSocket", exc_info=True)
        sys.exit(0)

    elif flag == "EXIT" and signum == signal.SIGBREAK:
        logging.warning("SIGBREAK EXIT. Closing all trades and exiting...")
        try:
            run_ticks = False
            logging.info("🛑 Stopped on_ticks before scheduled exit.")
            scheduled_exit(args)  # Your function to close all open positions
            #clear_state(args.state_file)
            logging.info("Trades closed and state cleared.")
        except Exception as e:
            logging.error("Error during graceful shutdown", exc_info=True)
        sys.exit(0)
    elif signum == signal.SIGTERM or signum == signal.SIGINT:
        logging.warning("SIGTERM/SIGINT received. Closing all trades and exiting...")
        try:
            run_ticks = False
            logging.info("🛑 Stopped on_ticks before scheduled exit.")
            scheduled_exit(args)  # Your function to close all open positions
            #clear_state(args.state_file)
            logging.info("Trades closed and state cleared.")
        except Exception as e:
            logging.error("Error during graceful shutdown", exc_info=True)
        sys.exit(0)

def sync_open_orders_with_state(kite, state):
    """
    Sync live open orders from broker with local state.
    Only consider orders with tag `algo_order_tag`.
    Update 'orders', 'sl_triggered', and 'entry_price' fields accordingly.
    """
    algo_order_tag = ORDER_TAG
    state_file_path = default_state_file

    try:
        open_orders = read_latest_orders()
    except Exception as e:
        logging.error(f"Failed to fetch open orders: {e}")
        return state  # fallback to whatever is loaded

    if state is None:
        # Initialize minimal structure if needed
        state = {
                "orders": {"CE": {"order_id": None, "symbol": None, "entry_price": None, "filled_quantity": None, "exit_order_id": None, "exit_price": None, "exit_qty": None},
                           "PE": {"order_id": None, "symbol": None, "entry_price": None, "filled_quantity": None, "exit_order_id": None, "exit_price": None, "exit_qty": None}},
                "sl_triggered": {"CE": False, "PE": False},
                "ltp": {"CE": None, "PE": None},
                "sl_exit_price": {"CE": None, "PE": None},
                "mtm": {"CE": None, "PE": None},
                "slippage": {"CE": None, "PE": None},
                "market_snapshot": {
                    "timestamp": None,
                    "nifty_spot": None,
                    "india_vix": None,
                    "iv": {"CE": None, "PE": None}
                    },
                "momo_fut": {
                    "timestamps": [],
                    "combined_premiums": [],
                    "vwap": []
                    }
                }

    orders_state = state.setdefault("orders", {})
    sl_triggered_state = state.setdefault("sl_triggered", {})
     # Reset SL flags by default
    sl_triggered_state["CE"] = False
    sl_triggered_state["PE"] = False

    for order in open_orders:
        # Identify orders that belong to this algo by tag
        # Adjust this field name to what your broker API returns for order tag/remark
        order_tag = order.get("tag") or ""

        if algo_order_tag not in order_tag:
            continue  # skip unrelated orders

        tradingsymbol = order.get("tradingsymbol")
        order_id = order.get("order_id")
        status = order.get("status")  # e.g. OPEN, COMPLETE, CANCELLED
        average_price = order.get("average_price", 0)
        transaction_type = order.get("transaction_type")  # SELL or BUY
        filled_quantity = order.get("filled_quantity")

        
        leg = None
        if "CE" in tradingsymbol:
            leg = "CE"
        elif "PE" in tradingsymbol:
            leg = "PE"
        else:
            continue  # skip non CE/PE orders
        # Update entry order info only for SELL side
        if transaction_type == "SELL":
            orders_state[leg]["order_id"] = order_id
            orders_state[leg]["symbol"] = tradingsymbol
            orders_state[leg]["filled_quantity"] = filled_quantity

            if average_price > 0:
                orders_state[leg]["entry_price"] = average_price
            else:
                logging.info(f"From broker: {tradingsymbol}, {average_price}")
                

 
        # Update sl_triggered: True if order is COMPLETE (assuming SL hit means order closed)
        if transaction_type == "BUY" and status in ["COMPLETE"]:
            sl_triggered_state[leg] = True
            orders_state[leg]["exit_order_id"] = order_id
            orders_state[leg]["exit_price"] = average_price
            orders_state[leg]["exit_qty"] = filled_quantity

    # Save updated state
    save_state(state, state_file_path)
    logging.info(f"Synced orders and sl_triggered flags from broker: {orders_state}, {sl_triggered_state}")

    return state


if __name__ == "__main__":
    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)
    signal.signal(signal.SIGBREAK, exit_gracefully) # Stop button (Windows only)

    parser = argparse.ArgumentParser(description="momofut Algo")
    parser.add_argument("--auto-resume", action="store_true", help="Skip resume prompt and auto-resume")
    parser.add_argument("--no-resume", action="store_true", help="Force fresh start, ignore saved state")
    parser.add_argument("--state-file", type=str, default=default_state_file, help="Path to state file")
    args = parser.parse_args()

    logging.info("Starting momofut Algo Strategy")
    send_telegram_message("Starting momofut Algo")

    try:
        access_token = load_access_token()
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)

        save_nfo_instruments_to_csv(kite)
        build_symbol_token_map(kite)
        token_to_symbol = load_token_to_symbol_map()
    except Exception as e:
        logging.error(f"❌ Failed during setup: {e}")
        send_telegram_message(f"❌ Setup failed: {e}")
        exit(1)

    saved_state = load_state(args.state_file)

    if saved_state:
        if args.auto_resume:
            resume = True
            logging.info("Resuming from saved state (--resume flag given).")
        elif args.no_resume:
            resume = False
            clear_state(args.state_file)
            saved_state = None
            logging.info("Not resuming (--no-resume flag given). Starting fresh.")
        else:
            if is_interactive():
                resume = prompt_resume()
                if not resume:
                    clear_state(args.state_file)
                    saved_state = None
                    logging.info("User chose not to resume. Starting fresh.")
            else:
                resume = False 
                logging.info("Non-interactive mode without flags - starting fresh by default.")
                clear_state(args.state_file)
                saved_state = None
    else:
        resume = False
        logging.info("No saved state found. Starting fresh.")

    if resume and saved_state is not None:
        saved_state = sync_open_orders_with_state(kite, saved_state)

    selected_strategy = "momofut" 
    if saved_state is None:
        logging.info(f"Selected strategy: {selected_strategy}")
    else:
        logging.info(f"Resuming strategy: {selected_strategy}") 
    run_thread = threading.Thread(target=run_strategy, args=(selected_strategy,args))
    run_thread.start()

    while run_thread.is_alive():
        run_thread.join(timeout=1)
