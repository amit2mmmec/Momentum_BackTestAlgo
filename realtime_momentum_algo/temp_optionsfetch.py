from kiteconnect import KiteConnect
import math
import datetime
import csv
import pandas as pd

api_key = "mention here zerodhaapikey"

def load_access_token(file_path="access_token.txt"):
    try:
        with open(file_path, "r") as f:
            token = f.read().strip()
            print(f"Access Token: {token}")
            return token
    except FileNotFoundError:
        print("access_token.txt not found.")
        exit(1)

def get_next_expiry():
    today = datetime.date.today()
    weekday = today.weekday()
    days_to_tuesday = (1 - weekday) % 7
    expiry = today + datetime.timedelta(days=days_to_tuesday)
    return expiry

def get_nifty_option_chain():
    access_token = load_access_token()
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    try:
        spot = kite.ltp("NSE:NIFTY 50")["NSE:NIFTY 50"]["last_price"]
        print(f"NIFTY Spot Price: {spot}")

        atm_strike = round(spot / 50) * 50
        print(f"ATM Strike: {atm_strike}")

        expiry_date = get_next_expiry()
        expiry_str = expiry_date.strftime("%Y-%m-%d")
        print(f"Weekly Expiry (standard): {expiry_str}")

        print("Fetching instrument dump... (this may take a few seconds)")
        instruments = kite.instruments("NFO")

        ce_token = None
        pe_token = None

        for inst in instruments:
            if (
                inst["name"] == "NIFTY"
                and inst["expiry"] == expiry_date
                and inst["strike"] == atm_strike
                and inst["instrument_type"] == "CE"
            ):
                ce_token = inst["instrument_token"]
            if (
                inst["name"] == "NIFTY"
                and inst["expiry"] == expiry_date
                and inst["strike"] == atm_strike
                and inst["instrument_type"] == "PE"
            ):
                pe_token = inst["instrument_token"]

        if not ce_token or not pe_token:
            print("ATM CE or PE instrument not found for expiry or strike.")
            return

        ltp_data = kite.ltp([ce_token, pe_token])
        ce_ltp = ltp_data[str(ce_token)]["last_price"]
        pe_ltp = ltp_data[str(pe_token)]["last_price"]

        print(f"ATM CE LTP: {ce_ltp}")
        print(f"ATM PE LTP: {pe_ltp}")

    except Exception as e:
        print("Error fetching option chain:", e)
		
def save_nfo_instruments_to_csv():
    access_token = load_access_token()
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    try:
        print("Fetching NFO instrument dump...")
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

        print(f"Instrument dump saved to {filename}")

    except Exception as e:
        print("Error fetching/saving instrument dump:", e)

def fetch_vix_1min():
    access_token = load_access_token()
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    vix_token = 264969  # INDIA VIX spot

    # Define start and end
    start_date = datetime.date(2025, 7, 1)
    end_date = datetime.date.today()

    # Split into chunks of max 60 days
    chunk_start = start_date
    all_data = []

    while chunk_start <= end_date:
        chunk_end = min(chunk_start + datetime.timedelta(days=60), end_date)
        print(f"Fetching VIX {chunk_start} to {chunk_end}...")

        data = kite.historical_data(
            instrument_token=vix_token,
            from_date=chunk_start,
            to_date=chunk_end,
            interval="minute"
        )

        all_data.extend(data)
        chunk_start = chunk_end + datetime.timedelta(days=1)

    df = pd.DataFrame(all_data)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df[["date", "open", "high", "low", "close", "volume"]]
    df.to_csv("indiavix_1min_jul_sep.csv", index=False)
    print("Saved 1-minute OHLC data to indiavix_1min_jul_sep.csv")
    print(df.head(10))

def fetch_nifty_1min():
    access_token = load_access_token()
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    nifty_token = 256265  # NIFTY 50 spot

    # Define start and end
    start_date = datetime.date(2025, 7, 1)
    end_date = datetime.date.today()

    # Split into chunks of max 60 days
    chunk_start = start_date
    all_data = []

    while chunk_start <= end_date:
        chunk_end = min(chunk_start + datetime.timedelta(days=60), end_date)
        print(f"Fetching {chunk_start} to {chunk_end}...")

        data = kite.historical_data(
            instrument_token=nifty_token,
            from_date=chunk_start,
            to_date=chunk_end,
            interval="minute"
        )

        all_data.extend(data)
        chunk_start = chunk_end + datetime.timedelta(days=1)

    df = pd.DataFrame(all_data)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df[["date", "open", "high", "low", "close", "volume"]]
    df.to_csv("nifty50_1min_jul_sep.csv", index=False)
    print("Saved 1-minute OHLC data to nifty50_1min_jul_sep.csv")
    print(df.head(10))


def fetch_nifty_ohlc():
    access_token = load_access_token()
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    # NIFTY 50 instrument token (spot index)
    nifty_token = 256265

    # Date range: July 1st to today
    from_date = datetime.date(2025, 7, 1)
    to_date = datetime.date.today()

    print(f"Fetching NIFTY 50 OHLC from {from_date} to {to_date}...")
    data = kite.historical_data(
        instrument_token=nifty_token,
        from_date=from_date,
        to_date=to_date,
        interval="day"
    )

    df = pd.DataFrame(data)
    df.to_csv("nifty50_jul_sep.csv", index=False)
    print("Saved OHLC data to nifty50_jul_sep.csv")
		
if __name__ == "__main__":
    get_nifty_option_chain()
    save_nfo_instruments_to_csv()
    #fetch_nifty_ohlc()
    fetch_nifty_1min()
    fetch_vix_1min()
    

