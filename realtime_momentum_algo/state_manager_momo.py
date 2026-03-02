import json
import os
import logging
import configparser
from datetime import time
import tempfile
import shutil
import threading

config = configparser.ConfigParser()
config.read("config_momo.ini")

state_file_lock = threading.Lock()

def save_state(data, state_file):
    try:
        with state_file_lock:  # Ensure only one thread writes at a time
            # Write to a temporary file first
            dir_name = os.path.dirname(state_file)
            with tempfile.NamedTemporaryFile('w', dir=dir_name, delete=False) as tf:
                json.dump(data, tf, indent=2)
                temp_name = tf.name

            # Atomically replace the original file
            shutil.move(temp_name, state_file)

            # logging.info(f"State saved to {state_file}")
    except Exception as e:
        logging.error(f"Error saving state: {e}", exc_info=True)


def load_state(state_file):
    if not os.path.exists(state_file):
        logging.info("State file not found, returning None.")
        return None

    try:
        with state_file_lock:  # Protect the read operation
            with open(state_file, "r") as f:
                state = json.load(f)

        if "orders" not in state:
            state["orders"] = {}
        if "sl_triggered" not in state:
            state["sl_triggered"] = {"CE": False, "PE": False}

        for leg in ["CE", "PE"]:
            state["orders"].setdefault(leg, {
                "order_id": None,
                "symbol": None,
                "entry_price": None
            })
            state["sl_triggered"].setdefault(leg, False)

        return state

    except Exception as e:
        logging.error(f"Error loading state: {e}", exc_info=True)
        return None

def clear_state(state_file):
    try:
        if os.path.exists(state_file):
            os.remove(state_file)
            logging.info(f"State file {state_file} cleared.")
    except Exception as e:
        logging.error(f"Error clearing state: {e}", exc_info=True)
