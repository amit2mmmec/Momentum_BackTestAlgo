import requests
import configparser
from datetime import time

config = configparser.ConfigParser()
config.read("config_momo.ini")
TELEGRAM_BOT_TOKEN = config["TELEGRAM"]["BOT_TOKEN"]
TELEGRAM_CHAT_ID = config["TELEGRAM"]["CHAT_ID"]

def send_telegram_message(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
        response = requests.post(url, data=payload, timeout=5)
        if response.status_code != 200:
            logging.error(f"Telegram API error: {response.status_code} - {response.text}")
            return False
        return True
    except Exception as e:
        print(f"Telegram error: {e}")
        return False
