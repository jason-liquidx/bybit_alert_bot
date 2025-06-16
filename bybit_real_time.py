from pybit import WebSocket
from datetime import datetime, timedelta
from threading import Lock, Thread
from time import sleep
from flask import Flask
import smtplib
from email.mime.text import MIMEText
import pytz
import os
import schedule
import logging
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

# Setup timezone
TIMEZONE = pytz.timezone("Asia/Kuala_Lumpur")

# Flask app
app = Flask(__name__)

@app.route("/")
def home():
    return "✅ Heartbeat check — service is running."

# Trade storage
trade_data = []
lock = Lock()
ws = None

# Configure logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("pybit._websocket_stream").setLevel(logging.ERROR)

def log_heartbeat():
    logging.info("✅ Heartbeat check — service is running.")

def handle_message(message):
    print("📥", message)

def start_websocket():
    while True:
        try:
            ws = WebSocket(
                endpoint="wss://stream.bybit.com/spot/quote/ws/v1",  # Spot endpoint
                subscriptions=["trade.MONUSDT"]
            )
            ws.socket.on_message = lambda ws_, msg: handle_message(eval(msg))
            print("✅ WebSocket connected")
            while True:
                sleep(60)
        except Exception as e:
            print("❌ WebSocket error:", e)
            sleep(5)


def handle_message(message):
    if 'data' not in message:
        return

    trades = message['data']
    if isinstance(trades, dict):  # single trade
        trades = [trades]

    for trade in trades:
        timestamp = datetime.fromtimestamp(trade['T'] / 1000, tz=pytz.utc).astimezone(TIMEZONE)
        side = trade['S']
        qty = float(trade['v'])
        price = float(trade['p'])

        print(f"📥 Trade | {timestamp.strftime('%H:%M:%S')} | Side: {side} | Qty: {qty} | Price: {price}", flush=True)

        with lock:
            trade_data.append({
                "timestamp": timestamp,
                "side": side,
                "qty": qty,
                "price": price,
            })

            # Keep only the last 24h of data
            cutoff = datetime.now(TIMEZONE) - timedelta(hours=24)
            trade_data[:] = [t for t in trade_data if t["timestamp"] > cutoff]

def send_email(subject, body):
    sender = os.getenv("EMAIL_SENDER")
    recipients = os.getenv("EMAIL_RECIPIENTS").split(",")
    password = os.getenv("EMAIL_PASSWORD")

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    try:
        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.login(sender, password)
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        print("✅ Email sent.")
    except Exception as e:
        print(f"❌ Email error: {e}")

def aggregate_and_alert():
    with lock:
        now = datetime.now(TIMEZONE)

        if now.hour == 6:
            cutoff = now - timedelta(hours=6)
            window_desc = "Past 6 hours (from 12:00 AM to 6:00 AM)"
            max_minutes = 6 * 60
        elif now.hour == 18:
            cutoff = now - timedelta(hours=18)
            window_desc = "Past 18 hours (from 12:00 AM to 6:00 PM)"
            max_minutes = 18 * 60
        else:
            cutoff = now - timedelta(hours=24)
            window_desc = "Fallback: Past 24 hours"
            max_minutes = 24 * 60

        recent = [t for t in trade_data if t["timestamp"] > cutoff]

        buy_volume = sum(t["qty"] for t in recent if t["side"] == "Buy")
        sell_volume = sum(t["qty"] for t in recent if t["side"] == "Sell")
        usd_volume = sum(t["qty"] * t["price"] for t in recent)

        minute_buckets = defaultdict(int)
        for t in recent:
            minute_key = t["timestamp"].replace(second=0, microsecond=0)
            minute_buckets[minute_key] += 1

        trading_freq = (len(minute_buckets) / max_minutes) * 100

        body = (
            f"Time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
            f"{window_desc}\n\n"
            f"📊 Buy Volume: {int(buy_volume):,}\n"
            f"📉 Sell Volume: {int(sell_volume):,}\n"
            f"💵 USD Volume: {int(usd_volume):,}\n"
            f"📈 Trading Frequency: {trading_freq:.2f}%"
        )
    send_email("🪙 Bybit MONUSDT Report", body)

def schedule_loop():
    schedule.every().day.at("10:00").do(aggregate_and_alert)
    schedule.every().day.at("22:00").do(aggregate_and_alert)
    while True:
        schedule.run_pending()
        sleep(1)

def schedule_heartbeat():
    schedule.every(1).minute.do(log_heartbeat)

def run_web():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

def start():
    Thread(target=run_web).start()
    Thread(target=start_websocket).start()
    Thread(target=schedule_loop).start()
    Thread(target=schedule_heartbeat).start()

if __name__ == '__main__':
    start()

