from pybit.unified_trading import WebSocket
from datetime import datetime, timedelta
from threading import Lock, Thread
from time import sleep
from flask import Flask
import smtplib
from email.mime.text import MIMEText
import pytz
import os
import schedule
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

def start_websocket():
    global ws
    while True:
        try:
            ws = WebSocket(testnet=False, channel_type="spot")
            ws.trade_stream(symbol="MONUSDT", callback=handle_message)
            print("🔌 WebSocket connected.")
            break
        except Exception as e:
            print(f"❌ WebSocket connection failed: {e}")
            sleep(5)

def handle_message(message):
    if 'data' not in message:
        return

    trade = message['data']
    timestamp = datetime.fromtimestamp(trade['T'] / 1000, tz=pytz.utc).astimezone(TIMEZONE)
    side = trade['S']
    qty = float(trade['v'])
    price = float(trade['p'])

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
        cutoff = now - timedelta(hours=24)
        recent = [t for t in trade_data if t["timestamp"] > cutoff]

        buy_volume = sum(t["qty"] for t in recent if t["side"] == "Buy")
        sell_volume = sum(t["qty"] for t in recent if t["side"] == "Sell")
        usd_volume = sum(t["qty"] * t["price"] for t in recent)

        minute_buckets = defaultdict(int)
        for t in recent:
            minute_key = t["timestamp"].replace(second=0, microsecond=0)
            minute_buckets[minute_key] += 1

        trading_freq = (len(minute_buckets) / 1440) * 100

    body = (
        f"Time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
        f"Buy Volume: {buy_volume:.2f}\n"
        f"Sell Volume: {sell_volume:.2f}\n"
        f"USD Volume: {usd_volume:.2f}\n"
        f"Trading Frequency (last 24h): {trading_freq:.2f}%"
    )
    send_email("🪙 Bybit MONUSDT 24h Report", body)

def schedule_loop():
    schedule.every().day.at("06:00").do(aggregate_and_alert)
    schedule.every().day.at("15:30").do(aggregate_and_alert)

    while True:
        schedule.run_pending()
        sleep(1)

def run_web():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

def start():
    # Run Flask app and tasks in parallel threads
    Thread(target=run_web).start()
    Thread(target=start_websocket).start()
    Thread(target=schedule_loop).start()

if __name__ == "__main__":
    start()
