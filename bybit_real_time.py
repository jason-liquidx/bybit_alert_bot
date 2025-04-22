from pybit.unified_trading import WebSocket
from datetime import datetime, timedelta
from threading import Lock, Thread
from time import sleep
from flask import Flask
import schedule
import smtplib
from email.mime.text import MIMEText
import pytz
import os
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()

# Malaysia Timezone
TIMEZONE = pytz.timezone("Asia/Kuala_Lumpur")

# Email setup
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_RECIPIENTS = os.getenv("EMAIL_RECIPIENTS", "").split(",")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

# Global trade data store
trade_data = []
lock = Lock()

# Flask setup
app = Flask(__name__)

@app.route('/')
def home():
    return "✅ Bot is running"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
    
# Email alert function
def send_email(subject, body):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(EMAIL_RECIPIENTS)

    try:
        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENTS, msg.as_string())
        server.quit()
        print("✅ Email sent.")
    except Exception as e:
        print(f"❌ Email error: {e}")

# Aggregation logic
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

# Schedule report
schedule.every().day.at("06:00").do(aggregate_and_alert)
schedule.every().day.at("15:15").do(aggregate_and_alert)

# WebSocket logic
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
        cutoff = datetime.now(TIMEZONE) - timedelta(hours=24)
        trade_data[:] = [t for t in trade_data if t["timestamp"] > cutoff]

def run_ws():
    while True:
        try:
            ws = WebSocket(testnet=False, channel_type="spot")
            ws.trade_stream(symbol="MONUSDT", callback=handle_message)
            print("🔌 WebSocket connected.")
            while True:
                sleep(1)
        except Exception as e:
            print(f"⚠️ WebSocket error: {e}")
            sleep(5)
            print("🔁 Reconnecting...")

# Schedule loop
def run_schedule():
    while True:
        schedule.run_pending()
        sleep(1)

# Start everything
if __name__ == "__main__":
    Thread(target=run_web).start()
    Thread(target=run_ws).start()
    Thread(target=run_schedule).start()
