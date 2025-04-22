from pybit.unified_trading import WebSocket
from datetime import datetime, timedelta
from threading import Lock
from time import sleep
import smtplib
from email.mime.text import MIMEText
import pytz
import os
import schedule
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()

from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "✅ I'm alive"

def run_web():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run_web)
    t.daemon = True
    t.start()

# Malaysia Timezone
TIMEZONE = pytz.timezone("Asia/Kuala_Lumpur")

# Trade storage
trade_data = []
lock = Lock()

# WebSocket setup
ws = WebSocket(
    testnet=False,
    channel_type="spot",
)

# Heartbeat interval (seconds)
HEARTBEAT_INTERVAL = 60  # Send a heartbeat every 90 seconds

# Send heartbeat to keep the instance alive
def send_heartbeat():
    print("💓 Heartbeat: Keeping the instance alive.")
    sleep(HEARTBEAT_INTERVAL)
    send_heartbeat()  # Call itself to repeat every interval

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
        # Keep only last 24h of data
        cutoff = datetime.now(TIMEZONE) - timedelta(hours=24)
        trade_data[:] = [t for t in trade_data if t["timestamp"] > cutoff]

def send_email(subject, body):
    sender = os.getenv("EMAIL_SENDER")
    recipients = os.getenv("EMAIL_RECIPIENTS").split(",")  # Split the string into a list of email addresses
    password = os.getenv("EMAIL_PASSWORD")

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)  # Join the list of recipients into a string for the "To" header

    try:
        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.login(sender, password)
        server.sendmail(sender, recipients, msg.as_string())  # Send to multiple recipients
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

        # Count how many unique minutes had at least one trade
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

# Schedule report at 6AM and 6PM Malaysia Time
schedule.every().day.at("06:00").do(aggregate_and_alert)
schedule.every().day.at("14:00").do(aggregate_and_alert)

keep_alive()

# Start WebSocket
ws.trade_stream(symbol="MONUSDT", callback=handle_message)

# Start the heartbeat function to keep the instance alive
send_heartbeat()

# Main loop
while True:
    schedule.run_pending()
    sleep(1)
