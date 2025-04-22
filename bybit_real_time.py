from pybit.unified_trading import WebSocket
from datetime import datetime, timedelta
from threading import Timer, Lock
from time import sleep
import smtplib
from email.mime.text import MIMEText
import pytz
import os
from dotenv import load_dotenv
load_dotenv()

# Timezone
TIMEZONE = pytz.utc

# Trade storage
trade_data = []
lock = Lock()

# WebSocket setup
ws = WebSocket(
    testnet=False,
    channel_type="spot",
)

def handle_message(message):
    if 'data' not in message:
        return

    trade = message['data']
    timestamp = datetime.fromtimestamp(trade['T'] / 1000, tz=TIMEZONE)
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
        cutoff = datetime.now(tz=TIMEZONE) - timedelta(hours=24)
        trade_data[:] = [t for t in trade_data if t["timestamp"] > cutoff]

def send_email(subject, body):
    sender = os.getenv("EMAIL_SENDER")
    recipient = os.getenv("EMAIL_RECIPIENT")
    password = os.getenv("EMAIL_PASSWORD")

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    try:
        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.login(sender, password)
        server.sendmail(sender, recipient, msg.as_string())
        server.quit()
        print("✅ Email sent.")
    except Exception as e:
        print(f"❌ Email error: {e}")

def aggregate_and_alert():
    with lock:
        now = datetime.now(tz=TIMEZONE)
        cutoff = now - timedelta(hours=24)
        recent = [t for t in trade_data if t["timestamp"] > cutoff]

        buy_volume = sum(t["qty"] for t in recent if t["side"] == "Buy")
        sell_volume = sum(t["qty"] for t in recent if t["side"] == "Sell")
        usd_volume = sum(t["qty"] * t["price"] for t in recent)
        total = len(recent)
        buy_freq = (sum(1 for t in recent if t["side"] == "Buy") / total * 100) if total else 0

    body = (
        f"Time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
        f"Buy Volume: {buy_volume:.2f}\n"
        f"Sell Volume: {sell_volume:.2f}\n"
        f"USD Volume: {usd_volume:.2f}\n"
        f"Buy Frequency: {buy_freq:.2f}%"
    )
    send_email("🪙 MONUSDT 24h Report", body)

    # Schedule next alert
    Timer(0.02 * 3600, aggregate_and_alert).start()

# Start WebSocket and reporting
ws.trade_stream(symbol="MONUSDT", callback=handle_message)
Timer(0.02 * 3600, aggregate_and_alert).start()

while True:
    sleep(1)


