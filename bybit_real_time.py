from pybit.unified_trading import WebSocket
from datetime import datetime, timedelta
from threading import Lock, Thread, Event
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

TIMEZONE = pytz.timezone("Asia/Kuala_Lumpur")
app = Flask(__name__)

@app.route("/")
def home():
    return "✅ Heartbeat check — service is running."

trade_data = []
lock = Lock()
last_trade_time = datetime.now(TIMEZONE)
ws = None
reconnect_event = Event()

# Logging
logging.basicConfig(level=logging.INFO)
def log_heartbeat():
    logging.info("✅ Heartbeat check — service is running.")

# Handle trade messages
def handle_trade_message(message):
    global last_trade_time
    if 'data' not in message:
        return

    trades = message['data']
    if isinstance(trades, dict):
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
            cutoff = datetime.now(TIMEZONE) - timedelta(hours=24)
            trade_data[:] = [t for t in trade_data if t["timestamp"] > cutoff]
            last_trade_time = datetime.now(TIMEZONE)

# WebSocket connection handler with auto-reconnect
def run_websocket():
    global ws
    while True:
        try:
            ws = WebSocket(testnet=False, channel_type="spot")
            ws.trade_stream(symbol="MONUSDT", callback=handle_trade_message)
            print("✅ WebSocket connected")
            while not reconnect_event.wait(timeout=60):
                pass
        except Exception as e:
            print("❌ WebSocket error:", e)
        finally:
            if ws:
                try:
                    ws.stop()
                    print("🔌 WebSocket stopped")
                except Exception as e:
                    print("❌ Error stopping WebSocket:", e)
                ws = None
            print("🔁 Reconnecting WebSocket in 5 seconds...")
            sleep(5)
            reconnect_event.clear()

# WebSocket liveness watchdog
def websocket_watchdog():
    while True:
        sleep(60)
        now = datetime.now(TIMEZONE)
        if (now - last_trade_time).total_seconds() > 300:
            print("⚠️ No trade in 5 minutes. Triggering reconnect...")
            reconnect_event.set()

# Email sending function
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

# Scheduled email reports
def aggregate_and_alert():
    with lock:
        now = datetime.now(TIMEZONE)
        if now.hour == 6:
            cutoff = now - timedelta(hours=6)
            window_desc = "Past 6 hours (12 AM to 6 AM)"
            max_minutes = 6 * 60
        elif now.hour == 18:
            cutoff = now - timedelta(hours=18)
            window_desc = "Past 18 hours (12 AM to 6 PM)"
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

# Scheduling tasks
def schedule_loop():
    schedule.every().day.at("10:00").do(aggregate_and_alert)
    schedule.every().day.at("22:00").do(aggregate_and_alert)
    while True:
        schedule.run_pending()
        sleep(1)

def schedule_heartbeat():
    schedule.every(1).minute.do(log_heartbeat)

# Entry point
def start():
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))).start()
    Thread(target=run_websocket, daemon=True).start()
    Thread(target=websocket_watchdog, daemon=True).start()
    Thread(target=schedule_loop, daemon=True).start()
    Thread(target=schedule_heartbeat, daemon=True).start()

if __name__ == "__main__":
    start()
