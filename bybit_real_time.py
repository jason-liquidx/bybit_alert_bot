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
import requests
import json
from supabase import create_client

# === Supabase Credentials ===
SUPABASE_URL = "https://sjsezcsfevhehgprugye.supabase.co"
SUPABASE_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNqc2V6Y3NmZXZoZWhncHJ1Z3llIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTIwMjgxNzAsImV4cCI6MjA2NzYwNDE3MH0.kDcrtyBoNmHcu2AItmf3HKXzS8AuIfzCt8_hWx_DYdg"
supabase = create_client(SUPABASE_URL, SUPABASE_API_KEY)

# === Timezone and Flask App ===
TIMEZONE = pytz.timezone("Asia/Kuala_Lumpur")
app = Flask(__name__)
@app.route("/")
def home():
    return "✅ Heartbeat check — service is running."

# === Global State ===
trade_data = []
lock = Lock()
last_trade_time = datetime.now(TIMEZONE)
ws = None
reconnect_event = Event()

# === Logging ===
logging.basicConfig(level=logging.INFO)
def log_heartbeat():
    logging.info("✅ Heartbeat check — service is running.")

# === Supabase Insert Function ===
def insert_to_supabase(timestamp, side, qty, price, symbol="MONUSDT", source="bybit"):
    headers = {
        "apikey": SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "ts": timestamp.isoformat(),
        "side": side,
        "qty": qty,
        "price": price,
        "symbol": symbol,
        "source": source
    }
    try:
        resp = requests.post(f"{SUPABASE_URL}/rest/v1/trades", headers=headers, data=json.dumps(payload))
        if resp.status_code not in [200, 201]:
            print(f"❌ Supabase insert failed: {resp.status_code} {resp.text}")
    except Exception as e:
        print("❌ Error sending to Supabase:", e)

def fetch_recent_trades_from_supabase(cutoff):
    response = supabase.table("monusdt_trades") \
        .select("*") \
        .gte("timestamp", cutoff.isoformat()) \
        .execute()

    return response.data if response.data else []
    
# === Handle Trades ===
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

        insert_to_supabase(timestamp, side, qty, price)

# === WebSocket Runner ===
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

# === WebSocket Watchdog (with reboot on Render) ===
def websocket_watchdog():
    while True:
        sleep(60)
        now = datetime.now(TIMEZONE)
        if (now - last_trade_time).total_seconds() > 300:
            print("⚠️ No trade in 5 minutes. Triggering reconnect and Render reboot...")
            reconnect_event.set()
            sleep(10)
            os.system("kill 1")  # Trigger reboot on Render

# === Email Sender ===
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

# === Scheduled Alert ===
def aggregate_and_alert():
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

    # ✅ Fetch from Supabase instead of in-memory
    recent = fetch_recent_trades_from_supabase(cutoff)

    buy_volume = sum(float(t["qty"]) for t in recent if t["side"] == "Buy")
    sell_volume = sum(float(t["qty"]) for t in recent if t["side"] == "Sell")
    usd_volume = sum(float(t["qty"]) * float(t["price"]) for t in recent)

    minute_buckets = defaultdict(int)
    for t in recent:
        ts = datetime.fromisoformat(t["timestamp"])
        minute_key = ts.replace(second=0, microsecond=0)
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


# === Schedulers ===
def schedule_loop():
    schedule.every().day.at("10:00").do(aggregate_and_alert)
    schedule.every().day.at("22:00").do(aggregate_and_alert)
    while True:
        schedule.run_pending()
        sleep(1)

def schedule_heartbeat():
    schedule.every(1).minute.do(log_heartbeat)

# === Start Everything ===
def start():
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))).start()
    Thread(target=run_websocket, daemon=True).start()
    Thread(target=websocket_watchdog, daemon=True).start()
    Thread(target=schedule_loop, daemon=True).start()
    Thread(target=schedule_heartbeat, daemon=True).start()

if __name__ == "__main__":
    start()
