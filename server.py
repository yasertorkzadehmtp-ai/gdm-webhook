# GDM_Server_Render_v3_i6b_TelegramSanitize_FIX
# Fixes unterminated string literal bug from i6
# Purpose: Strip inline LOG:{...} from Telegram messages while preserving full payload for CSV/logs

from flask import Flask, request, jsonify
import requests
import json
import os
import time

app = Flask(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg
    }
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Telegram send error: {e}")

def sanitize_for_telegram(raw: str) -> str:
    # Remove inline LOG:{...} if present
    if " LOG:{" in raw:
        return raw.split(" LOG:{", 1)[0].strip()
    return raw.strip()

@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    raw = request.data.decode("utf-8", errors="ignore")

    # Always keep full raw for logging
    print(raw)

    clean = sanitize_for_telegram(raw)

    # Skip pure LOG-only messages
    if clean.startswith("LOG:"):
        return jsonify({"status": "log-only"}), 200

    # Send sanitized message to Telegram
    send_telegram(clean)

    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
