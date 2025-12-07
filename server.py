import os
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL_ID = os.environ.get("CHANNEL_ID")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")

TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def normalize_symbol(ticker: str) -> str:
    if not ticker:
        return "UNKNOWN"
    if ":" in ticker:
        _, symbol = ticker.split(":", 1)
    else:
        symbol = ticker
    if "." in symbol:
        symbol = symbol.split(".", 1)[0]
    return symbol


def parse_alert_name(alert_name: str):
    if not alert_name:
        return "UNKNOWN", "UNKNOWN"
    a = alert_name.upper()
    if "LONG" in a and "ENTRY" in a:
        return "LONG", "ENTRY"
    if "SHORT" in a and "ENTRY" in a:
        return "SHORT", "ENTRY"
    if "EXIT LONG" in a:
        return "LONG", "EXIT"
    if "EXIT SHORT" in a:
        return "SHORT", "EXIT"
    return "UNKNOWN", "UNKNOWN"


def build_message(data: dict) -> str:
    ticker = data.get("ticker")
    price = data.get("price", "0")
    alert_name = data.get("alert_name", "")
    time_str = data.get("time", "")

    symbol = normalize_symbol(ticker)
    side, sig_type = parse_alert_name(alert_name)

    msg = (
        "GDM 5.5.5 SIGNAL\n"
        f"SYMBOL: {symbol}\n"
        f"SIDE: {side}\n"
        f"TYPE: {sig_type}\n"
        f"PRICE: {price}\n"
        f"TIME: {time_str}\n"
        "LEV: 18x\n"
        "SIZE: 2%\n"
        "MODE: ISOLATED ONE-WAY\n"
    )
    return msg


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        payload = request.get_json(force=True)
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 400

    if payload.get("token") != WEBHOOK_SECRET:
        return jsonify({"status": "forbidden"}), 403

    text = build_message(payload)

    try:
        resp = requests.post(
            TELEGRAM_URL,
            json={"chat_id": CHANNEL_ID, "text": text},
            timeout=10
        )
        if resp.status_code != 200:
            return jsonify({"status": "telegram_failed", "detail": resp.text}), 500
    except Exception as e:
        return jsonify({"status": "telegram_error", "detail": str(e)}), 500

    return jsonify({"status": "ok"}), 200


@app.route("/test", methods=["GET"])
def test():
    dummy = {
        "ticker": "BYBIT:SOLUSDT.P",
        "price": "123.45",
        "alert_name": "GDM LONG ENTRY",
        "time": "MANUAL TEST"
    }
    text = build_message(dummy)

    resp = requests.post(
        TELEGRAM_URL,
        json={"chat_id": CHANNEL_ID, "text": text}
    )
    if resp.status_code != 200:
        return f"Telegram failed: {resp.text}", 500
    return "Test message sent to Telegram.", 200

@app.route("/debug-env", methods=["GET"])
def debug_env():
    # DO NOT share this output with anyone; it's just for you.
    bot_token_present = BOT_TOKEN is not None and BOT_TOKEN.strip() != ""
    token_len = len(BOT_TOKEN) if BOT_TOKEN else 0
    return (
        f"BOT_TOKEN present: {bot_token_present}, length: {token_len}<br>"
        f"CHANNEL_ID: {repr(CHANNEL_ID)}"
    )

if __name__ == "__main__":
    # For local run; Render will use gunicorn
    app.run(host="0.0.0.0", port=10000)
