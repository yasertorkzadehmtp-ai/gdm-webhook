import os
import logging
from flask import Flask, request, jsonify
import requests

# ---------------------------------------------------------
# Basic setup
# ---------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gdm-webhook")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logger.warning("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set in env vars!")

TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def build_cornix_style_message(raw_text: str) -> str:
    """
    raw_text is what comes from TradingView via alert():
        Example:

        BTC/USDT LONG
        Entry: 3025.5

        Exchange: Bybit

    We convert it to something like:

        BTC/USDT 📈 BUY
        Enter 3025.5

        📍Bybit
    """
    text = raw_text.strip()
    if not text:
        return "Empty alert received."

    lines = [l for l in text.splitlines() if l.strip() != ""]
    if not lines:
        return "Empty alert received."

    # Header: PAIR + ACTION
    header = lines[0].strip()           # e.g. "BTC/USDT LONG"
    parts  = header.split()
    pair   = parts[0] if parts else "UNKNOWN"
    side   = parts[1].upper() if len(parts) > 1 else "UNKNOWN"

    # Default values
    emoji = "🔻"
    word  = "CLOSE"
    if side == "LONG":
        emoji = "📈"
        word  = "BUY"
    elif side == "SHORT":
        emoji = "📉"
        word  = "SELL"

    title_line = f"{pair} {emoji} {word}"

    # Find price line ("Entry:" or "Exit:")
    price_line = ""
    for l in lines[1:]:
        ls = l.strip()
        if ls.lower().startswith("entry:"):
            price_line = ls.split(":", 1)[1].strip()
            verb = "Enter"
            break
        if ls.lower().startswith("exit:"):
            price_line = ls.split(":", 1)[1].strip()
            verb = "Close at"
            break

    body_line = f"{verb} {price_line}" if price_line else ""

    # Exchange line (optional)
    exchange_line = "📍Bybit"

    result_lines = [title_line]
    if body_line:
        result_lines.append(body_line)
    result_lines.append("")             # blank line
    result_lines.append(exchange_line)

    return "\n".join(result_lines)


def send_telegram_message(text: str) -> dict:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID.")
        return {"ok": False, "error": "Missing Telegram credentials"}

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    resp = requests.post(TELEGRAM_API_URL, json=payload, timeout=10)
    try:
        data = resp.json()
    except Exception:
        data = {"ok": False, "error": "Non-JSON response from Telegram", "status": resp.status_code}

    if not data.get("ok"):
        logger.error("Telegram failed: %s", data)
    else:
        logger.info("Telegram send OK")

    return data


# ---------------------------------------------------------
# Routes
# ---------------------------------------------------------
@app.route("/", methods=["GET"])
def index():
    return jsonify({"status": "ok", "message": "GDM webhook running"}), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        raw_body = request.get_data(as_text=True) or ""
        logger.info("Incoming webhook raw body: %r", raw_body)

        # If TradingView ever sends JSON, we can also handle that:
        if request.is_json:
            data = request.get_json(silent=True) or {}
            msg_from_json = data.get("message") or data.get("text")
            if msg_from_json:
                raw_body = msg_from_json

        message_to_send = build_cornix_style_message(raw_body)
        logger.info("Built Telegram message:\n%s", message_to_send)

        tg_response = send_telegram_message(message_to_send)

        return jsonify({"status": "ok", "telegram": tg_response}), 200

    except Exception as e:
        logger.exception("Error handling webhook")
        return jsonify({"status": "error", "error": str(e)}), 500


# ---------------------------------------------------------
# Local run (useful for testing)
//---------------------------------------------------------
if __name__ == "__main__":
    # For local development; Render will use gunicorn
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
