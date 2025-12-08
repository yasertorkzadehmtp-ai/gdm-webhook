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
def extract_tv_message(req) -> str:
    """
    Extract the actual text TradingView sent.

    Supports:
    - Raw text body (default TradingView webhook)
    - JSON with 'message' or 'text'
    - Form-encoded 'message'
    """
    # 1) Raw body
    raw_body = req.get_data(as_text=True) or ""

    # 2) JSON payload (if any)
    if req.is_json:
        try:
            data = req.get_json(silent=True) or {}
        except Exception:
            data = {}
        msg_from_json = None
        if isinstance(data, dict):
            msg_from_json = data.get("message") or data.get("text")
        if msg_from_json:
            return str(msg_from_json).strip()

    # 3) Form field (if somebody used 'message=')
    form_msg = req.form.get("message")
    if form_msg:
        return form_msg.strip()

    return raw_body.strip()


def send_telegram_message(text: str) -> dict:
    """
    Send text to Telegram as-is (no rewriting).
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID.")
        return {"ok": False, "error": "Missing Telegram credentials"}

    if not text:
        text = "(empty alert received)"

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        # we don't use HTML/Markdown formatting here to avoid
        # accidentally breaking Cornix parsing; Cornix wants plain text
        "disable_web_page_preview": True,
    }

    resp = requests.post(TELEGRAM_API_URL, json=payload, timeout=10)
    try:
        data = resp.json()
    except Exception:
        data = {
            "ok": False,
            "error": "Non-JSON response from Telegram",
            "status": resp.status_code,
            "body": resp.text[:200],
        }

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
        # Extract exactly what TradingView sent
        raw_body = extract_tv_message(request)
        logger.info("Incoming webhook payload: %r", raw_body)

        tg_response = send_telegram_message(raw_body)

        return jsonify({"status": "ok", "telegram": tg_response}), 200

    except Exception as e:
        logger.exception("Error handling webhook")
        return jsonify({"status": "error", "error": str(e)}), 500


# ---------------------------------------------------------
# Local run (useful for testing)
# ---------------------------------------------------------
if __name__ == "__main__":
    # For local development; Render will use gunicorn in production
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
