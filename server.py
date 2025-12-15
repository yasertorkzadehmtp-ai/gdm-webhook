import os
import logging
import json
import csv
import calendar
import glob
from datetime import datetime, timezone

from flask import Flask, request, jsonify, send_file
import requests


# BUILD TAG: Render v3-i4 (HB15 telegram guard)
# Changed in v3-i4:
# - Do NOT send Telegram messages for LOG-only HB15 telemetry (prevents empty Telegram posts)
# - If payload strips to empty, skip Telegram instead of sending '(empty alert received)'

# ---------------------------------------------------------
# Basic setup
# ---------------------------------------------------------
app = Flask(__name__)

@app.route("/health", methods=["GET"])
def health():
    return "ok", 200

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gdm-webhook")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logger.warning("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set in env vars!")

TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# ---------------------------------------------------------
# Logging to CSV (1 file per ~2 days)
# ---------------------------------------------------------
LOG_DIR = os.environ.get("GDM_LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

CSV_HEADER = [
    "recv_time_utc",
    "bar_time",
    "engine",
    "engine_version",
    "trade_id",
    "signal_kind",
    "direction",
    "symbol",
    "pair",
    "tf",
    "price",
    "entry_price",
    "current_profit_pct",
    "profile",
    "is_shitcoin",
    "rsi_4h",
    "macd_hist_4h",
    "trend_ema_4h",
    "atr",
    "atr_ratio",
    "vol_ratio",
    "rsi_1h",
    "macd_hist_1h",
    "wma_1h",
    "fast_bull",
    "fast_bear",
]


def _get_csv_path(now_utc: datetime) -> str:
    """Return log file path for a 2-day bucket."""
    year = now_utc.year
    month = now_utc.month
    day = now_utc.day

    # group days as (1-2, 3-4, 5-6, ...)
    period_start = day if day % 2 == 1 else day - 1
    last_day = calendar.monthrange(year, month)[1]
    period_end = min(period_start + 1, last_day)

    fname = f"gdm_signals_{year:04d}{month:02d}{period_start:02d}_{period_end:02d}.csv"
    return os.path.join(LOG_DIR, fname)


def append_log_record(log_data: dict) -> None:
    """Append one row to the rolling CSV log."""
    now = datetime.now(timezone.utc)
    csv_path = _get_csv_path(now)
    file_exists = os.path.exists(csv_path)

    try:
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
            if not file_exists:
                writer.writeheader()

            row = {
                "recv_time_utc": now.isoformat(),
                "bar_time": log_data.get("bar_time"),
                "engine": log_data.get("engine"),
                "engine_version": log_data.get("engine_version"),
                "trade_id": log_data.get("trade_id"),
                "signal_kind": log_data.get("signal_kind"),
                "direction": log_data.get("direction"),
                "symbol": log_data.get("symbol"),
                "pair": log_data.get("pair"),
                "tf": log_data.get("tf"),
                "price": log_data.get("price"),
                "entry_price": log_data.get("entry_price"),
                "current_profit_pct": log_data.get("current_profit_pct"),
                "profile": log_data.get("profile"),
                "is_shitcoin": log_data.get("is_shitcoin"),
                "rsi_4h": log_data.get("rsi_4h"),
                "macd_hist_4h": log_data.get("macd_hist_4h"),
                "trend_ema_4h": log_data.get("trend_ema_4h"),
                "atr": log_data.get("atr"),
                "atr_ratio": log_data.get("atr_ratio"),
                "vol_ratio": log_data.get("vol_ratio"),
                "rsi_1h": log_data.get("rsi_1h"),
                "macd_hist_1h": log_data.get("macd_hist_1h"),
                "wma_1h": log_data.get("wma_1h"),
                "fast_bull": log_data.get("fast_bull"),
                "fast_bear": log_data.get("fast_bear"),
            }
            writer.writerow(row)

        logger.info("Appended log row to %s", csv_path)
    except Exception:
        logger.exception("Failed to append log record")


def maybe_log_from_body(raw_body: str) -> None:
    """Find a line starting with 'LOG:' and append its JSON to CSV."""
    if not raw_body:
        return

    log_json_text = None
    for line in raw_body.splitlines():
        line = line.strip()
        if line.startswith("LOG:"):
            log_json_text = line[len("LOG:"):].strip()
            break

    if not log_json_text:
        return

    try:
        data = json.loads(log_json_text)
        if isinstance(data, dict):
            append_log_record(data)
        else:
            logger.warning("LOG payload is not a dict: %r", data)
    except Exception:
        logger.exception("Failed to parse LOG JSON")


def strip_log_from_body(raw_body: str) -> str:
    """Remove any lines starting with 'LOG:' before sending to Telegram."""
    if not raw_body:
        return raw_body

    lines = []
    for line in raw_body.splitlines():
        if line.strip().startswith("LOG:"):
            # skip this line entirely
            continue
        lines.append(line)

    # Trim trailing blank lines
    while lines and not lines[-1].strip():
        lines.pop()

    return "\n".join(lines)


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------


def extract_log_json(raw_body: str):
    """Return parsed dict from first LOG:{...} line, or None."""
    if not raw_body:
        return None

    for line in raw_body.splitlines():
        line = line.strip()
        if line.startswith("LOG:"):
            log_json_text = line[len("LOG:"):].strip()
            try:
                data = json.loads(log_json_text)
                return data if isinstance(data, dict) else None
            except Exception:
                return None

    return None

def extract_tv_message(req) -> str:
    """Extract the actual text TradingView sent.

    Supports:
    - Raw text body (default TradingView webhook)
    - JSON with 'message' or 'text'
    - Form-encoded 'message'
    """
    # 1) raw body
    raw_body = req.get_data(as_text=True) or ""

    # 2) JSON payload
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

    # 3) form field
    form_msg = req.form.get("message")
    if form_msg:
        return form_msg.strip()

    return raw_body.strip()


def send_telegram_message(text: str) -> dict:
    """Send text to Telegram as-is (no formatting changes)."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID.")
        return {"ok": False, "error": "Missing Telegram credentials"}

    if not text:
        return {"ok": True, "skipped": True, "reason": "empty"}

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }

    try:
        resp = requests.post(TELEGRAM_API_URL, json=payload, timeout=10)
        data = resp.json()
    except Exception as e:
        logger.exception("Telegram request failed")
        return {"ok": False, "error": str(e)}

    if not data.get("ok"):
        logger.error("Telegram send failed: %s", data)
    else:
        logger.info("Telegram send OK")

    return data


# ---------------------------------------------------------
# Routes
# ---------------------------------------------------------
@app.route("/logs", methods=["GET"])
def list_logs():
    """Return a list of CSV filenames in the logs folder."""
    try:
        files = sorted(glob.glob(os.path.join(LOG_DIR, "*.csv")))
        names = [os.path.basename(f) for f in files]
        return jsonify({"files": names}), 200
    except Exception as e:
        logger.exception("Error listing logs")
        return jsonify({"error": str(e)}), 500


@app.route("/download/<filename>", methods=["GET"])
def download_log(filename):
    """Download a CSV log file from the logs folder."""
    path = os.path.join(LOG_DIR, filename)
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404

    try:
        return send_file(
            path,
            mimetype="text/csv",
            as_attachment=True,
            download_name=filename,
        )
    except Exception as e:
        logger.exception("Error sending log file")
        return jsonify({"error": str(e)}), 500


@app.route("/", methods=["GET"])
def index():
    return jsonify({"status": "ok", "message": "GDM webhook running"}), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    # --- REQUEST LOG (v3-i2) ---
    try:
        print("[WEBHOOK] method=", request.method, "path=", request.path)
        print("[WEBHOOK] headers=", dict(request.headers))
        raw = request.get_data(as_text=True)
        print("[WEBHOOK] body=", raw)
    except Exception as e:
        print("[WEBHOOK] log_error:", e)
    # --- END REQUEST LOG ---
    try:
        # 1) Extract exactly what TradingView sent
        raw_body = extract_tv_message(request)
        logger.info("Incoming webhook payload: %r", raw_body)

        # 2) Try to log LOG:{...} if present
        try:
            maybe_log_from_body(raw_body)
        except Exception:
            logger.exception("Error while trying to log alert payload")

        # 3) Strip LOG line, send only signal text to Telegram
        clean_body = strip_log_from_body(raw_body)

        # If the alert is LOG-only telemetry (e.g., HB15), do NOT forward empty messages to Telegram.
        log_obj = extract_log_json(raw_body)
        if not clean_body:
            if isinstance(log_obj, dict) and log_obj.get("event") == "HB15":
                logger.info("Skipping Telegram for HB15 telemetry (LOG-only).")
                tg_response = {"ok": True, "skipped": True, "reason": "HB15 telemetry"}
            else:
                logger.info("Skipping Telegram for empty body.")
                tg_response = {"ok": True, "skipped": True, "reason": "empty"}
        else:
            tg_response = send_telegram_message(clean_body)

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

# GUNICORN_ACCESS_LOG_HINT: In Render, set Start Command to:
# gunicorn server:app --access-logfile - --error-logfile -
