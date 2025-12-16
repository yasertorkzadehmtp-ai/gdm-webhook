import os
import logging
import json
import csv
import calendar
import glob
import hashlib
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from flask import Flask, request, jsonify, send_file

# =========================================================
# GDM Server Render v3-i6 — Telegram Sanitize + Inline LOG Parse
# Purpose:
# - Reduce missed Telegram sends during cold-start / worker warmup
# - Prevent empty Telegram messages
# - Keep HB15 telemetry LOG-only (no Telegram spam)
# - Add lightweight de-dup to prevent accidental repeats
#
# Start command (Render):
# gunicorn server:app --access-logfile - --error-logfile -
# =========================================================

# --------- Logging ---------
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("gdm-webhook")

# --------- Config ---------
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

LOG_DIR = os.environ.get("GDM_LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Warmup window (seconds). During this window we try harder (retry) because cold-start is common on free tiers.
READY_AFTER_SEC = int(os.environ.get("GDM_READY_AFTER_SEC", "20"))

# Telegram send retries for SIGNAL messages (non-LOG). HB15 telemetry is always skipped.
TG_RETRIES = int(os.environ.get("GDM_TG_RETRIES", "2"))
TG_TIMEOUT_SEC = float(os.environ.get("GDM_TG_TIMEOUT_SEC", "12"))
TG_BACKOFF_SEC = float(os.environ.get("GDM_TG_BACKOFF_SEC", "2.0"))

# De-dup window (seconds): prevent sending identical Telegram text repeatedly (e.g., due to recalcs).
DEDUP_WINDOW_SEC = float(os.environ.get("GDM_DEDUP_WINDOW_SEC", "30"))
DEDUP_MAX_KEYS = int(os.environ.get("GDM_DEDUP_MAX_KEYS", "512"))

SERVER_START_TS = time.time()

app = Flask(__name__)

# In-memory de-dup store: hash -> last_sent_ts
_dedup: Dict[str, float] = {}


# ---------------------------------------------------------
# Helpers: time & CSV path
# ---------------------------------------------------------
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _get_csv_path(now_utc: datetime) -> str:
    """Return log file path for a 2-day bucket."""
    year = now_utc.year
    month = now_utc.month
    day = now_utc.day

    # group days as (1-2, 3-4, 5-6, ...)
    period_start = day if day % 2 == 1 else day - 1

    # Format: gdm_signals_YYYYMMDD_<periodStart>.csv (e.g. gdm_signals_20251215_15.csv)
    fname = f"gdm_signals_{year:04d}{month:02d}{day:02d}_{period_start:02d}.csv"
    return os.path.join(LOG_DIR, fname)


def _ensure_csv_header(path: str, header: list) -> None:
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)


# ---------------------------------------------------------
# Helpers: TradingView body extraction + LOG handling
# ---------------------------------------------------------
def extract_tv_message(req) -> str:
    """
    TradingView usually sends raw plain text.
    Some users may send JSON with {"message": "..."}.
    We try to extract the most useful message.
    """
    raw = req.get_data(as_text=True) or ""
    raw = raw.strip()

    # If JSON, try to extract "message" or "text"
    if raw.startswith("{") and raw.endswith("}"):
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                for k in ("message", "text", "body"):
                    if k in obj and isinstance(obj[k], str):
                        return obj[k].strip()
        except Exception:
            pass

    return raw


def strip_log_from_body(raw_body: str) -> str:
    """
    Remove telemetry appended via 'LOG:' and return only the human signal text.

    Supported engine conventions:
      1) Multi-line:
         <signal text>
         LOG:{...json...}

      2) Single-line (Cornix style):
         /open ... LOG:{...json...}
         /close ... LOG:{...json...}

    Behavior:
      - If a line starts with LOG:, it is removed.
      - If a line contains LOG: inline, everything from LOG: onward is removed (signal prefix kept).
    """
    if not raw_body:
        return ""

    keep = []
    for ln in raw_body.splitlines():
        s = ln.rstrip()

        # Case A: LOG line only (telemetry)
        if s.strip().startswith("LOG:"):
            continue

        # Case B: Cornix command with inline LOG:
        if "LOG:" in s:
            prefix = s.split("LOG:", 1)[0].rstrip()
            if prefix:
                keep.append(prefix)
            # Anything after LOG: is telemetry; ignore remainder and stop consuming further lines
            break

        keep.append(s)

    clean = "
".join([ln for ln in keep]).strip()
    return clean


def parse_log_json(raw_body: str) -> Optional[Dict[str, Any]]:
    """
    Extract JSON after the first occurrence of 'LOG:'.

    Works for:
      - A dedicated line starting with 'LOG:'
      - A single-line command containing '... LOG:{json...}'
    Returns dict or None.
    """
    if not raw_body:
        return None

    for ln in raw_body.splitlines():
        s = ln.strip()
        idx = s.find("LOG:")
        if idx == -1:
            continue

        payload = s[idx + 4 :].strip()
        try:
            obj = json.loads(payload)
            if isinstance(obj, dict):
                return obj
        except Exception:
            logger.exception("Failed to parse LOG JSON")
        return None

    return None

    # Find first line that starts with LOG:
    for ln in raw_body.splitlines():
        s = ln.strip()
        if s.startswith("LOG:"):
            payload = s[4:].strip()
            try:
                obj = json.loads(payload)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                logger.exception("Failed to parse LOG JSON")
            return None
    return None


# CSV schema for LOG payloads (append-only)
LOG_FIELDS = [
    "ts_utc",
    "event",
    "engine_version",
    "symbol",
    "host_tf",
    "t15_time",
    "time",
    "posDir",
    "raw_log_json",
]


def append_log_row(log_obj: Dict[str, Any]) -> None:
    now = _utc_now()
    path = _get_csv_path(now)
    _ensure_csv_header(path, LOG_FIELDS)

    row = [
        now.isoformat(),
        log_obj.get("event", ""),
        log_obj.get("engine_version", ""),
        log_obj.get("symbol", ""),
        log_obj.get("host_tf", ""),
        log_obj.get("t15_time", ""),
        log_obj.get("time", ""),
        log_obj.get("posDir", ""),
        json.dumps(log_obj, ensure_ascii=False),
    ]

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)

    logger.info("Appended log row to %s", path)


# ---------------------------------------------------------
# Telegram: de-dup + send with retry
# ---------------------------------------------------------
def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def _dedup_allows(text: str) -> bool:
    """
    Return True if we should send; False if it's a duplicate within the window.
    """
    now = time.time()
    h = _hash_text(text)

    # purge old entries
    if len(_dedup) > DEDUP_MAX_KEYS:
        # simple purge: remove oldest half
        items = sorted(_dedup.items(), key=lambda kv: kv[1])
        for k, _ts in items[: max(1, len(items) // 2)]:
            _dedup.pop(k, None)

    # expire window
    expired = [k for k, ts in _dedup.items() if (now - ts) > DEDUP_WINDOW_SEC]
    for k in expired:
        _dedup.pop(k, None)

    last = _dedup.get(h)
    if last is not None and (now - last) <= DEDUP_WINDOW_SEC:
        return False

    _dedup[h] = now
    return True


def _telegram_post(text: str) -> Tuple[bool, str]:
    """
    Returns (ok, detail). detail is response text or error.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False, "Missing Telegram credentials"

    url = f"{TELEGRAM_API_BASE}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }

    try:
        r = requests.post(url, json=payload, timeout=TG_TIMEOUT_SEC)
        if r.status_code == 200:
            return True, "ok"
        return False, f"telegram_http_{r.status_code}:{r.text[:200]}"
    except Exception as e:
        return False, f"telegram_exception:{e!r}"


def send_telegram_message(text: str, *, kind: str = "SIGNAL") -> Dict[str, Any]:
    """
    kind:
      - SIGNAL: real trading/ops message, send (with retries)
      - LOG: telemetry, never send to Telegram
    """
    if kind == "LOG":
        return {"ok": True, "skipped": True, "reason": "LOG-only"}

    # Prevent empty telegram messages.
    if not text or not text.strip():
        return {"ok": True, "skipped": True, "reason": "empty_message"}

    # De-dup for SIGNAL only.
    if not _dedup_allows(text):
        return {"ok": True, "skipped": True, "reason": "dedup_window"}

    # Retry more aggressively during warmup
    uptime = time.time() - SERVER_START_TS
    retries = TG_RETRIES + (1 if uptime < READY_AFTER_SEC else 0)

    last_detail = ""
    for attempt in range(0, max(1, retries + 1)):
        ok, detail = _telegram_post(text)
        last_detail = detail
        if ok:
            logger.info("Telegram send OK")
            return {"ok": True, "detail": detail, "attempt": attempt + 1}

        logger.warning("Telegram send failed (attempt %s/%s): %s", attempt + 1, retries + 1, detail)
        # Backoff (do not sleep too long; keep webhook responsive)
        if attempt < retries:
            time.sleep(TG_BACKOFF_SEC)

    return {"ok": False, "detail": last_detail, "attempts": retries + 1}


# ---------------------------------------------------------
# Routes
# ---------------------------------------------------------
@app.route("/", methods=["GET"])
def index():
    uptime = time.time() - SERVER_START_TS
    return jsonify({"status": "ok", "message": "GDM webhook running", "uptime_sec": round(uptime, 2)}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True}), 200


@app.route("/logs", methods=["GET"])
def list_logs():
    files = sorted(glob.glob(os.path.join(LOG_DIR, "*.csv")))
    return jsonify({"count": len(files), "files": [os.path.basename(p) for p in files]}), 200


@app.route("/download/<filename>", methods=["GET"])
def download_log(filename: str):
    path = os.path.join(LOG_DIR, filename)
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404

    try:
        return send_file(path, mimetype="text/csv", as_attachment=True, download_name=filename)
    except Exception as e:
        logger.exception("Error sending log file")
        return jsonify({"error": str(e)}), 500


@app.route("/webhook", methods=["POST"])
def webhook():
    # REQUEST TRACE (optional)
    try:
        logger.info("[WEBHOOK] method=%s path=%s", request.method, request.path)
        logger.info("[WEBHOOK] headers=%s", dict(request.headers))
        raw = request.get_data(as_text=True)
        logger.info("[WEBHOOK] body=%s", raw)
    except Exception:
        logger.exception("Request trace failed")

    try:
        raw_body = extract_tv_message(request)
        logger.info("Incoming webhook payload: %r", raw_body)

        # 1) LOG payload parse (telemetry)
        log_obj = parse_log_json(raw_body)
        if log_obj:
            # Always append LOG rows if parse succeeds
            try:
                append_log_row(log_obj)
            except Exception:
                logger.exception("Failed to append log row")

            # If HB15 telemetry, skip Telegram intentionally
            if str(log_obj.get("event", "")).upper() == "HB15":
                logger.info("Skipping Telegram for HB15 telemetry (LOG-only).")
                return jsonify({"status": "ok", "telemetry": "HB15_logged"}), 200

        # 2) Non-telemetry text
        clean_body = strip_log_from_body(raw_body)

        # If this webhook is ONLY telemetry (LOG line only), do not send empty Telegram.
        if not clean_body:
            return jsonify({"status": "ok", "skipped": True, "reason": "telemetry_only_or_empty"}), 200

        tg = send_telegram_message(clean_body, kind="SIGNAL")
        return jsonify({"status": "ok", "telegram": tg}), (200 if tg.get("ok") else 202)

    except Exception as e:
        logger.exception("Error handling webhook")
        return jsonify({"status": "error", "error": str(e)}), 500


# ---------------------------------------------------------
# Local run (Render uses gunicorn in production)
# ---------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
