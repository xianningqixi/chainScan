#!/usr/bin/env python3
"""FOMO Terminal SSE listener (GET /api/feed/events) -> inbox -> ingest.
Auth via env:
  FOMOSCAN_BEARER=...   (session Authorization bearer from logged-in terminal)
Optional:
  FOMOSCAN_COOKIE=...
No model tokens.
"""
from __future__ import annotations
import json, os, sys, time, hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from common import ROOT, TZ, load_config, atomic_write
INBOX = ROOT / "inbox"
LOG = ROOT / "state" / "fomo_sse.log"
URL = "https://terminal.fomoscan.sh/api/feed/events"

def log(msg: str):
    LOG.parent.mkdir(parents=True, exist_ok=True)
    line = f"{datetime.now(TZ).isoformat()} {msg}"
    print(line, flush=True)
    with LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

def event_to_signals(payload: dict) -> list[dict]:
    """Best-effort map of SSE feed payloads to schema v1 signals."""
    out = []
    # payload may be {feed:[...]} or a single event or {changedEventIds:[], ...}
    items = []
    if isinstance(payload.get("feed"), list):
        items = payload["feed"]
    elif isinstance(payload.get("events"), list):
        items = payload["events"]
    elif isinstance(payload.get("data"), list):
        items = payload["data"]
    elif isinstance(payload, dict) and (payload.get("token") or payload.get("mint") or payload.get("symbol")):
        items = [payload]
    for it in items:
        if not isinstance(it, dict):
            continue
        ca = it.get("mint") or it.get("tokenAddress") or it.get("ca") or it.get("address") or ""
        if isinstance(ca, dict):
            ca = ca.get("address") or ca.get("mint") or ""
        name = it.get("symbol") or it.get("ticker") or it.get("name") or it.get("token") or ""
        eid = it.get("id") or it.get("eventId") or ""
        action = (it.get("side") or it.get("action") or it.get("type") or "buy")
        action_l = str(action).lower()
        direction = "buy" if "buy" in action_l or "long" in action_l else ("sell" if "sell" in action_l else "watch")
        buy = it.get("amountUsd") or it.get("usd") or it.get("value") or it.get("buy_usd")
        mcap = it.get("mcap") or it.get("marketCap") or it.get("mc")
        sid = f"fomo-sse-{eid}" if eid else f"fomo-sse-{hashlib.sha1((str(ca)+str(name)+str(it.get('ts') or it.get('createdAt') or '')).encode()).hexdigest()[:16]}"
        out.append({
            "schema_version": 1,
            "signal_id": sid,
            "ts": it.get("createdAt") or it.get("ts") or datetime.now(TZ).isoformat(),
            "source": "fomo",
            "source_url": "https://terminal.fomoscan.sh",
            "chain": "sol",
            "token_name": name if not isinstance(name, dict) else name.get("symbol", ""),
            "ca": ca,
            "signal_type": "other",
            "direction": direction,
            "smart_money_count": it.get("traders") or it.get("followers"),
            "buy_usd": buy,
            "price": it.get("price"),
            "mcap": mcap,
            "status": "active",
            "notes": f"sse; keys={sorted(it.keys())[:20]}",
        })
    return out

def ingest():
    import subprocess
    r = subprocess.run([sys.executable, str(Path(__file__).with_name("ingest.py"))], capture_output=True, text=True)
    log(f"ingest {(r.stdout or r.stderr or '').strip()}")

def handle_data(data_str: str):
    data_str = data_str.strip()
    if not data_str or data_str == "[DONE]":
        return
    try:
        payload = json.loads(data_str)
    except json.JSONDecodeError:
        log(f"non-json event: {data_str[:200]}")
        return
    sigs = event_to_signals(payload)
    if not sigs:
        # log shape once in a while
        log(f"no mapped signals; top_keys={list(payload.keys())[:15] if isinstance(payload, dict) else type(payload)}")
        return
    INBOX.mkdir(parents=True, exist_ok=True)
    path = INBOX / f"fomo_sse_{time.time_ns()}.jsonl"
    atomic_write(path, "\n".join(json.dumps(s, ensure_ascii=False) for s in sigs) + "\n")
    log(f"queued {len(sigs)} signals")
    ingest()

def stream_once(bearer: str, cookie: str | None):
    headers = {
        "Authorization": f"Bearer {bearer}",
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
        "User-Agent": "signal-pipeline/1.0",
    }
    if cookie:
        headers["Cookie"] = cookie
    req = Request(URL, headers=headers)
    with urlopen(req, timeout=120) as resp:
        log(f"connected status={resp.status} ctype={resp.headers.get('Content-Type')}")
        buf = b""
        event_data = []
        while True:
            chunk = resp.read(1)
            if not chunk:
                log("stream ended")
                break
            buf += chunk
            if buf.endswith(b"\n"):
                line = buf.decode("utf-8", errors="replace")
                buf = b""
                if line.startswith("data:"):
                    event_data.append(line[5:].lstrip())
                elif line.strip() == "":
                    if event_data:
                        handle_data("\n".join(event_data))
                        event_data = []

def main():
    bearer = os.environ.get("FOMOSCAN_BEARER", "").strip()
    cookie = os.environ.get("FOMOSCAN_COOKIE", "").strip() or None
    if not bearer:
        log("missing FOMOSCAN_BEARER"); sys.exit(2)
    backoff = 1
    while True:
        try:
            stream_once(bearer, cookie)
            backoff = 1
        except Exception as e:
            log(f"error {e!r}; retry {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    main()
