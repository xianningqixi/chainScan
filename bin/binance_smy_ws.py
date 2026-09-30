#!/usr/bin/env python3
"""Binance Web3 Smart Money WS listener -> inbox -> ingest. No API key. No model tokens."""
from __future__ import annotations
import asyncio, json, signal, sqlite3, time
from contextlib import suppress
from datetime import datetime

import websockets

from common import ROOT, TZ, atomic_write, log_event
from store import Store
from filter_signal import extract_tags
INBOX = ROOT / "inbox"
WS_URL = "wss://web3-stream.binance.com/w3w/stream"

CHAIN_MAP = {
    "56": "bsc",
    "CT_501": "sol",
    "501": "sol",
    "1": "eth",
    "8453": "base",
}

def log(msg: str):
    log_event("service_message", log_name="binance_smy_ws", message=msg)

def map_chain(cid) -> str:
    s = str(cid or "")
    return CHAIN_MAP.get(s, s.lower() or "unknown")

def map_status(st) -> str:
    s = (st or "active")
    s = str(s).lower()
    if s in {"valid", "active"}: return "active"
    if s in {"timeout", "completed", "expired"}: return "expired"
    if s in {"invalid"}: return "invalid"
    return s or "active"

def to_signal(data: dict) -> dict:
    sid = data.get("signalId") or data.get("id") or f"bn-{int(time.time()*1000)}"
    ca = data.get("contractAddress") or data.get("ca") or ""
    direction = (data.get("direction") or "buy").lower()
    if direction not in {"buy", "sell", "watch"}:
        direction = "buy"
    # maxGain is decimal fraction per docs
    mg = data.get("maxGain")
    notes_bits = []
    if mg is not None:
        try:
            notes_bits.append(f"maxGain={float(mg)*100:.2f}%")
        except Exception:
            notes_bits.append(f"maxGain={mg}")
    if data.get("isAlpha"):
        notes_bits.append("isAlpha")
    if data.get("tokenTag"):
        notes_bits.append(f"tag={data.get('tokenTag')}")
    ts_ms = data.get("signalTriggerTime")
    if ts_ms:
        try:
            ts = datetime.fromtimestamp(int(ts_ms)/1000, TZ).isoformat()
        except Exception:
            ts = datetime.now(TZ).isoformat()
    else:
        ts = datetime.now(TZ).isoformat()
    return {
        "schema_version": 1,
        "signal_id": f"binance-{sid}",
        "ts": ts,
        "source": "binance_smart_money",
        "source_url": f"https://web3.binance.com/zh-CN/token/{map_chain(data.get('chainId'))}/{ca}" if ca else "https://web3.binance.com/zh-CN/signals",
        "chain": map_chain(data.get("chainId")),
        "token_name": data.get("ticker") or data.get("symbol") or "",
        "ca": ca,
        "signal_type": "smart_money_buy" if direction == "buy" else "other",
        "direction": direction if direction != "sell" else "sell",
        "smart_money_count": data.get("smartMoneyCount"),
        "buy_usd": data.get("buyUsd") or data.get("amountUsd"),
        "price": data.get("currentPrice") or data.get("alertPrice"),
        "mcap": data.get("marketCap") or data.get("mcap"),
        "status": map_status(data.get("status")),
        "notes": "; ".join(notes_bits),
        "tags": extract_tags({"tokenTag": data.get("tokenTag")}),
    }

async def handle(raw: str, store: Store):
    started = time.monotonic()
    outcome = "error"
    try:
        outcome = _queue_message(raw, store)
    finally:
        log_event("ws_handle", outcome=outcome,
                  ms=round((time.monotonic() - started) * 1000, 3))


def _queue_message(raw: str, store: Store):
    log_event("ws_message")
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        log_event("ws_error", error="bad_json")
        return "bad_json"
    if not isinstance(msg, dict):
        return "ignored"
    # ping reply handled by library; also app-level PING/PONG
    if msg.get("method") == "PING" or msg.get("ping") is not None:
        return "ignored"
    stream = msg.get("stream")
    data = msg.get("data")
    if stream != "w3w@signal_smart_money" or not isinstance(data, dict):
        # subscription ack etc
        if "result" in msg or "id" in msg:
            log("subscription ack")
        return "ignored"
    sid = str(data.get("signalId") or data.get("id") or "")
    # merge/dedupe: only ingest when new signalId or meaningful status/price change
    fingerprint = json.dumps({
        "status": data.get("status"),
        "currentPrice": data.get("currentPrice"),
        "smartMoneyCount": data.get("smartMoneyCount"),
        "direction": data.get("direction"),
        "tags": extract_tags({"tokenTag": data.get("tokenTag")}),
    }, sort_keys=True)
    try:
        if sid and store.ws_fingerprint(sid) == fingerprint:
            return "duplicate"
    except sqlite3.Error as exc:
        # Dedupe is an optimization: queue on database contention or failure.
        log_event("ws_error", operation="fingerprint_read", error=type(exc).__name__)
    sig = to_signal(data)
    INBOX.mkdir(parents=True, exist_ok=True)
    path = INBOX / f"binance_{time.time_ns()}.jsonl"
    atomic_write(path, json.dumps(sig, ensure_ascii=False) + "\n")
    # Never remember an update before its complete inbox file is published.
    # A crash here can replay a file, which ingest deduplicates by signal_id.
    if sid:
        try:
            store.record_ws_fingerprint(sid, fingerprint)
        except sqlite3.Error as exc:
            log_event("ws_error", operation="fingerprint_write", error=type(exc).__name__)
    log_event("sig_queued", signal_id=sig["signal_id"], source="binance_smart_money")
    log(f"queued {sig['signal_id']} {sig.get('token_name')} {sig.get('chain')} status={sig.get('status')}")
    return "queued"


async def pinger(ws):
    while True:
        await asyncio.sleep(20)
        try:
            pong = await ws.ping()
            await asyncio.wait_for(pong, timeout=20)
        except (TimeoutError, websockets.exceptions.ConnectionClosed) as exc:
            log_event("ws_error", operation="ping", error=type(exc).__name__)
            await ws.close(code=1011, reason="heartbeat failed")
            return
        log_event("ws_heartbeat")

async def main():
    store = Store(ROOT)
    backoff = 1
    while True:
        try:
            log("connecting")
            async with websockets.connect(WS_URL, ping_interval=None, max_size=8_000_000) as ws:
                log_event("ws_connected")
                sub = {"method": "SUBSCRIBE", "params": ["w3w@signal_smart_money"], "id": 1}
                await ws.send(json.dumps(sub))
                log("subscribed w3w@signal_smart_money")
                backoff = 1
                ping_task = asyncio.create_task(pinger(ws))
                try:
                    async for message in ws:
                        await handle(message, store)
                finally:
                    ping_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await ping_task
        except Exception as e:
            log_event("ws_error", error=type(e).__name__, retry_s=backoff)
            log(f"error {type(e).__name__}; retry {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def run_service():
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(main())
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)
    try:
        with suppress(asyncio.CancelledError):
            await task
    finally:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    asyncio.run(run_service())
