#!/usr/bin/env python3
"""FOMO WebSocket listener -> inbox ingest. No model tokens.
Env: FOMOSCAN_API_KEY=fsk_live_...
"""
from __future__ import annotations
import asyncio, json, os, sys, time, hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import websockets
except ImportError:
    print("pip install websockets", file=sys.stderr); sys.exit(1)

from common import ROOT, TZ, load_config, atomic_write
INBOX = ROOT / "inbox"
LOG = ROOT / "state" / "fomo_ws.log"
WS_URL = "wss://api.fomoscan.sh/v2/ws"

def log(msg: str):
    LOG.parent.mkdir(parents=True, exist_ok=True)
    line = f"{datetime.now(TZ).isoformat()} {msg}"
    print(line, flush=True)
    with LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

def thesis_to_signal(frame: dict) -> dict | None:
    # Accept either wrapped {type:thesis, data:{...}} or raw thesis object
    data = frame.get("data") if isinstance(frame.get("data"), dict) else frame
    if not isinstance(data, dict):
        return None
    # common field guesses from docs naming
    ca = (data.get("tokenAddress") or data.get("mint") or data.get("token") or data.get("ca") or "")
    if isinstance(ca, dict):
        ca = ca.get("address") or ca.get("mint") or ""
    name = data.get("symbol") or data.get("tokenSymbol") or data.get("name") or data.get("token_name") or ""
    tid = data.get("id") or data.get("thesisId") or ""
    mcap = data.get("mcap") or data.get("marketCap") or data.get("mc")
    buy = data.get("amountUsd") or data.get("buy_usd") or data.get("volumeUsd")
    smart = data.get("traders") or data.get("smart_money_count") or data.get("buyers")
    ts = data.get("createdAt") or data.get("ts") or datetime.now(TZ).isoformat()
    if not ca and not tid:
        return None
    sid = f"fomo-{tid}" if tid else f"fomo-{hashlib.sha1((str(ca)+str(ts)).encode()).hexdigest()[:16]}"
    return {
        "schema_version": 1,
        "signal_id": sid,
        "ts": ts,
        "source": "fomo",
        "source_url": "https://terminal.fomoscan.sh",
        "chain": "sol",
        "token_name": name,
        "ca": ca,
        "signal_type": "other",
        "direction": "buy",
        "smart_money_count": smart,
        "buy_usd": buy,
        "price": data.get("price"),
        "mcap": mcap,
        "status": "active",
        "notes": json.dumps({"raw_keys": sorted(data.keys())[:30]}, ensure_ascii=False),
    }

async def run_ingest_file(path: Path):
    proc = await asyncio.create_subprocess_exec(
        sys.executable, str(Path(__file__).with_name("ingest.py")),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    log(f"ingest {path.name}: {(out or err).decode().strip()}")

async def handle_message(msg: str):
    try:
        frame = json.loads(msg)
    except json.JSONDecodeError:
        log(f"non-json: {msg[:200]}"); return
    # ping/pong
    if frame.get("type") == "ping":
        return "pong"
    sig = thesis_to_signal(frame)
    if not sig:
        # still log type for debugging first payloads
        log(f"skip frame type={frame.get('type')} keys={list(frame.keys())[:12]}")
        return
    INBOX.mkdir(parents=True, exist_ok=True)
    path = INBOX / f"fomo_{time.time_ns()}.jsonl"
    atomic_write(path, json.dumps(sig, ensure_ascii=False) + "\n")
    await run_ingest_file(path)

async def main():
    key = os.environ.get("FOMOSCAN_API_KEY", "").strip()
    if not key:
        log("missing FOMOSCAN_API_KEY"); sys.exit(2)
    headers = {"Authorization": f"Bearer {key}"}
    # some APIs also accept X-Api-Key
    backoff = 1
    while True:
        try:
            log(f"connecting {WS_URL}")
            async with websockets.connect(WS_URL, additional_headers=headers, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"type": "subscribe", "subscription": {"type": "all"}}))
                log("subscribed all")
                backoff = 1
                async for message in ws:
                    maybe = await handle_message(message)
                    if maybe == "pong":
                        await ws.send(json.dumps({"type": "pong"}))
        except Exception as e:
            log(f"ws error: {e!r}; retry in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(main())
