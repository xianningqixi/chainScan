#!/usr/bin/env python3
"""Offline GMGN payload -> schema v1 -> inbox. No polling or network client.

Explicit use: gmgn_adapter.py payload.json [--json]
With no input, show help and exit without I/O. --json returns a result envelope;
callers must discard skipped results. Inbox entries still require normal ingest.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import math
from pathlib import Path
import shlex
from urllib.parse import quote
from uuid import uuid4

import common
from store import Store, token_key

RECENT_WINDOW_S = 30 * 60


def _first(data, *names, default=None):
    for name in names:
        value = data.get(name)
        if value is not None and value != "":
            return value
    return default


def _text(value, field):
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    return value.strip()


def _timestamp(value):
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            raise ValueError
        if isinstance(value, (int, float)) or str(value).replace(".", "", 1).isdigit():
            stamp = float(value)
            if abs(stamp) >= 100_000_000_000:
                stamp /= 1000
            return datetime.fromtimestamp(stamp, common.TZ).isoformat()
        stamp = datetime.fromisoformat(value)
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=common.TZ)
        return stamp.astimezone(common.TZ).isoformat()
    except (TypeError, ValueError, OverflowError, OSError):
        raise ValueError("invalid GMGN timestamp") from None


def to_signal(payload: dict, *, received_at=None) -> dict:
    """Pure mapping of one event, optionally containing a nested token object.

    Require a stable event ID (never synthesize it from time or CA). Preserve
    numeric scalars/strings as in the Binance adapter, including zero and null.
    Without an event timestamp or supplied received_at, ts is null for ingest
    to fill. Unknown directions become watch, never an inferred buy.
    """
    if not isinstance(payload, dict):
        raise ValueError("GMGN payload must be an object")
    sid = _first(payload, "id", "signal_id", "signalId", "event_id", "eventId")
    if isinstance(sid, bool) or not isinstance(sid, (str, int)) or not str(sid).strip():
        raise ValueError("GMGN payload requires a stable id")
    token = payload.get("token")
    data = {**(token if isinstance(token, dict) else {}), **payload}
    ca = _text(_first(data, "ca", "address", "token_address", "tokenAddress",
                      "contract_address", "contractAddress", default=""), "ca")
    if not ca:
        raise ValueError("GMGN payload requires a CA")
    chain = str(_first(data, "chain", "chain_id", "chainId", default="unknown")).strip().lower()
    chain = {"binance": "bsc", "bnb": "bsc", "bnbchain": "bsc",
             "arbitrum": "arb", "optimism": "op", "137": "matic"}.get(chain, chain)
    chain = token_key(chain, ca)[0]
    direction = _text(_first(data, "direction", "side", "event", "type", default="buy"), "direction").lower()
    direction = {"smart_money_buy": "buy", "smart_money_sell": "sell"}.get(direction, direction)
    if direction not in {"buy", "sell", "watch"}:
        direction = "watch"
    status = _text(_first(data, "status", default="active"), "status").lower()
    status = {"valid": "active", "timeout": "expired", "completed": "expired"}.get(status, status)
    sig = {
        "schema_version": 1,
        "signal_id": "gmgn-" + str(sid).strip(),
        "ts": _timestamp(_first(data, "ts", "timestamp", "time", "signalTriggerTime",
                                 "created_at", default=received_at)),
        "source": "gmgn_smart_money",
        "source_url": _text(_first(data, "source_url", "sourceUrl", default=
            f"https://gmgn.ai/{quote(chain, safe='')}/token/{quote(ca, safe='')}"), "source_url"),
        "chain": chain,
        "token_name": _text(_first(data, "token_name", "symbol", "ticker", "name", default=""), "token_name"),
        "ca": ca,
        "signal_type": "smart_money_buy" if direction == "buy" else "other",
        "direction": direction,
        "status": status,
        "notes": _text(_first(data, "notes", default=""), "notes"),
    }
    for field, aliases in {
        "smart_money_count": ("smart_money_count", "smartMoneyCount", "smart_count"),
        "buy_usd": ("buy_usd", "buyUsd", "amount_usd", "amountUsd", "volume_usd"),
        "price": ("price", "current_price", "currentPrice", "price_usd"),
        "mcap": ("mcap", "market_cap", "marketCap", "marketcap"),
    }.items():
        value = _first(data, *aliases)
        if (value is not None and (isinstance(value, bool) or not isinstance(value, (str, int, float)))
                or isinstance(value, float) and not math.isfinite(value)):
            raise ValueError(f"{field} must be a finite scalar or null")
        sig[field] = value
    return sig


def read_auth(*, root=None) -> dict:
    """Explicitly read only GMGN_API_KEY/GMGN_AUTH_TOKEN from state/secrets.env.

    Return headers; never consult process environment, execute shell expansions,
    log values or open the file on import. Missing file/config returns {}.
    """
    path = (Path(root) if root is not None else common.ROOT) / "state/secrets.env"
    headers = {}
    names = {"GMGN_API_KEY": "X-API-Key", "GMGN_AUTH_TOKEN": "Authorization"}
    try:
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                key, sep, raw = line.strip().removeprefix("export ").partition("=")
                key = key.strip()
                if not sep or key not in names:
                    continue
                try:
                    values = shlex.split(raw, comments=True)
                except ValueError:
                    continue
                if len(values) != 1 or not values[0] or any(c in values[0] for c in "\r\n\0"):
                    continue
                headers[names[key]] = ("Bearer " if key == "GMGN_AUTH_TOKEN" else "") + values[0]
    except FileNotFoundError:
        return {}
    return headers


def prepare_signal(payload, store, *, within_s=RECENT_WINDOW_S, received_at=None):
    """Read-only advisory dedupe. Never claim a push or bypass ingest filters.

    The check sees committed pushes, not other sources' still-pending inbox
    files. Stable IDs also leave GMGN repeat delivery to normal ingest dedupe.
    """
    sig = to_signal(payload, received_at=received_at)
    duplicate = store.token_recent(sig["chain"], sig["ca"], within_s)
    if duplicate:
        store.record_also_seen(sig["chain"], sig["ca"], "gmgn")
    if duplicate and "also_seen=gmgn" not in sig["notes"].split("; "):
        sig["notes"] = "; ".join(filter(None, [sig["notes"], "also_seen=gmgn"]))
    return {"signal": sig, "skipped": duplicate, "duplicate": duplicate}


def handle_payload(payload, *, store=None, root=None, json_only=False):
    """Explicit one-shot entry: atomically enqueue, or return a JSON-ready result.

    Duplicate signals are diagnostic only and never written to inbox. No auth
    is needed for local output. No webhook or network transport is implemented.
    """
    root = Path(root) if root is not None else store.root if store is not None else common.ROOT
    received_at = datetime.now(common.TZ).isoformat()
    # Validate before even creating a local Store.
    to_signal(payload, received_at=received_at)
    store = store if store is not None else Store(root)
    result = prepare_signal(payload, store, received_at=received_at)
    result["queued"] = None
    if not json_only and not result["skipped"]:
        sig = result["signal"]
        if not store.reserve_token(sig["chain"], sig["ca"], sig["signal_id"], sig["source"]):
            result["skipped"] = result["duplicate"] = True
            store.record_also_seen(sig["chain"], sig["ca"], "gmgn")
            if "also_seen=gmgn" not in sig["notes"].split("; "):
                sig["notes"] = "; ".join(filter(None, [sig["notes"], "also_seen=gmgn"]))
        else:
            path = root / "inbox" / f"gmgn_{uuid4().hex}.jsonl"
            try:
                common.atomic_write(path, json.dumps(sig, ensure_ascii=False, allow_nan=False) + "\n")
            except Exception:
                # A failed publication must not hold the token against another
                # source for the full reservation TTL.
                store.release_token_reservation(sig["signal_id"])
                raise
            result["queued"] = str(path)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("payload", nargs="?", type=Path, help="local JSON event file")
    parser.add_argument("--json", action="store_true", help="return result JSON without enqueueing")
    args = parser.parse_args(argv)
    if args.payload is None:
        parser.print_help()
        return 0
    try:
        payload = json.loads(args.payload.read_text(encoding="utf-8"))
        result = handle_payload(payload, json_only=args.json)
    except (OSError, ValueError):
        parser.exit(2, "GMGN adapter: unable to process local payload\n")
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
