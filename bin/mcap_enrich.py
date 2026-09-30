#!/usr/bin/env python3
"""Fetch market cap for (chain, ca) via public APIs (no API keys).

Prefer DexScreener; fall back to GeckoTerminal within a shared time budget.
Cache, source rate limits and 429 backoff are persisted in SQLite.

Alpha band (see filter_signal MCAP_MIN/MAX): roughly $30k–$5M — big coins like
ONDO/ANSEM sit far above and are rejected by mcap, not by ticker denylist.
"""
from __future__ import annotations

import json
import math
import os
import queue
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Optional

from common import load_config, log_event
from store import default_store
from filter_signal import parse_num
TTL_SEC = load_config()["enrich"]["ttl_ok"]  # 20 min for successful lookups
FAIL_TTL_SEC = load_config()["enrich"]["ttl_fail"]  # shorter for enrich failures
UA = "signals-mcap-enrich/1.0"

# Map our chain labels -> DexScreener chainId / GeckoTerminal network id
CHAIN_TO_DS = {
    "eth": "ethereum",
    "ethereum": "ethereum",
    "bsc": "bsc",
    "sol": "solana",
    "solana": "solana",
    "base": "base",
    "arb": "arbitrum",
    "arbitrum": "arbitrum",
    "op": "optimism",
    "optimism": "optimism",
    "matic": "polygon",
    "polygon": "polygon",
}
CHAIN_TO_GT = {
    "eth": "eth",
    "ethereum": "eth",
    "bsc": "bsc",
    "sol": "solana",
    "solana": "solana",
    "base": "base",
    "arb": "arbitrum",
    "arbitrum": "arbitrum",
    "op": "optimism",
    "optimism": "optimism",
    "matic": "polygon_pos",
    "polygon": "polygon_pos",
}


def _cache_key(chain: str, ca: str) -> str:
    return f"{(chain or '').lower().strip()}|{(ca or '').lower().strip()}"


def _http_get_json(url: str, timeout: float = load_config()["enrich"]["timeout"]) -> tuple[int | str, Optional[dict]]:
    """Return a classified status and JSON; retain numeric status in telemetry."""
    source = "dexscreener" if url.startswith("https://api.dexscreener.com/") else "geckoterminal"
    started = time.monotonic()
    status = "error"
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    try:
        # urllib's socket timeout alone does not bound DNS or a trickling body.
        # At most one read per source can outlive its caller; it cannot write state.
        timeout = float(load_config()["enrich"]["timeout"] if timeout is None else timeout)
        status, raw = _bounded_read(req, source, timeout)
        if status == 200:
            if not raw:
                return "not_found", None
            data = json.loads(raw.decode("utf-8", errors="replace"))
            return (200, data) if isinstance(data, dict) else ("bad_json", None)
        return ("not_found" if status == 404 else "5xx" if 500 <= status < 600 else status), None
    except urllib.error.HTTPError as exc:
        status = exc.code
        exc.close()
        return ("not_found" if status == 404 else "5xx" if 500 <= status < 600 else status), None
    except (urllib.error.URLError, TimeoutError) as exc:
        status = "timeout" if isinstance(exc, TimeoutError) or isinstance(getattr(exc, "reason", None), TimeoutError) else "network_error"
        return status, None
    except (json.JSONDecodeError, ValueError):
        status = "bad_json"
        return status, None
    finally:
        log_event("enrich", source=source, status=status,
                  ms=round((time.monotonic() - started) * 1000, 3))


_http_slots = {source: threading.BoundedSemaphore(1) for source in ("dexscreener", "geckoterminal")}


def _bounded_read(req, source, timeout):
    if timeout <= 0 or not _http_slots[source].acquire(blocking=False):
        raise TimeoutError()
    slot = _http_slots[source]
    output = queue.Queue(maxsize=1)
    deadline = time.monotonic() + timeout

    def read():
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError()
            with urllib.request.urlopen(req, timeout=remaining) as resp:
                response = (resp.status, resp.read())
        except Exception as exc:
            response = exc
        finally:
            slot.release()
        output.put(response)

    threading.Thread(target=read, name="mcap-http-" + source, daemon=True).start()
    try:
        response = output.get(timeout=max(0.0, deadline - time.monotonic()))
    except queue.Empty:
        raise TimeoutError() from None
    if isinstance(response, Exception):
        raise response
    return response


def _num(x) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
        if not math.isfinite(v):
            return None
        return v
    except (TypeError, ValueError):
        return None


def _best_mcap_from_ds_pairs(pairs: list, chain: str, ca: str, *, metadata=None) -> Optional[float]:
    """Pick highest-liquidity pair matching CA as baseToken; prefer marketCap then fdv."""
    ca_l = ca.lower()
    want_chain = CHAIN_TO_DS.get((chain or "").lower())
    matched = []
    for p in pairs or []:
        if not isinstance(p, dict):
            continue
        base = (p.get("baseToken") or {}).get("address") or ""
        quote = (p.get("quoteToken") or {}).get("address") or ""
        if base.lower() != ca_l and quote.lower() != ca_l:
            continue
        # Prefer pairs where our token is the base (mcap refers to base)
        if base.lower() != ca_l:
            continue
        if want_chain and str(p.get("chainId") or "").lower() != want_chain:
            continue
        liq = _num((p.get("liquidity") or {}).get("usd")) or 0.0
        mcap = _num(p.get("marketCap"))
        fdv = _num(p.get("fdv"))
        val = mcap if mcap is not None and mcap > 0 else fdv
        if val is None or val <= 0:
            continue
        matched.append((liq, val, p))
    if not matched:
        # retry without chain filter (Binance sometimes mislabels chain)
        for p in pairs or []:
            if not isinstance(p, dict):
                continue
            base = ((p.get("baseToken") or {}).get("address") or "").lower()
            if base != ca_l:
                continue
            liq = _num((p.get("liquidity") or {}).get("usd")) or 0.0
            mcap = _num(p.get("marketCap"))
            fdv = _num(p.get("fdv"))
            val = mcap if mcap is not None and mcap > 0 else fdv
            if val is None or val <= 0:
                continue
            matched.append((liq, val, p))
    if not matched:
        return None
    matched.sort(key=lambda t: t[0], reverse=True)
    if metadata is not None:
        selected = matched[0][2]
        metadata.update(_metadata(selected.get("priceUsd"),
                                  (selected.get("liquidity") or {}).get("usd"),
                                  selected.get("pairCreatedAt")))
    return matched[0][1]


def _metadata(price=None, liquidity=None, created=None):
    price, liquidity = _num(price), _num(liquidity)
    result = {}
    if price is not None and price > 0:
        result["price_at_fetch"] = price
    if liquidity is not None and liquidity >= 0:
        result["liq_usd"] = liquidity
    if isinstance(created, str) or (isinstance(created, (int, float)) and math.isfinite(created)):
        result["pair_created_at"] = created
    return result


def fetch_dexscreener(ca: str, chain: str = "", *, timeout=None, metadata=None):
    url = f"https://api.dexscreener.com/latest/dex/tokens/{ca}"
    status, data = _http_get_json(url, timeout=timeout)
    if status != 200 or not data:
        return status, None
    value = _best_mcap_from_ds_pairs(data.get("pairs") or [], chain, ca, metadata=metadata)
    return (200 if value is not None else "not_found"), value


def fetch_geckoterminal(ca: str, chain: str = "", *, timeout=None, metadata=None):
    net = CHAIN_TO_GT.get((chain or "").lower())
    if not net:
        return "not_found", None
    url = f"https://api.geckoterminal.com/api/v2/networks/{net}/tokens/{ca}/pools?page=1"
    status, data = _http_get_json(url, timeout=timeout)
    if status != 200 or not data:
        return status, None
    pools = data.get("data") or []
    best = None
    best_reserve = -1.0
    for pool in pools:
        if not isinstance(pool, dict):
            continue
        attrs = pool.get("attributes") or {}
        reserve = _num(attrs.get("reserve_in_usd")) or 0.0
        mcap = _num(attrs.get("market_cap_usd"))
        fdv = _num(attrs.get("fdv_usd"))
        val = mcap if mcap is not None and mcap > 0 else fdv
        if val is None or val <= 0:
            continue
        if reserve > best_reserve:
            best_reserve = reserve
            best = val
            if metadata is not None:
                # Pool prices describe the base token, not necessarily the CA requested.
                base = ((pool.get("relationships") or {}).get("base_token") or {}).get("data") or {}
                base_ca = str(base.get("id") or "").removeprefix(net + "_")
                same_base = base_ca == ca if net == "solana" else base_ca.lower() == ca.lower()
                metadata.clear()
                metadata.update(_metadata(attrs.get("base_token_price_usd") if same_base else None,
                                          attrs.get("reserve_in_usd"), attrs.get("pool_created_at")))
    return (200 if best is not None else "not_found"), best


_offline_calls = {}


def _offline_mcap(chain, ca):
    path = Path(os.environ.get("ENRICH_FIXTURE", Path(__file__).resolve().parents[1] / "tests/fixtures/dexscreener_offline.json"))
    samples = json.loads(path.read_text()) if path.exists() else {}
    key = (chain, ca)
    attempt = _offline_calls.get(key, 0)
    _offline_calls[key] = attempt + 1
    data = samples.get(ca, {}) if attempt else {}
    metadata = {}
    mcap = _best_mcap_from_ds_pairs(data.get("pairs", []), chain, ca, metadata=metadata)
    return {"mcap": mcap, "source": "dexscreener" if mcap is not None else None,
            "cached": False, "error": None if mcap is not None else "offline_missing", **metadata}


# A capacity of one prevents bursts; missing config retains these compatible defaults.
SOURCE_RATES = {"dexscreener": 4.0, "geckoterminal": 0.4}
SOURCES = frozenset({"dexscreener", "geckoterminal", "pumpfun", "binance"})


def lookup_mcap(chain: str, ca: str, *, use_cache: bool = True, store=None,
                budget_ms=None) -> dict[str, Any]:
    """Return mcap/source/cached/error plus source failures for notes."""
    started = time.monotonic()
    ca = (ca or "").strip()
    chain = (chain or "").strip().lower()
    failures = []

    def result(mcap=None, source=None, error=None, cached=False, metadata=None):
        return {"mcap": mcap, "source": source, "cached": cached,
                "error": error, "failures": failures.copy(), **(metadata or {})}

    if not ca or len(ca) < 8:
        return result(error="bad_ca")
    store = default_store() if store is None else store
    cfg = store.cfg.get("enrich", {})
    deadline = started + float(cfg.get("budget_ms", 6000) if budget_ms is None else budget_ms) / 1000

    def remaining():
        return deadline - time.monotonic()

    if remaining() <= 0:
        return result(error="budget")
    if os.environ.get("ENRICH_OFFLINE") == "1":
        return _offline_mcap(chain, ca)

    key = _cache_key(chain, ca)
    if use_cache:
        store.migrate_mcap_cache()
        entry = store.get_mcap_cache(key)
        if remaining() <= 0:
            return result(error="budget")
        if entry:
            ttl = cfg.get("ttl_fail", FAIL_TTL_SEC) if entry["error"] or entry["mcap"] is None else cfg.get("ttl_ok", TTL_SEC)
            # Never let a legacy transient failure suppress a fresh fallback.
            if (entry["error"] not in {"429", "5xx", "timeout", "budget", "network_error"}
                    and 0 <= store.clock() - entry["ts"] < ttl):
                metadata = _metadata(entry.get("price_at_fetch"), entry.get("liq_usd"), entry.get("pair_created_at"))
                return result(entry["mcap"], entry["source"], entry["error"], cached=True, metadata=metadata)

    for source, fetch in (("dexscreener", fetch_dexscreener), ("geckoterminal", fetch_geckoterminal)):
        if remaining() <= 0:
            return result(error="budget")
        name = "mcap:" + source
        if store.breaker_is_open(name):
            failures.append({"source": source, "error": "429"})
            log_event("enrich_skip", source=source, reason="429", breaker_open=True)
            continue
        rate = float(cfg.get(source + "_rps", SOURCE_RATES[source]))
        if not math.isfinite(rate) or rate <= 0:
            rate = SOURCE_RATES[source]
        rate = min(rate, SOURCE_RATES[source])
        while wait := store.take_mcap_token(source, rate):
            if wait >= remaining():
                return result(error="budget")
            time.sleep(wait)
        if remaining() <= 0:
            return result(error="budget")
        if store.breaker_is_open(name):
            failures.append({"source": source, "error": "429"})
            log_event("enrich_skip", source=source, reason="429", breaker_open=True)
            continue
        metadata = {}
        status, mcap = fetch(ca, chain, timeout=min(float(cfg.get("timeout", 4)), remaining()), metadata=metadata)
        if status == 429:
            cooldown = store.backoff_breaker(name, float(cfg.get("backoff_base", 1)))
            log_event("enrich_backoff", source=source, status=429, cooldown=cooldown)
        elif status in {200, "not_found"}:
            store.reset_breaker(name)
        if status != 200:
            failures.append({"source": source, "error": str(status)})
        if remaining() <= 0:
            return result(error="budget")
        if mcap is not None:
            if use_cache:
                store.put_mcap_cache(key, mcap, source, None, **metadata)
            return result(mcap, source, metadata=metadata) if remaining() > 0 else result(error="budget")

    errors = [f["error"] for f in failures]
    error = next((e for e in ("429", "timeout", "5xx", "network_error", "bad_json") if e in errors),
                 errors[-1] if errors else "not_found")
    # Cache a definitive absence, but allow transient errors to recover on retry.
    if use_cache and error == "not_found":
        store.put_mcap_cache(key, None, None, error)
    return result(error=error) if remaining() > 0 else result(error="budget")


def enrich_signal(sig: dict, *, store=None) -> dict:
    """Fill missing mcap; annotate every source failure, including successful fallback."""
    existing = parse_num(sig.get("mcap"))
    if not isinstance(sig.get("mcap_source"), str) or sig["mcap_source"] not in SOURCES:
        sig.pop("mcap_source", None)
    if existing is not None and existing > 0:
        if sig.get("source") == "binance_smart_money" and "mcap_source" not in sig:
            sig["mcap_source"] = "binance"
        return sig
    result = lookup_mcap(str(sig.get("chain") or ""), str(sig.get("ca") or ""), store=store)
    notes = []
    for failure in result.get("failures", []):
        note = f"mcap_enrich=failed:{failure['error']}"
        if note not in notes:
            notes.append(note)
    mcap = result.get("mcap")
    if mcap is not None and mcap > 0:
        sig["mcap"] = mcap
        sig.update(_metadata(result.get("price_at_fetch"), result.get("liq_usd"), result.get("pair_created_at")))
        if result.get("source") in SOURCES:
            sig["mcap_source"] = result["source"]
        notes.append(f"mcap_enrich={result.get('source')}:{int(mcap)}")
    else:
        sig.pop("mcap_source", None)
        note = f"mcap_enrich=failed:{result.get('error') or 'enrich_failed'}"
        if note not in notes:
            notes.append(note)
    prev = str(sig.get("notes") or "").strip()
    sig["notes"] = "; ".join(([prev] if prev else []) + notes)
    return sig


def main():
    import sys
    if len(sys.argv) < 3:
        print(json.dumps({"error": "usage: mcap_enrich.py <chain> <ca>"}))
        return
    print(json.dumps(lookup_mcap(sys.argv[1], sys.argv[2]), ensure_ascii=False))


if __name__ == "__main__":
    main()
