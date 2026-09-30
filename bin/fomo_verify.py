#!/usr/bin/env python3
"""On-demand FOMO REST dual-verify for a Binance (or other) signal.

ONE cheap REST call per signal — never keep WS running.
Endpoint: GET /v2/thesis/token/{tokenAddress}?limit=1  (~200 CU)
Env:
  FOMOSCAN_API_KEY  or  state/fomo_api_key.env
  FOMO_VERIFY=0     skip verify (caller should check too)
  FOMO_VERIFY_DRY=1 return mock without HTTP
"""
from __future__ import annotations
import json, os, sys, urllib.error, urllib.parse, urllib.request
from pathlib import Path
from typing import Any
from store import default_store

from common import ROOT, TZ, load_config
KEY_ENV_FILE = ROOT / "state" / "fomo_api_key.env"
BASE = os.environ.get("FOMOSCAN_API_BASE", "https://api.fomoscan.sh").rstrip("/")
TIMEOUT = float(os.environ.get("FOMO_VERIFY_TIMEOUT", load_config()["fomo"]["timeout"]))
# Price: 200 CU per started block of 20 requested rows; limit=1 => 200 CU
DEFAULT_LIMIT = 1


def breaker_is_open() -> bool:
    return default_store().breaker_is_open("fomo")


def open_breaker(reason: str, cooldown: float | None = None) -> None:
    if cooldown is None:
        cooldown = load_config()["fomo"]["breaker_cooldown"]
    default_store().open_breaker("fomo", reason, cooldown)


def load_api_key() -> str:
    key = (os.environ.get("FOMOSCAN_API_KEY") or "").strip()
    if key:
        return key
    if KEY_ENV_FILE.exists():
        for line in KEY_ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("FOMOSCAN_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def _result(
    *,
    ok: bool | None,
    matched: bool,
    summary: str,
    hits: int = 0,
    http_status: int | None = None,
    credits_note: str | None = None,
    extra: dict | None = None,
) -> dict:
    out: dict[str, Any] = {
        "ok": ok,
        "matched": matched,
        "summary": summary,
        "hits": hits,
        "http_status": http_status,
    }
    if credits_note:
        out["credits_note"] = credits_note
    if extra:
        out.update(extra)
    return out


def _normalize_ca(ca: str) -> str:
    ca = (ca or "").strip()
    if ca.startswith("0x") or ca.startswith("0X"):
        return ca.lower()
    return ca


def verify_signal(sig: dict, *, dry_run: bool | None = None) -> dict:
    """Query FOMO once for theses on this token/CA. Never raises."""
    if not load_config()["fomo"]["enabled"] or os.environ.get("FOMO_VERIFY", "1").strip() in {"0", "false", "False", "no", "OFF"}:
        return _result(ok=None, matched=False, summary="skipped", credits_note="FOMO_VERIFY=0")

    if dry_run is None:
        dry_run = os.environ.get("FOMO_VERIFY_DRY", "").strip() in {"1", "true", "True", "yes"}

    ca = _normalize_ca(str(sig.get("ca") or ""))
    name = str(sig.get("token_name") or sig.get("symbol") or "").strip()
    chain = str(sig.get("chain") or "").strip()

    if not ca or len(ca) < 8:
        return _result(ok=False, matched=False, summary="missing_ca")

    try:
        if breaker_is_open():
            return _result(ok=False, matched=False, summary="quota_exceeded",
                           credits_note="breaker_open")
    except Exception as e:
        return _result(ok=False, matched=False, summary=f"error:{type(e).__name__}")

    if dry_run:
        return _result(
            ok=True,
            matched=False,
            summary="dry_run",
            hits=0,
            http_status=None,
            credits_note="no_http",
            extra={"ca": ca, "token_name": name, "chain": chain},
        )

    key = load_api_key()
    if not key:
        return _result(ok=False, matched=False, summary="error:missing_api_key")

    # Cheapest useful per-token check: one page, limit=1 (~200 CU)
    path = f"/v2/thesis/token/{urllib.parse.quote(ca, safe='')}?limit={DEFAULT_LIMIT}"
    url = BASE + path
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
            "User-Agent": "signal-pipeline/fomo_verify/1.0",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            status = resp.status
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            pass
        code = e.code
        low = (body or "").lower()
        if code in (402, 429) or "quota" in low or "credit" in low or "insufficient" in low:
            try:
                open_breaker(f"http_{code}")
            except Exception:
                pass  # Storage trouble must not turn an annotation into a gate.
            return _result(
                ok=False,
                matched=False,
                summary="quota_exceeded",
                http_status=code,
                credits_note="http_%s" % code,
            )
        return _result(
            ok=False,
            matched=False,
            summary=f"error:http_{code}",
            http_status=code,
        )
    except Exception as e:
        # Never leak key; keep message short
        et = type(e).__name__
        return _result(ok=False, matched=False, summary=f"error:{et}")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return _result(ok=False, matched=False, summary="error:bad_json", http_status=status)

    error_text = " ".join(str(data.get(k) or "") for k in ("error", "message", "code")).lower() if isinstance(data, dict) else ""
    if any(word in error_text for word in ("quota", "credit", "insufficient")):
        try:
            open_breaker("quota")
        except Exception:
            pass
        return _result(ok=False, matched=False, summary="quota_exceeded",
                       http_status=status, credits_note="quota")

    items = data.get("items") if isinstance(data, dict) else None
    if not isinstance(items, list):
        items = data.get("theses") if isinstance(data, dict) else None
    if not isinstance(items, list):
        items = []

    try:
        hits = int(data.get("count") if isinstance(data, dict) and data.get("count") is not None else len(items))
    except (ValueError, TypeError):
        return _result(ok=False, matched=False, summary="error:bad_json", http_status=status)
    # Prefer count; if hasMore and count==limit, note more exist
    has_more = bool(isinstance(data, dict) and data.get("hasMore"))
    matched = hits > 0 or len(items) > 0

    summary = "matched" if matched else "no_hit"
    note_parts = ["~200CU"]
    if has_more:
        note_parts.append("has_more")
    sym = None
    if isinstance(data, dict):
        sym = data.get("symbol") or data.get("tokenSymbol")
    if not sym and items and isinstance(items[0], dict):
        sym = items[0].get("tokenSymbol") or items[0].get("symbol")
    extra = {}
    if sym:
        extra["fomo_symbol"] = sym
    if name and sym and str(sym).upper() != name.upper():
        # soft negative: name mismatch — still enrichment, not a drop
        extra["name_mismatch"] = True
        if matched:
            summary = "matched_name_mismatch"

    return _result(
        ok=True,
        matched=matched,
        summary=summary,
        hits=hits,
        http_status=status,
        credits_note="+".join(note_parts),
        extra=extra or None,
    )


def fetch_me() -> dict | None:
    """Optional /v2/me probe. Returns usage dict or None. Never prints key."""
    key = load_api_key()
    if not key:
        return None
    req = urllib.request.Request(
        BASE + "/v2/me",
        headers={"Authorization": f"Bearer {key}", "Accept": "application/json"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception:
        return None


def remaining_credits(me: dict | None = None) -> int | None:
    me = me if me is not None else fetch_me()
    if not me:
        return None
    usage = me.get("usage") or {}
    # Explore: additionalUnits holds free pack; unitsRemaining is monthly
    add = usage.get("additionalUnits")
    rem = usage.get("unitsRemaining")
    total = 0
    if isinstance(add, (int, float)):
        total += int(add)
    if isinstance(rem, (int, float)):
        total += int(rem)
    return total


def main():
    dry = "--dry" in sys.argv or os.environ.get("FOMO_VERIFY_DRY") == "1"
    if "--me" in sys.argv:
        me = fetch_me()
        if me is None:
            print(json.dumps({"ok": False, "summary": "error:me_failed"}))
            return
        # Redact key material if present
        if isinstance(me.get("key"), dict):
            me["key"] = {k: v for k, v in me["key"].items() if k != "secret"}
        print(json.dumps({"ok": True, "usage": me.get("usage"), "plan": me.get("plan"), "remaining": remaining_credits(me)}, ensure_ascii=False))
        return

    # Prefer argv JSON (skip flags); else non-blocking-ish stdin
    raw = ""
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    if args:
        raw = args[0].strip()
    else:
        if not sys.stdin.isatty():
            raw = sys.stdin.read().strip()

    if not raw:
        print(json.dumps(_result(ok=False, matched=False, summary="error:empty_input")))
        return
    try:
        sig = json.loads(raw)
    except json.JSONDecodeError:
        print(json.dumps(_result(ok=False, matched=False, summary="error:bad_input_json")))
        return
    print(json.dumps(verify_signal(sig, dry_run=dry), ensure_ascii=False))


if __name__ == "__main__":
    main()
