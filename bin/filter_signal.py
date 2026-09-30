#!/usr/bin/env python3
"""Score inbound raw signals; return True if useful enough to push.

v4: big-coin vs alpha is MARKET CAP only (no token-name / CA denylists).
Alpha band: MCAP_MIN..MCAP_MAX ($30k–$5M). Tokens above (e.g. ONDO, ANSEM)
or below are rejected as mcap_out_of_range. Missing mcap after enrich ->
mcap_pending by default (ingest must enrich before calling is_useful).
"""
from __future__ import annotations
import ast, hashlib, json, math, re, sys
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from common import load_config

_FILTER = load_config()["filter"]
MIN_BUY_USD = _FILTER["min_buy_usd"]
MIN_SMART = _FILTER["min_smart"]
MCAP_MIN = _FILTER["mcap_min"]
MCAP_MAX = _FILTER["mcap_max"]
ALLOWED_CHAINS = set(_FILTER["allowed_chains"])
ALLOWED_CHAIN_IDS = set(_FILTER["allowed_chain_ids"])
BAD_STATUS = set(_FILTER["bad_status"])
TEST_CA = re.compile(r"^(abc123|0xdead|test)", re.I)

# Notes / tags that mark launchpad / meme alpha (Binance tokenTag blobs) — informational only
ALPHA_TAG_HINTS = (
    "pumpfun", "pump.fun", "flap", "dynamic bc", "genius.fun", "four.meme",
    "launch platform", "living token", "moonshot", "letsbonk", "raydium launchlab",
    "alpha-dynamic", "wmp-label-title-pumpfun", "wmp-label-title-flap",
)

def parse_num(x):
    if x is None: return None
    s = str(x).replace(",", "").replace("$", "").strip().upper()
    mult = 1.0
    if s.endswith("K"):
        mult = 1_000; s = s[:-1]
    elif s.endswith("M"):
        mult = 1_000_000; s = s[:-1]
    try:
        value = float(s) * mult
        return value if math.isfinite(value) else None
    except (ValueError, OverflowError):
        return None

def chain_ok(chain, cfg=None) -> bool:
    cfg = _FILTER if cfg is None else cfg
    c = str(chain or "").lower().strip()
    if not c:
        return False
    if c in cfg["allowed_chains"] or c in {x.lower() for x in cfg["allowed_chain_ids"]}:
        return True
    # unknown numeric chain -> reject for now (e.g. 4663 noise)
    if c.isdigit():
        return False
    return False

def has_alpha_hint(sig: dict) -> bool:
    ca = str(sig.get("ca") or "")
    if ca.lower().endswith("pump"):
        return True
    notes = str(sig.get("notes") or "").lower()
    return any(h in notes for h in ALPHA_TAG_HINTS)

def _baseline(sig: dict, cfg=None) -> tuple[bool, str]:
    cfg = _FILTER if cfg is None else {**_FILTER, **cfg.get("filter", cfg)}
    # drop test / smoke only (no bluechip name/CA denylists — use mcap)
    name = str(sig.get("token_name") or "")
    name_key = name.strip().casefold()
    if name.upper() in {"TEST", "DEMO"} or "test-sol" in str(sig.get("signal_id") or ""):
        return False, "test_signal"
    if "smoke" in name_key or "wsol-smoke" in name_key:
        return False, "test_signal"

    ca = str(sig.get("ca") or "").strip()
    if not ca or len(ca) < 8:
        return False, "missing_ca"
    if TEST_CA.search(ca):
        return False, "test_ca"

    if not chain_ok(sig.get("chain"), cfg):
        return False, f"chain_blocked:{sig.get('chain')}"

    status = str(sig.get("status") or "active").lower()
    if status in cfg["bad_status"]:
        return False, f"status_{status}"

    direction = str(sig.get("direction") or "buy").lower()
    if direction == "sell":
        return False, "sell_skip"

    buy = parse_num(sig.get("buy_usd"))
    smart = parse_num(sig.get("smart_money_count"))
    mcap = parse_num(sig.get("mcap"))

    # Unknown policy only relaxes the mcap gate; all other filters still apply.
    if mcap is None:
        policy = cfg.get("mcap_unknown_policy", "pending")
        if policy != "pass_flagged":
            return False, "mcap_pending" if policy == "pending" else "mcap_unknown"
    elif not (cfg['mcap_min'] <= mcap <= cfg['mcap_max']):
        return False, f"mcap_out_of_range:{mcap}"

    reasons = []
    if mcap is None:
        reasons.append("mcap_unknown")
    ok_flow = False
    if buy is not None and buy >= cfg['min_buy_usd']:
        ok_flow = True; reasons.append(f"buy_usd>={cfg['min_buy_usd']}")
    if smart is not None and smart >= cfg['min_smart']:
        ok_flow = True; reasons.append(f"smart>={cfg['min_smart']}")
    if not ok_flow:
        return False, "flow_too_small"

    if direction == "watch" and not (buy and buy >= cfg['min_buy_usd'] * cfg['watch_buy_multiplier']):
        return False, "watch_without_strong_buy"

    # notes risk tags from binance
    notes = str(sig.get("notes") or "").lower()
    if "insider wash trading" in notes or "wash trading" in notes:
        return False, "wash_trading_tag"
    if cfg.get("baseline_dev_close_gate", True) and "dev close position" in notes and (smart or 0) < cfg["dev_close_min_smart"]:
        return False, "dev_close_weak"

    if has_alpha_hint(sig):
        reasons.append("alpha_hint")
    return True, ",".join(reasons)


@dataclass(frozen=True)
class EvalContext:
    now_ts: float = 0
    first_seen_ts: float = 0
    first_price: float | None = None
    eval_count: int = 0
    token_last_push_ts: float | None = None
    token_last_push_smart: float | None = None
    token_sig_count_60m: int = 1
    token_previous_max_smart: float | None = None


@dataclass(frozen=True)
class FilterResult:
    verdict: str
    reason: str
    layer: str = "L0"
    terminal: bool = False
    flags: list[str] = field(default_factory=list)
    score: float = 0
    would_reject: list[str] = field(default_factory=list)
    mcap_eval: float | None = None


def _config(cfg=None):
    supplied = (cfg or {}).get("filter", cfg or {})
    merged = {**_FILTER, **supplied}
    for key in ("l0", "l1", "l2"):
        merged[key] = {**_FILTER.get(key, {}), **supplied.get(key, {})}
    return merged


def extract_tags(sig):
    """Read tagName values only; historical notes are parsed as data, never code.

    An explicit empty tags list is authoritative. Exact standalone legacy risk
    labels remain supported, but arbitrary enrichment prose is never scanned.
    """
    if isinstance(sig.get("tags"), list):
        return sorted({t.strip() for t in sig["tags"] if isinstance(t, str) and t.strip()})
    blob = sig.get("tokenTag")
    if blob is None:
        notes = str(sig.get("notes") or "")
        match = re.search(r"(?:^|;\s*)tag=", notes)
        if match:
            tail = notes[match.end():]
            # Locate the balanced container, respecting quoted semicolons.
            depth, quote, escape, end = 0, None, False, 0
            for end, char in enumerate(tail, 1):
                if quote:
                    if escape:
                        escape = False
                    elif char == "\\":
                        escape = True
                    elif char == quote:
                        quote = None
                elif char in "\"'":
                    quote = char
                elif char in "[{":
                    depth += 1
                elif char in "]}":
                    depth -= 1
                    if depth == 0:
                        break
                elif char == ";" and depth == 0:
                    end -= 1
                    break
            try:
                blob = json.loads(tail[:end])
            except (ValueError, TypeError, RecursionError):
                try:
                    blob = ast.literal_eval(tail[:end])
                except (ValueError, SyntaxError, TypeError, RecursionError):
                    # A plain legacy tag is also data, confined to tag=.
                    blob = tail[:end].strip() if not tail.lstrip().startswith(("{", "[")) else None
        elif notes.strip().casefold() in {"wash trading", "insider wash trading", "dev close position"}:
            return [notes.strip()]
    if isinstance(blob, str):
        try:
            blob = json.loads(blob)
        except (ValueError, TypeError, RecursionError):
            try:
                blob = ast.literal_eval(blob)
            except (ValueError, SyntaxError, TypeError, RecursionError):
                if blob.lstrip().startswith(("{", "[")):
                    return []
    tags, seen = set(), set()
    def visit(value, depth=0):
        if depth > 64 or id(value) in seen:
            return
        if isinstance(value, (dict, list)):
            seen.add(id(value))
        if isinstance(value, dict):
            if isinstance(value.get("tagName"), str):
                tags.add(value["tagName"].strip())
            for child in value.values():
                if isinstance(child, (dict, list)):
                    visit(child, depth + 1)
        elif isinstance(value, list):
            for child in value:
                visit(child, depth + 1)
        elif isinstance(value, str) and value.strip():
            tags.add(value.strip())
    visit(blob)
    return sorted(tags - {""})


def _finite(value):
    value = parse_num(value)
    return value if value is not None and math.isfinite(value) else None


def signal_metrics(sig, ctx):
    start = ctx.first_seen_ts
    try:
        ts = datetime.fromisoformat(str(sig.get("ts")).replace("Z", "+00:00"))
        if ts.tzinfo is not None:
            start = ts.timestamp()
    except (ValueError, TypeError, OverflowError):
        pass
    price = _finite(sig.get("price"))
    first_price = _finite(ctx.first_price)
    chase = (price / first_price - 1) * 100 if price and price > 0 and first_price and first_price > 0 else None
    chase = _finite(chase)
    return max(0, (ctx.now_ts - start) / 60), chase


def market_cap_eval(sig, config, ctx):
    mcap = _finite(sig.get("mcap"))
    if config.get("mcap_scale_policy", "shadow") == "off":
        return mcap
    fetch_price = _finite(sig.get("price_at_fetch"))
    basis = _finite(ctx.first_price if config.get("mcap_band_basis") == "first_seen" else sig.get("price"))
    if mcap is None or mcap <= 0 or not fetch_price or fetch_price <= 0 or not basis or basis <= 0:
        return mcap
    # Avoid intermediate float overflow when the final ratio is representable.
    scaled = float(Decimal(str(mcap)) * Decimal(str(basis)) / Decimal(str(fetch_price)))
    return scaled if math.isfinite(scaled) and scaled > 0 else mcap


def filter_fingerprint(sig, cfg=None):
    config = _config(cfg)
    base = (cfg or {}).get("reeval", {}).get("price_bucket_base", 1.25)
    if not isinstance(base, (int, float)) or not math.isfinite(base) or base <= 1:
        base = 1.25
    def bucket(value):
        value = _finite(value)
        return math.floor(math.log(value) / math.log(base)) if value and value > 0 else None
    # Include baseline flow inputs and band membership so a boundary crossing
    # cannot disappear inside a logarithmic bucket. Future labels are excluded.
    mcap = _finite(sig.get("mcap"))
    def band(value):
        return None if value is None else ("low" if value < config["mcap_min"] else "high" if value > config["mcap_max"] else "in")
    corrected = market_cap_eval(sig, config, EvalContext())
    liq = _finite(sig.get("liq_usd"))
    fields = {k: sig.get(k) for k in ("source", "chain", "ca", "token_name", "status", "direction")}
    fields.update(smart=_finite(sig.get("smart_money_count")), buy=_finite(sig.get("buy_usd")),
                  tags=sorted({t.casefold() for t in extract_tags(sig)}), price=bucket(sig.get("price")),
                  mcap=(band(mcap), bucket(mcap)), scaled_mcap=(band(corrected), bucket(corrected)),
                  liq=(bucket(liq), liq is not None and liq < config["l1"].get("low_liq_flag_usd", 10000)))
    return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()


def evaluate(sig, cfg=None, ctx=None):
    """Pure layered evaluation. New rules annotate unless explicitly enabled."""
    ctx = ctx or EvalContext()
    config = _config(cfg)
    l0, l1, l2 = (config[k] for k in ("l0", "l1", "l2"))
    tags = {t.casefold() for t in extract_tags(sig)}
    has = lambda text: any(text in t for t in tags)
    smart = _finite(sig.get("smart_money_count")) or 0
    flags, would, rejects = [], [], []
    def rule(code, matched, policy, flag=None, layer="L1", terminal=False):
        if not matched or policy == "off":
            return
        flags.append(flag or code)
        if policy == "shadow":
            would.append(code)
        elif policy == "reject":
            rejects.append((code, layer, terminal))

    ca, chain = str(sig.get("ca") or "").strip(), str(sig.get("chain") or "").lower().strip()
    pattern = r"[1-9A-HJ-NP-Za-km-z]{32,44}" if chain in {"sol", "501", "ct_501"} else r"0x[0-9a-fA-F]{40}"
    rule("bad_ca_format", bool(ca) and not re.fullmatch(pattern, ca), l0.get("ca_format_policy", "flag"), layer="L0", terminal=True)
    status = str(sig.get("status") or "active").lower()
    rule("status_unknown", status not in l0.get("allowed_status", ["active"]) and status not in config["bad_status"], l0.get("unknown_status_policy", "flag"), layer="L0")
    rule("binance_smart_missing", sig.get("source") == "binance_smart_money" and _finite(sig.get("smart_money_count")) is None, l0.get("binance_smart_policy", "flag"), layer="L0")

    age, chase = signal_metrics(sig, ctx)
    dev = has("dev close position")
    rule("dev_close", dev and smart < l1.get("dev_close_min_smart", 6), l1.get("dev_close_policy", "flag"), "dev_closed")
    rule("smart_remove", has("remove holdings") and not has("add holdings"), l1.get("smart_remove_policy", "flag"))
    cooling = (ctx.token_last_push_ts is not None and 0 <= ctx.now_ts - ctx.token_last_push_ts < l1.get("token_cooldown_min", 30) * 60
               and smart < (ctx.token_last_push_smart or 0) + l1.get("escalate_smart_delta", 2))
    rule("token_cooldown", cooling, l1.get("token_cooldown_policy", "shadow"))
    if ctx.token_last_push_ts is not None:
        flags.append(f"repeat_signal={ctx.token_sig_count_60m}")
    rule("stale", age > l1.get("late_flag_min", 15), l1.get("stale_policy", "flag"), f"late:{age:.1f}")
    rule("chase", chase is not None and chase >= l1.get("chase_flag_pct", 100), l1.get("chase_policy", "flag"), "chased")
    rule("high_tax", has("high tax"), l1.get("high_tax_policy", "flag"))
    liq = _finite(sig.get("liq_usd"))
    rule("low_liq", has("low liquidity") or (liq is not None and liq < l1.get("low_liq_flag_usd", 10000)), l1.get("low_liq_policy", "flag"))
    rule("dev_rm_liq", has("dev remove liquidity"), l1.get("dev_rm_liq_policy", "shadow"))
    confluence = ctx.token_sig_count_60m >= 2 and ctx.token_previous_max_smart is not None and smart > ctx.token_previous_max_smart
    if confluence:
        flags.append("confluence")
    score = sum(l2.get(key, weight) for matched, key, weight in (
        (smart >= 4, "w_smart_ge4", 1), (smart >= 6, "w_smart_ge6", 1),
        (has("add holdings"), "w_sm_add", 1), (has("volume surging"), "w_vol_surge", 1),
        (has("remove holdings"), "w_sm_remove", -1), (has("volume plunging"), "w_vol_plunge", -1),
        (confluence, "w_confluence", 1)) if matched)

    mcap = _finite(sig.get("mcap"))
    mcap_eval = market_cap_eval(sig, config, ctx)
    if mcap_eval != mcap:
        flags.append("mcap_scaled")
        if not config["mcap_min"] <= mcap_eval <= config["mcap_max"] and config.get("mcap_scale_policy", "shadow") == "shadow":
            would.append("mcap_scaled_out_of_range")

    # The v4 checks consume only extracted tags, never enrichment prose.
    baseline_sig = {**sig, "notes": "; ".join(sorted(tags))}
    if config.get("mcap_scale_policy") == "apply":
        baseline_sig["mcap"] = mcap_eval
    useful, reason = _baseline(baseline_sig, config)
    # Keep legacy informational reason values, without using prose as risk tags.
    if useful and has_alpha_hint(sig) and "alpha_hint" not in reason.split(","):
        reason += ",alpha_hint"
    terminal = reason.split(":", 1)[0] in {"test_signal", "test_ca", "missing_ca", "chain_blocked", "sell_skip", "wash_trading_tag"}
    if reason.startswith("mcap_out_of_range:"):
        value = float(reason.split(":", 1)[1])
        terminal = value > config["mcap_max"]
        reason = ("mcap_above_max:" if terminal else "mcap_below_min:") + str(value)
    if reason.startswith("chain_blocked:") and chain.isdigit():
        reason = f"chain_unmapped:{sig.get('chain')}"
    verdict, layer = ("accept" if useful else "reject"), "L0"
    if reason == "dev_close_weak":
        layer = "L1"
    # Unknown-cap rejections can retry on changed input; pending expiry is terminal.
    if reason == "mcap_pending":
        window = (cfg or {}).get("reeval", {}).get("window_min", 120) * 60
        if ctx.now_ts - ctx.first_seen_ts > window:
            reason, terminal = "mcap_unknown", True
        else:
            verdict = "pending"
    if useful and rejects:
        reason, layer, terminal = rejects[0]
        verdict = "reject"
    elif useful and l2.get("score_gate_enabled", False) and score < l2.get("min_score", 0):
        verdict, reason, layer = "reject", "score_below_min", "L2"
    return FilterResult(verdict, reason, layer, terminal, flags, score, would, mcap_eval)


def compatibility_reason(reason):
    for prefix in ("mcap_below_min:", "mcap_above_max:"):
        if reason.startswith(prefix):
            return "mcap_out_of_range:" + reason.split(":", 1)[1]
    return reason.replace("chain_unmapped:", "chain_blocked:", 1)


def is_useful(sig: dict, cfg=None) -> tuple[bool, str]:
    result = evaluate(sig, cfg)
    return result.verdict == "accept", compatibility_reason(result.reason)

def main():
    raw = sys.stdin.read().strip()
    if not raw:
        print(json.dumps({"useful": False, "reason": "empty"})); return
    lines = [ln for ln in raw.splitlines() if ln.strip()]
    out = []
    for ln in lines:
        sig = json.loads(ln)
        useful, reason = is_useful(sig)
        out.append({"useful": useful, "reason": reason, "signal_id": sig.get("signal_id")})
    print(json.dumps(out[0] if len(out) == 1 else out, ensure_ascii=False))

if __name__ == "__main__":
    main()
