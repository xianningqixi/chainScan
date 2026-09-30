#!/usr/bin/env python3
"""Read-only pipeline health and event statistics. Exit: 0 healthy, 1 warning, 2 critical."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import json
import math
from pathlib import Path
import re
import sqlite3
import time

from common import ROOT, TZ


def timestamp(value):
    try:
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value.timestamp() if value.tzinfo is not None else None
    except (AttributeError, TypeError, ValueError, OverflowError):
        return None


def duration(value):
    match = re.fullmatch(r"([1-9]\d*)([mhd])", value)
    if not match:
        raise ValueError("--stats requires a positive duration such as 30m, 24h or 7d")
    return int(match[1]) * {"m": 60, "h": 3600, "d": 86400}[match[2]]


def percentile(values, fraction):
    if not values:
        return None
    values = sorted(values)
    position = (len(values) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    return round(values[lower] + (values[upper] - values[lower]) * (position - lower), 3)


def event_labels(value):
    """Count each valid annotation once per eval; tolerate old/partial events."""
    if not isinstance(value, list):
        return set()
    return {item for item in value if isinstance(item, str) and item.strip()}


def event_label(value):
    return value if isinstance(value, str) and value.strip() else "unknown"


def inspect(root=ROOT, *, now=None, stats=None):
    root = Path(root)
    now = time.time() if now is None else now
    window = duration(stats) if stats else None
    checks = {}
    issues = []
    severity = 0

    def issue(code, message):
        nonlocal severity
        severity = max(severity, code)
        issues.append(message)

    last_message = None
    requests = rate_limited = malformed = 0
    sources = {}
    rejections = Counter()
    flags, would_reject = Counter(), Counter()
    layers, verdicts = Counter(), Counter()
    layer_reasons, shadow_verdicts = {}, {}
    latencies = []
    accepted = 0
    missing_latency = 0
    try:
        with (root / "state/events.jsonl").open(encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    event = json.loads(line)
                except ValueError:
                    malformed += 1
                    continue
                ts = timestamp(event.get("ts")) if isinstance(event, dict) else None
                if ts is None:
                    malformed += 1
                    continue
                if ts > now:
                    continue
                kind = event.get("event")
                if kind in {"ws_message", "ws_heartbeat"}:
                    last_message = max(last_message or ts, ts)
                if kind == "enrich" and now - ts <= 3600:
                    requests += 1
                    limited = str(event.get("status")) == "429"
                    rate_limited += limited
                    source = str(event.get("source") or "unknown")
                    count = sources.setdefault(source, {"requests": 0, "http_429": 0})
                    count["requests"] += 1
                    count["http_429"] += limited
                if window is not None and now - ts <= window:
                    # Eval is the sole source for addon statistics. A later
                    # rejected/accepted event describes the same evaluation or
                    # its delivery and must not count the annotations again.
                    if kind == "eval":
                        layer = event_label(event.get("layer"))
                        verdict = event_label(event.get("verdict"))
                        layers[layer] += 1
                        verdicts[verdict] += 1
                        layer_reasons.setdefault(layer, Counter())[event_label(event.get("reason"))] += 1
                        flags.update(event_labels(event.get("flags")))
                        shadow = event_labels(event.get("would_reject"))
                        would_reject.update(shadow)
                        shadow_verdicts.setdefault(verdict, Counter()).update(shadow)
                    if kind == "rejected":
                        rejections[str(event.get("reason") or "unknown")] += 1
                    if kind == "accepted":
                        accepted += 1
                        latency = event.get("pipeline_latency_ms")
                        if isinstance(latency, (int, float)) and not isinstance(latency, bool) and math.isfinite(latency) and latency >= 0:
                            latencies.append(latency)
                        else:
                            missing_latency += 1
    except OSError as exc:
        issue(2, f"events_unavailable:{type(exc).__name__}")
    if malformed:
        issue(1, "malformed_event_rows")
    age = now - last_message if last_message is not None else None
    checks["ws"] = {"last_message_ts": datetime.fromtimestamp(last_message, TZ).isoformat() if last_message is not None else None,
                    "age_s": round(age, 3) if age is not None else None,
                    "ok": age is not None and age < 600}
    if not checks["ws"]["ok"]:
        issue(2, "ws_stale_or_unobserved")
    try:
        inbox = root / "inbox"
        backlog = sum(1 for p in inbox.iterdir() if p.is_file() and
                      (p.suffix in {".json", ".jsonl"} or p.name.endswith(".processing"))) if inbox.exists() else 0
        checks["inbox"] = {"files": backlog, "ok": backlog < 50}
        if backlog >= 50:
            issue(2, "inbox_backlog")
    except OSError as exc:
        checks["inbox"] = {"error": type(exc).__name__}
        issue(2, "inbox_unavailable")
    checks["enrich_1h"] = {"requests": requests, "http_429": rate_limited,
                           "ratio_429": rate_limited / requests if requests else None,
                           "by_source": sources}
    if rate_limited:
        issue(1, "enrich_429")

    database = root / "state/pipeline.db"
    checks["fomo_breaker"] = {"available": False}
    if database.exists():
        try:
            db = sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
            try:
                row = db.execute("SELECT open_until,reason FROM breaker WHERE name='fomo'").fetchone()
            finally:
                db.close()
            is_open = bool(row and row[0] > now)
            checks["fomo_breaker"] = {"available": True, "open": is_open,
                                      "open_until": row[0] if row else None, "reason": row[1] if row else None}
            if is_open:
                issue(1, "fomo_breaker_open")
        except (sqlite3.Error, TypeError) as exc:
            checks["fomo_breaker"]["error"] = type(exc).__name__
            issue(1, "fomo_state_unavailable")
    else:
        issue(1, "fomo_state_unavailable")

    # Supervisor is optional until P1-3. Never create or start one here.
    supervisor = root / "state/supervisor.json"
    checks["supervisor"] = {"available": False, "reason": "not_installed"}
    if supervisor.exists():
        try:
            state = json.loads(supervisor.read_text())
            services = state.get("services", state)
            restarts = {name: int(service.get("restarts", 0)) for name, service in services.items() if isinstance(service, dict)}
            if any(n < 0 for n in restarts.values()):
                raise ValueError("negative restart count")
            checks["supervisor"] = {"available": True, "restarts": restarts, "total_restarts": sum(restarts.values())}
            if any(restarts.values()):
                issue(1, "supervisor_restarts")
        except (OSError, ValueError, TypeError, AttributeError):
            checks["supervisor"] = {"available": False, "reason": "unreadable"}
            issue(1, "supervisor_state_unavailable")
    result = {"status": ("ok", "warning", "critical")[severity], "exit_code": severity,
              "checks": checks, "issues": issues, "malformed_event_rows": malformed}
    if window is not None:
        result["stats"] = {"window": stats, "rejected_by_reason": dict(sorted(rejections.items())),
                           "evals": sum(layers.values()),
                           "evals_by_layer": dict(sorted(layers.items())),
                           "evals_by_layer_reason": {key: dict(sorted(counts.items())) for key, counts in sorted(layer_reasons.items())},
                           "evals_by_verdict": dict(sorted(verdicts.items())),
                           "flags": dict(sorted(flags.items())),
                           "would_reject": dict(sorted(would_reject.items())),
                           "would_reject_by_verdict": {key: dict(sorted(counts.items())) for key, counts in sorted(shadow_verdicts.items())},
                           "accepted": accepted, "latency_samples": len(latencies),
                           "missing_latency": missing_latency,
                           "pipeline_latency_ms": {"p50": percentile(latencies, .5), "p95": percentile(latencies, .95)}}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stats", metavar="24h")
    args = parser.parse_args()
    try:
        result = inspect(stats=args.stats)
    except ValueError as exc:
        result = {"status": "critical", "exit_code": 2, "error": str(exc)}
    print(json.dumps(result, ensure_ascii=False))
    return result["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
