#!/usr/bin/env python3
"""Ingest raw signals from inbox/: enrich mcap, filter, dedupe, append useful to latest.jsonl + outbox/.

Binance-accepted signals get on-demand FOMO REST dual-verify (enrichment only; unchanged).
"""
from __future__ import annotations
import json, os, time, hashlib, sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter

from common import ROOT, TZ, append_jsonl, atomic_write, file_lock, log_event
from store import Store
INBOX = ROOT / "inbox"
OUTBOX = ROOT / "outbox"
STATE = ROOT / "state"
LATEST = ROOT / "latest.jsonl"
from filter_signal import (FilterResult, evaluate, extract_tags, filter_fingerprint,
                           parse_num, signal_metrics)  # noqa
import fomo_verify  # noqa
import mcap_enrich  # noqa



def signal_id(sig):
    if sig.get("signal_id"):
        return str(sig["signal_id"])
    base = f"{sig.get('source')}|{sig.get('chain')}|{sig.get('ca')}|{sig.get('ts')}|{sig.get('signal_type')}"
    return hashlib.sha1(base.encode()).hexdigest()[:24]


def normalize(sig):
    sig = dict(sig)
    sig.setdefault("schema_version", 1)
    sig["signal_id"] = signal_id(sig)
    if not sig.get("ts"):
        sig["ts"] = datetime.now(TZ).isoformat()
    return sig


def _append_note(sig: dict, note: str) -> None:
    prev = str(sig.get("notes") or "").strip()
    sig["notes"] = f"{prev}; {note}" if prev else note


def enrich_mcap(sig: dict, store=None) -> dict:
    """Fill mcap from DexScreener/GeckoTerminal when Binance (etc.) omits it."""
    try:
        return mcap_enrich.enrich_signal(sig, store=store)
    except (OSError, sqlite3.Error):
        raise
    except Exception as e:
        _append_note(sig, f"mcap_enrich=error:{type(e).__name__}")
        return sig


def enrich_fomo_verify(sig: dict) -> dict:
    """Dual-verify Binance signals via FOMO REST. Never drops the signal."""
    if os.environ.get("FOMO_VERIFY", "1").strip() in {"0", "false", "False", "no", "OFF"}:
        return sig
    if sig.get("source") != "binance_smart_money":
        return sig
    started = time.monotonic()
    try:
        result = fomo_verify.verify_signal(sig)
    except Exception as e:
        result = {
            "ok": False,
            "matched": False,
            "summary": f"error:{type(e).__name__}",
            "hits": 0,
            "http_status": None,
        }
    sig["fomo_verify"] = result
    summary = str(result.get("summary") or "error")
    log_event("fomo", signal_id=sig["signal_id"], summary=summary,
              ms=round((time.monotonic() - started) * 1000, 3))
    _append_note(sig, f"fomo_verify={summary}")
    # Optional soft negative annotation (still push)
    if result.get("name_mismatch"):
        _append_note(sig, "fomo_name_mismatch")
    return sig


def fingerprint(sig, cfg=None):
    return filter_fingerprint(sig, cfg)


class BadInput(ValueError):
    pass


def process_signal(raw, cfg=None, store=None):
    """Three arguments return FilterResult; legacy (raw, store) returns its tuple.

    Delivery still uses the existing atomic claim/payload journal.
    """
    legacy = isinstance(cfg, Store) or (cfg is None and store is not None)
    if isinstance(cfg, Store):
        store, cfg = cfg, cfg.cfg
    if store is None:
        raise TypeError("process_signal requires a store")
    cfg = store.cfg if cfg is None else cfg
    accepted, rejected, result = _process_signal(raw, store, cfg)
    return (accepted, rejected) if legacy else result


def _process_signal(raw, store, cfg):
    # Validate before any I/O. Only errors in these pure input operations are
    # classified as bad input; database/filesystem/programming failures retry.
    if not isinstance(raw, dict):
        raise BadInput("entry must be an object")
    for field in ("ca", "ts", "source", "chain", "token_name", "signal_type",
                  "direction", "status", "notes", "source_url"):
        if raw.get(field) is not None and not isinstance(raw[field], str):
            raise BadInput(f"{field} must be a string or null")
    for field in ("signal_id", "smart_money_count", "buy_usd", "price", "mcap"):
        if isinstance(raw.get(field), (dict, list)):
            raise BadInput(f"{field} must be scalar")
    if "tags" in raw and (not isinstance(raw["tags"], list) or any(not isinstance(t, str) for t in raw["tags"])):
        raise BadInput("tags must be a list of strings")
    try:
        for field in ("smart_money_count", "buy_usd", "price", "mcap"):
            parse_num(raw.get(field))
        sig = normalize(raw)
        sid, fp = sig["signal_id"], fingerprint(sig, cfg)
    except (TypeError, ValueError, AttributeError, OverflowError) as exc:
        raise BadInput(str(exc)) from exc
    previous = store.observe(sid, sig)
    notify_cfg = cfg.get("notify", {})
    push_updates = notify_cfg.get("push_updates", False) is True
    if not store.should_evaluate(sid, fp):
        if push_updates:
            threshold = notify_cfg.get("max_gain_threshold", notify_cfg.get("max_gain_pct", 100))
            changes = store.record_status_update(sid, previous, sig, threshold)
            if changes:
                import notify_outbox
                notify_outbox.append_update(sig, changes, root=ROOT)
        reason = "mcap_unknown" if store.mcap_window_expired(sid, unknown_only=True) else "duplicate"
        log_event("rejected", signal_id=sid, reason=reason)
        return None, {"signal_id": sid, "reason": reason}, FilterResult("reject", reason, terminal=reason == "mcap_unknown")
    # A committed push from another source, or a GMGN reservation while a
    # Binance file is still waiting in inbox, is a cross-source duplicate.
    if (store.pushed_token_other_source(sig.get("chain"), sig.get("ca"), sid, sig.get("source"))
            or store.pending_token_other_source(sig.get("chain"), sig.get("ca"), sid, sig.get("source"))):
        reason = "token_cross_source_duplicate"
        store.record_eval(sid, fp, reason, False)
        log_event("rejected", signal_id=sid, reason=reason)
        return None, {"signal_id": sid, "reason": reason}, FilterResult("reject", reason)
    # Reserve every producer's CA before enrichment. This closes the inbox
    # race in both directions: a Binance file already being evaluated cannot
    # be claimed by GMGN, while a same-source reservation remains retryable.
    if not store.reserve_token(sig.get("chain"), sig.get("ca"), sid, sig.get("source")):
        if (store.pushed_token_other_source(sig.get("chain"), sig.get("ca"), sid, sig.get("source"))
                or store.pending_token_other_source(sig.get("chain"), sig.get("ca"), sid, sig.get("source"))):
            reason = "token_cross_source_duplicate"
            store.record_eval(sid, fp, reason, False)
            log_event("rejected", signal_id=sid, reason=reason)
            return None, {"signal_id": sid, "reason": reason}, FilterResult("reject", reason)
    sig = enrich_mcap(sig, store=store)
    ctx = store.build_context(sig)
    result = evaluate(sig, cfg, ctx)
    useful, reason = result.verdict == "accept", result.reason
    if useful and "mcap_unknown" in reason.split(","):
        _append_note(sig, "mcap_unknown")
    log_event("eval", signal_id=sid, useful=useful, reason=reason, layer=result.layer,
              terminal=result.terminal, flags=result.flags, would_reject=result.would_reject,
              score=result.score, verdict=result.verdict)
    if not useful:
        store.record_eval(sid, fp, result, mcap_source=sig.get("mcap_source"))
        store.release_token_reservation(sid)
        log_event("rejected", signal_id=sid, reason=reason)
        return None, {"signal_id": sid, "reason": reason, "mcap": sig.get("mcap")}, result
    age, chase = signal_metrics(sig, ctx)
    sig.update(tags=extract_tags(sig), filter_version=cfg["filter"].get("filter_version", "v5-shadow"),
               filter_flags=result.flags, filter_score=result.score, eval_reason=reason,
               eval_count=ctx.eval_count + 1, first_seen_ts=datetime.fromtimestamp(ctx.first_seen_ts, TZ).isoformat(),
               age_min=age, chase_pct=chase, token_sig_count=ctx.token_sig_count_60m,
               mcap_eval=result.mcap_eval)
    if result.flags and cfg["filter"].get("append_flags_to_notes", False):
        _append_note(sig, "flags=" + ",".join(result.flags))
    sig = enrich_fomo_verify(sig)
    # Journal before recording the evaluation, so interruption remains retryable.
    if not store.queue_push(sig):
        log_event("rejected", signal_id=sid, reason="duplicate")
        store.record_eval(sid, fp, result, mcap_source=sig.get("mcap_source"))
        store.release_token_reservation(sid)
        return None, {"signal_id": sid, "reason": "duplicate"}, FilterResult("reject", "duplicate")
    store.record_eval(sid, fp, result, mcap_source=sig.get("mcap_source"))
    return sig, None, result


def process_file(path: Path, store):
    data = path.read_bytes()
    accepted, rejected = [], []

    def reject(exc, content, **location):
        rejection = {"signal_id": None, "file": path.name.removesuffix('.processing'),
                     "reason": "bad_input", "error": f"{type(exc).__name__}: {exc}", **location}
        failed = path.parent / "failed"
        artifact = failed / f"{time.time_ns()}_{rejection['file']}"
        atomic_write(artifact, content)
        atomic_write(artifact.with_name(artifact.name + ".error.json"),
                     json.dumps(rejection, ensure_ascii=False) + "\n")
        rejected.append(rejection)
        log_event("rejected", **rejection)

    def entries():
        # Accept legacy JSON arrays, including arrays in .jsonl files. If the
        # whole document is not an array, parse JSONL one physical line at a time.
        if data.lstrip().startswith(b"["):
            try:
                items = json.loads(data)
            except (ValueError, UnicodeDecodeError):
                pass
            else:
                for index, raw in enumerate(items, 1):
                    yield raw, json.dumps(raw, ensure_ascii=False).encode(), {"entry": index}
                return
        for line, content in enumerate(data.splitlines(), 1):
            if not content.strip():
                continue
            try:
                raw = json.loads(content)
            except (ValueError, UnicodeDecodeError) as exc:
                reject(exc, content, line=line)
                continue
            yield raw, content, {"line": line}

    for raw, content, location in entries():
        try:
            sig, rejection = process_signal(raw, store)
        except BadInput as exc:
            reject(exc, content, **location)
            continue
        if sig is not None:
            accepted.append(sig)
        if rejection is not None:
            rejected.append(rejection)
    return accepted, rejected


def _existing_ids(path):
    if not path.exists():
        return set()
    ids = set()
    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if isinstance(obj, dict) and isinstance(obj.get("signal_id"), str):
                ids.add(obj["signal_id"])
    return ids


def pipeline_latency_ms(sig):
    try:
        ts = datetime.fromisoformat(sig["ts"].replace("Z", "+00:00"))
        if ts.tzinfo is None:
            return None
        return max(0, round((datetime.now(TZ) - ts).total_seconds() * 1000, 3))
    except (KeyError, TypeError, ValueError, AttributeError):
        return None


def publish_pending(store):
    pending = store.pending_pushes()
    if not pending:
        return
    unassigned = [r["signal_id"] for r in pending if r["batch"] is None]
    if unassigned:
        stamp = datetime.now(TZ)
        reserved = {r["batch"] for r in pending}
        while True:
            batch = OUTBOX / f"push_{stamp.strftime('%Y%m%d_%H%M%S')}.jsonl"
            if not batch.exists() and batch.name not in reserved:
                break
            stamp += timedelta(seconds=1)
        store.assign_batch(unassigned, batch.name)
        pending = store.pending_pushes()
    batches = {}
    for row in pending:
        batches.setdefault(row["batch"], []).append(json.loads(row["payload"]))
    latest_ids = store.existing_output_ids(LATEST)
    for name, sigs in batches.items():
        batch = OUTBOX / name
        batch_ids = _existing_ids(batch)
        for sig in sigs:
            if sig["signal_id"] not in batch_ids:
                sig["pipeline_latency_ms"] = pipeline_latency_ms(sig)
                store.update_pending_payload(sig)
        append_jsonl(batch, [s for s in sigs if s["signal_id"] not in batch_ids])
        append_jsonl(LATEST, [s for s in sigs if s["signal_id"] not in latest_ids])
        latest_ids.update(s["signal_id"] for s in sigs)
        with file_lock(STATE / "outbox.lock"):
            marker = OUTBOX / "NEW"
            queued = marker.read_text().splitlines() if marker.exists() else []
            if str(batch) not in queued:
                with marker.open("a", encoding="utf-8") as f:
                    f.write(str(batch) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
        store.delivered([s["signal_id"] for s in sigs])
        for sig in sigs:
            log_event("accepted", signal_id=sig["signal_id"], batch=name,
                      first_seen_ts=sig.get("first_seen_ts"),
                      pipeline_latency_ms=sig.get("pipeline_latency_ms"))


def notify_outbox_best_effort():
    try:
        import notify_outbox  # noqa
        return notify_outbox.run()
    except Exception as e:
        return {"ok": False, "reason": f"notify:{type(e).__name__}"}


def _run_locked(store=None):
    INBOX.mkdir(parents=True, exist_ok=True)
    OUTBOX.mkdir(parents=True, exist_ok=True)
    STATE.mkdir(parents=True, exist_ok=True)
    store = store if store is not None else Store(ROOT)
    publish_pending(store)
    # Recover abandoned claims first, then claim complete producer files.
    files = sorted(INBOX.glob("*.processing"))
    for path in sorted([*INBOX.glob("*.json"), *INBOX.glob("*.jsonl")]):
        claimed = path.with_name(path.name + ".processing")
        path.rename(claimed)
        files.append(claimed)
    all_acc, all_rej, errors = [], [], []
    processed = []
    for path in files:
        try:
            acc, rej = process_file(path, store)
        except Exception as exc:
            # Preserve the original claim for retry. Never quarantine infrastructure
            # failures (including failure to persist a bad-row artifact).
            error = {"file": path.name, "error": type(exc).__name__}
            errors.append(error)
            log_event("ingest_error", **error)
            continue
        all_acc.extend(acc)
        all_rej.extend(rej)
        processed.append(path)
    publish_pending(store)
    done = INBOX / "done"
    done.mkdir(exist_ok=True)
    for path in processed:
        path.rename(done / f"{time.time_ns()}_{path.name.removesuffix('.processing')}")
    notify_outbox_best_effort()
    report = {"accepted": len(all_acc), "rejected": len(all_rej), "files": len(files),
              "outbox": bool(all_acc), "rejected_detail": all_rej, "errors": errors,
              "rejected_by_reason": dict(Counter(r["reason"].split(":", 1)[0] for r in all_rej))}
    return report


def process_batch(store=None):
    """Process one inbox snapshot, serialized with CLI and other workers."""
    with file_lock(STATE / "ingest.lock"):
        return _run_locked(store)


def main():
    report = process_batch()
    print(json.dumps({**report, "rejected_detail": report["rejected_detail"][:20]}, ensure_ascii=False))
    return report


if __name__ == "__main__":
    raise SystemExit(1 if main()["errors"] else 0)
