#!/usr/bin/env python3
"""Replay public JSONL into an empty isolated ROOT, without network or secrets."""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import sqlite3
import subprocess
import sys
import tomllib


def mcap_mode(value):
    if value == "fixture":
        return value
    try:
        number = float(value.removeprefix("fixed:"))
        if value.startswith("fixed:") and math.isfinite(number) and number > 0:
            return value
    except ValueError:
        pass
    raise argparse.ArgumentTypeError("expected fixture or fixed:<positive finite value>")


def read_labels(payload, labels, signal_id):
    """Collect future outcomes separately; never attach derived labels to signals."""
    try:
        entries = json.loads(payload) if payload.lstrip().startswith(b"[") else None
    except (ValueError, UnicodeDecodeError):
        entries = None
    for entry in entries if entries is not None else payload.splitlines():
        try:
            row = json.loads(entry) if isinstance(entry, bytes) else entry
            if not isinstance(row, dict):
                continue
            values = [row.get("maxGain"), row.get("maxgain")]
            values += re.findall(r"(?:^|;)\s*maxGain=([+-]?(?:\d+(?:\.\d*)?|\.\d+))%", str(row.get("notes") or ""))
            for value in values:
                try:
                    gain = float(str(value).rstrip("%"))
                except (ValueError, TypeError):
                    continue
                if math.isfinite(gain):
                    sid = signal_id(row)
                    labels[sid] = max(gain, labels.get(sid, -math.inf))
        except (ValueError, UnicodeDecodeError):
            continue


def ratio(numerator, denominator):
    return {"numerator": numerator, "denominator": denominator,
            "rate": numerator / denominator if denominator else None}


def metrics(target, rows, labels, label_enabled, rejections):
    from store import token_key

    with sqlite3.connect((target / "state/pipeline.db").as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        evals = list(db.execute("SELECT * FROM evals"))
        signals = list(db.execute("SELECT * FROM signals"))
    pushed = {r["signal_id"] for r in rows}
    tokens = {token_key(r.get("chain"), r.get("ca")) for r in rows}
    pending = {r["signal_id"] for r in evals if r["verdict"] == "pending"}
    delays = sorted(r["pushed_at"] - r["first_seen_ts"] for r in signals if r["pushed_at"] is not None)
    flags, shadow, layers = Counter(), Counter(), Counter()
    shadow_ids = {}
    for row in evals:
        layers[row["layer"]] += 1
        flags.update(json.loads(row["flags_json"]))
        rules = json.loads(row["would_reject_json"])
        shadow.update(rules)
        for rule in rules:
            shadow_ids.setdefault(rule, set()).add(row["signal_id"])
    report = {
        "pushes": len(rows), "tokens_pushed": len(tokens),
        "dup_push_rate": (len(rows) - len(tokens)) / len(rows) if rows else 0.0,
        "reject_by_reason": dict(sorted(rejections.items())),
        "pending_resolved": len(pending & pushed),
        "pending_expired": sum(r["signal_id"] in pending and r["last_reason"] == "mcap_unknown"
                               and bool(r["last_terminal"]) for r in signals),
        "pending_remaining": sum(r["last_verdict"] == "pending" for r in signals),
        "first_seen_to_push": {
            "unit": "seconds", "basis": "replay wall clock; not historical arrival latency",
            "p50": delays[math.ceil(len(delays) * .50) - 1] if delays else None,
            "p95": delays[math.ceil(len(delays) * .95) - 1] if delays else None,
        },
        "evals_by_layer": dict(sorted(layers.items())),
        "flags": dict(sorted(flags.items())),
        "would_reject": {rule: {"evaluations": shadow[rule], "signals": len(ids)}
                         for rule, ids in sorted(shadow_ids.items())},
        "timing_note": "Input is processed in file/row order without advancing historical time; cooldown and pending expiry use replay time.",
        "metric_note": "Rejection counts include pending and duplicate input rows; flags count evaluations. Shadow outcome counts are unique signal IDs. Outcome rates exclude unlabeled signals.",
    }
    if label_enabled:
        # Only observed, valid signals belong in the evaluation denominator.
        observed = {r["signal_id"] for r in signals}
        labels = {sid: gain for sid, gain in labels.items() if sid in observed}
        winners = {sid for sid, gain in labels.items() if gain >= 100}
        labeled_pushes = pushed & labels.keys()
        report.update(
            label="maxgain", label_note="maxGain is a post-hoc label, never a filtering input; maximum observed per signal_id, in percent.",
            n=len(labels), unlabeled_signals=len(observed - labels.keys()),
            unlabeled_pushes=len(pushed - labels.keys()),
            winner_recall=ratio(len(pushed & winners), len(winners)),
            precision_50=ratio(sum(labels[sid] >= 50 for sid in labeled_pushes), len(labeled_pushes)),
        )
        for rule, ids in shadow_ids.items():
            report["would_reject"][rule].update(
                winners=len(ids & winners), non_winners=len((ids & labels.keys()) - winners),
                unlabeled=len(ids - labels.keys()))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="source", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--config", type=Path, help="TOML overrides merged with repository defaults")
    parser.add_argument("--mcap-mode", type=mcap_mode, default="fixture",
                        help="fixture (default), or fixed:<v> to override every valuation offline")
    parser.add_argument("--compare", nargs="?", const="", metavar="CONFIG",
                        help="compare against defaults, or another TOML config, in an isolated baseline root")
    parser.add_argument("--label", choices=["maxgain"], help="report-only post-hoc outcome metrics")
    args = parser.parse_args()
    source, target = args.source.resolve(), args.out.resolve()
    if not source.is_dir():
        parser.error("--from must be a directory")
    if target.exists() and (not target.is_dir() or any(target.iterdir())):
        parser.error("--out must be empty (production data must never be overwritten)")
    if target == source or target in source.parents or source in target.parents:
        parser.error("input and output must be separate directories")
    repo = Path(__file__).resolve().parents[1]
    for root in {repo, Path("/workspace/signals"), Path(os.environ.get("SIGNALS_ROOT", repo)).resolve()}:
        for name in ("inbox", "outbox", "state", "logs", "config", "bin", "tests", "deploy",
                     ".venv", ".git", ".agents", ".codex", ".aws"):
            protected = (root / name).resolve()
            if target == protected or protected in target.parents:
                parser.error("--out must not be inside a production or code directory")
    for config in (args.config, Path(args.compare) if args.compare else None):
        if config is not None:
            try:
                with config.open("rb") as f:
                    tomllib.load(f)
            except (OSError, ValueError) as exc:
                parser.error(f"invalid config: {exc}")
    os.environ["SIGNALS_ROOT"] = str(target)
    os.environ.pop("SIGNALS_CONFIG", None)
    if args.config:
        os.environ["SIGNALS_CONFIG"] = str(args.config.resolve())
    os.environ.pop("NOTIFY_WEBHOOK_URL", None)
    os.environ["ENRICH_OFFLINE"] = "1"
    os.environ["ENRICH_FIXTURE"] = str(Path(__file__).resolve().parents[1] / "tests/fixtures/dexscreener_offline.json")
    os.environ["FOMO_VERIFY_DRY"] = "1"

    def offline(*args, **kwargs):
        raise RuntimeError("replay network disabled")

    socket.socket.connect = offline
    socket.socket.connect_ex = offline
    socket.create_connection = offline
    socket.getaddrinfo = offline
    socket.socket.sendto = offline
    sys.dont_write_bytecode = True
    import ingest
    from common import atomic_write
    from filter_signal import compatibility_reason

    if args.mcap_mode.startswith("fixed:"):
        fixed = float(args.mcap_mode.split(":", 1)[1])

        def fixed_mcap(sig, store=None):
            sig = dict(sig, mcap=fixed, mcap_source="replay_fixed")
            # Fixed means fixed, even when inputs contain cached price metadata.
            sig.pop("price_at_fetch", None)
            return sig

        ingest.enrich_mcap = fixed_mcap

    rejections = {}
    process = ingest._process_signal

    def record_outcome(raw, store, cfg):
        outcome = process(raw, store, cfg)
        _, rejected, result = outcome
        if rejected is not None:
            code = result.reason.split(":", 1)[0]
            counts = rejections.setdefault(code, {"total": 0, "terminal": 0, "non_terminal": 0})
            counts["total"] += 1
            counts["terminal" if result.terminal else "non_terminal"] += 1
        return outcome

    ingest._process_signal = record_outcome

    inbox = target / "inbox"
    inbox.mkdir(parents=True)
    count = 0
    input_hash = hashlib.sha256()
    labels = {}
    for p in sorted(source.iterdir()):
        if p.suffix not in {".jsonl", ".json"} or not p.is_file():
            continue
        payload = p.read_bytes()
        input_hash.update(hashlib.sha256(payload).digest())
        if args.label:
            read_labels(payload, labels, ingest.signal_id)
        atomic_write(inbox / f"{count:08d}{p.suffix}", payload)
        count += 1
    result = ingest.main()
    rows = [json.loads(line) for line in ingest.LATEST.read_text().splitlines()] if ingest.LATEST.exists() else []
    reasons = Counter(compatibility_reason(r["reason"]).split(":", 1)[0] for r in result.get("rejected_detail", []))
    bad_inputs = sum(r["reason"] == "bad_input" for r in result.get("rejected_detail", []))
    if bad_inputs:
        rejections["bad_input"] = {"total": bad_inputs, "terminal": bad_inputs, "non_terminal": 0}
    rejected_ids = {r["signal_id"] for r in result.get("rejected_detail", []) if r["reason"] != "duplicate"}
    report = {
        "source": str(source), "offline": True,
        "valuation_note": "Synthetic provider responses for five fixture tokens: first lookup missing, later $100K. Not historical valuations. Other tokens have no offline response.",
        "files": count, "accepted": len(rows), "rejected": result["rejected"],
        "input_sha256": input_hash.hexdigest(),
        "rejection_reasons": dict(sorted(reasons.items())),
        "unique_accepted": len({r["signal_id"] for r in rows}),
        "recovered_after_rejection": sorted({r["signal_id"] for r in rows} & rejected_ids),
        "mcap_mode": args.mcap_mode,
        "config": str(args.config.resolve()) if args.config else None,
        "errors": result.get("errors", []),
    }
    if args.mcap_mode.startswith("fixed:"):
        report["valuation_note"] = f"All replay valuations fixed at {fixed:g}; synthetic, not historical valuations. Cached price scaling disabled for this mode."
    report.update(metrics(target, rows, labels, bool(args.label), rejections))
    if args.compare is not None:
        baseline_out = target / "baseline"
        command = [sys.executable, str(Path(__file__).resolve()), "--from", str(source),
                   "--out", str(baseline_out), "--mcap-mode", args.mcap_mode]
        if args.compare:
            command += ["--config", str(Path(args.compare).resolve())]
        if args.label:
            command += ["--label", args.label]
        baseline_run = subprocess.run(command, capture_output=True, text=True)
        if baseline_run.returncode:
            raise RuntimeError(f"baseline replay failed: {baseline_run.stderr}")
        baseline = json.loads((baseline_out / "report.json").read_text())
        if baseline["input_sha256"] != report["input_sha256"]:
            raise RuntimeError("source changed during comparison; use a stable input snapshot")
        report["comparison"] = {
            "note": "Candidate minus baseline; same input order and mcap mode, separate empty state. Baseline uses current defaults unless CONFIG is supplied; this is not a frozen v4 comparison.",
            "baseline": baseline,
            "delta": {key: report[key] - baseline[key] for key in
                      ("pushes", "tokens_pushed", "dup_push_rate", "pending_resolved", "pending_expired")},
        }
        for key in ("winner_recall", "precision_50"):
            if key in report:
                current, previous = report[key]["rate"], baseline[key]["rate"]
                report["comparison"]["delta"][key] = current - previous if current is not None and previous is not None else None
    atomic_write(target / "report.json", json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False))
    return 1 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
