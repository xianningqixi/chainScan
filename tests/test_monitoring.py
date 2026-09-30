import asyncio
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.error import HTTPError

import pytest

import common
import healthcheck
import ingest
from conftest import REPO
from store import Store
from test_filter_signal import signal


def events(root):
    return [json.loads(line) for line in (root / "state/events.jsonl").read_text().splitlines()]


def event(kind, now, **fields):
    return {"event": kind, "ts": datetime.fromtimestamp(now, timezone.utc).isoformat(), **fields}


def test_events_pipeline_latency_first_seen_and_fomo(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "ROOT", tmp_path)
    monkeypatch.setattr(ingest, "OUTBOX", tmp_path / "outbox")
    monkeypatch.setattr(ingest, "LATEST", tmp_path / "latest.jsonl")
    monkeypatch.setattr(ingest, "STATE", tmp_path / "state")
    monkeypatch.setenv("FOMO_VERIFY", "1")
    monkeypatch.setattr(ingest, "enrich_mcap", lambda s, **kw: s)
    clock = [100.]
    store = Store(tmp_path, clock=lambda: clock[0])
    assert ingest.process_signal(signal(mcap=None), store)[0] is None
    clock[0] = 120.
    ts = datetime.now(timezone.utc).isoformat()
    accepted, _ = ingest.process_signal(signal(source="binance_smart_money", ts=ts), store)
    assert accepted["first_seen_ts"] == datetime.fromtimestamp(100, common.TZ).isoformat()
    ingest.publish_pending(store)
    latest = json.loads(ingest.LATEST.read_text())
    assert 0 <= latest["pipeline_latency_ms"] < 5000
    assert latest["ts"] == ts and latest["schema_version"] == 1
    assert json.loads(next(ingest.OUTBOX.glob("push_*.jsonl")).read_text()) == latest
    rows = events(tmp_path)
    assert [e["event"] for e in rows] == ["eval", "rejected", "eval", "fomo", "accepted"]
    assert rows[1]["reason"] == "mcap_pending"
    assert rows[3]["summary"] == "dry_run" and rows[3]["ms"] >= 0
    assert rows[4]["pipeline_latency_ms"] == latest["pipeline_latency_ms"]


def test_logging_failure_does_not_stop_signal(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(common, "append_jsonl", lambda *a: (_ for _ in ()).throw(OSError("disk full")))
    accepted, rejection = ingest.process_signal(signal(), Store(tmp_path))
    assert accepted and rejection is None
    assert "unable to record event" in capsys.readouterr().err


def test_ws_activity_precedes_deduplication(tmp_path, monkeypatch):
    import binance_smy_ws as ws
    monkeypatch.setattr(common, "ROOT", tmp_path)
    monkeypatch.setattr(ws, "INBOX", tmp_path / "inbox")
    monkeypatch.setattr(ws, "log", lambda *a: None)
    store = Store(tmp_path)
    message = json.dumps({"stream": "w3w@signal_smart_money", "data": {"signalId": 123}})
    asyncio.run(ws.handle(message, store))
    asyncio.run(ws.handle(message, store))
    assert [e["event"] for e in events(tmp_path)] == [
        "ws_message", "sig_queued", "ws_handle", "ws_message", "ws_handle"]
    assert [e["outcome"] for e in events(tmp_path) if e["event"] == "ws_handle"] == ["queued", "duplicate"]
    assert len(list(ws.INBOX.glob("*.jsonl"))) == 1


def test_enrich_429_is_observed(tmp_path, monkeypatch):
    import mcap_enrich as enrich
    monkeypatch.setattr(common, "ROOT", tmp_path)

    def quota(*a, **kw):
        raise HTTPError("https://example.org", 429, "quota", {}, io.BytesIO(b"private response"))

    monkeypatch.setattr(enrich.urllib.request, "urlopen", quota)
    assert enrich._http_get_json("https://api.dexscreener.com/latest/dex/tokens/public") == (429, None)
    row = events(tmp_path)[0]
    assert row["source"] == "dexscreener" and row["status"] == 429 and row["ms"] >= 0
    assert "private response" not in json.dumps(row)


def test_health_stats_and_read_only(tmp_path):
    now = 1_800_000_000.
    store = Store(tmp_path, clock=lambda: now)
    store.open_breaker("fomo", "quota", 1800)
    rows = [event("ws_message", now - 10), event("enrich", now - 60, source="dexscreener", status=429),
            event("enrich", now - 60, source="geckoterminal", status=200),
            event("enrich", now - 3601, source="dexscreener", status=429),
            event("rejected", now - 70, reason="mcap_unknown"),
            event("rejected", now - 86401, reason="sell_skip")]
    rows += [event("accepted", now - 20, pipeline_latency_ms=n) for n in (100, 200, 300, 400, None)]
    common.append_jsonl(tmp_path / "state/events.jsonl", rows)
    common.atomic_write(tmp_path / "state/supervisor.json", json.dumps({"services": {"ws": {"restarts": 3}}}))
    snapshot = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    result = healthcheck.inspect(tmp_path, now=now, stats="24h")
    assert result["exit_code"] == 1
    assert result["checks"]["ws"]["ok"]
    assert result["checks"]["enrich_1h"]["ratio_429"] == .5
    assert result["checks"]["fomo_breaker"]["open"]
    assert result["checks"]["supervisor"]["total_restarts"] == 3
    assert result["stats"]["rejected_by_reason"] == {"mcap_unknown": 1}
    assert result["stats"]["pipeline_latency_ms"] == {"p50": 250, "p95": 385}
    assert result["stats"]["missing_latency"] == 1
    assert all(p.read_bytes() == data for p, data in snapshot.items())
    assert healthcheck.inspect(tmp_path, now=now + 600)["exit_code"] == 2


def test_health_backlog_boundaries_and_missing_monitoring(tmp_path):
    now = 1_800_000_000.
    Store(tmp_path)
    common.append_jsonl(tmp_path / "state/events.jsonl", [event("ws_message", now - 599)])
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    for i in range(49):
        (inbox / f"{i}.jsonl.processing").touch()
    assert healthcheck.inspect(tmp_path, now=now)["exit_code"] == 0
    (inbox / "last.jsonl").touch()
    assert healthcheck.inspect(tmp_path, now=now)["exit_code"] == 2
    assert healthcheck.inspect(tmp_path, now=now + 1)["checks"]["ws"]["ok"] is False
    empty = tmp_path / "empty"
    assert healthcheck.inspect(empty, now=now)["exit_code"] == 2
    assert not empty.exists()


def test_health_cli_and_corrupt_events(tmp_path):
    common.append_jsonl(tmp_path / "state/events.jsonl", [None, {"ts": "broken"}])
    with (tmp_path / "state/events.jsonl").open("a") as f:
        f.write('{"truncated":')
    result = subprocess.run([sys.executable, str(REPO / "bin/healthcheck.py"), "--stats", "24h"],
                            env={**os.environ, "SIGNALS_ROOT": str(tmp_path)}, capture_output=True, text=True)
    assert result.returncode == 2
    report = json.loads(result.stdout)
    assert report["malformed_event_rows"] == 3
    assert report["stats"]["pipeline_latency_ms"] == {"p50": None, "p95": None}


def test_first_seen_precedes_slow_enrichment(tmp_path, monkeypatch):
    clock = [100.]
    store = Store(tmp_path, clock=lambda: clock[0])

    def slow_enrich(sig, **kw):
        clock[0] += 30
        return sig

    monkeypatch.setattr(ingest, "enrich_mcap", slow_enrich)
    accepted, _ = ingest.process_signal(signal(), store)
    assert accepted["first_seen_ts"] == datetime.fromtimestamp(100, common.TZ).isoformat()
