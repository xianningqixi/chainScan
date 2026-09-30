"""Unknown valuations remain retryable; optional passing never bypasses other gates."""
import copy
import json

import pytest

import common
import ingest
from common import load_config
from filter_signal import is_useful
from store import Store
from test_filter_signal import signal


@pytest.mark.parametrize("policy,expected", [
    ("pending", (False, "mcap_pending")), ("reject", (False, "mcap_unknown")),
])
def test_unknown_filter_is_pure(policy, expected):
    sig = signal(mcap=None, notes="original")
    original = copy.deepcopy(sig)
    assert is_useful(sig, {"mcap_unknown_policy": policy}) == expected
    assert sig == original


@pytest.mark.parametrize("updates,reason", [
    ({"direction": "sell"}, "sell_skip"), ({"notes": "wash trading"}, "wash_trading_tag"),
    ({"smart_money_count": 0}, "flow_too_small"), ({"direction": "watch"}, "watch_without_strong_buy"),
    ({"notes": "dev close position"}, "dev_close_weak"), ({"mcap": 5000001}, "mcap_out_of_range:5000001.0"),
])
def test_pass_flagged_preserves_other_gates(updates, reason):
    sig = signal(**{"mcap": None, **updates})
    assert is_useful(sig, {"mcap_unknown_policy": "pass_flagged"}) == (False, reason)


def test_pending_same_update_retry_accepts_once(tmp_path, monkeypatch):
    values = iter([None, 100000])
    def enrich(sig, **kw):
        sig["mcap"] = next(values)
        return sig
    monkeypatch.setattr(ingest, "enrich_mcap", enrich)
    store = Store(tmp_path)
    raw = signal(mcap=None)
    assert ingest.process_signal(raw, store)[1]["reason"] == "mcap_pending"
    assert store.should_evaluate(raw["signal_id"], ingest.fingerprint(raw))
    accepted, rejection = ingest.process_signal(raw, store)
    assert accepted and rejection is None
    assert ingest.process_signal(raw, store)[1]["reason"] == "duplicate"
    assert len(store.pending_pushes()) == 1


def test_pending_only_expires_after_window(tmp_path, monkeypatch):
    now = [100.0]
    cfg = load_config()
    cfg["reeval"]["window_min"] = 1
    store = Store(tmp_path, cfg, clock=lambda: now[0])
    calls = []
    def missing(sig, **kw):
        calls.append(1)
        return sig
    monkeypatch.setattr(ingest, "enrich_mcap", missing)
    raw = signal(mcap=None)
    assert ingest.process_signal(raw, store)[1]["reason"] == "mcap_pending"
    now[0] += 60
    assert ingest.process_signal(raw, store)[1]["reason"] == "mcap_pending"
    now[0] += .001
    assert ingest.process_signal(raw, store)[1]["reason"] == "mcap_unknown"
    with store.connect() as db:
        row = db.execute("SELECT * FROM signals").fetchone()
    assert row["last_reason"] == "mcap_unknown" and row["eval_count"] == 2
    assert row["last_terminal"] == 1
    assert len(calls) == 2
    assert not store.should_evaluate(raw["signal_id"], "changed")


def test_slow_lookup_crosses_window(tmp_path, monkeypatch):
    now = [100.0]
    cfg = load_config()
    cfg["reeval"]["window_min"] = .001
    store = Store(tmp_path, cfg, clock=lambda: now[0])
    def missing(sig, **kw):
        now[0] += 1
        return sig
    monkeypatch.setattr(ingest, "enrich_mcap", missing)
    assert ingest.process_signal(signal(mcap=None), store)[1]["reason"] == "mcap_unknown"


def test_legacy_unknown_can_retry_with_default_pending(tmp_path):
    store = Store(tmp_path)
    store.record_eval("legacy", "same", "mcap_unknown", False)
    assert store.should_evaluate("legacy", "same")


def test_reject_retains_update_based_reevaluation(tmp_path, monkeypatch):
    cfg = load_config()
    cfg["filter"]["mcap_unknown_policy"] = "reject"
    store = Store(tmp_path, cfg)
    monkeypatch.setattr(ingest, "enrich_mcap", lambda sig, **kw: sig)
    assert ingest.process_signal(signal(mcap=None), store)[1]["reason"] == "mcap_unknown"
    with store.connect() as db:
        row = db.execute("SELECT * FROM signals").fetchone()
    assert row["last_verdict"] == "reject" and row["last_terminal"] == 0
    assert ingest.process_signal(signal(mcap=None), store)[1]["reason"] == "duplicate"
    assert ingest.process_signal(signal(mcap=100000), store)[0]
    assert ingest.process_signal(signal(mcap=200000), store)[1]["reason"] == "duplicate"
    assert len(store.pending_pushes()) == 1


@pytest.mark.parametrize("fomo", ["quota_exceeded", "timeout", "no_hit", "error"])
def test_pass_flagged_publishes_with_fomo_annotation_only(tmp_path, monkeypatch, fomo):
    cfg = load_config()
    cfg["filter"]["mcap_unknown_policy"] = "pass_flagged"
    store = Store(tmp_path, cfg)
    monkeypatch.setattr(common, "ROOT", tmp_path)
    monkeypatch.setattr(ingest, "STATE", tmp_path / "state")
    monkeypatch.setattr(ingest, "LATEST", tmp_path / "latest.jsonl")
    monkeypatch.setattr(ingest, "OUTBOX", tmp_path / "outbox")
    monkeypatch.setattr(ingest, "enrich_mcap", lambda sig, **kw: sig)
    monkeypatch.setattr(ingest.fomo_verify, "verify_signal", lambda sig: {"summary": fomo})
    monkeypatch.setenv("FOMO_VERIFY", "1")
    raw = signal(mcap=None, notes="original", source="binance_smart_money")
    accepted, rejected = ingest.process_signal(raw, store)
    assert accepted and rejected is None
    assert accepted["mcap"] is None
    assert accepted["notes"] == f"original; mcap_unknown; fomo_verify={fomo}"
    assert accepted["fomo_verify"]["summary"] == fomo
    ingest.publish_pending(store)
    row = json.loads(ingest.LATEST.read_text())
    push = next(ingest.OUTBOX.glob("push_????????_??????.jsonl"))
    assert json.loads(push.read_text()) == row
    assert row["schema_version"] == 1
    assert (ingest.OUTBOX / "NEW").read_text().strip() == str(push)
