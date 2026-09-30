import json

import pytest

from common import load_config
from filter_signal import EvalContext, evaluate
from store import Store


@pytest.mark.parametrize("elapsed,smart,verdict", [(0, 3, "reject"), (1799, 3, "reject"),
    (1799, 4, "reject"), (1799, 5, "accept"), (1800, 3, "accept"), (1801, 3, "accept"),
    (-1, 3, "accept")])
def test_cooldown_boundaries_with_explicit_reject(addon_signal, elapsed, smart, verdict):
    ctx = EvalContext(now_ts=10000 + elapsed, first_seen_ts=10000 + elapsed,
                      token_last_push_ts=10000, token_last_push_smart=3, token_sig_count_60m=2)
    result = evaluate({**addon_signal, "smart_money_count": smart},
                      {"l1": {"token_cooldown_policy": "reject"}}, ctx)
    assert result.verdict == verdict and result.terminal is False
    assert ("token_cooldown" in result.flags) is (verdict == "reject")
    assert "repeat_signal=2" in result.flags
    if verdict == "reject":
        assert result.reason == "token_cooldown" and result.layer == "L1"


def test_first_push_unaffected_and_default_remains_shadow(addon_signal):
    first = evaluate(addon_signal, {"l1": {"token_cooldown_policy": "reject"}}, EvalContext(now_ts=10000, first_seen_ts=10000))
    assert first.verdict == "accept" and first.flags == first.would_reject == []
    repeated = evaluate(addon_signal, ctx=EvalContext(now_ts=10000, first_seen_ts=10000,
                        token_last_push_ts=9999, token_last_push_smart=3))
    assert repeated.verdict == "accept"
    assert repeated.would_reject == ["token_cooldown"]


@pytest.mark.parametrize("first_chain,next_chain,first_ca,next_ca,cooling", [
    ("eth", "1", "0x" + "aB" * 20, "0x" + "Ab" * 20, True),
    ("eth", "base", "0x" + "ab" * 20, "0x" + "ab" * 20, False),
    ("sol", "CT_501", "Ab" * 16, "Ab" * 16, True),
    ("sol", "501", "Ab" * 16, "ab" * 16, False),
])
def test_token_identity_and_push_state_persist(tmp_path, addon_signal, first_chain, next_chain, first_ca, next_ca, cooling):
    now = [10000]
    store = Store(tmp_path, clock=lambda: now[0])
    first = {**addon_signal, "chain": first_chain, "ca": first_ca}
    store.observe(first["signal_id"], first)
    assert store.queue_push(first)
    now[0] += 60
    # A repeated claim must not overwrite the original token's smart/time.
    assert not store.mark_pushed(first["signal_id"], first_chain, first_ca, 99)
    store = Store(tmp_path, clock=lambda: now[0])
    second = {**addon_signal, "signal_id": "addon-2", "chain": next_chain, "ca": next_ca}
    store.observe(second["signal_id"], second)
    ctx = store.build_context(second)
    assert ctx.token_last_push_ts == (10000 if cooling else None)
    assert ctx.token_last_push_smart == (3 if cooling else None)
    assert ctx.token_sig_count_60m == (2 if cooling else 1)
    result = evaluate(second, {"l1": {"token_cooldown_policy": "reject"}}, ctx)
    assert result.verdict == ("reject" if cooling else "accept")


def test_ingest_records_cooldown_without_claiming_rejected_push(tmp_path, monkeypatch, addon_signal):
    import common
    import ingest

    monkeypatch.setattr(common, "ROOT", tmp_path)
    monkeypatch.setattr(ingest, "enrich_mcap", lambda sig, **kwargs: sig)
    cfg = load_config()
    cfg["filter"]["l1"]["token_cooldown_policy"] = "reject"
    now = [10000]
    store = Store(tmp_path, cfg, clock=lambda: now[0])
    assert ingest.process_signal(addon_signal, cfg, store).verdict == "accept"
    now[0] += 60
    second = {**addon_signal, "signal_id": "addon-2"}
    blocked = ingest.process_signal(second, cfg, store)
    assert blocked.reason == "token_cooldown" and blocked.terminal is False
    with store.connect() as db:
        row = db.execute("SELECT * FROM evals WHERE signal_id='addon-2'").fetchone()
        assert row["verdict"] == "reject" and row["layer"] == "L1" and row["terminal"] == 0
        assert "token_cooldown" in json.loads(row["flags_json"])
        assert json.loads(row["would_reject_json"]) == []
        token = db.execute("SELECT * FROM tokens").fetchone()
        assert token["last_push_sid"] == "addon-1" and token["last_push_ts"] == 10000
    assert len(store.pending_pushes()) == 1
    assert ingest.process_signal({**second, "smart_money_count": 5}, cfg, store).verdict == "accept"
    assert len(store.pending_pushes()) == 2


def test_context_retains_first_price_and_counts_recent_distinct_signals(tmp_path, addon_signal):
    now = [10000]
    store = Store(tmp_path, clock=lambda: now[0])
    first = addon_signal
    store.observe(first["signal_id"], first)
    store.observe(first["signal_id"], {**first, "price": 3})
    second = {**first, "signal_id": "addon-2", "smart_money_count": 4}
    store.observe(second["signal_id"], second)
    assert store.build_context(first).first_price == 1
    ctx = store.build_context(second)
    assert ctx.token_sig_count_60m == 2 and ctx.token_previous_max_smart == 3
    assert "confluence" in evaluate(second, ctx=ctx).flags
    now[0] += 3601
    store.observe(second["signal_id"], second)
    ctx = store.build_context(second)
    assert ctx.token_sig_count_60m == 1 and ctx.token_previous_max_smart is None
