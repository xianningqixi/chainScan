from concurrent.futures import ThreadPoolExecutor

import pytest
from common import load_config
from store import Store
from test_filter_signal import signal


def test_reevaluation_limits_and_atomic_claim(tmp_path):
    clock = [100.]
    cfg = load_config()
    cfg["reeval"] = {"max_evals": 2, "window_min": 1}
    store = Store(tmp_path, cfg, clock=lambda: clock[0])
    assert store.should_evaluate("a", "v1")
    store.record_eval("a", "v1", "flow_too_small", False)
    assert not store.should_evaluate("a", "v1")
    assert store.should_evaluate("a", "v2")
    store.record_eval("a", "v2", "flow_too_small", False)
    assert not store.should_evaluate("a", "v3")
    store.record_eval("b", "v1", "flow_too_small", False)
    clock[0] += 61
    assert not store.should_evaluate("b", "v2")
    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(pool.map(lambda _: store.mark_pushed("c"), range(20)))
    assert sum(claims) == 1
    assert not store.should_evaluate("c", "v1")


@pytest.mark.parametrize("reason", ["test_signal", "test_ca", "missing_ca", "chain_blocked:4663",
                                     "sell_skip", "wash_trading_tag", "mcap_out_of_range:5000001"])
def test_terminal(tmp_path, reason):
    store = Store(tmp_path)
    store.record_eval("a", "v1", reason, False)
    assert not store.should_evaluate("a", "v2")


def test_low_mcap_and_status_are_retryable(tmp_path):
    store = Store(tmp_path)
    for reason in ("mcap_out_of_range:29999", "status_expired", "dev_close_weak"):
        store.record_eval(reason, "v1", reason, False)
        assert store.should_evaluate(reason, "v2")


def test_legacy_migration_only_once_and_readonly(tmp_path):
    path = tmp_path / "state/seen_ids.txt"
    path.parent.mkdir()
    path.write_text("legacy\nlegacy\n")
    store = Store(tmp_path)
    assert not store.should_evaluate("legacy", "v1")
    assert path.read_text() == "legacy\nlegacy\n"
    path.write_text("legacy\nlater\n")
    assert Store(tmp_path).should_evaluate("later", "v1")


def test_recover_after_rejection(tmp_path, monkeypatch):
    import ingest
    monkeypatch.setattr(ingest, "enrich_mcap", lambda s, **kw: s)
    store = Store(tmp_path)
    first = signal(mcap=None, smart_money_count=3)
    assert ingest.process_signal(first, store)[0] is None
    second = signal(mcap=100000, smart_money_count=5)
    assert ingest.process_signal(second, store)[0] is not None
    assert ingest.process_signal(signal(price=2), store)[0] is None
    assert len(store.pending_pushes()) == 1


def test_null_source_legacy_row_does_not_cross_dedupe_same_binance_ca(tmp_path, monkeypatch):
    import ingest

    monkeypatch.setattr(ingest, "enrich_mcap", lambda sig, **kw: sig)
    monkeypatch.setattr(ingest, "enrich_fomo_verify", lambda sig: sig)
    store = Store(tmp_path, clock=lambda: 10000)
    first = signal(signal_id="binance-legacy-1", source="binance_smart_money")
    assert ingest.process_signal(first, store)[0] is not None

    # Rows written before source was added have NULL, not an empty source.
    with store.connect() as db:
        db.execute("UPDATE signals SET source=NULL WHERE signal_id=?", (first["signal_id"],))

    replay = signal(signal_id="binance-legacy-2", source="binance_smart_money")
    accepted, rejected = ingest.process_signal(replay, store)
    assert accepted is not None
    assert rejected is None
    assert len(store.pending_pushes()) == 2


def test_delivery_recovery_without_duplicate_rows(tmp_path, monkeypatch):
    import ingest
    import json
    store = Store(tmp_path)
    monkeypatch.setattr(ingest, "OUTBOX", tmp_path / "outbox")
    monkeypatch.setattr(ingest, "LATEST", tmp_path / "latest.jsonl")
    ingest.OUTBOX.mkdir()
    assert store.queue_push(signal())
    real_delivered = store.delivered
    monkeypatch.setattr(store, "delivered", lambda ids: (_ for _ in ()).throw(OSError("interruption")))
    with pytest.raises(OSError):
        ingest.publish_pending(store)
    monkeypatch.setattr(store, "delivered", real_delivered)
    ingest.publish_pending(store)
    assert len([json.loads(x) for x in ingest.LATEST.read_text().splitlines()]) == 1
    assert len(list(ingest.OUTBOX.glob("push_*.jsonl"))) == 1
    assert not store.pending_pushes()


def test_latest_index_is_incremental_and_tolerates_bad_history(tmp_path, monkeypatch):
    import ingest
    import json
    from pathlib import Path
    store = Store(tmp_path)
    latest = tmp_path / "latest.jsonl"
    latest.write_text('garbage\n{}\nnull\n' + json.dumps(signal(signal_id="existing")) + '\n')
    assert store.queue_push(signal(signal_id="existing"))
    assert store.existing_output_ids(latest) == {"existing"}
    offset = latest.stat().st_size
    with latest.open("a") as f:
        f.write(json.dumps(signal(signal_id="new")) + '\n')
    assert store.queue_push(signal(signal_id="new"))
    original_open = Path.open
    reads = []

    class Reader:
        def __init__(self, f):
            self.f = f
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.f.close()
        def __getattr__(self, name):
            return getattr(self.f, name)
        def readline(self):
            reads.append(self.f.tell())
            return self.f.readline()

    def tracking(path, *args, **kwargs):
        f = original_open(path, *args, **kwargs)
        return Reader(f) if path == latest and args == ("rb",) else f

    monkeypatch.setattr(Path, "open", tracking)
    assert store.existing_output_ids(latest) == {"existing", "new"}
    assert min(reads) == offset
    monkeypatch.setattr(ingest, "LATEST", latest)
    monkeypatch.setattr(ingest, "OUTBOX", tmp_path / "outbox")
    monkeypatch.setattr(ingest, "STATE", tmp_path / "state")
    original = latest.read_bytes()
    ingest.publish_pending(store)
    assert latest.read_bytes() == original
    assert not store.pending_pushes()
    # Explicit external truncation must invalidate the saved cursor and IDs.
    latest.write_text('{}\n')
    assert store.existing_output_ids(latest) == set()


@pytest.mark.parametrize("tail", ['{"broken":', '{"signal_id":"already"}'])
def test_unterminated_history_preserves_new_rows(tmp_path, monkeypatch, tail):
    import ingest
    import json
    latest = tmp_path / "latest.jsonl"
    latest.write_text(tail)
    store = Store(tmp_path)
    monkeypatch.setattr(ingest, "LATEST", latest)
    monkeypatch.setattr(ingest, "OUTBOX", tmp_path / "outbox")
    monkeypatch.setattr(ingest, "STATE", tmp_path / "state")
    if "already" in tail:
        store.queue_push(signal(signal_id="already"))
    store.queue_push(signal(signal_id="new"))
    ingest.publish_pending(store)
    lines = latest.read_text().splitlines()
    assert lines[0] == tail
    assert len(lines) == 2
    assert json.loads(lines[1])["signal_id"] == "new"


def test_push_updates_are_chat_only_and_idempotent(tmp_path, monkeypatch):
    import ingest

    monkeypatch.setattr(ingest, "enrich_mcap", lambda sig, **kw: sig)
    monkeypatch.setattr(ingest, "enrich_fomo_verify", lambda sig: sig)
    monkeypatch.setattr(ingest, "ROOT", tmp_path)
    cfg = load_config()
    cfg["notify"] = {"push_updates": True, "max_gain_threshold": 100}
    store = Store(tmp_path, cfg, clock=lambda: 10000)

    first = signal(signal_id="status-update-1", source="binance_smart_money")
    assert ingest.process_signal(first, store)[0] is not None
    update = signal(signal_id="status-update-1", source="binance_smart_money",
                    status="expired", notes="maxGain=101%")
    accepted, rejected = ingest.process_signal(update, store)
    assert accepted is None and rejected["reason"] == "duplicate"

    pending = tmp_path / "outbox/PENDING_CHAT"
    body = pending.read_text(encoding="utf-8")
    assert "🔄 状态更新" in body
    assert "状态→expired" in body and "maxGain≥100%" in body
    assert not (tmp_path / "latest.jsonl").exists()
    assert not list((tmp_path / "outbox").glob("push_*.jsonl"))

    # Re-observing the same state does not append a second update block.
    ingest.process_signal(update, store)
    assert pending.read_text(encoding="utf-8") == body


def test_ingest_reservation_arbitrates_both_source_orders(tmp_path, monkeypatch):
    import ingest

    monkeypatch.setattr(ingest, "enrich_mcap", lambda sig, **kw: sig)
    monkeypatch.setattr(ingest, "enrich_fomo_verify", lambda sig: sig)
    store = Store(tmp_path, clock=lambda: 10000)
    ca = "ReservationAddresspump"

    assert store.reserve_token("sol", ca, "gmgn-pending", "gmgn_smart_money")
    accepted, rejected = ingest.process_signal(
        signal(signal_id="binance-pending", source="binance_smart_money", ca=ca), store)
    assert accepted is None and rejected["reason"] == "token_cross_source_duplicate"
    store.release_token_reservation("gmgn-pending")

    assert store.reserve_token("sol", ca, "binance-pending-2", "binance_smart_money")
    accepted, rejected = ingest.process_signal(
        signal(signal_id="gmgn-pending-2", source="gmgn_smart_money", ca=ca), store)
    assert accepted is None and rejected["reason"] == "token_cross_source_duplicate"
