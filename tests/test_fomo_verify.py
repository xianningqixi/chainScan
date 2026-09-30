import io
import json
from urllib.error import HTTPError

import pytest
import fomo_verify as fomo
from store import Store
from test_filter_signal import signal


@pytest.fixture
def verify_store(tmp_path, monkeypatch):
    store = Store(tmp_path)
    monkeypatch.setattr(fomo, "default_store", lambda: store)
    monkeypatch.setenv("FOMO_VERIFY_DRY", "0")
    monkeypatch.setenv("FOMO_VERIFY", "1")
    monkeypatch.setenv("FOMOSCAN_API_KEY", "fixture-only")
    return store


@pytest.mark.parametrize("code", [402, 429])
def test_quota_breaker_keeps_all_signals(tmp_path, monkeypatch, verify_store, code):
    import ingest
    calls = []

    def quota(*args, **kwargs):
        calls.append(1)
        raise HTTPError("https://example.org", code, "quota", {}, io.BytesIO(b"quota"))

    monkeypatch.setattr(fomo.urllib.request, "urlopen", quota)
    monkeypatch.setattr(ingest, "OUTBOX", tmp_path / "outbox")
    monkeypatch.setattr(ingest, "LATEST", tmp_path / "latest.jsonl")
    monkeypatch.setattr(ingest, "STATE", tmp_path / "state")
    ingest.OUTBOX.mkdir()
    for i in range(11):
        accepted, rejected = ingest.process_signal(signal(signal_id=str(i), source="binance_smart_money"), verify_store)
        assert rejected is None
        assert "fomo_verify=quota_exceeded" in accepted["notes"]
        if i:
            assert accepted["fomo_verify"]["credits_note"] == "breaker_open"
    ingest.publish_pending(verify_store)
    assert len(ingest.LATEST.read_text().splitlines()) == 11
    assert len(calls) == 1
    # State survives a new Store instance; cooldown expiry allows HTTP again.
    assert Store(tmp_path).breaker_is_open("fomo")
    fomo.open_breaker("expired", -1)
    fomo.verify_signal(signal())
    assert len(calls) == 2


@pytest.mark.parametrize("body,summary", [
    ({"items":[{"symbol":"ALPHA"}],"count":1}, "matched"),
    ({"items":[{"symbol":"DIFFERENT"}],"count":1}, "matched_name_mismatch"),
    ({"items":[],"count":0}, "no_hit"),
    ({"error":"quota exhausted"}, "quota_exceeded"),
    ({"count":"broken"}, "error:bad_json"),
])
def test_200(verify_store, monkeypatch, body, summary):
    class Response(io.BytesIO):
        status = 200
    monkeypatch.setattr(fomo.urllib.request, "urlopen", lambda *a, **kw: Response(json.dumps(body).encode()))
    assert fomo.verify_signal(signal())["summary"] == summary


@pytest.mark.parametrize("error,summary", [(TimeoutError(),"error:TimeoutError"),
    (HTTPError("https://example.org", 500, "error", {}, io.BytesIO(b"error")),"error:http_500")])
def test_errors_are_annotations(verify_store, monkeypatch, error, summary):
    import ingest
    monkeypatch.setattr(fomo.urllib.request, "urlopen", lambda *a, **kw: (_ for _ in ()).throw(error))
    sig, rejection = ingest.process_signal(signal(source="binance_smart_money"), verify_store)
    assert rejection is None
    assert sig["fomo_verify"]["summary"] == summary
    assert not fomo.breaker_is_open()


def test_open_breaker_same_acceptance_as_disabled(tmp_path, verify_store, monkeypatch):
    import ingest
    monkeypatch.setattr(fomo.urllib.request, "urlopen", lambda *a, **kw: pytest.fail("breaker made HTTP request"))
    fomo.open_breaker("quota", 1800)
    with_breaker = Store(tmp_path / "enabled")
    disabled = Store(tmp_path / "disabled")
    rows = [signal(signal_id=str(i), source="binance_smart_money") for i in range(10)]
    assert all(ingest.process_signal(s, with_breaker)[0]["fomo_verify"]["summary"] == "quota_exceeded" for s in rows)
    monkeypatch.setenv("FOMO_VERIFY", "0")
    assert all(ingest.process_signal(s, disabled)[0] for s in rows)
