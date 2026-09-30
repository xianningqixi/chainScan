"""P1-2: all HTTP responses and clocks are local; never use production state."""
import io
import json
import sqlite3
import threading
import time
from urllib.error import HTTPError, URLError

import pytest

import common
import ingest
import mcap_enrich as enrich
from common import load_config
from store import Store
from test_filter_signal import signal
from test_mcap_enrich import pair


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.delenv("ENRICH_OFFLINE", raising=False)
    monkeypatch.setattr(common, "ROOT", tmp_path)
    now = [1000.0]
    monkeypatch.setattr(enrich.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(enrich.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))
    return Store(tmp_path, clock=lambda: now[0]), now


def gecko(value=123000):
    return {"data": [{"attributes": {"reserve_in_usd": "200", "market_cap_usd": str(value)}}]}


def test_429_fallback_skip_persist_and_cap(client, monkeypatch):
    store, now = client
    calls = []

    def http(url, **kw):
        calls.append(url)
        return (429, None) if "dexscreener" in url else (200, gecko())

    monkeypatch.setattr(enrich, "_http_get_json", http)
    # Backoff grows across reopened stores; second lookup never calls Dex.
    for cooldown in (1, 2, 4, 8, 16, 32, 64, 120, 120):
        before = now[0]
        enriched = enrich.enrich_signal(signal(mcap=None), store=store)
        assert enriched["mcap_source"] == "geckoterminal"
        assert "mcap_enrich=failed:429" in enriched["notes"]
        with store.connect() as db:
            row = db.execute("SELECT * FROM breaker WHERE name='mcap:dexscreener'").fetchone()
        assert row["reason"] == "429"
        assert row["open_until"] == before + cooldown
        count = len([url for url in calls if "dexscreener" in url])
        reopened = Store(store.root, clock=lambda: now[0])
        result = enrich.lookup_mcap("sol", "OtherAddresspump", store=reopened, use_cache=False)
        assert result["mcap"] == 123000
        assert {"source": "dexscreener", "error": "429"} in result["failures"]
        assert len([url for url in calls if "dexscreener" in url]) == count
        now[0] = max(now[0], before + cooldown) + 1
        # Clear only the temporary test cache to force another provider lookup.
        with store.connect() as db:
            db.execute("DELETE FROM mcap_cache")


def test_real_http_429_event_notes_and_no_negative_cache(tmp_path, monkeypatch):
    monkeypatch.delenv("ENRICH_OFFLINE", raising=False)
    monkeypatch.setattr(common, "ROOT", tmp_path)
    store = Store(tmp_path)
    calls = []

    def request(req, **kw):
        calls.append(req.full_url)
        code = 429 if "dexscreener" in req.full_url else 404
        raise HTTPError(req.full_url, code, "failure", {}, io.BytesIO(b"not logged"))

    monkeypatch.setattr(enrich.urllib.request, "urlopen", request)
    row = enrich.enrich_signal(signal(mcap=None), store=store)
    assert "mcap_enrich=failed:429" in row["notes"]
    assert "mcap_source" not in row
    assert store.get_mcap_cache("sol|publicaddresspump") is None
    events = [json.loads(line) for line in (tmp_path / "state/events.jsonl").read_text().splitlines()]
    assert any(e["event"] == "enrich" and e["status"] == 429 for e in events)
    assert "not logged" not in json.dumps(events)


@pytest.mark.parametrize("error,status", [
    (HTTPError("https://example.org", 429, "", {}, None), 429),
    (HTTPError("https://example.org", 503, "", {}, None), "5xx"),
    (HTTPError("https://example.org", 404, "", {}, None), "not_found"),
    (TimeoutError(), "timeout"), (URLError(TimeoutError()), "timeout"),
    (URLError("DNS failure"), "network_error"),
])
def test_http_classification(monkeypatch, error, status):
    def request(*a, **kw):
        raise error
    monkeypatch.setattr(enrich.urllib.request, "urlopen", request)
    assert enrich._http_get_json("https://api.dexscreener.com/public") == (status, None)


def test_budget_counts_http_and_wait(client, monkeypatch):
    store, now = client
    store.cfg["enrich"]["budget_ms"] = 100
    timeouts = []

    def slow(url, timeout):
        timeouts.append(timeout)
        now[0] += .101
        return 200, {"pairs": [pair()]}

    monkeypatch.setattr(enrich, "_http_get_json", slow)
    assert enrich.lookup_mcap("sol", "PublicAddresspump", store=store)["error"] == "budget"
    assert len(timeouts) == 1 and timeouts[0] == pytest.approx(.1)
    assert store.get_mcap_cache("sol|publicaddresspump") is None
    timeouts.clear()
    assert enrich.lookup_mcap("sol", "PublicAddresspump", store=store)["error"] == "budget"
    assert not timeouts  # token refill needs more than the remaining budget
    assert enrich.lookup_mcap("sol", "PublicAddresspump", store=store, budget_ms=0)["error"] == "budget"


def test_budget_shrinks_for_fallback(client, monkeypatch):
    store, now = client
    timeouts = []

    def http(url, timeout):
        timeouts.append(timeout)
        now[0] += 3.5
        return "timeout", None

    monkeypatch.setattr(enrich, "_http_get_json", http)
    assert enrich.lookup_mcap("sol", "PublicAddresspump", store=store)["error"] == "budget"
    assert timeouts == [4, 2.5]


def test_wall_budget_bounds_a_stalled_read(tmp_path, monkeypatch):
    monkeypatch.delenv("ENRICH_OFFLINE", raising=False)
    store = Store(tmp_path)
    entered, release = threading.Event(), threading.Event()
    finished = threading.Event()
    monkeypatch.setattr(enrich, "_http_slots", {s: threading.BoundedSemaphore(1) for s in enrich.SOURCE_RATES})

    class Response:
        status = 200
        def __enter__(self):
            return self
        def __exit__(self, *args):
            finished.set()
        def read(self):
            entered.set()
            release.wait(2)
            return b'{}'

    monkeypatch.setattr(enrich.urllib.request, "urlopen", lambda *a, **kw: Response())
    started = time.monotonic()
    try:
        result = enrich.lookup_mcap("sol", "PublicAddresspump", store=store, budget_ms=100)
        assert entered.is_set()
        assert result["error"] == "budget"
        assert time.monotonic() - started < .8
        assert store.get_mcap_cache("sol|publicaddresspump") is None
    finally:
        release.set()
        assert finished.wait(2)


@pytest.mark.parametrize("source,rate", [("dexscreener", 4), ("geckoterminal", .4)])
def test_shared_token_bucket(client, source, rate):
    store, now = client
    assert store.take_mcap_token(source, rate) == 0
    second = Store(store.root, clock=lambda: now[0])
    assert second.take_mcap_token(source, rate) == pytest.approx(1 / rate)
    now[0] += 1 / rate
    assert second.take_mcap_token(source, rate) == 0
    now[0] += 100
    assert second.take_mcap_token(source, rate) == 0
    assert second.take_mcap_token(source, rate) == pytest.approx(1 / rate)  # no burst


@pytest.mark.parametrize("configured,interval", [(None, .25), (100, .25), (2, .5)])
def test_configured_rate_and_compatibility_defaults(client, monkeypatch, configured, interval):
    store, now = client
    if configured is None:
        store.cfg["enrich"].pop("dexscreener_rps")
    else:
        store.cfg["enrich"]["dexscreener_rps"] = configured
    calls = []
    def http(*a, **kw):
        calls.append(now[0])
        return 200, {"pairs": [pair()]}
    monkeypatch.setattr(enrich, "_http_get_json", http)
    for _ in range(2):
        assert enrich.lookup_mcap("sol", "PublicAddresspump", store=store, use_cache=False)["mcap"]
    assert calls[1] - calls[0] == interval


def test_breaker_reset_and_fomo_compatibility(client, monkeypatch):
    store, now = client
    store.open_breaker("fomo", "quota", 1800)
    store.backoff_breaker("mcap:dexscreener")
    now[0] += 2
    monkeypatch.setattr(enrich, "_http_get_json", lambda *a, **kw: (200, {"pairs": [pair()]}))
    assert enrich.lookup_mcap("sol", "PublicAddresspump", store=store)["mcap"] == 100000
    assert store.backoff_breaker("mcap:dexscreener") == 1
    assert store.breaker_is_open("fomo")


def test_existing_breaker_schema_compatible(tmp_path):
    path = tmp_path / "state/pipeline.db"
    path.parent.mkdir()
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE breaker(name TEXT PRIMARY KEY, open_until REAL, reason TEXT)")
        db.execute("INSERT INTO breaker VALUES('fomo',9999999999,'quota')")
    store = Store(tmp_path)
    assert store.breaker_is_open("fomo")
    assert store.backoff_breaker("mcap:dexscreener") == 1


def test_cache_migration_readonly_precedence_ttl_and_invalid_rows(client, monkeypatch):
    store, now = client
    key = "sol|publicaddresspump"
    path = store.root / "state/mcap_cache.json"
    entries = {key: {"mcap": 50000, "source": "dexscreener", "ts": now[0], "error": None},
               "bad": {"mcap": "bad", "ts": "bad"}, "bad-source": {"mcap": 1, "source": "external", "ts": now[0]},
               "bad-row": None}
    path.write_text(json.dumps(entries))
    original, mtime = path.read_bytes(), path.stat().st_mtime_ns
    calls = []
    def http(*a, **kw):
        calls.append(1)
        return 200, {"pairs": [pair()]}
    monkeypatch.setattr(enrich, "_http_get_json", http)
    assert enrich.lookup_mcap("sol", "PublicAddresspump", store=store) == {
        "mcap": 50000, "source": "dexscreener", "error": None, "cached": True, "failures": []}
    assert not calls
    assert store.get_mcap_cache("bad-source") is None
    now[0] += store.cfg["enrich"]["ttl_ok"]
    assert enrich.lookup_mcap("sol", "PublicAddresspump", store=store)["mcap"] == 100000
    reopened = Store(store.root, clock=lambda: now[0])
    assert enrich.lookup_mcap("sol", "PublicAddresspump", store=reopened)["mcap"] == 100000
    assert len(calls) == 1
    assert (path.read_bytes(), path.stat().st_mtime_ns) == (original, mtime)


def test_sqlite_cache_wins_over_legacy(client):
    store, now = client
    key = "sol|publicaddresspump"
    store.put_mcap_cache(key, 120000, "geckoterminal", None)
    (store.root / "state/mcap_cache.json").write_text(json.dumps({key: {
        "mcap": 50000, "source": "dexscreener", "ts": now[0], "error": None}}))
    assert enrich.lookup_mcap("sol", "PublicAddresspump", store=store)["mcap"] == 120000


def test_binance_source_does_not_fetch(monkeypatch):
    monkeypatch.setattr(enrich, "lookup_mcap", lambda *a, **kw: pytest.fail("unexpected lookup"))
    enriched = enrich.enrich_signal(signal(source="binance_smart_money", mcap_source="invalid"))
    assert enriched["mcap_source"] == "binance"


def test_not_found_ttl_and_readonly_absent_legacy(client, monkeypatch):
    store, now = client
    calls = []
    def missing(*a, **kw):
        calls.append(1)
        return "not_found", None
    monkeypatch.setattr(enrich, "_http_get_json", missing)
    first = enrich.lookup_mcap("sol", "PublicAddresspump", store=store)
    assert first["error"] == "not_found" and not first["cached"]
    assert enrich.lookup_mcap("sol", "PublicAddresspump", store=store)["cached"]
    assert len(calls) == 2
    now[0] += store.cfg["enrich"]["ttl_fail"]
    assert not enrich.lookup_mcap("sol", "PublicAddresspump", store=store)["cached"]
    assert len(calls) == 4
    assert not (store.root / "state/mcap_cache.json").exists()


@pytest.mark.parametrize("content", ['{broken', '[]', '{"bad":{"source":[]}}'])
def test_corrupt_legacy_does_not_block_fetch(client, monkeypatch, content):
    store, _ = client
    path = store.root / "state/mcap_cache.json"
    path.write_text(content)
    monkeypatch.setattr(enrich, "_http_get_json", lambda *a, **kw: (200, {"pairs": [pair()]}))
    assert enrich.lookup_mcap("sol", "PublicAddresspump", store=store)["mcap"] == 100000
    assert path.read_text() == content


@pytest.mark.parametrize("value", ["$100,000", "100K", 100000])
def test_existing_mcap_format_preserved_and_invalid_source_removed(monkeypatch, value):
    monkeypatch.setattr(enrich, "lookup_mcap", lambda *a, **kw: pytest.fail("unexpected lookup"))
    enriched = enrich.enrich_signal(signal(mcap=value, mcap_source=[]))
    assert enriched["mcap"] == value
    assert "mcap_source" not in enriched
