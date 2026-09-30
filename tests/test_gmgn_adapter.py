import importlib
import io
import json
from pathlib import Path
import secrets
import subprocess
import threading
import tomllib

import pytest

import common
import gmgn_adapter as gmgn
import store as store_module
from conftest import REPO
from store import Store


@pytest.fixture
def payload():
    return dict(id=123, chain="solana", token_address="PublicAddresspump",
                symbol="ALPHA", side="buy", smart_money_count=3,
                amount_usd="1000", price="0.001", market_cap=100000,
                status="valid", timestamp=1700000000000, notes="observed")


def test_schema_v1_mapping_is_pure(payload):
    from ingest import normalize

    original = json.loads(json.dumps(payload))
    expected = {
        "schema_version": 1, "signal_id": "gmgn-123",
        "ts": "2023-11-15T06:13:20+08:00", "source": "gmgn_smart_money",
        "source_url": "https://gmgn.ai/sol/token/PublicAddresspump",
        "chain": "sol", "ca": "PublicAddresspump", "token_name": "ALPHA",
        "signal_type": "smart_money_buy", "direction": "buy",
        "smart_money_count": 3, "buy_usd": "1000", "price": "0.001",
        "mcap": 100000, "status": "active", "notes": "observed",
    }
    actual = gmgn.to_signal(payload)
    assert actual == expected == gmgn.to_signal(payload)
    assert {k: type(v) for k, v in actual.items()} == {k: type(v) for k, v in expected.items()}
    assert normalize(actual) == expected
    assert payload == original


@pytest.mark.parametrize("chain,expected", [("CT_501", "sol"), (501, "sol"),
    ("56", "bsc"), ("bnb", "bsc"), ("Ethereum", "eth"), (1, "eth"),
    (8453, "base"), ("arbitrum", "arb"), ("optimism", "op"), ("polygon", "matic")])
def test_aliases_and_zero_values(chain, expected):
    sig = gmgn.to_signal({"signalId": "abc", "chainId": chain,
        "token": {"contractAddress": "AbCd", "ticker": "ALPHA"},
        "smartMoneyCount": 0, "buyUsd": 0, "currentPrice": 0, "marketCap": 0})
    assert sig["chain"] == expected and sig["ca"] == "AbCd"
    assert sig["signal_id"] == "gmgn-abc" and sig["token_name"] == "ALPHA"
    assert sig["ts"] is None
    assert all(sig[k] == 0 for k in ("smart_money_count", "buy_usd", "price", "mcap"))


@pytest.mark.parametrize("direction,expected", [("BUY", "buy"), ("sell", "sell"),
    ("watch", "watch"), ("smart_money_sell", "sell"), ("unknown", "watch")])
def test_direction_nullable_fields(direction, expected):
    sig = gmgn.to_signal(dict(id=0, ca="AbCd", direction=direction))
    assert sig["signal_id"] == "gmgn-0"
    assert sig["direction"] == expected
    assert sig["signal_type"] == ("smart_money_buy" if expected == "buy" else "other")
    assert all(sig[k] is None for k in ("smart_money_count", "buy_usd", "price", "mcap"))


@pytest.mark.parametrize("payload,match", [({}, "stable id"), ({"ca": "AbCd"}, "stable id"),
    ({"id": " "}, "stable id"), ({"id": True}, "stable id"), ({"id": []}, "stable id"),
    ({"id": 1.2}, "stable id"), ({"id": 1}, "CA"), ({"id": 1, "ca": " "}, "CA"),
    ({"id": 1, "ca": {}}, "ca"), ([], "object")])
def test_missing_or_unstable_identity(payload, match):
    with pytest.raises(ValueError, match=match):
        gmgn.to_signal(payload)


@pytest.mark.parametrize("stamp", [1700000000, "1700000000", 1700000000000,
                                    "2023-11-14T22:13:20Z"])
def test_timestamp_variants(payload, stamp):
    assert gmgn.to_signal({**payload, "timestamp": stamp})["ts"] == "2023-11-15T06:13:20+08:00"


def test_default_off_and_import_has_no_side_effects(tmp_path, monkeypatch, capsys):
    with (REPO / "config/services.toml").open("rb") as stream:
        services = tomllib.load(stream)["services"]
    assert services["gmgn_adapter"] == {"enabled": False}
    assert all(services[name]["enabled"] is False for name in
               ("binance_smy_ws", "ingest_worker", "webhook_server"))

    def blocked(*args, **kwargs):
        raise AssertionError("unexpected import/default side effect")

    monkeypatch.setattr(common, "ROOT", tmp_path)
    monkeypatch.setattr(Path, "open", blocked)
    monkeypatch.setattr(Store, "__init__", blocked)
    monkeypatch.setattr(threading.Thread, "start", blocked)
    monkeypatch.setattr(subprocess, "Popen", blocked)
    importlib.reload(gmgn)
    assert gmgn.main([]) == 0
    assert "payload" in capsys.readouterr().out
    assert list(tmp_path.iterdir()) == []


def test_auth_only_reads_allowlisted_names_without_logging(tmp_path, monkeypatch, capsys, caplog):
    # Ephemeral synthetic values in memory only; no .env/fixture is read/written.
    api, bearer = secrets.token_hex(16), secrets.token_hex(16)
    contents = (f'UNRELATED="ignored"\nexport GMGN_API_KEY="{api}" # comment\n'
                f"GMGN_AUTH_TOKEN='{bearer}'\nGMGN_OTHER=ignored\n"
                'GMGN_API_KEY="unterminated\n')
    opened = []

    def fake_open(path, *args, **kwargs):
        assert path == tmp_path / "state/secrets.env"
        opened.append(path)
        return io.StringIO(contents)

    monkeypatch.setattr(common, "ROOT", tmp_path)
    monkeypatch.setenv("GMGN_API_KEY", secrets.token_hex(16))
    monkeypatch.setattr(Path, "open", fake_open)
    assert gmgn.read_auth() == {"X-API-Key": api, "Authorization": "Bearer " + bearer}
    assert len(opened) == 1
    assert capsys.readouterr() == ("", "") and not caplog.records
    assert list(tmp_path.iterdir()) == []


def test_auth_missing_or_empty_does_not_use_environment(tmp_path, monkeypatch, capsys, caplog):
    monkeypatch.setenv("GMGN_API_KEY", secrets.token_hex(16))

    def missing(path, *args, **kwargs):
        assert path == tmp_path / "state/secrets.env"
        raise FileNotFoundError

    monkeypatch.setattr(Path, "open", missing)
    assert gmgn.read_auth(root=tmp_path) == {}
    monkeypatch.setattr(Path, "open", lambda *a, **kw: io.StringIO(
        'GMGN_API_KEY=""\nGMGN_AUTH_TOKEN=\nGMGN_OTHER=ignored\n'))
    assert gmgn.read_auth(root=tmp_path) == {}
    assert capsys.readouterr() == ("", "") and not caplog.records


@pytest.mark.parametrize("chain,ca,query_chain,query_ca,expected", [
    ("eth", "0xaBc", "1", " 0xAbC ", True),
    ("bsc", "0xaBc", "56", "0xabc", True),
    ("eth", "0xaBc", "base", "0xabc", False),
    ("sol", "AbCd", "CT_501", "AbCd", True),
    ("sol", "AbCd", "501", "abcd", False),
])
def test_token_recent_identity_and_read_only(tmp_path, chain, ca, query_chain, query_ca, expected):
    store = Store(tmp_path, clock=lambda: 10000)
    store.observe("unpublished", {"chain": chain, "ca": ca})
    assert not store.token_recent(query_chain, query_ca, 1800)
    store.mark_pushed("binance-1", chain, ca)
    with store.connect() as db:
        before = list(db.iterdump())
    assert store.token_recent(query_chain, query_ca, 1800) is expected
    with store.connect() as db:
        assert list(db.iterdump()) == before


@pytest.mark.parametrize("elapsed,window,expected", [(0, 1800, True), (1799, 1800, True),
    (1800, 1800, True), (1801, 1800, False), (-1, 1800, False), (0, 0, False), (0, -1, False)])
def test_token_recent_window_and_wrapper(tmp_path, monkeypatch, elapsed, window, expected):
    clock = [10000]
    store = Store(tmp_path, clock=lambda: clock[0])
    store.mark_pushed("binance-1", "eth", "0xabc")
    clock[0] += elapsed
    monkeypatch.setattr(store_module, "_default", store)
    assert store_module.token_recent("ethereum", "0xABC", window) is expected
    assert not store.token_recent("", "", window)


def test_atomic_inbox_json_mode_and_duplicate_skip(tmp_path, payload, monkeypatch):
    store = Store(tmp_path, clock=lambda: 10000)
    monkeypatch.setattr(common, "ROOT", tmp_path)
    result = gmgn.handle_payload(payload, store=store, json_only=True)
    assert not result["skipped"] and result["queued"] is None
    assert not (tmp_path / "inbox").exists()
    queued = gmgn.handle_payload(payload, store=store)
    path = Path(queued["queued"])
    assert path.parent == tmp_path / "inbox"
    assert json.loads(path.read_text()) == queued["signal"]
    assert list(path.parent.iterdir()) == [path]
    assert not store.pending_pushes()
    store.mark_pushed("binance-1", "CT_501", payload["token_address"])
    duplicate = gmgn.handle_payload(payload, store=store)
    assert duplicate["skipped"] and duplicate["duplicate"] and duplicate["queued"] is None
    assert duplicate["signal"]["notes"] == "observed; also_seen=gmgn"
    assert list(path.parent.iterdir()) == [path]
    assert not (tmp_path / "latest.jsonl").exists() and not (tmp_path / "outbox").exists()


def test_explicit_cli_output_and_invalid_input(tmp_path, payload, monkeypatch, capsys):
    monkeypatch.setattr(common, "ROOT", tmp_path)
    path = tmp_path / "payload.json"
    path.write_text(json.dumps(payload))
    assert gmgn.main([str(path), "--json"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["signal"]["signal_id"] == "gmgn-123"
    assert result["queued"] is None and not result["skipped"]
    assert not (tmp_path / "inbox").exists()
    assert gmgn.main([str(path)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert Path(result["queued"]).parent == tmp_path / "inbox"

    untouched = tmp_path / "invalid-root"
    monkeypatch.setattr(common, "ROOT", untouched)
    path.write_text('{}')
    with pytest.raises(SystemExit) as exc:
        gmgn.main([str(path)])
    assert exc.value.code == 2
    assert not untouched.exists()
    assert capsys.readouterr().err == "GMGN adapter: unable to process local payload\n"


def test_expired_push_is_eligible_and_note_is_idempotent(tmp_path, payload):
    now = [10000]
    store = Store(tmp_path, clock=lambda: now[0])
    store.mark_pushed("binance-1", "sol", payload["token_address"])
    seen = {**payload, "notes": "observed; also_seen=gmgn"}
    result = gmgn.prepare_signal(seen, store)
    assert result["skipped"] and result["signal"]["notes"] == seen["notes"]
    now[0] += 1801
    result = gmgn.prepare_signal(payload, store)
    assert not result["skipped"] and result["signal"]["notes"] == "observed"


def test_ingest_reservation_blocks_gmgn_during_binance_evaluation(tmp_path, monkeypatch, payload):
    import ingest

    monkeypatch.setattr(common, "ROOT", tmp_path)
    monkeypatch.setattr(ingest, "enrich_fomo_verify", lambda sig: sig)
    store = Store(tmp_path)
    seen = {}

    def enrich(sig, **kwargs):
        seen["gmgn"] = gmgn.handle_payload(payload, store=store)
        return sig

    monkeypatch.setattr(ingest, "enrich_mcap", enrich)
    first = {**gmgn.to_signal(payload), "signal_id": "binance-in-flight",
             "source": "binance_smart_money"}
    accepted, rejected = ingest.process_signal(first, store)
    assert accepted is not None and rejected is None
    assert seen["gmgn"]["skipped"] and seen["gmgn"]["queued"] is None
    assert len(store.pending_pushes()) == 1


def test_cross_source_push_and_gmgn_ids_use_normal_ingest(tmp_path, monkeypatch, payload):
    import ingest

    monkeypatch.setattr(common, "ROOT", tmp_path)
    monkeypatch.setattr(ingest, "enrich_mcap", lambda sig, **kw: sig)
    monkeypatch.setattr(ingest, "enrich_fomo_verify", lambda sig: sig)
    store = Store(tmp_path)
    first = {**gmgn.to_signal(payload), "signal_id": "binance-1", "source": "binance_smart_money"}
    assert ingest.process_signal(first, store)[0] is not None
    result = gmgn.handle_payload(payload, store=store)
    assert result["skipped"] and result["queued"] is None
    assert len(store.pending_pushes()) == 1
    assert not (tmp_path / "inbox").exists()

    other = {**payload, "id": "own", "token_address": "OtherPublicAddresspump"}
    sig = gmgn.prepare_signal(other, store)["signal"]
    assert ingest.process_signal(sig, store)[0] is not None
    assert ingest.process_signal(gmgn.to_signal(other), store)[0] is None
    assert len(store.pending_pushes()) == 2

    # A fresh GMGN event still goes through the existing sell filter.
    rejected = gmgn.prepare_signal({**other, "id": "sell", "side": "sell",
                                    "token_address": "ThirdAddresspump"}, store)
    assert not rejected["skipped"]
    assert ingest.process_signal(rejected["signal"], store)[0] is None
    assert len(store.pending_pushes()) == 2


def test_failed_gmgn_publication_releases_reservation(tmp_path, payload, monkeypatch):
    store = Store(tmp_path, clock=lambda: 10000)
    monkeypatch.setattr(gmgn.common, "atomic_write", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("disk")))
    with pytest.raises(OSError):
        gmgn.handle_payload(payload, store=store, root=tmp_path)
    with store.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM token_reservations").fetchone()[0] == 0
