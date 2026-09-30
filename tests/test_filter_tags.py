import asyncio
import json

import pytest

from filter_signal import evaluate, extract_tags


@pytest.mark.parametrize("value", [
    {"risk": [{"tagName": "High Tax Token"}], "flow": [{"tagName": "Add Holdings"}]},
    [{"tagName": "High Tax Token"}, {"tagName": "Add Holdings"}],
    '[{"tagName":"High Tax Token"},{"tagName":"Add Holdings"}]',
    "[{'tagName': 'High Tax Token'}, {'tagName': 'Add Holdings'}]",
])
def test_structured_and_historical_tags(value):
    expected = ["Add Holdings", "High Tax Token"]
    assert extract_tags({"tokenTag": value}) == expected
    blob = value if isinstance(value, str) else repr(value)
    assert extract_tags({"notes": "maxGain=999%; tag=" + blob + "; fomo_verify=Wash Trading"}) == expected


def test_explicit_tags_are_authoritative_and_normalized():
    sig = {"tags": [], "tokenTag": {"tagName": "Wash Trading"}, "notes": "DEV Close Position"}
    assert extract_tags(sig) == []
    sig["tags"] = [" High Tax Token ", "High Tax Token", "", None, 3, {"tagName": "Wash Trading"}]
    assert extract_tags(sig) == ["High Tax Token"]


@pytest.mark.parametrize("notes", [
    "fomo_verify=Wash Trading; mcap_enrich=DEV Close Position",
    "mcap_enrich={'tagName': 'Wash Trading'}; fomo_verify=DEV Remove Liquidity",
    "not_tag=[{'tagName': 'Wash Trading'}]",
    "tag=[{'tagName': 'Add Holdings'}]; fomo_verify=Wash Trading",
    "tag=[{'tagName': 'Add Holdings'}; fomo_verify=Wash Trading",  # truncated data
    "tag=__import__('os').system('false'); fomo_verify=Wash Trading",
])
def test_enrichment_prose_and_malformed_tags_do_not_trigger_risk(addon_signal, notes):
    sig = {key: value for key, value in addon_signal.items() if key != "tags"}
    result = evaluate({**sig, "notes": notes})
    assert result.verdict == "accept"
    assert result.flags == [] and result.would_reject == []


def test_balanced_parser_handles_quoted_delimiters_and_ignores_descriptions():
    blob = {"items": [{"tagName": "Label; [quoted] 'value'"}], "description": "Wash Trading"}
    assert extract_tags({"notes": "tag=" + repr(blob) + "; fomo_verify=High Tax Token"}) == ["Label; [quoted] 'value'"]
    cyclic = {"tagName": "Add Holdings"}
    cyclic["self"] = cyclic
    assert extract_tags({"tokenTag": cyclic}) == ["Add Holdings"]


@pytest.mark.parametrize("label,reason", [("Wash Trading", "wash_trading_tag"),
                                         ("Insider Wash Trading", "wash_trading_tag"),
                                         ("DEV Close Position", "dev_close_weak")])
def test_exact_legacy_risk_labels(addon_signal, label, reason):
    sig = {key: value for key, value in addon_signal.items() if key != "tags"}
    assert evaluate({**sig, "notes": label}).reason == reason
    assert evaluate({**sig, "tags": [label.swapcase()]}).reason == reason


def test_ws_tag_only_update_is_queued_and_notes_remain_legacy(tmp_path, monkeypatch):
    import binance_smy_ws as ws
    import common
    from store import Store

    monkeypatch.setattr(common, "ROOT", tmp_path)
    monkeypatch.setattr(ws, "INBOX", tmp_path / "inbox")
    monkeypatch.setattr(ws, "log", lambda *args: None)
    store = Store(tmp_path)
    data = {"signalId": 123, "signalTriggerTime": 1700000000000, "currentPrice": "1",
            "tokenTag": [{"tagName": "Add Holdings"}]}
    async def send():
        await ws.handle(json.dumps({"stream": "w3w@signal_smart_money", "data": data}), store)
    asyncio.run(send())
    asyncio.run(send())
    data["tokenTag"] = [{"tagName": "Wash Trading"}]
    asyncio.run(send())
    rows = [json.loads(p.read_text()) for p in sorted(ws.INBOX.glob("*.jsonl"))]
    assert len(rows) == 2
    assert [row["tags"] for row in rows] == [["Add Holdings"], ["Wash Trading"]]
    assert rows[1]["notes"] == "tag=[{'tagName': 'Wash Trading'}]"
