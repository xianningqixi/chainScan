import json
from pathlib import Path

import mcap_enrich as enrich


def pair(ca="PublicAddresspump", chain="solana", liquidity=100, mcap=100000, fdv=None):
    return {"baseToken":{"address":ca}, "quoteToken":{"address":"quote-address"},
            "chainId":chain, "liquidity":{"usd":liquidity}, "marketCap":mcap, "fdv":fdv}


def test_best_pair():
    choose = enrich._best_mcap_from_ds_pairs
    assert choose([pair(), pair(liquidity=200, mcap=200000)], "sol", "PublicAddresspump") == 200000
    assert choose([pair(mcap=None, fdv=300000)], "sol", "PublicAddresspump") == 300000
    assert choose([pair()], "sol", "quote-address") is None
    assert choose([pair(chain="bsc")], "sol", "PublicAddresspump") == 100000
    assert choose([pair(), pair(chain="bsc", liquidity=1000, mcap=999999)], "sol", "PublicAddresspump") == 100000
    assert choose([None, pair(mcap=-1)], "sol", "PublicAddresspump") is None


def test_recorded_response():
    data = json.loads((Path(__file__).parent / "fixtures/dexscreener_recorded.json").read_text())
    ca = data["pairs"][0]["baseToken"]["address"]
    assert enrich._best_mcap_from_ds_pairs(data["pairs"], "sol", ca) > 0


def test_offline_never_fetches(monkeypatch):
    monkeypatch.setattr(enrich, "_http_get_json", lambda *a, **kw: (_ for _ in ()).throw(AssertionError("HTTP")))
    monkeypatch.setattr(enrich, "_offline_calls", {})
    samples = json.loads((Path(__file__).parent / "fixtures/dexscreener_offline.json").read_text())
    ca = next(iter(samples))
    assert enrich.lookup_mcap("sol", ca)["mcap"] is None
    assert enrich.lookup_mcap("sol", ca)["mcap"] == 100000
