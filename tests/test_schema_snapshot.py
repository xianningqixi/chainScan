from binance_smy_ws import to_signal
from ingest import normalize


def test_schema_snapshot():
    raw = dict(signalId=123, chainId="CT_501", contractAddress="PublicAddresspump",
               ticker="ALPHA", direction="buy", smartMoneyCount=3, buyUsd="1000",
               currentPrice="0.001", marketCap=100000, status="valid",
               signalTriggerTime=1700000000000, maxGain="0.25", isAlpha=True,
               tokenTag="pumpfun")
    expected = {
        "schema_version": 1, "signal_id": "binance-123",
        "ts": "2023-11-15T06:13:20+08:00", "source": "binance_smart_money",
        "source_url": "https://web3.binance.com/zh-CN/token/sol/PublicAddresspump",
        "chain": "sol", "token_name": "ALPHA", "ca": "PublicAddresspump",
        "signal_type": "smart_money_buy", "direction": "buy", "smart_money_count": 3,
        "buy_usd": "1000", "price": "0.001", "mcap": 100000, "status": "active",
        "notes": "maxGain=25.00%; isAlpha; tag=pumpfun",
        "tags": ["pumpfun"],
    }
    actual = to_signal(raw)
    assert actual == expected
    assert {k: type(v) for k, v in actual.items()} == {k: type(v) for k, v in expected.items()}
    assert normalize(actual) == expected


def test_nullable_fields_and_direction():
    for direction, kind in [("buy", "smart_money_buy"), ("sell", "other"), ("watch", "other")]:
        sig = to_signal(dict(signalId="id", chainId="56", direction=direction,
                             signalTriggerTime=1700000000000))
        assert sig["chain"] == "bsc"
        assert sig["direction"] == direction
        assert sig["signal_type"] == kind
        for key in ("mcap", "price", "buy_usd", "smart_money_count"):
            assert sig[key] is None
