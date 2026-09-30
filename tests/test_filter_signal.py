import pytest
from common import load_config
from filter_signal import is_useful, parse_num


def signal(**updates):
    return {"signal_id": "public-1", "token_name": "ALPHA", "ca": "PublicAddresspump",
            "chain": "sol", "status": "active", "direction": "buy", "mcap": 100000,
            "smart_money_count": 1, "buy_usd": None, **updates}


def test_defaults():
    cfg = load_config()
    assert [cfg["filter"][k] for k in ("mcap_min", "mcap_max", "min_buy_usd", "min_smart")] == [30000, 5000000, 1000, 1]
    assert (cfg["enrich"]["ttl_ok"], cfg["enrich"]["ttl_fail"]) == (1200, 300)
    # Additive addon knobs retain the legacy limits and immediate retry default.
    assert cfg["reeval"] == {"max_evals": 30, "window_min": 120,
                             "pending_retry_s": 0, "price_bucket_base": 1.25}
    assert is_useful(signal(smart_money_count=3))[0]
    assert is_useful(signal(smart_money_count=5))[0]
    assert not is_useful(signal(), {"min_smart": 5})[0]


@pytest.mark.parametrize("value,expected", [(None,None), (123,123.), (1.5,1.5),
    ("$1,200",1200.), ("2K",2000.), ("1.5m",1500000.), ("bad",None), ("",None)])
def test_parse_num(value, expected):
    assert parse_num(value) == expected


@pytest.mark.parametrize("updates,reason", [
    ({"token_name":"TEST"},"test_signal"), ({"token_name":"smoke-coin"},"test_signal"),
    ({"signal_id":"test-sol-1"},"test_signal"), ({"ca":None},"missing_ca"),
    ({"ca":"abc123000"},"test_ca"), ({"chain":"4663"},"chain_blocked:4663"),
    ({"status":"expired"},"status_expired"), ({"direction":"sell"},"sell_skip"),
    ({"mcap":None},"mcap_pending"), ({"mcap":29999},"mcap_out_of_range:29999.0"),
    ({"mcap":5000001},"mcap_out_of_range:5000001.0"),
    ({"smart_money_count":0,"buy_usd":999},"flow_too_small"),
    ({"direction":"watch"},"watch_without_strong_buy"),
    ({"notes":"insider wash trading"},"wash_trading_tag"),
    ({"notes":"dev close position"},"dev_close_weak"),
])
def test_rejections(updates, reason):
    assert is_useful(signal(**updates)) == (False, reason)


@pytest.mark.parametrize("updates", [{"mcap":30000}, {"mcap":5000000},
    {"buy_usd":1000,"smart_money_count":0}, {"direction":"watch","buy_usd":2000},
    {"notes":"dev close position","smart_money_count":6}, {"token_name":"ONDO"}])
def test_accepts(updates):
    assert is_useful(signal(**updates))[0]
