"""Pin shipped L0 semantics, including the review's intentionally deferred gaps."""
import pytest

from common import load_config
from filter_signal import EvalContext, evaluate


@pytest.mark.parametrize("bad,good,reason,terminal", [
    ({"token_name": "TEST"}, {"token_name": "ALPHA"}, "test_signal", True),
    ({"token_name": "smoke-coin"}, {"token_name": "ALPHA"}, "test_signal", True),
    ({"signal_id": "test-sol-1"}, {"signal_id": "public-1"}, "test_signal", True),
    ({"ca": ""}, {}, "missing_ca", True),
    ({"ca": "short"}, {}, "missing_ca", True),
    ({"ca": "abc123000"}, {}, "test_ca", True),
    # The 0xdead prefix remains v4 behavior; this follow-up does not fix it.
    ({"ca": "0xdead" + "a" * 36}, {}, "test_ca", True),
    ({"chain": "4663"}, {"chain": "1"}, "chain_unmapped:4663", True),
    ({"chain": "unknown"}, {"chain": "eth"}, "chain_blocked:unknown", True),
    ({"direction": "sell"}, {"direction": "buy"}, "sell_skip", True),
    ({"tags": ["Insider Wash Trading"]}, {"tags": []}, "wash_trading_tag", True),
    ({"mcap": 29999}, {"mcap": 30000}, "mcap_below_min:29999.0", False),
    ({"mcap": 5000001}, {"mcap": 5000000}, "mcap_above_max:5000001.0", True),
    ({"smart_money_count": 0, "buy_usd": 999}, {"buy_usd": 1000}, "flow_too_small", False),
    ({"direction": "watch", "buy_usd": 1999}, {"buy_usd": 2000}, "watch_without_strong_buy", False),
])
def test_l0_gate_and_counterexample(addon_signal, bad, good, reason, terminal):
    result = evaluate({**addon_signal, **bad})
    assert (result.verdict, result.reason, result.layer, result.terminal) == ("reject", reason, "L0", terminal)
    # Empty good overrides mean restore the valid baseline.
    passing = {**addon_signal, **bad, **good} if good else addon_signal
    result = evaluate(passing)
    assert result.verdict == "accept"
    assert result.terminal is False


@pytest.mark.parametrize("status", load_config()["filter"]["bad_status"])
def test_known_dead_status_remains_retryable(addon_signal, status):
    result = evaluate({**addon_signal, "status": status})
    assert (result.verdict, result.reason, result.layer, result.terminal) == ("reject", "status_" + status, "L0", False)
    assert evaluate(addon_signal).verdict == "accept"


@pytest.mark.parametrize("policy", ["off", "flag", "shadow", "reject"])
@pytest.mark.parametrize("updates,knob,code,terminal", [
    ({"ca": "malformed-address"}, "ca_format_policy", "bad_ca_format", True),
    ({"status": "novel"}, "unknown_status_policy", "status_unknown", False),
    ({"source": "binance_smart_money", "smart_money_count": None, "buy_usd": 1000},
     "binance_smart_policy", "binance_smart_missing", False),
])
def test_optional_l0_policy(addon_signal, policy, updates, knob, code, terminal):
    cfg = {"l0": {knob: policy}}
    result = evaluate({**addon_signal, **updates}, cfg)
    assert result.verdict == ("reject" if policy == "reject" else "accept")
    assert result.flags == ([] if policy == "off" else [code])
    assert result.would_reject == ([code] if policy == "shadow" else [])
    assert result.terminal is (terminal if policy == "reject" else False)
    assert result.layer == "L0"
    assert evaluate(addon_signal, cfg).flags == []


@pytest.mark.parametrize("elapsed,verdict,reason,terminal", [
    (7199, "pending", "mcap_pending", False),
    (7200, "pending", "mcap_pending", False),
    (7201, "reject", "mcap_unknown", True),
])
def test_pending_window_boundary(addon_signal, elapsed, verdict, reason, terminal):
    result = evaluate({**addon_signal, "mcap": None}, ctx=EvalContext(now_ts=10000 + elapsed, first_seen_ts=10000))
    assert (result.verdict, result.reason, result.terminal) == (verdict, reason, terminal)


def test_unknown_mcap_policies(addon_signal):
    sig = {**addon_signal, "mcap": None}
    rejected = evaluate(sig, {"mcap_unknown_policy": "reject"})
    assert (rejected.verdict, rejected.reason, rejected.terminal) == ("reject", "mcap_unknown", False)
    passing = evaluate(sig, {"mcap_unknown_policy": "pass_flagged"})
    assert passing.verdict == "accept" and "mcap_unknown" in passing.reason.split(",")
    assert evaluate({**sig, "smart_money_count": 0}, {"mcap_unknown_policy": "pass_flagged"}).reason == "flow_too_small"


@pytest.mark.parametrize("chain,ca", [("sol", "A" * 32), ("CT_501", "a" * 44), ("eth", "0x" + "Ab" * 20), ("56", "0x" + "01" * 20)])
def test_valid_chain_address_formats(addon_signal, chain, ca):
    result = evaluate({**addon_signal, "chain": chain, "ca": ca}, {"l0": {"ca_format_policy": "reject"}})
    assert result.verdict == "accept" and "bad_ca_format" not in result.flags
