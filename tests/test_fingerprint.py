import pytest

from common import load_config
from filter_signal import EvalContext, FilterResult, evaluate, filter_fingerprint
from store import Store


def test_price_buckets_not_raw_price(addon_signal):
    fingerprint = filter_fingerprint(addon_signal)
    assert filter_fingerprint({**addon_signal, "price": 1.24}) == fingerprint
    assert filter_fingerprint({**addon_signal, "price": 1.26}) != fingerprint
    # Log buckets have fixed edges: a small move across an edge still changes.
    assert filter_fingerprint({**addon_signal, "price": 1.24}) != filter_fingerprint({**addon_signal, "price": 1.26})
    assert filter_fingerprint({**addon_signal, "price": 1.5}, {"reeval": {"price_bucket_base": 2}}) == filter_fingerprint(addon_signal, {"reeval": {"price_bucket_base": 2}})


@pytest.mark.parametrize("updates", [{"smart_money_count": 4}, {"status": "expired"},
    {"direction": "watch"}, {"tags": ["Wash Trading"]}, {"buy_usd": 1000},
    {"liq_usd": 9999}, {"mcap": 30000}])
def test_meaningful_inputs_change_fingerprint(addon_signal, updates):
    assert filter_fingerprint({**addon_signal, **updates}) != filter_fingerprint(addon_signal)


def test_tag_order_case_and_future_labels_do_not_change_fingerprint(addon_signal):
    sig = {**addon_signal, "tags": ["Add Holdings", "High Tax Token"]}
    other = {**sig, "tags": ["high tax token", "ADD HOLDINGS", "Add Holdings"],
             "maxGain": 100000, "maxgain": -100, "notes": "maxGain=9999%; fomo_verify=Wash Trading"}
    assert filter_fingerprint(sig) == filter_fingerprint(other)
    assert evaluate(sig) == evaluate(other)


@pytest.mark.parametrize("low,high", [(29999, 30000), (5000000, 5000001)])
def test_mcap_band_crossing_inside_bucket_changes_fingerprint(addon_signal, low, high):
    assert filter_fingerprint({**addon_signal, "mcap": low}) != filter_fingerprint({**addon_signal, "mcap": high})
    assert filter_fingerprint({**addon_signal, "mcap": 1000000, "price_at_fetch": 1, "price": low / 1000000}) != filter_fingerprint({**addon_signal, "mcap": 1000000, "price_at_fetch": 1, "price": high / 1000000})


@pytest.mark.parametrize("base", [None, "bad", 0, 1, float("inf"), float("nan")])
def test_invalid_bucket_config_falls_back(addon_signal, base):
    assert filter_fingerprint(addon_signal, {"reeval": {"price_bucket_base": base}}) == filter_fingerprint(addon_signal)


def test_structured_terminal_result_controls_reevaluation(tmp_path):
    store = Store(tmp_path, clock=lambda: 10000)
    # Explicit terminal overrides the legacy reason table in either direction.
    store.record_eval("terminal", "fp", FilterResult("reject", "new_rule", terminal=True))
    store.record_eval("retryable", "fp", FilterResult("reject", "sell_skip", terminal=False))
    assert not store.should_evaluate("terminal", "changed")
    assert store.should_evaluate("retryable", "changed")
    assert not store.should_evaluate("retryable", "fp")
    assert store.mark_pushed("retryable")
    assert not store.should_evaluate("retryable", "changed")


def test_pending_retries_ignore_fingerprint_but_obey_timer_and_limit(tmp_path, addon_signal):
    cfg = load_config()
    cfg["reeval"].update(pending_retry_s=60, max_evals=2)
    now = [10000]
    store = Store(tmp_path, cfg, clock=lambda: now[0])
    sig = {**addon_signal, "mcap": None}
    sid, fp = sig["signal_id"], filter_fingerprint(sig)
    store.observe(sid, sig)
    result = evaluate(sig, cfg, EvalContext(now_ts=now[0], first_seen_ts=now[0]))
    store.record_eval(sid, fp, result)
    now[0] += 59
    assert not store.should_evaluate(sid, "changed")
    now[0] += 1
    assert store.should_evaluate(sid, fp)
    store.record_eval(sid, fp, result)
    now[0] += 60
    assert not store.should_evaluate(sid, "changed")
