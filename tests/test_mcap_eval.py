import pytest

from filter_signal import EvalContext, evaluate
from store import Store


def test_cached_mcap_scaled_without_mutating_input(addon_signal):
    sig = {**addon_signal, "mcap": 1000000, "price_at_fetch": 1, "price": 3}
    result = evaluate(sig)
    assert result.mcap_eval == 3000000
    assert result.flags == ["mcap_scaled"] and result.would_reject == []
    assert result.verdict == "accept" and sig["mcap"] == 1000000


@pytest.mark.parametrize("price,scaled,applied_reason,terminal", [
    (6, 6000000, "mcap_above_max:6000000.0", True),
    (.02, 20000, "mcap_below_min:20000.0", False),
])
def test_scale_policy_shadow_off_and_explicit_apply(addon_signal, price, scaled, applied_reason, terminal):
    sig = {**addon_signal, "mcap": 1000000, "price_at_fetch": 1, "price": price}
    shadow = evaluate(sig)
    assert shadow.verdict == "accept" and shadow.mcap_eval == scaled
    assert shadow.would_reject == ["mcap_scaled_out_of_range"]
    off = evaluate(sig, {"mcap_scale_policy": "off"})
    assert off.mcap_eval == 1000000 and off.flags == off.would_reject == []
    applied = evaluate(sig, {"mcap_scale_policy": "apply"})
    assert (applied.verdict, applied.reason, applied.terminal) == ("reject", applied_reason, terminal)
    assert applied.mcap_eval == scaled and applied.would_reject == []


def test_first_seen_basis_is_explicit_and_can_recover_late_enrichment(addon_signal):
    sig = {**addon_signal, "mcap": 6000000, "price_at_fetch": 6, "price": 6}
    ctx = EvalContext(first_price=1)
    assert evaluate(sig, ctx=ctx).reason == "mcap_above_max:6000000.0"
    shadow = evaluate(sig, {"mcap_band_basis": "first_seen"}, ctx)
    assert shadow.mcap_eval == 1000000 and shadow.verdict == "reject"
    applied = evaluate(sig, {"mcap_band_basis": "first_seen", "mcap_scale_policy": "apply"}, ctx)
    assert applied.mcap_eval == 1000000 and applied.verdict == "accept"


@pytest.mark.parametrize("field", ["price", "price_at_fetch"])
@pytest.mark.parametrize("value", [None, 0, -1, "bad", "NaN", "inf"])
def test_invalid_scaling_prices_fall_back(addon_signal, field, value):
    sig = {**addon_signal, "mcap": 1000000, "price_at_fetch": 1, "price": 3, field: value}
    result = evaluate(sig)
    assert result.mcap_eval == 1000000 and "mcap_scaled" not in result.flags
    assert result.would_reject == []


def test_overflow_and_missing_mcap_do_not_create_invalid_valuations(addon_signal):
    sig = {**addon_signal, "mcap": 1e308, "price": 1e308, "price_at_fetch": 1e308}
    assert evaluate(sig).mcap_eval == 1e308
    assert evaluate({**sig, "price_at_fetch": 1e-308}).mcap_eval == 1e308
    missing = evaluate({**addon_signal, "mcap": None, "price_at_fetch": 1, "price": 3})
    assert missing.mcap_eval is None and missing.verdict == "pending"


def test_cached_metadata_and_evaluation_survive_store_reopen(tmp_path, addon_signal):
    store = Store(tmp_path, clock=lambda: 10000)
    store.put_mcap_cache("token", 1000000, "dexscreener", None, price_at_fetch=1,
                         liq_usd=12000, pair_created_at="2026-09-01T00:00:00Z")
    cached = Store(tmp_path).get_mcap_cache("token")
    assert cached["price_at_fetch"] == 1 and cached["liq_usd"] == 12000
    assert cached["pair_created_at"] == "2026-09-01T00:00:00Z"
    result = evaluate({**addon_signal, **cached, "price": 3})
    assert result.mcap_eval == 3000000
