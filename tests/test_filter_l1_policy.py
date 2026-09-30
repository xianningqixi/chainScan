from copy import deepcopy

import pytest

from common import load_config
from filter_signal import EvalContext, evaluate


# Each row supplies a match and a nearby non-match, with deterministic time.
RULES = [
    ("dev_close", {"tags": ["DEV Close Position"]}, {}, {"smart_money_count": 6}, {}, "dev_closed"),
    ("smart_remove", {"tags": ["Remove Holdings"]}, {}, {"tags": ["Remove Holdings", "Add Holdings"]}, {}, "smart_remove"),
    ("token_cooldown", {}, {"token_last_push_ts": 19999, "token_last_push_smart": 3},
     {"smart_money_count": 5}, {}, "token_cooldown"),
    ("stale", {}, {"first_seen_ts": 19040}, {}, {"first_seen_ts": 19100}, "late:16.0"),
    ("chase", {"price": 2}, {"first_price": 1}, {"price": 1.99}, {}, "chased"),
    ("high_tax", {"tags": ["High Tax Token"]}, {}, {"tags": []}, {}, "high_tax"),
    ("low_liq", {"tags": ["Low Liquidity"]}, {}, {"tags": []}, {}, "low_liq"),
    ("low_liq", {"liq_usd": 9999}, {}, {"liq_usd": 10000}, {}, "low_liq"),
    ("dev_rm_liq", {"tags": ["DEV Remove Liquidity"]}, {}, {"tags": []}, {}, "dev_rm_liq"),
]


@pytest.mark.parametrize("policy", ["off", "flag", "shadow", "reject"])
@pytest.mark.parametrize("rule,updates,context,good,good_context,flag", RULES, ids=[r[0] for r in RULES])
def test_l1_policy_matrix(addon_signal, policy, rule, updates, context, good, good_context, flag):
    # F10 requires disabling the independent v4 gate to exercise the new knob.
    cfg = {"baseline_dev_close_gate": False, "l1": {rule + "_policy": policy}}
    ctx = {"now_ts": 20000, "first_seen_ts": 20000, **context}
    sig = {**addon_signal, **updates}
    original = deepcopy(sig)
    result = evaluate(sig, cfg, EvalContext(**ctx))
    assert sig == original
    assert result.verdict == ("reject" if policy == "reject" else "accept")
    incidental = ["repeat_signal=1"] if rule == "token_cooldown" else []
    assert set(result.flags) == set(incidental + ([] if policy == "off" else [flag]))
    assert result.would_reject == ([rule] if policy == "shadow" else [])
    assert result.terminal is False
    if policy == "reject":
        assert (result.reason, result.layer) == (rule, "L1")
    passing = evaluate({**sig, **good}, cfg, EvalContext(**{**ctx, **good_context}))
    assert passing.verdict == "accept"
    assert flag not in passing.flags and passing.would_reject == []


def test_shipped_shadow_defaults_and_independent_dev_gate(addon_signal):
    cfg = load_config()["filter"]
    assert cfg["baseline_dev_close_gate"] is True
    assert cfg["append_flags_to_notes"] is False
    assert cfg["mcap_scale_policy"] == "shadow"
    assert cfg["l1"]["token_cooldown_policy"] == cfg["l1"]["dev_rm_liq_policy"] == "shadow"
    assert all(value in {"flag", "shadow", "off"} for key, value in cfg["l1"].items() if key.endswith("_policy"))
    assert all(value == "flag" for key, value in cfg["l0"].items() if key.endswith("_policy"))
    assert cfg["l2"]["score_gate_enabled"] is False
    result = evaluate({**addon_signal, "tags": ["DEV Close Position"]})
    assert (result.verdict, result.reason, result.layer, result.terminal) == ("reject", "dev_close_weak", "L1", False)
    assert result.flags == ["dev_closed"] and result.would_reject == []


def test_annotations_and_score_survive_l0_rejection(addon_signal):
    sig = {**addon_signal, "direction": "sell", "smart_money_count": 6,
           "tags": ["Add Holdings", "Volume Surging", "Remove Holdings", "Volume Plunging", "DEV Remove Liquidity"]}
    ctx = EvalContext(token_sig_count_60m=2, token_previous_max_smart=5)
    result = evaluate(sig, ctx=ctx)
    assert (result.reason, result.layer, result.terminal) == ("sell_skip", "L0", True)
    assert result.score == 3  # smart +2, tags net zero, confluence +1
    assert set(result.flags) == {"dev_rm_liq", "confluence"}
    assert result.would_reject == ["dev_rm_liq"]
    negative = evaluate({**addon_signal, "tags": ["Remove Holdings", "Volume Plunging"]})
    assert negative.score == -2 and negative.verdict == "accept"


def test_first_reject_wins_but_all_annotations_are_retained(addon_signal):
    result = evaluate({**addon_signal, "tags": ["Remove Holdings", "High Tax Token", "DEV Remove Liquidity"]},
                      {"l1": {"smart_remove_policy": "reject", "high_tax_policy": "reject"}})
    assert result.reason == "smart_remove"
    assert result.flags == ["smart_remove", "high_tax", "dev_rm_liq"]
    assert result.would_reject == ["dev_rm_liq"]
