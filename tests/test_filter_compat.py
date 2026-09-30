"""Compare against frozen pre-addon answers, never an oracle from current code."""
import hashlib
import json
from pathlib import Path

import pytest

from common import load_config
from filter_signal import compatibility_reason, evaluate, is_useful


@pytest.mark.parametrize("mode", ["original", "fixed"])
def test_v4_golden_corpus(mode):
    fixtures = Path(__file__).parent / "fixtures"
    golden = json.loads((fixtures / "filter_v4_golden.json").read_text())
    inputs = (fixtures / "filter_v4_inputs.jsonl").read_bytes().splitlines()
    assert len(golden) == len(inputs) == 1882
    cfg = load_config()
    for expected, raw in zip(golden, inputs, strict=True):
        location = f"{expected['file']}:{expected['line']} ({mode})"
        assert hashlib.sha256(raw).hexdigest() == expected["sha256"], location
        sig = json.loads(raw)
        if mode == "fixed":
            sig["mcap"] = 200000
        result = evaluate(sig, cfg)
        assert [result.verdict == "accept", compatibility_reason(result.reason)] == expected[mode], location
        assert list(is_useful(sig, cfg)) == expected[mode], location


@pytest.mark.parametrize("reason,legacy", [
    ("mcap_below_min:29999.0", "mcap_out_of_range:29999.0"),
    ("mcap_above_max:5000001.0", "mcap_out_of_range:5000001.0"),
    ("chain_unmapped:4663", "chain_blocked:4663"),
    ("mcap_pending", "mcap_pending"),
    ("dev_close_weak", "dev_close_weak"),
])
def test_compatibility_reason_mapping(reason, legacy):
    assert compatibility_reason(reason) == legacy
