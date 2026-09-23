from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


SCRIPT = Path(__file__).parents[1] / "audit_current_usdt_usdc_identity.py"
SPEC = importlib.util.spec_from_file_location("audit_current_usdt_usdc_identity", SCRIPT)
assert SPEC and SPEC.loader
AUDIT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = AUDIT
SPEC.loader.exec_module(AUDIT)


def test_native_ratio_is_clean() -> None:
    assert AUDIT.classify_ratio(10.90, 10.895, 0.0) == "NATIVE_SOURCE_IDENTITY"


def test_same_price_and_events_is_mislabeled_duplicate() -> None:
    assert AUDIT.classify_ratio(1.0006, 10.895, 0.99) == "ERROR_MISLABELED_DUPLICATE"


def test_same_price_without_event_overlap_is_suspect() -> None:
    assert AUDIT.classify_ratio(1.0006, 10.895, 0.0) == "SUSPECT_CONVERTED_PRICE_AT_SOURCE"
