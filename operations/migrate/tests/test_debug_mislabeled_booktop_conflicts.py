from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pandas as pd


SCRIPT = Path(__file__).parents[1] / "debug_mislabeled_booktop_conflicts.py"
SPEC = importlib.util.spec_from_file_location("debug_mislabeled_booktop_conflicts", SCRIPT)
assert SPEC and SPEC.loader
DEBUG = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = DEBUG
SPEC.loader.exec_module(DEBUG)


def _booktop(*rows: tuple[str, float, float, float, float]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=["timestamp", "bid_price", "ask_price", "bid_qty", "ask_qty"],
    )


def test_schema_and_order_normalization_do_not_create_differences() -> None:
    source = _booktop(
        ("2026-07-08 12:00:02", 100, 102, 4, 5),
        ("2026-07-08 12:00:01", 99, 101, 2, 3),
    )
    destination = source.iloc[::-1].assign(symbol="PERP-GLD-USDC")

    result = DEBUG.analyze_frames(source, destination)

    assert result["overlap_rows"] == 2
    assert result["different_overlap_rows"] == 0
    assert result["median_abs_mid_diff_bps"] == 0


def test_reports_price_and_quantity_differences_at_shared_timestamp() -> None:
    source = _booktop(("2026-07-08 12:00:01", 99, 101, 2, 3))
    destination = _booktop(("2026-07-08 12:00:01", 99.5, 101.5, 7, 3))

    result = DEBUG.analyze_frames(source, destination)

    assert result["overlap_rows"] == 1
    assert result["bid_price_different_rows"] == 1
    assert result["ask_price_different_rows"] == 1
    assert result["bid_qty_different_rows"] == 1
    assert result["ask_qty_different_rows"] == 0
    assert 49 < result["median_abs_mid_diff_bps"] < 50


def test_repeated_timestamps_are_paired_without_many_to_many_expansion() -> None:
    source = _booktop(
        ("2026-07-08 12:00:01", 99, 101, 2, 3),
        ("2026-07-08 12:00:01", 100, 102, 4, 5),
    )
    destination = _booktop(
        ("2026-07-08 12:00:01", 99, 101, 2, 3),
        ("2026-07-08 12:00:01", 100, 102, 4, 6),
    )

    result = DEBUG.analyze_frames(source, destination)

    assert result["overlap_rows"] == 2
    assert result["different_overlap_rows"] == 1
