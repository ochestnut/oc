from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pandas as pd
import pytest


SCRIPT = Path(__file__).parents[1] / "migrate_mislabeled_booktop.py"
SPEC = importlib.util.spec_from_file_location("migrate_mislabeled_booktop", SCRIPT)
assert SPEC and SPEC.loader
MIGRATE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MIGRATE
SPEC.loader.exec_module(MIGRATE)


def _booktop(*rows: tuple[str, float, float, float, float]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=["timestamp", "bid_price", "ask_price", "bid_qty", "ask_qty"],
    ).assign(timestamp=lambda frame: pd.to_datetime(frame["timestamp"]))


def test_equivalent_destination_ignores_row_order_and_parquet_encoding() -> None:
    source = _booktop(
        ("2026-07-08 12:00:01", 99.0, 101.0, 2.0, 3.0),
        ("2026-07-08 12:00:02", 100.0, 102.0, 4.0, 5.0),
    )
    destination = source.iloc[::-1].reset_index(drop=True)
    destination["symbol"] = "PERP-GLD-USDC"

    status, merged = MIGRATE._destination_relationship(source, destination)

    assert status == "ALREADY_EQUIVALENT"
    assert merged is None


def test_destination_superset_is_safe() -> None:
    source = _booktop(("2026-07-08 12:00:01", 99.0, 101.0, 2.0, 3.0))
    destination = pd.concat([
        source,
        _booktop(("2026-07-08 12:00:02", 100.0, 102.0, 4.0, 5.0)),
    ], ignore_index=True)

    status, merged = MIGRATE._destination_relationship(source, destination)

    assert status == "DESTINATION_SUPERSET"
    assert merged is None


def test_complementary_chunks_are_merged_without_duplicates() -> None:
    source = _booktop(
        ("2026-07-08 12:00:01", 99.0, 101.0, 2.0, 3.0),
        ("2026-07-08 12:00:02", 100.0, 102.0, 4.0, 5.0),
    )
    destination = _booktop(
        ("2026-07-08 12:00:02", 100.0, 102.0, 4.0, 5.0),
        ("2026-07-08 12:00:03", 101.0, 103.0, 6.0, 7.0),
    )

    status, merged = MIGRATE._destination_relationship(source, destination)

    assert status == "VERIFIED_NEEDS_MERGE"
    assert merged is not None
    assert len(merged) == 3
    assert merged["timestamp"].is_monotonic_increasing


def test_incompatible_values_at_same_timestamp_are_a_conflict() -> None:
    source = _booktop(("2026-07-08 12:00:01", 99.0, 101.0, 2.0, 3.0))
    destination = _booktop(("2026-07-08 12:00:01", 9.0, 11.0, 2.0, 3.0))

    status, merged = MIGRATE._destination_relationship(source, destination)

    assert status == "ERROR_DESTINATION_CONFLICT"
    assert merged is None


def test_reviewed_quote_normalization_is_already_present() -> None:
    source = _booktop(
        ("2026-07-08 12:00:01", 99.0, 101.0, 2.0, 3.0),
        ("2026-07-08 12:00:02", 100.0, 102.0, 4.0, 5.0),
    )
    destination = source.copy()
    destination[["bid_price", "ask_price"]] *= 1.000625

    status, merged = MIGRATE._destination_relationship(source, destination, 10.0)

    assert status == "DESTINATION_NORMALIZED_EQUIVALENT"
    assert merged is None


def test_normalized_equivalence_requires_exact_event_identity() -> None:
    source = _booktop(("2026-07-08 12:00:01", 99.0, 101.0, 2.0, 3.0))
    destination = _booktop(("2026-07-08 12:00:01", 99.006, 101.006, 7.0, 3.0))

    status, _ = MIGRATE._destination_relationship(source, destination, 10.0)

    assert status == "ERROR_DESTINATION_CONFLICT"


def test_normalized_equivalence_rejects_price_difference_outside_tolerance() -> None:
    source = _booktop(("2026-07-08 12:00:01", 99.0, 101.0, 2.0, 3.0))
    destination = source.copy()
    destination[["bid_price", "ask_price"]] *= 1.002

    status, _ = MIGRATE._destination_relationship(source, destination, 10.0)

    assert status == "ERROR_DESTINATION_CONFLICT"


def test_partial_normalized_snapshots_merge_without_replacing_destination() -> None:
    source = _booktop(
        ("2026-07-08 12:00:01", 99.0, 101.0, 2.0, 3.0),
        ("2026-07-08 12:00:02", 100.0, 102.0, 4.0, 5.0),
    )
    destination = _booktop(
        ("2026-07-08 12:00:01", 99.061875, 101.063125, 2.0, 3.0),
        ("2026-07-08 12:00:03", 103.0, 105.0, 6.0, 7.0),
    )

    status, merged = MIGRATE._destination_relationship(source, destination, 15.0)

    assert status == "VERIFIED_NEEDS_NORMALIZED_MERGE"
    assert merged is not None
    assert len(merged) == 3
    overlap = merged["timestamp"].eq(pd.Timestamp("2026-07-08 12:00:01"))
    source_only = merged["timestamp"].eq(pd.Timestamp("2026-07-08 12:00:02"))
    assert merged.loc[overlap, "bid_price"].item() == 99.061875
    assert merged.loc[source_only, "bid_price"].item() == pytest.approx(100.0625)


def test_partial_normalized_merge_rejects_large_adjustment() -> None:
    source = _booktop(
        ("2026-07-08 12:00:01", 99.0, 101.0, 2.0, 3.0),
        ("2026-07-08 12:00:02", 100.0, 102.0, 4.0, 5.0),
    )
    destination = _booktop(
        ("2026-07-08 12:00:01", 99.198, 101.202, 2.0, 3.0),
        ("2026-07-08 12:00:03", 103.0, 105.0, 6.0, 7.0),
    )

    status, merged = MIGRATE._destination_relationship(source, destination, 15.0)

    assert status == "ERROR_DESTINATION_CONFLICT"
    assert merged is None


class _MemoryFs:
    def __init__(self) -> None:
        self.objects = {
            "bucket/source": {"size": 10, "ETag": '"source-etag"'},
        }

    def exists(self, key: str) -> bool:
        return key in self.objects

    def copy(self, source: str, destination: str) -> None:
        self.objects[destination] = dict(self.objects[source])

    def info(self, key: str) -> dict[str, object]:
        return self.objects[key]

    def rm(self, key: str) -> None:
        del self.objects[key]


def test_source_cleanup_is_backed_up_and_verified() -> None:
    fs = _MemoryFs()

    result = MIGRATE._backup_and_delete_source(fs, "bucket", "bucket/source")

    assert result is True
    assert not fs.exists("bucket/source")
    assert fs.exists(
        "bucket/migration_backups/historical_ty04_output_names/sources/source"
    )


def test_source_cleanup_stops_when_backup_does_not_verify() -> None:
    fs = _MemoryFs()

    def corrupt_copy(source: str, destination: str) -> None:
        fs.objects[destination] = {"size": 9, "ETag": '"wrong"'}

    fs.copy = corrupt_copy  # type: ignore[method-assign]

    result = MIGRATE._backup_and_delete_source(fs, "bucket", "bucket/source")

    assert result is False
    assert fs.exists("bucket/source")
