#!/usr/bin/env python3
"""Explain destination conflicts from a mislabeled-booktop migration report.

This tool is read-only. It downloads source and destination Parquet objects
listed in an existing migration CSV, compares their canonical booktop values,
and writes one diagnostic row per chunk. It never copies, replaces, or deletes
an S3 object.

Example:
    venv/bin/python ../oc/operations/migrate/\
debug_mislabeled_booktop_conflicts.py \
      --report /tmp/ty04-output-name-migration-v2.csv \
      --output /tmp/ty04-conflict-debug.csv
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

from umm.analytics.config import load_config
from umm.tools.s3.io import read_parquet_key


VALUE_COLUMNS = ("bid_price", "ask_price", "bid_qty", "ask_qty")
PRICE_COLUMNS = ("bid_price", "ask_price")


def _canonical(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"timestamp", *PRICE_COLUMNS}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"booktop chunk missing required columns: {sorted(missing)}")
    columns = ["timestamp", *[column for column in VALUE_COLUMNS if column in frame.columns]]
    result = frame[columns].copy()
    result["timestamp"] = pd.to_datetime(result["timestamp"], errors="raise")
    for column in columns[1:]:
        result[column] = pd.to_numeric(result[column], errors="raise")
    return result.sort_values(columns, kind="mergesort").reset_index(drop=True)


def _quantile(series: pd.Series, value: float) -> float | None:
    clean = series.replace([float("inf"), float("-inf")], pd.NA).dropna()
    return None if clean.empty else float(clean.quantile(value))


def analyze_frames(source: pd.DataFrame, destination: pd.DataFrame) -> dict[str, object]:
    """Return exact-overlap and value-difference diagnostics for two chunks."""
    left = _canonical(source)
    right = _canonical(destination)
    shared_values = [column for column in VALUE_COLUMNS if column in left and column in right]

    # Pair rows by timestamp and occurrence number. This preserves repeated
    # updates at one timestamp without creating a many-to-many merge.
    left = left.assign(_occurrence=left.groupby("timestamp").cumcount())
    right = right.assign(_occurrence=right.groupby("timestamp").cumcount())
    joined = left.merge(
        right,
        on=["timestamp", "_occurrence"],
        how="inner",
        suffixes=("_source", "_destination"),
    )

    result: dict[str, object] = {
        "source_rows": len(left),
        "destination_rows": len(right),
        "source_unique_timestamps": left["timestamp"].nunique(),
        "destination_unique_timestamps": right["timestamp"].nunique(),
        "overlap_rows": len(joined),
        "source_only_rows": len(left) - len(joined),
        "destination_only_rows": len(right) - len(joined),
        "source_columns": ",".join(map(str, source.columns)),
        "destination_columns": ",".join(map(str, destination.columns)),
        "source_dtypes": ",".join(f"{name}:{dtype}" for name, dtype in source.dtypes.items()),
        "destination_dtypes": ",".join(
            f"{name}:{dtype}" for name, dtype in destination.dtypes.items()
        ),
    }

    any_difference = pd.Series(False, index=joined.index)
    for column in shared_values:
        source_values = joined[f"{column}_source"]
        destination_values = joined[f"{column}_destination"]
        different = ~(
            source_values.eq(destination_values)
            | (source_values.isna() & destination_values.isna())
        )
        result[f"{column}_different_rows"] = int(different.sum())
        any_difference |= different

    result["different_overlap_rows"] = int(any_difference.sum())
    result["exact_overlap_rows"] = len(joined) - int(any_difference.sum())

    if joined.empty:
        result["median_abs_mid_diff_bps"] = None
        result["p95_abs_mid_diff_bps"] = None
        result["max_abs_mid_diff_bps"] = None
    else:
        source_mid = (joined["bid_price_source"] + joined["ask_price_source"]) / 2
        destination_mid = (
            joined["bid_price_destination"] + joined["ask_price_destination"]
        ) / 2
        denominator = destination_mid.abs().where(destination_mid.ne(0))
        absolute_mid_diff_bps = ((source_mid - destination_mid).abs() / denominator) * 10_000
        result["median_abs_mid_diff_bps"] = _quantile(absolute_mid_diff_bps, 0.5)
        result["p95_abs_mid_diff_bps"] = _quantile(absolute_mid_diff_bps, 0.95)
        result["max_abs_mid_diff_bps"] = _quantile(absolute_mid_diff_bps, 1.0)

    return result


def _diagnose(row: object) -> dict[str, object]:
    record = row._asdict()  # type: ignore[attr-defined]
    try:
        source = read_parquet_key(record["source_key"])
        destination = read_parquet_key(record["destination_key"])
        return {**record, **analyze_frames(source, destination), "debug_status": "OK"}
    except Exception as exc:
        return {
            **record,
            "debug_status": "ERROR",
            "debug_error": f"{type(exc).__name__}: {exc}",
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True, help="Migration dry-run CSV")
    parser.add_argument("--output", type=Path, required=True, help="Diagnostic CSV destination")
    parser.add_argument(
        "--status", default="ERROR_DESTINATION_CONFLICT", help="Migration status to inspect"
    )
    parser.add_argument("--source-instrument", help="Limit diagnostics to one source symbol")
    parser.add_argument("--limit", type=int, default=0, help="Inspect at most N rows; 0 means all")
    parser.add_argument("--max-workers", type=int, default=16)
    args = parser.parse_args()

    if args.limit < 0:
        parser.error("--limit cannot be negative")
    if args.max_workers < 1:
        parser.error("--max-workers must be positive")

    report = pd.read_csv(args.report)
    required = {"status", "source_key", "destination_key"}
    missing = required.difference(report.columns)
    if missing:
        parser.error(f"report missing required columns: {sorted(missing)}")
    selected = report[report["status"].eq(args.status)]
    if args.source_instrument:
        selected = selected[selected["source_instrument"].eq(args.source_instrument)]
    if args.limit:
        selected = selected.head(args.limit)
    if selected.empty:
        print("No matching report rows.")
        return 0

    # Besides loading analytics settings, this establishes the repository's
    # default AWS profile and refreshes its SAML session when required. The
    # migration command does this before touching S3; the debugger must too.
    load_config()
    print(f"Inspecting {len(selected)} read-only source/destination pairs...")
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        rows = list(executor.map(_diagnose, selected.itertuples(index=False)))
    diagnostics = pd.DataFrame(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    diagnostics.to_csv(args.output, index=False)

    print(diagnostics["debug_status"].value_counts().sort_index().to_string())
    successful = diagnostics[diagnostics["debug_status"].eq("OK")]
    if not successful.empty:
        group_columns = [
            column
            for column in ("source_instrument", "target_instrument")
            if column in successful
        ]
        metrics = [
            "source_rows", "destination_rows", "overlap_rows",
            "different_overlap_rows", "median_abs_mid_diff_bps",
            "p95_abs_mid_diff_bps", "max_abs_mid_diff_bps",
        ]
        print("\nConflict summary (median per chunk)")
        print(successful.groupby(group_columns)[metrics].median().to_string())
    print(f"\nWrote diagnostic report: {args.output}")
    return 1 if diagnostics["debug_status"].eq("ERROR").any() else 0


if __name__ == "__main__":
    raise SystemExit(main())
