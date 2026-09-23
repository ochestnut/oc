#!/usr/bin/env python3
"""Read-only audit of historical TY04 booktop output-name cutovers.

The converted prices were historically stored under their raw input symbols.
This script samples every day in a requested window, compares TY04 with TY03 at
the old identity, and also checks whether TY04 exists at the intended identity.
It does not copy, delete, or otherwise modify S3.
"""

from __future__ import annotations

import sys as _path_sys
from pathlib import Path as _Path
_path_sys.path.insert(0, str(_Path(__file__).resolve().parents[3] / "analytics/scripts"))
from analytics_paths import output_path

import argparse
import math
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig
import pandas as pd


_JST_ROOT = Path(os.environ.get("JST_ROOT") or Path(__file__).resolve().parents[4])
_UMM_SRC = next(
    (path for path in (_JST_ROOT / "umm" / "src", _JST_ROOT / "src") if path.exists()),
    _JST_ROOT / "umm" / "src",
)
if str(_UMM_SRC) not in sys.path:
    sys.path.insert(0, str(_UMM_SRC))

from umm.analytics.config import load_config
from umm.analytics.market_data._common import instrument_prefix, parse_chunk_name
from umm.tools.s3.io import read_parquet_key
from umm.tools.progress import track


@dataclass(frozen=True)
class Conversion:
    source: str
    target: str
    expected_ratio: float
    tolerance: float = 0.08


CONVERSIONS = (
    Conversion("PERP-XAU-USDT", "PERP-GLD-USDC", 0.09178),
    Conversion("PERP-XAG-USDT", "PERP-SLV-USDC", 0.904568),
    # The oil tracking factor changed over time; allow the observed 1.52-1.55 range.
    Conversion("PERP-CL-USDT", "PERP-USO-USDC", 1.54545, 0.10),
    Conversion("PERP-SPY-USDT", "PERP-US500-USDC", 10.032907938036761),
    Conversion("PERP-QQQ-USDT", "PERP-US100-USDC", 41.0861),
    Conversion("PERP-SKHYNIX-USDT", "PERP-SKHY-USDT", 0.1, 0.12),
)


def _list_window(client: object, bucket: str, prefix: str, start: pd.Timestamp, end: pd.Timestamp) -> list[str]:
    start_after = f"{prefix}/{start.normalize():%Y-%m-%d}/"
    stop_day = (end - pd.Timedelta("1ns")).normalize() + pd.Timedelta(days=1)
    stop_before = f"{prefix}/{stop_day:%Y-%m-%d}/"
    kwargs: dict[str, object] = {
        "Bucket": bucket, "Prefix": f"{prefix}/", "StartAfter": start_after, "MaxKeys": 1000,
    }
    keys: list[str] = []
    while True:
        response = client.list_objects_v2(**kwargs)  # type: ignore[attr-defined]
        reached_end = False
        for item in response.get("Contents", []):
            key = item["Key"]
            if key >= stop_before:
                reached_end = True
                break
            if key.endswith(".parquet"):
                keys.append(f"{bucket}/{key}")
        if reached_end or not response.get("IsTruncated"):
            return keys
        kwargs.pop("StartAfter", None)
        kwargs["ContinuationToken"] = response["NextContinuationToken"]


def _by_day_chunk(keys: list[str], recorder: str) -> dict[str, dict[pd.Timestamp, str]]:
    result: dict[str, dict[pd.Timestamp, str]] = {}
    for key in keys:
        info = parse_chunk_name(Path(key).name)
        if info is not None and info.recorder_id == recorder:
            result.setdefault(info.date, {})[info.chunk_start] = key
    return result


def _sample_overlap(left: dict[pd.Timestamp, str], right: dict[pd.Timestamp, str], count: int) -> list[pd.Timestamp]:
    overlap = sorted(set(left) & set(right))
    if len(overlap) <= count:
        return overlap
    if count == 1:
        return [overlap[len(overlap) // 2]]
    indexes = {round(i * (len(overlap) - 1) / (count - 1)) for i in range(count)}
    return [overlap[index] for index in sorted(indexes)]


def _median_mid(key: str) -> float:
    frame = read_parquet_key(key, columns=["bid_price", "ask_price"])
    if frame.empty:
        return float("nan")
    return float(((frame["bid_price"] + frame["ask_price"]) / 2).median())


def _near(value: float, expected: float, tolerance: float) -> bool:
    return math.isfinite(value) and abs(value / expected - 1.0) <= tolerance


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--exchange", default="BNBFUT")
    parser.add_argument("--source-recorder", default="ap-northeast-1c_TY04")
    parser.add_argument("--compare-recorder", default="ap-northeast-1a_TY03")
    parser.add_argument("--samples-per-day", type=int, default=2)
    parser.add_argument("--max-workers", type=int, default=32)
    parser.add_argument("--report", type=Path, default=output_path("audit_booktop_conversion_cutovers.csv", "audits"))
    args = parser.parse_args()

    start, end = pd.Timestamp(args.start), pd.Timestamp(args.end)
    if end <= start:
        parser.error("--end must be after --start")
    if args.samples_per_day < 1 or args.max_workers < 1:
        parser.error("sample and worker counts must be positive")

    bucket = load_config()["S3_BUCKET"]
    client = boto3.client("s3", config=BotoConfig(max_pool_connections=args.max_workers))
    identities = sorted({item.source for item in CONVERSIONS} | {item.target for item in CONVERSIONS})

    def list_identity(instrument: str) -> tuple[str, list[str]]:
        prefix = instrument_prefix(args.exchange, "booktop", instrument)
        return instrument, _list_window(client, bucket, prefix, start, end)

    listed: dict[str, list[str]] = {}
    with ThreadPoolExecutor(max_workers=min(args.max_workers, len(identities))) as executor:
        results = track(executor.map(list_identity, identities), len(identities), "Listing identities", unit="identities")
        for instrument, keys in results:
            listed[instrument] = keys

    rows: list[dict[str, object]] = []
    read_jobs: list[tuple[int, str, str, str | None]] = []
    for conversion in CONVERSIONS:
        old_ty04 = _by_day_chunk(listed[conversion.source], args.source_recorder)
        old_ty03 = _by_day_chunk(listed[conversion.source], args.compare_recorder)
        new_ty04 = _by_day_chunk(listed[conversion.target], args.source_recorder)
        all_days = sorted(set(old_ty04) | set(old_ty03) | set(new_ty04))
        for day in all_days:
            source_chunks = old_ty04.get(day, {})
            compare_chunks = old_ty03.get(day, {})
            target_chunks = new_ty04.get(day, {})
            sampled = _sample_overlap(source_chunks, compare_chunks, args.samples_per_day)
            row_index = len(rows)
            rows.append({
                "day": day,
                "exchange": args.exchange,
                "source_instrument": conversion.source,
                "target_instrument": conversion.target,
                "expected_ty04_ty03_ratio": conversion.expected_ratio,
                "source_ty04_chunks": len(source_chunks),
                "source_ty03_chunks": len(compare_chunks),
                "target_ty04_chunks": len(target_chunks),
                "sampled_chunks": len(sampled),
                "min_ty04_ty03_ratio": None,
                "median_ty04_ty03_ratio": None,
                "max_ty04_ty03_ratio": None,
                "status": "PENDING",
            })
            for chunk in sampled:
                read_jobs.append((row_index, source_chunks[chunk], compare_chunks[chunk], target_chunks.get(chunk)))

    unique_keys = sorted({key for _, left, right, target in read_jobs for key in (left, right, target) if key})
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        mids = dict(zip(unique_keys, track(executor.map(_median_mid, unique_keys), len(unique_keys), "Reading daily samples", unit="chunks")))

    ratios_by_row: dict[int, list[float]] = {}
    target_matches_by_row: dict[int, list[bool]] = {}
    for row_index, source_key, compare_key, target_key in read_jobs:
        compare_mid = mids[compare_key]
        source_mid = mids[source_key]
        if math.isfinite(source_mid) and math.isfinite(compare_mid) and compare_mid != 0:
            ratios_by_row.setdefault(row_index, []).append(source_mid / compare_mid)
        if target_key:
            target_mid = mids[target_key]
            if math.isfinite(source_mid) and math.isfinite(target_mid) and source_mid != 0:
                target_matches_by_row.setdefault(row_index, []).append(abs(target_mid / source_mid - 1.0) <= 0.02)

    for index, row in enumerate(rows):
        ratios = ratios_by_row.get(index, [])
        target_matches = target_matches_by_row.get(index, [])
        ratio = float(pd.Series(ratios).median()) if ratios else float("nan")
        row["min_ty04_ty03_ratio"] = min(ratios) if ratios else None
        row["median_ty04_ty03_ratio"] = ratio if math.isfinite(ratio) else None
        row["max_ty04_ty03_ratio"] = max(ratios) if ratios else None
        conversion = next(item for item in CONVERSIONS if item.source == row["source_instrument"])
        source_count = int(row["source_ty04_chunks"])
        target_count = int(row["target_ty04_chunks"])
        if not source_count and target_count:
            status = "CORRECT_TARGET_ONLY"
        elif not source_count and not target_count:
            status = "NO_TY04_DATA"
        elif not ratios:
            status = "UNCLASSIFIED_NO_TY03_OVERLAP"
        elif (
            any(_near(value, 1.0, 0.03) for value in ratios)
            and any(_near(value, conversion.expected_ratio, conversion.tolerance) for value in ratios)
        ):
            status = "MIXED_CUTOVER_DAY"
        elif all(_near(value, 1.0, 0.03) for value in ratios):
            status = "RAW_SOURCE_IDENTITY"
        elif all(_near(value, conversion.expected_ratio, conversion.tolerance) for value in ratios):
            status = "MISLABELED_CONVERTED_AT_SOURCE"
            if target_count and target_matches and all(target_matches):
                status = "DUPLICATED_AT_SOURCE_AND_TARGET"
        else:
            status = "AMBIGUOUS_DIVERGENCE"
        row["status"] = status

    report = pd.DataFrame(rows).sort_values(["source_instrument", "day"])
    args.report.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(args.report, index=False)
    print("\nClassification")
    print(report["status"].value_counts().sort_index().to_string())
    suspicious = report[report["status"].isin({"MISLABELED_CONVERTED_AT_SOURCE", "DUPLICATED_AT_SOURCE_AND_TARGET", "MIXED_CUTOVER_DAY", "AMBIGUOUS_DIVERGENCE"})]
    if not suspicious.empty:
        print("\nAffected daily windows")
        summary = suspicious.groupby(["source_instrument", "target_instrument", "status"]).agg(
            first_day=("day", "min"), last_day=("day", "max"), days=("day", "size")
        )
        print(summary.to_string())
    print(f"\nWrote read-only audit: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
