#!/usr/bin/env python3
"""Fast, read-only screening for cross-recorder booktop misalignment.

This is deliberately a detector, not a migration tool. It samples evenly
spaced business days, lists only those exact S3 day partitions, discovers all
recorders from filenames, and compares a small number of overlapping chunks.
Persistent unit/product mismatches (for example 0.09x, 10x, or 41x prices) are
found quickly; suspicious pairs can then receive an exhaustive targeted scan.
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
from itertools import combinations
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
from umm.tools.s3.io import list_dirs, read_parquet_key
from umm.tools.progress import track


def _sample_days(start: pd.Timestamp, end: pd.Timestamp, count: int) -> list[pd.Timestamp]:
    days = pd.bdate_range(start.normalize(), (end - pd.Timedelta("1ns")).normalize())
    if len(days) <= count:
        return list(days)
    if count == 1:
        return [days[len(days) // 2]]
    indexes = {
        round(i * (len(days) - 1) / (count - 1))
        for i in range(count)
    }
    return [days[i] for i in sorted(indexes)]


def _list_day(client: object, bucket: str, prefix: str) -> list[str]:
    kwargs: dict[str, object] = {"Bucket": bucket, "Prefix": f"{prefix}/", "MaxKeys": 1000}
    keys: list[str] = []
    while True:
        response = client.list_objects_v2(**kwargs)  # type: ignore[attr-defined]
        keys.extend(
            f"{bucket}/{item['Key']}"
            for item in response.get("Contents", [])
            if item["Key"].endswith(".parquet")
        )
        if not response.get("IsTruncated"):
            return keys
        kwargs["ContinuationToken"] = response["NextContinuationToken"]


def _median_mid(key: str) -> float:
    frame = read_parquet_key(key, columns=["bid_price", "ask_price"])
    if frame.empty:
        return float("nan")
    return float(((frame["bid_price"] + frame["ask_price"]) / 2).median())


def _evenly_spaced(values: list[pd.Timestamp], count: int) -> list[pd.Timestamp]:
    if len(values) <= count:
        return values
    if count == 1:
        return [values[len(values) // 2]]
    indexes = {
        round(i * (len(values) - 1) / (count - 1))
        for i in range(count)
    }
    return [values[i] for i in sorted(indexes)]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument(
        "--exchange",
        action="append",
        help="Exchange to scan; repeatable. Omit to scan every exchange.",
    )
    parser.add_argument("--sample-days", type=int, default=6)
    parser.add_argument("--chunks-per-day", type=int, default=2)
    parser.add_argument("--divergence", type=float, default=0.05)
    parser.add_argument("--max-workers", type=int, default=32)
    parser.add_argument("--report", type=Path, default=output_path("check_recorder_alignment.csv", "audits"))
    parser.add_argument(
        "--price-cache",
        type=Path,
        help="Sampled key-to-median cache (default: REPORT with .mid-cache.csv suffix)",
    )
    args = parser.parse_args()

    start, end = pd.Timestamp(args.start), pd.Timestamp(args.end)
    if end <= start:
        parser.error("--end must be after --start")
    if args.sample_days < 1 or args.chunks_per_day < 1 or args.max_workers < 1:
        parser.error("sample/worker counts must be positive")

    bucket = load_config()["S3_BUCKET"]
    exchanges = sorted(set(args.exchange or list_dirs(bucket, "market_data")))
    days = _sample_days(start, end, args.sample_days)
    print("Sample days: " + ", ".join(f"{day:%Y-%m-%d}" for day in days))

    # Directory discovery is shallow and parallel: no historical object scan.
    with ThreadPoolExecutor(max_workers=min(args.max_workers, len(exchanges))) as executor:
        symbol_lists = list(track(
            executor.map(
                lambda exchange: list_dirs(bucket, f"market_data/{exchange}/booktop"),
                exchanges,
            ),
            len(exchanges),
            "Discovering exchanges",
            unit="exchanges",
        ))
    symbols_by_exchange = dict(zip(exchanges, symbol_lists))

    client = boto3.client(
        "s3", config=BotoConfig(max_pool_connections=args.max_workers)
    )
    partitions = [
        (exchange, symbol, day)
        for exchange in exchanges
        for symbol in symbols_by_exchange[exchange]
        for day in days
    ]
    print(
        f"Scanning {len(exchanges)} exchanges, "
        f"{sum(map(len, symbol_lists))} instruments, {len(partitions)} sampled day partitions"
    )

    def list_partition(item: tuple[str, str, pd.Timestamp]) -> tuple[tuple[str, str, pd.Timestamp], list[str]]:
        exchange, symbol, day = item
        prefix = f"{instrument_prefix(exchange, 'booktop', symbol)}/{day:%Y-%m-%d}"
        return item, _list_day(client, bucket, prefix)

    listed: dict[tuple[str, str, pd.Timestamp], list[str]] = {}
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        results = track(
            executor.map(list_partition, partitions),
            len(partitions),
            "Listing sampled partitions",
            unit="partitions",
        )
        for item, keys in results:
            listed[item] = keys

    candidates: list[dict[str, object]] = []
    identities_with_data = 0
    multi_recorder_identities = 0
    for exchange in exchanges:
        for symbol in symbols_by_exchange[exchange]:
            identity_has_data = False
            identity_is_multi = False
            for day in days:
                by_chunk: dict[pd.Timestamp, dict[str, str]] = {}
                for key in listed[(exchange, symbol, day)]:
                    info = parse_chunk_name(Path(key).name)
                    if info is None:
                        continue
                    identity_has_data = True
                    by_chunk.setdefault(info.chunk_start, {})[info.recorder_id] = key
                recorder_ids = sorted({rec for chunk in by_chunk.values() for rec in chunk})
                if len(recorder_ids) < 2:
                    continue
                identity_is_multi = True
                for left, right in combinations(recorder_ids, 2):
                    overlap = sorted(
                        chunk for chunk, keys in by_chunk.items()
                        if left in keys and right in keys
                    )
                    for chunk in _evenly_spaced(overlap, args.chunks_per_day):
                        candidates.append({
                            "exchange": exchange,
                            "instrument": symbol,
                            "day": f"{day:%Y-%m-%d}",
                            "chunk_start": chunk,
                            "left_recorder": left,
                            "right_recorder": right,
                            "left_key": by_chunk[chunk][left],
                            "right_key": by_chunk[chunk][right],
                        })
            identities_with_data += int(identity_has_data)
            multi_recorder_identities += int(identity_is_multi)

    print(
        f"Found {identities_with_data} identities with sampled data; "
        f"{multi_recorder_identities} had multiple recorders; "
        f"comparing {len(candidates)} sampled chunks"
    )

    unique_keys = sorted({
        str(row[column])
        for row in candidates
        for column in ("left_key", "right_key")
    })
    cache_path = args.price_cache or args.report.with_suffix(".mid-cache.csv")
    mids: dict[str, float] = {}
    if cache_path.exists():
        cached = pd.read_csv(cache_path)
        if {"key", "median_mid"}.issubset(cached.columns):
            mids = dict(zip(cached["key"].astype(str), cached["median_mid"].astype(float)))
    missing_keys = [key for key in unique_keys if key not in mids]
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        mid_values = track(
            executor.map(_median_mid, missing_keys),
            len(missing_keys),
            "Reading sampled prices",
            unit="chunks",
        )
        mids.update(zip(missing_keys, mid_values))
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        ((key, mids[key]) for key in unique_keys), columns=["key", "median_mid"]
    ).to_csv(cache_path, index=False)
    print(f"Saved sampled-price cache: {cache_path}")

    for row in candidates:
        left_mid = mids[str(row["left_key"])]
        right_mid = mids[str(row["right_key"])]
        row["left_mid"] = left_mid
        row["right_mid"] = right_mid
        if not math.isfinite(left_mid) or not math.isfinite(right_mid) or right_mid == 0:
            row["ratio"] = None
            row["status"] = "INVALID_ZERO_OR_NAN_MID"
        else:
            ratio = left_mid / right_mid
            row["ratio"] = ratio
            row["status"] = (
                "RECORDER_DIVERGENCE"
                if abs(ratio - 1.0) > args.divergence
                else "RECORDER_AGREEMENT"
            )

    detail = pd.DataFrame(candidates)
    if detail.empty:
        summary = pd.DataFrame(columns=[
            "exchange", "instrument", "left_recorder", "right_recorder",
            "sampled_chunks", "divergent_samples", "min_ratio", "median_ratio",
            "max_ratio", "status",
        ])
    else:
        grouped = detail.groupby(
            ["exchange", "instrument", "left_recorder", "right_recorder"],
            dropna=False,
        )
        summary = grouped.agg(
            sampled_chunks=("ratio", "size"),
            divergent_samples=("status", lambda values: int((values == "RECORDER_DIVERGENCE").sum())),
            invalid_samples=("status", lambda values: int((values == "INVALID_ZERO_OR_NAN_MID").sum())),
            min_ratio=("ratio", "min"),
            median_ratio=("ratio", "median"),
            max_ratio=("ratio", "max"),
        ).reset_index()
        summary["status"] = summary.apply(
            lambda row: (
                "INVALID_SAMPLE"
                if row["invalid_samples"]
                else "RECORDER_DIVERGENCE"
                if row["divergent_samples"]
                else "RECORDER_AGREEMENT"
            ),
            axis=1,
        )

    args.report.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(args.report, index=False)
    samples_report = args.report.with_suffix(".samples.csv")
    detail.to_csv(samples_report, index=False)
    print("\nResult")
    print(summary["status"].value_counts().sort_index().to_string())
    suspicious = summary[summary["status"] != "RECORDER_AGREEMENT"]
    if not suspicious.empty:
        print("\nSuspicious recorder pairs")
        print(suspicious.to_string(index=False))
    print(f"\nWrote report: {args.report}")
    print(f"Wrote sample detail: {samples_report}")
    return 2 if not suspicious.empty else 0


if __name__ == "__main__":
    raise SystemExit(main())
