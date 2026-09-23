#!/usr/bin/env python3
"""Read-only audit for current BNBFUT USDT/USDC commodity identity mixing."""

from __future__ import annotations

import sys as _path_sys
from pathlib import Path as _Path
_path_sys.path.insert(0, str(_Path(__file__).resolve().parents[3] / "analytics/scripts"))
from analytics_paths import output_path

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


_JST_ROOT = Path(os.environ.get("JST_ROOT") or Path(__file__).resolve().parents[4])
_UMM_SRC = _JST_ROOT / "umm" / "src"
if str(_UMM_SRC) not in sys.path:
    sys.path.insert(0, str(_UMM_SRC))

from umm.analytics.config import load_config
from umm.analytics.market_data._common import RECORDER_REGISTRY, instrument_prefix, parse_chunk_name
from umm.tools.s3.io import list_parquet_keys, read_parquet_key


@dataclass(frozen=True)
class Pair:
    source: str
    target: str
    native_source_target_ratio: float


PAIRS = (
    Pair("PERP-XAU-USDT", "PERP-GLD-USDC", 1 / 0.091785),
    Pair("PERP-XAG-USDT", "PERP-SLV-USDC", 1 / 0.904568),
    Pair("PERP-CL-USDT", "PERP-USO-USDC", 1 / 1.519757),
)


def _keys(bucket: str, exchange: str, instrument: str, recorder: str, start: pd.Timestamp, end: pd.Timestamp) -> dict[pd.Timestamp, str]:
    prefix = instrument_prefix(exchange, "booktop", instrument)
    result: dict[pd.Timestamp, str] = {}
    for day in pd.date_range(start.normalize(), (end - pd.Timedelta("1ns")).normalize()):
        for key in list_parquet_keys(bucket, f"{prefix}/{day:%Y-%m-%d}"):
            info = parse_chunk_name(Path(key).name)
            if info and info.recorder_id == recorder and start <= info.chunk_start < end:
                result[info.chunk_start] = key
    return result


def _frame_stats(source_key: str, target_key: str) -> dict[str, float | int]:
    columns = ["timestamp", "bid_price", "ask_price", "bid_qty", "ask_qty"]
    source = read_parquet_key(source_key, columns=columns)
    target = read_parquet_key(target_key, columns=columns)
    for frame in (source, target):
        frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    source_mid = ((source.bid_price + source.ask_price) / 2).median()
    target_mid = ((target.bid_price + target.ask_price) / 2).median()
    identity = ["timestamp", "bid_qty", "ask_qty"]
    left = source[identity].drop_duplicates()
    right = target[identity].drop_duplicates()
    overlap = len(left.merge(right, on=identity, how="inner"))
    return {
        "source_rows": len(source),
        "target_rows": len(target),
        "source_target_ratio": float(source_mid / target_mid),
        "event_identity_overlap": overlap,
        "source_event_overlap_fraction": float(overlap / len(left)) if len(left) else 0.0,
    }


def classify_ratio(observed: float, native: float, overlap_fraction: float) -> str:
    native_error = abs(observed / native - 1.0)
    converted_error = abs(observed - 1.0)
    if converted_error <= 0.02 and overlap_fraction >= 0.90:
        return "ERROR_MISLABELED_DUPLICATE"
    if converted_error <= 0.02:
        return "SUSPECT_CONVERTED_PRICE_AT_SOURCE"
    if native_error <= 0.05:
        return "NATIVE_SOURCE_IDENTITY"
    return "UNVERIFIED_AMBIGUOUS_RATIO"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--exchange", default="BNBFUT")
    parser.add_argument("--recorder", default="S3_TY01")
    parser.add_argument("--max-workers", type=int, default=16)
    parser.add_argument("--report", type=Path, default=output_path("audit_current_usdt_usdc_identity.csv", "audits"))
    args = parser.parse_args()
    start, end = pd.Timestamp(args.start), pd.Timestamp(args.end)
    if end <= start or args.max_workers < 1:
        parser.error("invalid date window or worker count")

    bucket = load_config()["S3_BUCKET"]
    recorder = RECORDER_REGISTRY.get(args.recorder.upper(), args.recorder)
    jobs: list[tuple[Pair, pd.Timestamp, str, str]] = []
    rows: list[dict[str, object]] = []
    for pair in PAIRS:
        source = _keys(bucket, args.exchange, pair.source, recorder, start, end)
        target = _keys(bucket, args.exchange, pair.target, recorder, start, end)
        overlap = sorted(set(source) & set(target))
        for chunk in overlap:
            jobs.append((pair, chunk, source[chunk], target[chunk]))
        for chunk in sorted(set(source) - set(target)):
            rows.append({"chunk_start": chunk, "source_instrument": pair.source, "target_instrument": pair.target, "status": "UNVERIFIED_NO_TARGET_CHUNK"})

    def inspect(job: tuple[Pair, pd.Timestamp, str, str]) -> dict[str, object]:
        pair, chunk, source_key, target_key = job
        try:
            stats = _frame_stats(source_key, target_key)
            status = classify_ratio(
                float(stats["source_target_ratio"]),
                pair.native_source_target_ratio,
                float(stats["source_event_overlap_fraction"]),
            )
            return {"chunk_start": chunk, "source_instrument": pair.source, "target_instrument": pair.target, "native_source_target_ratio": pair.native_source_target_ratio, **stats, "status": status}
        except Exception as exc:
            return {"chunk_start": chunk, "source_instrument": pair.source, "target_instrument": pair.target, "status": "ERROR_PROCESSING", "error": f"{type(exc).__name__}: {exc}"}

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        rows.extend(executor.map(inspect, jobs))
    report = pd.DataFrame(rows)
    if not report.empty:
        report = report.sort_values(["source_instrument", "chunk_start"])
    args.report.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(args.report, index=False)
    print(report.status.value_counts().sort_index().to_string() if not report.empty else "No matching source chunks.")
    print(f"\nWrote read-only audit: {args.report}")
    blocking = report.status.astype(str).str.startswith(("ERROR", "SUSPECT", "UNVERIFIED")).any() if not report.empty else True
    return 1 if blocking else 0


if __name__ == "__main__":
    raise SystemExit(main())
