#!/usr/bin/env python3
"""Exhaustively inventory Bitstamp ETC/FARTCOIN recorder price regimes."""

from __future__ import annotations

import sys as _path_sys
from pathlib import Path as _Path
_path_sys.path.insert(0, str(_Path(__file__).resolve().parents[3] / "analytics/scripts"))
from analytics_paths import output_path

import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys

import boto3
from botocore.config import Config as BotoConfig
import pandas as pd


_JST_ROOT = Path(__file__).resolve().parents[4]
_UMM_SRC = _JST_ROOT / "umm" / "src"
if str(_UMM_SRC) not in sys.path:
    sys.path.insert(0, str(_UMM_SRC))

from umm.analytics.config import load_config
from umm.analytics.market_data._common import instrument_prefix, parse_chunk_name
from umm.tools.s3.io import read_parquet_key
try:
    from umm.tools.progress import track
except ImportError:
    def track(iterable: object, _total: int, _description: str, **_kwargs: object) -> object:
        return iterable


SYMBOLS = ("PAIR-ETC-USD", "PAIR-FARTCOIN-USD")


def _list(client: object, bucket: str, prefix: str, start: pd.Timestamp, end: pd.Timestamp) -> list[str]:
    kwargs: dict[str, object] = {"Bucket": bucket, "Prefix": f"{prefix}/", "MaxKeys": 1000}
    keys = []
    while True:
        response = client.list_objects_v2(**kwargs)  # type: ignore[attr-defined]
        for item in response.get("Contents", []):
            key = item["Key"]
            if not key.endswith(".parquet"):
                continue
            info = parse_chunk_name(Path(key).name)
            if info and start <= info.chunk_start < end:
                keys.append(f"{bucket}/{key}")
        if not response.get("IsTruncated"):
            return keys
        kwargs["ContinuationToken"] = response["NextContinuationToken"]


def _mid(key: str) -> tuple[float, int, int]:
    frame = read_parquet_key(key, columns=["bid_price", "ask_price"])
    valid = frame.bid_price.gt(0) & frame.ask_price.gt(0) & frame.bid_price.le(frame.ask_price)
    return float(((frame.bid_price + frame.ask_price) / 2).median()), len(frame), int((~valid).sum())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--max-workers", type=int, default=64)
    parser.add_argument("--report", type=Path, default=output_path("audit_bitstamp_divergence_scope.csv", "audits"))
    args = parser.parse_args()
    start, end = pd.Timestamp(args.start), pd.Timestamp(args.end)
    bucket = load_config()["S3_BUCKET"]
    client = boto3.client("s3", config=BotoConfig(max_pool_connections=args.max_workers))
    keys = []
    for symbol in SYMBOLS:
        keys.extend(_list(client, bucket, instrument_prefix("BITSTAMP", "booktop", symbol), start, end))
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        values = list(track(executor.map(_mid, keys), len(keys), "Reading Bitstamp chunks", unit="chunks"))
    rows = []
    for key, (mid, count, invalid) in zip(keys, values):
        info = parse_chunk_name(Path(key).name)
        assert info is not None
        parts = key.split("/")
        symbol = parts[parts.index("booktop") + 1]
        rows.append({"instrument": symbol, "chunk_start": info.chunk_start,
                     "recorder": info.recorder_id, "median_mid": mid,
                     "rows": count, "invalid_quotes": invalid, "key": key})
    report = pd.DataFrame(rows).sort_values(["instrument", "chunk_start", "recorder"])
    args.report.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(args.report, index=False)
    print(report.groupby(["instrument", "recorder"])["median_mid"].agg(["count", "min", "median", "max"]).to_string())
    print("\nPrice buckets")
    report["price_bucket"] = pd.cut(report.median_mid, bins=[0, .12, .2, .8, 8, 100])
    print(report.groupby(["instrument", "recorder", "price_bucket"], observed=True).size().to_string())
    print(f"\nWrote: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
