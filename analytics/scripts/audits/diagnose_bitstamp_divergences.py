#!/usr/bin/env python3
"""Read-only root-cause diagnostics for flagged Bitstamp recorder divergences."""

from __future__ import annotations

import sys as _path_sys
from pathlib import Path as _Path
_path_sys.path.insert(0, str(_Path(__file__).resolve().parents[3] / "analytics/scripts"))
from analytics_paths import output_path

import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys

import pandas as pd


_JST_ROOT = Path(__file__).resolve().parents[4]
_UMM_SRC = _JST_ROOT / "umm" / "src"
if str(_UMM_SRC) not in sys.path:
    sys.path.insert(0, str(_UMM_SRC))

from umm.analytics.config import load_config
from umm.analytics.market_data._common import instrument_prefix, local_path
from umm.tools.s3.client import s3_client
from umm.tools.s3.io import list_dirs, read_parquet_key


def _key(bucket: str, symbol: str, recorder: str, chunk: pd.Timestamp) -> str:
    relative = local_path(
        Path("market_data"), "BITSTAMP", "booktop", symbol, recorder,
        chunk.to_pydatetime(),
    )
    return f"{bucket}/{relative.as_posix()}"


def _stats(key: str) -> dict[str, object]:
    frame = read_parquet_key(key, columns=["timestamp", "bid_price", "ask_price"])
    mid = (frame.bid_price + frame.ask_price) / 2
    valid = frame.bid_price.gt(0) & frame.ask_price.gt(0) & frame.bid_price.le(frame.ask_price)
    return {
        "rows": len(frame), "median_mid": float(mid.median()),
        "first_mid": float(mid.iloc[0]), "last_mid": float(mid.iloc[-1]),
        "min_mid": float(mid.min()), "max_mid": float(mid.max()),
        "invalid_quotes": int((~valid).sum()),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=Path, required=True)
    parser.add_argument("--max-workers", type=int, default=32)
    parser.add_argument("--report", type=Path, default=output_path("diagnose_bitstamp_divergences.csv", "audits"))
    args = parser.parse_args()
    bucket = load_config()["S3_BUCKET"]
    fs = s3_client()
    samples = pd.read_csv(args.samples)
    flagged = samples[
        samples.exchange.eq("BITSTAMP") & samples.status.eq("RECORDER_DIVERGENCE")
    ].copy()
    symbols = list_dirs(bucket, "market_data/BITSTAMP/booktop")

    jobs: dict[str, tuple[str, str, pd.Timestamp, str]] = {}
    for row in flagged.itertuples(index=False):
        chunk = pd.Timestamp(row.chunk_start)
        for recorder in (row.left_recorder, row.right_recorder):
            for offset, label in ((-15, "previous"), (0, "flagged"), (15, "next")):
                at = chunk + pd.Timedelta(minutes=offset)
                key = _key(bucket, row.instrument, recorder, at)
                jobs[key] = (row.instrument, recorder, at, label)
            for candidate in symbols:
                key = _key(bucket, candidate, recorder, chunk)
                jobs.setdefault(key, (candidate, recorder, chunk, "peer"))

    job_keys = list(jobs)
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        exists = list(executor.map(fs.exists, job_keys))
    existing = [key for key, present in zip(job_keys, exists) if present]
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        stats = dict(zip(existing, executor.map(_stats, existing)))

    output = []
    for row in flagged.itertuples(index=False):
        chunk = pd.Timestamp(row.chunk_start)
        record = row._asdict()
        for side, recorder in (("left", row.left_recorder), ("right", row.right_recorder)):
            for offset, label in ((-15, "previous"), (0, "flagged"), (15, "next")):
                at = chunk + pd.Timedelta(minutes=offset)
                values = stats.get(_key(bucket, row.instrument, recorder, at), {})
                for name, value in values.items():
                    record[f"{side}_{label}_{name}"] = value

        for side, recorder, anomalous_mid in (
            ("left", row.left_recorder, row.left_mid),
            ("right", row.right_recorder, row.right_mid),
        ):
            peers = []
            for candidate in symbols:
                if candidate == row.instrument:
                    continue
                values = stats.get(_key(bucket, candidate, recorder, chunk))
                if values and values["median_mid"]:
                    ratio = float(anomalous_mid) / float(values["median_mid"])
                    peers.append((abs(ratio - 1), candidate, values["median_mid"], ratio))
            peers.sort()
            for rank, (_, candidate, mid, ratio) in enumerate(peers[:3], start=1):
                record[f"{side}_nearest_{rank}_instrument"] = candidate
                record[f"{side}_nearest_{rank}_mid"] = mid
                record[f"{side}_nearest_{rank}_ratio"] = ratio
        output.append(record)

    result = pd.DataFrame(output)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(args.report, index=False)
    shown = [
        "instrument", "chunk_start", "left_mid", "right_mid",
        "left_previous_median_mid", "left_next_median_mid",
        "right_previous_median_mid", "right_next_median_mid",
        "left_nearest_1_instrument", "left_nearest_1_ratio",
        "right_nearest_1_instrument", "right_nearest_1_ratio",
    ]
    print(result[shown].to_string(index=False))
    print(f"\nWrote read-only diagnostics: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
