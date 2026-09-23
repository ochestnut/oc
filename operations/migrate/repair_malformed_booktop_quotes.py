#!/usr/bin/env python3
"""Repair five reviewed malformed quotes blocking the TY04 name migration.

Dry-run by default. The allowlist is intentionally exact. A repair is eligible
only when precisely one side of a matched event has a crossed/non-positive
quote and all sane matched events establish a stable <=15 bps normalization.
"""

from __future__ import annotations

import argparse
from io import BytesIO
from pathlib import Path
import sys

import pandas as pd


_JST_ROOT = Path(__file__).resolve().parents[3]
_UMM_SRC = _JST_ROOT / "umm" / "src"
if str(_UMM_SRC) not in sys.path:
    sys.path.insert(0, str(_UMM_SRC))

from umm.analytics.config import load_config
from umm.tools.s3.client import s3_client
from umm.tools.s3.io import read_parquet_key, write_parquet


ALLOWLIST = (
    ("PERP-XAU-USDT", "PERP-GLD-USDC", "2026-07-23", "1345"),
    ("PERP-XAG-USDT", "PERP-SLV-USDC", "2026-07-09", "2130"),
    ("PERP-CL-USDT", "PERP-USO-USDC", "2026-07-13", "1415"),
    ("PERP-CL-USDT", "PERP-USO-USDC", "2026-07-14", "1330"),
    ("PERP-CL-USDT", "PERP-USO-USDC", "2026-07-24", "1315"),
)
MALFORMED_SOURCE_ALLOWLIST = {ALLOWLIST[0], ALLOWLIST[4]}
RECORDER = "ap-northeast-1c_TY04"


def _key(bucket: str, instrument: str, day: str, hhmm: str) -> str:
    name = f"BNBFUT_{instrument}_booktop_{day}_{RECORDER}_{hhmm}.parquet"
    return f"{bucket}/market_data/BNBFUT/booktop/{instrument}/{day}/{name}"


def _valid(frame: pd.DataFrame, suffix: str = "") -> pd.Series:
    bid, ask = frame[f"bid_price{suffix}"], frame[f"ask_price{suffix}"]
    return bid.gt(0) & ask.gt(0) & bid.le(ask)


def repair_frame(source: pd.DataFrame, destination: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    identity = ["timestamp", "bid_qty", "ask_qty"]
    left = source.copy()
    right = destination.copy()
    for frame in (left, right):
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="raise")
        frame["_original_index"] = range(len(frame))
        frame["_occurrence"] = frame.sort_values([*identity, "bid_price", "ask_price"], kind="mergesort").groupby(identity, dropna=False).cumcount()
    joined = left.merge(right, on=[*identity, "_occurrence"], how="inner", suffixes=("_source", "_destination"))
    source_valid = _valid(joined, "_source")
    destination_valid = _valid(joined, "_destination")
    malformed_source = ~source_valid & destination_valid
    malformed_destination = source_valid & ~destination_valid
    ambiguous = ~source_valid & ~destination_valid
    malformed_count = int(malformed_source.sum() + malformed_destination.sum())
    if ambiguous.any() or malformed_count > 1:
        raise ValueError("expected at most one one-sided malformed matched quote")

    sane = joined[source_valid & destination_valid]
    ratios = pd.concat([
        sane["bid_price_destination"] / sane["bid_price_source"],
        sane["ask_price_destination"] / sane["ask_price_source"],
    ]).dropna()
    multiplier = float(ratios.median())
    max_deviation_bps = float(((ratios / multiplier) - 1).abs().max() * 10_000)
    if not (0.99 <= multiplier <= 1.01) or max_deviation_bps > 15:
        raise ValueError(f"unstable normalization: multiplier={multiplier}, max={max_deviation_bps} bps")

    repaired = destination.copy()
    changed = 0
    if malformed_destination.any():
        row = joined.loc[malformed_destination].iloc[0]
        index = int(row["_original_index_destination"])
        repaired.loc[index, "bid_price"] = float(row["bid_price_source"]) * multiplier
        repaired.loc[index, "ask_price"] = float(row["ask_price_source"]) * multiplier
        changed = 1
    if not _valid(repaired).all():
        raise ValueError("destination still contains malformed quotes after repair")
    return repaired, {
        "normalization_multiplier": multiplier,
        "max_sane_deviation_bps": max_deviation_bps,
        "malformed_source_rows": int(malformed_source.sum()),
        "malformed_destination_rows": int(malformed_destination.sum()),
        "destination_rows_changed": changed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--delete-malformed-sources",
        action="store_true",
        help="Back up and delete only allowlisted sources proven malformed",
    )
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.delete_malformed_sources and not args.execute:
        parser.error("--delete-malformed-sources requires --execute")
    bucket = load_config()["S3_BUCKET"]
    fs = s3_client()
    rows = []
    selected = MALFORMED_SOURCE_ALLOWLIST if args.delete_malformed_sources else ALLOWLIST
    for source_symbol, target_symbol, day, hhmm in selected:
        source_key = _key(bucket, source_symbol, day, hhmm)
        destination_key = _key(bucket, target_symbol, day, hhmm)
        row = {"source_instrument": source_symbol, "target_instrument": target_symbol,
               "chunk_start": f"{day} {hhmm[:2]}:{hhmm[2:]}", "source_key": source_key,
               "destination_key": destination_key}
        try:
            source = read_parquet_key(source_key)
            destination = read_parquet_key(destination_key)
            repaired, metrics = repair_frame(source, destination)
            row.update(metrics)
            row["status"] = "VERIFIED_REPAIR" if metrics["destination_rows_changed"] else "DESTINATION_ALREADY_SANE"
            if args.execute and metrics["destination_rows_changed"]:
                relative = destination_key.removeprefix(f"{bucket}/")
                backup = f"{bucket}/migration_backups/malformed_booktop_quotes/{relative}"
                if not fs.exists(backup):
                    fs.copy(destination_key, backup)
                payload = BytesIO()
                write_parquet(repaired, payload)
                fs.pipe(destination_key, payload.getvalue())
                written = read_parquet_key(destination_key)
                _, verified = repair_frame(source, written)
                if verified["destination_rows_changed"]:
                    raise RuntimeError("written destination did not verify")
                row["status"] = "REPAIRED"
            if args.delete_malformed_sources and metrics["malformed_source_rows"] == 1:
                relative = source_key.removeprefix(f"{bucket}/")
                backup = (
                    f"{bucket}/migration_backups/historical_ty04_output_names/"
                    f"sources/{relative}"
                )
                if not fs.exists(backup):
                    fs.copy(source_key, backup)
                source_info = fs.info(source_key)
                backup_info = fs.info(backup)
                source_etag = str(source_info.get("ETag") or source_info.get("etag") or "").strip('"')
                backup_etag = str(backup_info.get("ETag") or backup_info.get("etag") or "").strip('"')
                if source_info.get("size") != backup_info.get("size") or (
                    source_etag and backup_etag and source_etag != backup_etag
                ):
                    raise RuntimeError("malformed source backup did not verify")
                fs.rm(source_key)
                if fs.exists(source_key):
                    raise RuntimeError("malformed source still exists after deletion")
                row["status"] = "MALFORMED_SOURCE_MOVED"
        except Exception as exc:
            row["status"] = "ERROR"
            row["error"] = f"{type(exc).__name__}: {exc}"
        rows.append(row)
    report = pd.DataFrame(rows)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(args.report, index=False)
    print(report[["chunk_start", "source_instrument", "target_instrument", "status"]].to_string(index=False))
    print(f"\nWrote report: {args.report}")
    return 1 if report.status.eq("ERROR").any() else 0


if __name__ == "__main__":
    raise SystemExit(main())
