#!/usr/bin/env python3
"""Audit post-cutover booktop output in recorder InfluxDBs and S3.

The pre-cutover Influx window is the expected inventory.  Every
recorder/exchange/instrument seen there must appear at least once after the
cutover.  S3 checks use the recorder id embedded in parquet chunk names, so a
copy written by another recorder cannot hide a missing upload.

Example:
    python analytics/scripts/audits/audit_recorder_outputs.py \
        --cutover "2026-09-02T14:00:00-04:00"

STALE is informational by default because scheduled and closed-market feeds
can legitimately be quiet.  Missing post-cutover Influx output is a failure;
use --strict-freshness and/or --strict-s3 to make those checks fail the run.
"""

from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

_UMM_SRC = Path(os.environ.get("JST_ROOT") or Path(__file__).resolve().parents[4]) / "umm" / "src"
if str(_UMM_SRC) not in sys.path:
    sys.path.insert(0, str(_UMM_SRC))

from umm.analytics.config import load_config
from umm.analytics.storage.influx import query_influx_raw
from umm.analytics.market_data._common import (
    PREDICTION_NAMESPACE,
    RECORDER_REGISTRY,
    canonical_exchange,
    sanitize,
)
from umm.analytics.market_data.api import MarketData
from umm.tools.s3.io import list_parquet_keys


# TY01 and TY02 write to the central TY03 InfluxDB. Querying them as separate
# Influx hosts is both incorrect and impossible (they do not expose :8086).
DEFAULT_RECORDERS = ("TY03", "FR01", "NY01")
MEASUREMENTS = ("md_booktop", "md_booktop_pred")


@dataclass(frozen=True, order=True)
class Feed:
    recorder: str
    measurement: str
    exchange: str
    instrument: str


def _utc(value: str | pd.Timestamp) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _last_points(
    recorder: str,
    measurement: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    cfg: dict,
) -> dict[Feed, pd.Timestamp]:
    conn_args = MarketData._catalog_conn_args(recorder, cfg, timeout=60)
    query = (
        f'SELECT LAST("bid_price") AS value FROM "{measurement}" '
        "WHERE time >= $start AND time < $end "
        'GROUP BY "exchange_id", "instrument_id"'
    )
    result = query_influx_raw(
        conn_args,
        query,
        {"start": start.isoformat(), "end": end.isoformat()},
        epoch="ns",
    )
    points: dict[Feed, pd.Timestamp] = {}
    for (_, tags), frame in result.items():
        tag_map = dict(tags) if tags else {}
        exchange = tag_map.get("exchange_id")
        instrument = tag_map.get("instrument_id")
        if not exchange or not instrument or frame.empty:
            continue
        timestamp = _utc(pd.DatetimeIndex(frame.index).max())
        points[Feed(recorder, measurement, str(exchange), str(instrument))] = timestamp
    return points


def _s3_prefix(feed: Feed, day: str) -> str:
    exchange = sanitize(canonical_exchange(feed.exchange))
    instrument = sanitize(feed.instrument)
    if feed.measurement.endswith("_pred") or feed.instrument.startswith("PRED-"):
        return f"market_data/{exchange}/booktop/{PREDICTION_NAMESPACE}/{day}/{instrument}"
    return f"market_data/{exchange}/booktop/{instrument}/{day}"


def _has_s3_chunk(
    feed: Feed,
    bucket: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> bool:
    recorder_id = RECORDER_REGISTRY.get(feed.recorder, feed.recorder)
    for day in pd.date_range(start.normalize(), end.normalize(), freq="D"):
        day_text = day.strftime("%Y-%m-%d")
        for key in list_parquet_keys(bucket, _s3_prefix(feed, day_text)):
            name = key.rsplit("/", 1)[-1]
            if f"_{recorder_id}_" not in name:
                continue
            # The filename's final HHMM is the UTC chunk start.
            try:
                hhmm = name.removesuffix(".parquet").rsplit("_", 1)[-1]
                chunk_start = _utc(f"{day_text}T{hhmm[:2]}:{hhmm[2:]}:00Z")
            except (ValueError, IndexError):
                continue
            if start <= chunk_start < end:
                return True
    return False


def _format_age(now: pd.Timestamp, timestamp: pd.Timestamp | None) -> str:
    if timestamp is None:
        return "-"
    seconds = max(0, int((now - timestamp).total_seconds()))
    hours, remainder = divmod(seconds, 3600)
    minutes = remainder // 60
    return f"{hours}h{minutes:02d}m" if hours else f"{minutes}m"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cutover",
        required=True,
        help="Migration boundary, with UTC offset (for example '2026-09-02T14:00:00-04:00').",
    )
    parser.add_argument("--baseline-hours", type=float, default=24)
    parser.add_argument("--fresh-minutes", type=float, default=30)
    parser.add_argument("--recorders", nargs="+", default=list(DEFAULT_RECORDERS))
    parser.add_argument("--bucket", help="S3 bucket; defaults to analytics config.")
    parser.add_argument("--skip-s3", action="store_true")
    parser.add_argument("--strict-freshness", action="store_true")
    parser.add_argument("--strict-s3", action="store_true")
    parser.add_argument("--max-workers", type=int, default=16)
    args = parser.parse_args()
    if args.baseline_hours <= 0 or args.fresh_minutes <= 0 or args.max_workers <= 0:
        parser.error("window and worker values must be positive")
    args.recorders = list(dict.fromkeys(value.upper() for value in args.recorders))
    unknown = sorted(set(args.recorders) - set(RECORDER_REGISTRY))
    if unknown:
        parser.error(f"unknown recorder(s): {', '.join(unknown)}")
    return args


def main() -> int:
    args = _parse_args()
    try:
        cutover = _utc(args.cutover)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"invalid --cutover: {exc}") from exc
    now = pd.Timestamp.now(tz="UTC")
    if cutover >= now:
        raise SystemExit("--cutover must be earlier than now")
    baseline_start = cutover - pd.Timedelta(hours=args.baseline_hours)
    fresh_after = now - pd.Timedelta(minutes=args.fresh_minutes)
    cfg = load_config()

    expected: dict[Feed, pd.Timestamp] = {}
    recent: dict[Feed, pd.Timestamp] = {}
    query_errors: list[str] = []
    jobs = []
    with ThreadPoolExecutor(max_workers=min(args.max_workers, len(args.recorders) * 2)) as pool:
        for recorder in args.recorders:
            for measurement in MEASUREMENTS:
                jobs.append(("before", recorder, measurement, pool.submit(
                    _last_points, recorder, measurement, baseline_start, cutover, cfg,
                )))
                jobs.append(("after", recorder, measurement, pool.submit(
                    _last_points, recorder, measurement, cutover, now, cfg,
                )))
        for period, recorder, measurement, future in jobs:
            try:
                values = future.result()
            except Exception as exc:
                query_errors.append(f"{period}/{recorder}/{measurement}: {exc}")
                continue
            (expected if period == "before" else recent).update(values)

    print(f"baseline: {baseline_start.isoformat()} .. {cutover.isoformat()}")
    print(f"post:     {cutover.isoformat()} .. {now.isoformat()}")
    print(f"expected: {len(expected)} recorder/exchange/instrument streams")
    if query_errors:
        print("\nINFLUX QUERY ERRORS")
        for error in query_errors:
            print(f"  ERROR {error}")

    missing = sorted(set(expected) - set(recent))
    added = sorted(set(recent) - set(expected))
    stale = sorted(feed for feed in expected if feed in recent and recent[feed] < fresh_after)
    fresh = sorted(feed for feed in expected if feed in recent and recent[feed] >= fresh_after)

    print(
        f"\nINFLUX  fresh={len(fresh)} stale={len(stale)} "
        f"missing={len(missing)} new={len(added)}"
    )
    for feed in missing:
        print(f"  MISSING {feed.recorder:<4} {feed.exchange:<20} {feed.instrument}")
    for feed in stale:
        print(
            f"  STALE   {feed.recorder:<4} {feed.exchange:<20} {feed.instrument:<32} "
            f"age={_format_age(now, recent[feed])}"
        )
    for feed in added:
        print(f"  NEW     {feed.recorder:<4} {feed.exchange:<20} {feed.instrument}")

    s3_missing: list[Feed] = []
    s3_errors: list[str] = []
    if not args.skip_s3 and expected:
        bucket = args.bucket or cfg["S3_BUCKET"]
        with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
            futures = {
                pool.submit(_has_s3_chunk, feed, bucket, cutover, now): feed
                for feed in expected
            }
            for future in as_completed(futures):
                feed = futures[future]
                try:
                    if not future.result():
                        s3_missing.append(feed)
                except Exception as exc:
                    s3_errors.append(f"{feed.recorder}/{feed.exchange}/{feed.instrument}: {exc}")
        s3_missing.sort()
        print(
            f"\nS3      covered={len(expected) - len(s3_missing) - len(s3_errors)} "
            f"missing={len(s3_missing)} errors={len(s3_errors)}"
        )
        for feed in s3_missing:
            print(f"  MISSING {feed.recorder:<4} {feed.exchange:<20} {feed.instrument}")
        for error in s3_errors:
            print(f"  ERROR   {error}")

    failed = bool(query_errors or missing)
    failed = failed or (args.strict_freshness and bool(stale))
    failed = failed or (args.strict_s3 and bool(s3_missing or s3_errors))
    if not expected:
        print("\nFAIL: no pre-cutover streams found; check credentials, recorders, and window.")
        return 1
    print(f"\n{'FAIL' if failed else 'PASS'}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
