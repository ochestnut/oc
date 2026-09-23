#!/usr/bin/env python3
"""Move historically mislabeled booktop chunks to their correct instrument path.

Dry-run by default. A chunk is eligible only when its price ratio versus the
same-window comparison recorder falls inside the mapping's configured range.
This prevents blindly moving legitimate raw recordings that share the source
symbol. Objects without a comparison chunk are reported but not copied.

For the reviewed TY04 repair, prefer the built-in plan:

    venv/bin/python ../oc/operations/migrate/migrate_mislabeled_booktop.py \
      --plan historical-ty04-output-names --report /tmp/ty04-migration.csv

Ad hoc mapping syntax:
    SOURCE=TARGET:MIN_RATIO:MAX_RATIO

Example (dry run):
    venv/bin/python ../oc/operations/migrate/migrate_mislabeled_booktop.py \
      --start 2026-07-08 --end 2026-07-09 \
      --mapping PERP-XAU-USDT=PERP-GLD-USDC:0.08:0.11

Add --execute to copy verified objects. Add --delete-source only in a later,
reviewed run to remove sources after their destination copies are verified.
"""

from __future__ import annotations

import argparse
from io import BytesIO
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path

import pandas as pd
import boto3
from botocore.config import Config as BotoConfig


_JST_ROOT = Path(os.environ.get("JST_ROOT") or Path(__file__).resolve().parents[3])
_UMM_SRC = next(
    (path for path in (_JST_ROOT / "umm" / "src", _JST_ROOT / "src") if path.exists()),
    _JST_ROOT / "umm" / "src",
)
if str(_UMM_SRC) not in sys.path:
    sys.path.insert(0, str(_UMM_SRC))

from umm.analytics.config import load_config
from umm.analytics.market_data._common import (
    RECORDER_REGISTRY,
    instrument_prefix,
    local_path,
    parse_chunk_name,
)
from umm.tools.s3.client import s3_client
from umm.tools.s3.io import list_dirs, list_parquet_keys, read_parquet_key, write_parquet
try:
    from umm.tools.progress import track
except ImportError:
    # Keep the one-off migration runnable from UMM branches predating the
    # shared progress helper. This affects display only, never migration logic.
    def track(iterable: object, _total: int, _description: str, **_kwargs: object) -> object:
        return iterable


@dataclass(frozen=True)
class Mapping:
    source: str
    target: str
    min_ratio: float
    max_ratio: float
    start: str | None = None
    end: str | None = None
    normalized_destination_max_bps: float = 0.0


# Reviewed candidate windows from audit_booktop_conversion_cutovers.py.  These
# deliberately include mixed cutover days; ratio validation decides eligibility
# for every individual chunk, so genuine raw observations in the same day skip.
MIGRATION_PLANS: dict[str, tuple[Mapping, ...]] = {
    "historical-ty04-output-names": (
        Mapping("PERP-SPY-USDT", "PERP-US500-USDC", 9.8, 10.3, "2026-06-26", "2026-07-01"),
        Mapping("PERP-QQQ-USDT", "PERP-US100-USDC", 40.0, 42.0, "2026-06-26", "2026-07-01"),
        # These USDT -> USDC conversions can coexist with a destination copy
        # carrying the live quote-currency adjustment (observed near 6.25 bps).
        # Event identity must still match exactly; only prices get tolerance.
        Mapping("PERP-XAU-USDT", "PERP-GLD-USDC", 0.08, 0.11, "2026-07-07", "2026-07-28", 15.0),
        Mapping("PERP-XAG-USDT", "PERP-SLV-USDC", 0.89, 0.92, "2026-07-07", "2026-07-28", 15.0),
        Mapping("PERP-CL-USDT", "PERP-USO-USDC", 1.48, 1.57, "2026-07-07", "2026-07-28", 15.0),
        Mapping("PERP-SKHYNIX-USDT", "PERP-SKHY-USDT", 0.095, 0.105, "2026-07-09", "2026-07-28"),
    ),
}


def _parse_mapping(value: str) -> Mapping:
    try:
        names, low, high = value.rsplit(":", 2)
        source, target = names.split("=", 1)
        mapping = Mapping(source.upper(), target.upper(), float(low), float(high))
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            "mapping must be SOURCE=TARGET:MIN_RATIO:MAX_RATIO"
        ) from exc
    if not source or not target or mapping.min_ratio <= 0 or mapping.max_ratio <= mapping.min_ratio:
        raise argparse.ArgumentTypeError("mapping names/range are invalid")
    return mapping


def _median_mid(key: str) -> float:
    frame = read_parquet_key(key, columns=["bid_price", "ask_price"])
    return _median_mid_frame(frame)


def _median_mid_frame(frame: pd.DataFrame) -> float:
    if frame.empty:
        return float("nan")
    return float(((frame["bid_price"] + frame["ask_price"]) / 2).median())


def _key(
    bucket: str,
    exchange: str,
    instrument: str,
    date: str,
    hhmm: str,
    recorder_id: str,
) -> str:
    dt = pd.Timestamp(f"{date} {hhmm[:2]}:{hhmm[2:]}").to_pydatetime()
    relative = local_path(
        Path("market_data"), exchange, "booktop", instrument, recorder_id, dt
    )
    return f"{bucket}/{relative.as_posix()}"


def _same_object(fs: object, source: str, destination: str) -> bool:
    source_info = fs.info(source)  # type: ignore[attr-defined]
    destination_info = fs.info(destination)  # type: ignore[attr-defined]
    if source_info.get("size") != destination_info.get("size"):
        return False
    source_etag = str(source_info.get("ETag") or source_info.get("etag") or "").strip('"')
    destination_etag = str(destination_info.get("ETag") or destination_info.get("etag") or "").strip('"')
    # S3 server-side copies normally retain the ETag. Size remains the fallback
    # for backends that do not expose it.
    return not source_etag or not destination_etag or source_etag == destination_etag


_BOOKTOP_VALUE_COLUMNS = ("bid_price", "ask_price", "bid_qty", "ask_qty")
_BOOKTOP_VALIDATION_COLUMNS = ["timestamp", *_BOOKTOP_VALUE_COLUMNS]


def _canonical_booktop(frame: pd.DataFrame) -> pd.DataFrame:
    """Return the event identity used to compare independently encoded chunks."""
    required = {"timestamp", "bid_price", "ask_price"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"booktop chunk missing required columns: {sorted(missing)}")
    columns = ["timestamp", *[c for c in _BOOKTOP_VALUE_COLUMNS if c in frame.columns]]
    canonical = frame[columns].copy()
    canonical["timestamp"] = pd.to_datetime(canonical["timestamp"], errors="raise")
    for column in columns[1:]:
        canonical[column] = pd.to_numeric(canonical[column], errors="raise")
    return canonical.sort_values(columns, kind="mergesort").reset_index(drop=True)


def _row_counts(frame: pd.DataFrame) -> pd.Series:
    return frame.value_counts(sort=False, dropna=False)


def _normalized_destination_equivalent(
    source: pd.DataFrame,
    destination: pd.DataFrame,
    max_price_diff_bps: float,
) -> bool:
    """Whether destination contains every source event after quote normalization.

    Timestamp and quantities form the event identity. Prices may differ only by
    the explicitly reviewed tolerance. Requiring destination coverage prevents
    this from treating two merely nearby market samples as equivalent.
    """
    if max_price_diff_bps <= 0:
        return False
    left = _canonical_booktop(source)
    right = _canonical_booktop(destination)
    identity = ["timestamp", "bid_qty", "ask_qty"]
    if any(column not in left or column not in right for column in identity):
        return False
    if len(right) < len(left):
        return False

    # Stable price ordering makes duplicate timestamp/quantity identities pair
    # deterministically without a many-to-many merge.
    sort_columns = [*identity, "bid_price", "ask_price"]
    left = left.sort_values(sort_columns, kind="mergesort").assign(
        _occurrence=lambda frame: frame.groupby(identity, dropna=False).cumcount()
    )
    right = right.sort_values(sort_columns, kind="mergesort").assign(
        _occurrence=lambda frame: frame.groupby(identity, dropna=False).cumcount()
    )
    paired = left.merge(
        right,
        on=[*identity, "_occurrence"],
        how="left",
        suffixes=("_source", "_destination"),
        indicator=True,
    )
    if len(paired) != len(left) or not paired["_merge"].eq("both").all():
        return False
    for column in ("bid_price", "ask_price"):
        source_price = paired[f"{column}_source"]
        destination_price = paired[f"{column}_destination"]
        if destination_price.eq(0).any():
            return False
        difference_bps = ((source_price / destination_price) - 1.0).abs() * 10_000
        if difference_bps.isna().any() or difference_bps.gt(max_price_diff_bps).any():
            return False
    return True


def _normalized_union(
    source: pd.DataFrame,
    destination: pd.DataFrame,
    max_price_diff_bps: float,
) -> pd.DataFrame | None:
    """Keep destination events and add normalized source-only observations."""
    if max_price_diff_bps <= 0:
        return None
    identity = ["timestamp", "bid_qty", "ask_qty"]
    source_ordered = source.copy()
    source_ordered["timestamp"] = pd.to_datetime(
        source_ordered["timestamp"], errors="raise"
    )
    for column in _BOOKTOP_VALUE_COLUMNS:
        if column in source_ordered:
            source_ordered[column] = pd.to_numeric(source_ordered[column], errors="raise")
    source_ordered = source_ordered.sort_values(
        ["timestamp", *[c for c in _BOOKTOP_VALUE_COLUMNS if c in source_ordered]],
        kind="mergesort",
    ).reset_index(drop=True)
    source_values = _canonical_booktop(source_ordered).reset_index(names="_source_index")
    destination_values = _canonical_booktop(destination)
    if any(column not in source_values or column not in destination_values for column in identity):
        return None

    sort_columns = [*identity, "bid_price", "ask_price"]
    left = source_values.sort_values(sort_columns, kind="mergesort").copy()
    right = destination_values.sort_values(sort_columns, kind="mergesort").copy()
    left["_occurrence"] = left.groupby(identity, dropna=False).cumcount()
    right["_occurrence"] = right.groupby(identity, dropna=False).cumcount()
    paired = left.merge(
        right,
        on=[*identity, "_occurrence"],
        how="left",
        suffixes=("_source", "_destination"),
        indicator=True,
    )
    matched = paired[paired["_merge"].eq("both")]
    if matched.empty:
        return None

    ratios = pd.concat(
        [
            matched[f"{column}_destination"] / matched[f"{column}_source"]
            for column in ("bid_price", "ask_price")
        ],
        ignore_index=True,
    )
    ratios = ratios[ratios.gt(0) & ratios.notna()]
    if ratios.empty:
        return None
    multiplier = float(ratios.median())
    if abs(multiplier - 1.0) * 10_000 > max_price_diff_bps:
        return None

    unmatched_indices = paired.loc[
        paired["_merge"].eq("left_only"), "_source_index"
    ].astype(int)
    source_only = source_ordered.iloc[unmatched_indices].copy()
    if source_only.empty:
        return None
    source_only[["bid_price", "ask_price"]] *= multiplier

    columns = list(dict.fromkeys([*destination.columns, *source_only.columns]))
    merged = pd.concat(
        [destination.reindex(columns=columns), source_only.reindex(columns=columns)],
        ignore_index=True,
    )
    merged["timestamp"] = pd.to_datetime(merged["timestamp"], errors="raise")
    return merged.sort_values("timestamp", kind="mergesort").reset_index(drop=True)


def _destination_relationship(
    source: pd.DataFrame,
    destination: pd.DataFrame,
    normalized_destination_max_bps: float = 0.0,
) -> tuple[str, pd.DataFrame | None]:
    """Classify destination content and return a safe union when compatible."""
    # Normalized destinations are the overwhelmingly common case for the
    # reviewed commodity mappings. Test that relationship first so we do not
    # also build two large exact-row multisets for every already-complete chunk.
    if normalized_destination_max_bps > 0 and _normalized_destination_equivalent(
        source, destination, normalized_destination_max_bps
    ):
        return "DESTINATION_NORMALIZED_EQUIVALENT", None

    source_values = _canonical_booktop(source)
    destination_values = _canonical_booktop(destination)
    source_counts = _row_counts(source_values)
    destination_counts = _row_counts(destination_values)
    all_rows = source_counts.index.union(destination_counts.index)
    source_aligned = source_counts.reindex(all_rows, fill_value=0)
    destination_aligned = destination_counts.reindex(all_rows, fill_value=0)
    if source_aligned.equals(destination_aligned):
        return "ALREADY_EQUIVALENT", None
    if (source_aligned <= destination_aligned).all():
        return "DESTINATION_SUPERSET", None

    normalized_union = _normalized_union(
        source, destination, normalized_destination_max_bps
    )
    if normalized_union is not None:
        return "VERIFIED_NEEDS_NORMALIZED_MERGE", normalized_union

    source_by_time = source_values.groupby("timestamp", sort=False)
    destination_by_time = destination_values.groupby("timestamp", sort=False)
    overlap = set(source_by_time.groups).intersection(destination_by_time.groups)
    value_columns = [column for column in source_values.columns if column != "timestamp"]
    for timestamp in overlap:
        left = set(map(tuple, source_by_time.get_group(timestamp)[value_columns].to_numpy()))
        right = set(map(tuple, destination_by_time.get_group(timestamp)[value_columns].to_numpy()))
        if not (left.issubset(right) or right.issubset(left)):
            return "ERROR_DESTINATION_CONFLICT", None

    columns = list(dict.fromkeys([*destination.columns, *source.columns]))
    merged = pd.concat(
        [destination.reindex(columns=columns), source.reindex(columns=columns)],
        ignore_index=True,
    )
    merged = merged.assign(timestamp=pd.to_datetime(merged["timestamp"], errors="raise"))
    sort_columns = [
        "timestamp", *[column for column in _BOOKTOP_VALUE_COLUMNS if column in merged.columns]
    ]
    for column in sort_columns[1:]:
        merged[column] = pd.to_numeric(merged[column], errors="raise")
    merged = merged.sort_values(sort_columns, kind="mergesort").reset_index(drop=True)
    merged = merged.drop_duplicates(sort_columns, keep="first").reset_index(drop=True)
    return "VERIFIED_NEEDS_MERGE", merged


def _conflict_metrics(row: object) -> dict[str, object]:
    """Describe why one source/destination pair conflicts without changing it."""
    source = _canonical_booktop(read_parquet_key(row.source_key))  # type: ignore[attr-defined]
    destination = _canonical_booktop(read_parquet_key(row.destination_key))  # type: ignore[attr-defined]
    source_times = set(source["timestamp"])
    destination_times = set(destination["timestamp"])
    shared_times = source_times & destination_times

    identity = ["timestamp", "bid_qty", "ask_qty"]
    can_match_identity = all(column in source and column in destination for column in identity)
    matched = pd.DataFrame()
    if can_match_identity:
        left = source.sort_values([*identity, "bid_price", "ask_price"], kind="mergesort").copy()
        right = destination.sort_values(
            [*identity, "bid_price", "ask_price"], kind="mergesort"
        ).copy()
        left["_occurrence"] = left.groupby(identity, dropna=False).cumcount()
        right["_occurrence"] = right.groupby(identity, dropna=False).cumcount()
        matched = left.merge(
            right,
            on=[*identity, "_occurrence"],
            how="inner",
            suffixes=("_source", "_destination"),
        )

    differences: list[pd.Series] = []
    if not matched.empty:
        for column in ("bid_price", "ask_price"):
            source_price = matched[f"{column}_source"]
            destination_price = matched[f"{column}_destination"]
            valid = destination_price.ne(0)
            differences.append(
                ((source_price[valid] / destination_price[valid]) - 1.0).abs() * 10_000
            )
    price_bps = pd.concat(differences, ignore_index=True) if differences else pd.Series(dtype=float)
    identity_coverage = len(matched) / len(source) if len(source) else 0.0

    if not shared_times:
        classification = "COMPLEMENTARY_NO_TIMESTAMP_OVERLAP"
    elif identity_coverage == 1.0 and not price_bps.empty:
        classification = "SAME_EVENTS_PRICE_DIFFERENCE"
    elif identity_coverage > 0:
        classification = "PARTIAL_EVENT_OVERLAP"
    else:
        classification = "DIFFERENT_EVENTS_SAME_TIMESTAMPS"

    return {
        **row._asdict(),  # type: ignore[attr-defined]
        "conflict_classification": classification,
        "source_rows": len(source),
        "destination_rows": len(destination),
        "source_timestamps": len(source_times),
        "destination_timestamps": len(destination_times),
        "shared_timestamps": len(shared_times),
        "source_only_timestamps": len(source_times - destination_times),
        "destination_only_timestamps": len(destination_times - source_times),
        "matched_event_identities": len(matched),
        "source_identity_coverage": identity_coverage,
        "median_price_difference_bps": (
            float(price_bps.median()) if not price_bps.empty else None
        ),
        "max_price_difference_bps": (
            float(price_bps.max()) if not price_bps.empty else None
        ),
    }


def _analyze_conflicts(path: Path, max_workers: int) -> pd.DataFrame:
    report = pd.read_csv(path)
    required = {"source_key", "destination_key", "status"}
    missing = required.difference(report.columns)
    if missing:
        raise ValueError(f"migration report missing columns: {sorted(missing)}")
    conflicts = report[report["status"].eq("ERROR_DESTINATION_CONFLICT")]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        analyzed = track(
            executor.map(_conflict_metrics, conflicts.itertuples(index=False)),
            len(conflicts),
            "Analyzing destination conflicts",
            unit="chunks",
        )
        return pd.DataFrame(analyzed)


def _write_frame(fs: object, key: str, frame: pd.DataFrame) -> None:
    payload = BytesIO()
    write_parquet(frame, payload)
    fs.pipe(key, payload.getvalue())  # type: ignore[attr-defined]


def _backup_and_delete_source(fs: object, bucket: str, source_key: str) -> bool:
    """Make source deletion recoverable, then verify removal from the active path."""
    relative_source = source_key.removeprefix(f"{bucket}/")
    backup_key = (
        f"{bucket}/migration_backups/historical_ty04_output_names/sources/"
        f"{relative_source}"
    )
    if not fs.exists(backup_key):  # type: ignore[attr-defined]
        fs.copy(source_key, backup_key)  # type: ignore[attr-defined]
    if not _same_object(fs, source_key, backup_key):
        return False
    fs.rm(source_key)  # type: ignore[attr-defined]
    return not fs.exists(source_key)  # type: ignore[attr-defined]


def _merge_destination(
    fs: object,
    bucket: str,
    source_key: str,
    destination_key: str,
    target_instrument: str,
    normalized_destination_max_bps: float = 0.0,
) -> bool:
    """Back up and atomically replace a compatible destination with its union."""
    source = read_parquet_key(source_key)
    destination = read_parquet_key(destination_key)
    relationship, merged = _destination_relationship(
        source, destination, normalized_destination_max_bps
    )
    merge_statuses = {"VERIFIED_NEEDS_MERGE", "VERIFIED_NEEDS_NORMALIZED_MERGE"}
    if relationship not in merge_statuses or merged is None:
        return relationship in {"ALREADY_EQUIVALENT", "DESTINATION_SUPERSET"}
    if "symbol" in merged.columns:
        merged["symbol"] = target_instrument
    relative_destination = destination_key.removeprefix(f"{bucket}/")
    backup_key = f"{bucket}/migration_backups/historical_ty04_output_names/{relative_destination}"
    if not fs.exists(backup_key):  # type: ignore[attr-defined]
        fs.copy(destination_key, backup_key)  # type: ignore[attr-defined]
    temporary_key = f"{destination_key}.migration-{os.getpid()}-{threading.get_ident()}"
    try:
        _write_frame(fs, temporary_key, merged)
        fs.copy(temporary_key, destination_key)  # type: ignore[attr-defined]
    finally:
        if fs.exists(temporary_key):  # type: ignore[attr-defined]
            fs.rm(temporary_key)  # type: ignore[attr-defined]
    written = read_parquet_key(destination_key)
    verification, _ = _destination_relationship(
        source, written, normalized_destination_max_bps
    )
    return verification in {
        "ALREADY_EQUIVALENT", "DESTINATION_SUPERSET",
        "DESTINATION_NORMALIZED_EQUIVALENT",
    }


def _in_window(key: str, recorder_id: str, start: pd.Timestamp, end: pd.Timestamp) -> bool:
    info = parse_chunk_name(Path(key).name)
    return bool(
        info is not None
        and info.recorder_id == recorder_id
        and start <= info.chunk_start < end
    )


def _keys_for_window(
    bucket: str,
    exchange: str,
    instrument: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> list[str]:
    """List only requested day partitions rather than recursively scanning all history."""
    prefix = instrument_prefix(exchange, "booktop", instrument)
    last_day = (end - pd.Timedelta("1ns")).normalize()
    keys: list[str] = []
    for day in pd.date_range(start.normalize(), last_day, freq="D"):
        keys.extend(list_parquet_keys(bucket, f"{prefix}/{day:%Y-%m-%d}"))
    return sorted(set(keys))


def _list_symbol_window(
    client: object,
    bucket: str,
    prefix: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> list[str]:
    """Bound an S3 listing lexicographically to the requested date partitions."""
    start_after = f"{prefix}/{start.normalize():%Y-%m-%d}/"
    stop_day = (end - pd.Timedelta("1ns")).normalize() + pd.Timedelta(days=1)
    stop_before = f"{prefix}/{stop_day:%Y-%m-%d}/"
    kwargs: dict[str, object] = {
        "Bucket": bucket,
        "Prefix": f"{prefix}/",
        "StartAfter": start_after,
        "MaxKeys": 1000,
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
            break
        kwargs.pop("StartAfter", None)
        kwargs["ContinuationToken"] = response["NextContinuationToken"]
    return keys


def _audit_all_symbols(
    *,
    bucket: str,
    exchange: str,
    source_recorder_id: str,
    compare_recorder_id: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    divergence: float,
) -> pd.DataFrame:
    """Audit every source-recorder booktop object under an exchange."""
    fs = s3_client()
    exchange_prefix = f"market_data/{exchange}/booktop"
    rows: list[dict[str, object]] = []
    symbols = list_dirs(bucket, exchange_prefix)

    for number, symbol in enumerate(symbols, start=1):
        print(f"[audit {number}/{len(symbols)}] {exchange}/{symbol}")
        source_keys = [
            key for key in _keys_for_window(bucket, exchange, symbol, start, end)
            if _in_window(key, source_recorder_id, start, end)
        ]
        for source_key in sorted(source_keys):
            info = parse_chunk_name(Path(source_key).name)
            assert info is not None
            comparison_key = _key(
                bucket, exchange, symbol, info.date, info.hhmm, compare_recorder_id
            )
            row: dict[str, object] = {
                "chunk_start": info.chunk_start,
                "exchange": exchange,
                "instrument": symbol,
                "source_key": source_key,
                "comparison_key": comparison_key,
                "ratio": None,
                "status": "NO_COMPARISON_CHUNK",
            }
            if fs.exists(comparison_key):
                comparison_mid = _median_mid(comparison_key)
                source_mid = _median_mid(source_key)
                ratio = source_mid / comparison_mid
                row["ratio"] = ratio
                row["status"] = (
                    "RECORDER_DIVERGENCE"
                    if abs(ratio - 1.0) > divergence
                    else "RECORDER_AGREEMENT"
                )
            rows.append(row)

    report = pd.DataFrame(rows)
    if report.empty:
        return report

    # A missing comparison chunk next to a proven divergent regime is exactly
    # the unsafe-backfill case. Use only a one-hour neighborhood so a product
    # transition elsewhere in the date range does not contaminate the label.
    report = report.sort_values(["exchange", "instrument", "chunk_start"])
    for _, indexes in report.groupby(["exchange", "instrument"]).groups.items():
        group = report.loc[indexes]
        observed = group["ratio"].astype(float)
        nearby = observed.ffill(limit=4).bfill(limit=4)
        suspect = (
            group["status"].eq("NO_COMPARISON_CHUNK")
            & nearby.notna()
            & ((nearby - 1.0).abs() > divergence)
        )
        report.loc[group.index[suspect], "status"] = "SUSPECT_GAP_IN_DIVERGENT_REGIME"
    return report


def _audit_all_recorders(
    *,
    bucket: str,
    exchange: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    divergence: float,
    max_workers: int,
    samples_per_pair: int,
) -> pd.DataFrame:
    """Compare every recorder pair, with one S3 listing per exchange.

    The S3 layout is instrument/date/file, so listing every requested date for
    every instrument causes symbols × days remote calls. Instead, recursively
    list the exchange once, filter the requested window locally, discard
    single-recorder identities, and only then read price columns.
    """
    exchange_prefix = f"market_data/{exchange}/booktop"
    rows: list[dict[str, object]] = []
    symbols = list_dirs(bucket, exchange_prefix)
    client = boto3.client(
        "s3", config=BotoConfig(max_pool_connections=max_workers)
    )
    prefixes = [f"{exchange_prefix}/{symbol}" for symbol in symbols]
    with ThreadPoolExecutor(max_workers=max_workers) as listing_executor:
        listed = listing_executor.map(
            lambda prefix: _list_symbol_window(client, bucket, prefix, start, end),
            prefixes,
        )
        all_keys = [key for symbol_keys in listed for key in symbol_keys]
    by_symbol: dict[str, dict[pd.Timestamp, dict[str, str]]] = {}
    recorder_ids_by_symbol: dict[str, set[str]] = {}
    prefix_parts = exchange_prefix.split("/")
    for key in all_keys:
        parts = key.split("/")
        try:
            offset = parts.index(prefix_parts[0])
        except ValueError:
            continue
        relative = parts[offset:]
        # Standard layout: market_data/exchange/booktop/instrument/date/file.
        # Prediction data has a separate nested layout and is audited by its
        # own tooling rather than being misidentified here as an instrument.
        if len(relative) != 6 or relative[:3] != prefix_parts:
            continue
        symbol = relative[3]
        info = parse_chunk_name(relative[-1])
        if info is None or not (start <= info.chunk_start < end):
            continue
        by_symbol.setdefault(symbol, {}).setdefault(info.chunk_start, {})[
            info.recorder_id
        ] = key
        recorder_ids_by_symbol.setdefault(symbol, set()).add(info.recorder_id)

    multi_recorder_symbols = sorted(
        symbol for symbol, recorders in recorder_ids_by_symbol.items() if len(recorders) >= 2
    )
    skipped = len(by_symbol) - len(multi_recorder_symbols)
    print(
        f"[{exchange}] {len(by_symbol)} symbols in window; "
        f"skipping {skipped} single-recorder, comparing {len(multi_recorder_symbols)}"
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for number, symbol in enumerate(multi_recorder_symbols, start=1):
            print(f"[compare {number}/{len(multi_recorder_symbols)}] {exchange}/{symbol}")
            by_chunk = by_symbol[symbol]
            recorder_ids = recorder_ids_by_symbol[symbol]
            all_chunks = sorted(by_chunk)
            for left_recorder, right_recorder in combinations(sorted(recorder_ids), 2):
                left_chunks = {c for c in all_chunks if left_recorder in by_chunk[c]}
                right_chunks = {c for c in all_chunks if right_recorder in by_chunk[c]}
                overlap = sorted(left_chunks & right_chunks)
                left_only = len(left_chunks - right_chunks)
                right_only = len(right_chunks - left_chunks)

                if samples_per_pair == 0 or len(overlap) <= samples_per_pair:
                    sampled_chunks = overlap
                elif overlap:
                    # Include both ends and evenly cover the interior. De-dupe
                    # rounded indexes while preserving chronological order.
                    indexes = {
                        round(i * (len(overlap) - 1) / (samples_per_pair - 1))
                        for i in range(samples_per_pair)
                    } if samples_per_pair > 1 else {len(overlap) // 2}
                    sampled_chunks = [overlap[i] for i in sorted(indexes)]
                else:
                    sampled_chunks = []

                sample_keys = [
                    key
                    for chunk in sampled_chunks
                    for key in (by_chunk[chunk][left_recorder], by_chunk[chunk][right_recorder])
                ]
                sample_mids = dict(zip(sample_keys, executor.map(_median_mid, sample_keys)))
                ratios = [
                    sample_mids[by_chunk[chunk][left_recorder]]
                    / sample_mids[by_chunk[chunk][right_recorder]]
                    for chunk in sampled_chunks
                ]
                divergent_samples = sum(abs(ratio - 1.0) > divergence for ratio in ratios)
                if divergent_samples:
                    status = "RECORDER_DIVERGENCE"
                elif not overlap:
                    status = "NO_OVERLAP"
                elif left_only or right_only:
                    status = "AGREEMENT_WITH_GAPS"
                else:
                    status = "RECORDER_AGREEMENT"

                rows.append({
                    "exchange": exchange,
                    "instrument": symbol,
                    "left_recorder": left_recorder,
                    "right_recorder": right_recorder,
                    "left_chunks": len(left_chunks),
                    "right_chunks": len(right_chunks),
                    "overlap_chunks": len(overlap),
                    "left_only_chunks": left_only,
                    "right_only_chunks": right_only,
                    "sampled_chunks": len(ratios),
                    "divergent_samples": divergent_samples,
                    "min_ratio": min(ratios) if ratios else None,
                    "median_ratio": float(pd.Series(ratios).median()) if ratios else None,
                    "max_ratio": max(ratios) if ratios else None,
                    "status": status,
                })
    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", help="First UTC date, inclusive (ad hoc mappings/audits)")
    parser.add_argument("--end", help="Last UTC date, exclusive (ad hoc mappings/audits)")
    parser.add_argument("--exchange", default="BNBFUT")
    parser.add_argument(
        "--all-exchanges",
        action="store_true",
        help="With --scan-all, enumerate every exchange under S3 market_data",
    )
    parser.add_argument(
        "--all-recorders",
        action="store_true",
        help="With --scan-all, compare every recorder pair discovered from S3 filenames",
    )
    parser.add_argument("--source-recorder", default="TY04")
    parser.add_argument("--compare-recorder", default="TY03")
    parser.add_argument("--mapping", action="append", type=_parse_mapping, default=[])
    parser.add_argument(
        "--plan",
        choices=sorted(MIGRATION_PLANS),
        help="Use a reviewed built-in migration plan with per-mapping windows",
    )
    parser.add_argument(
        "--analyze-conflicts",
        type=Path,
        metavar="REPORT",
        help="Analyze only ERROR_DESTINATION_CONFLICT rows from an existing dry-run CSV",
    )
    parser.add_argument(
        "--scan-all",
        action="store_true",
        help="Audit every exchange symbol with source-recorder data in the window",
    )
    parser.add_argument(
        "--divergence",
        type=float,
        default=0.05,
        help="Fractional cross-recorder divergence flagged by --scan-all (default: 0.05)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=16,
        help="Concurrent Parquet reads during all-recorder validation (default: 16)",
    )
    parser.add_argument(
        "--samples-per-pair",
        type=int,
        default=8,
        help="Evenly spaced overlap chunks read per recorder pair; 0 reads all (default: 8)",
    )
    parser.add_argument(
        "--audit-report",
        type=Path,
        help="CSV destination for the complete --scan-all audit",
    )
    parser.add_argument("--execute", action="store_true", help="Copy verified objects")
    parser.add_argument(
        "--delete-source",
        action="store_true",
        help="Delete each source only after its destination copy verifies",
    )
    parser.add_argument("--report", type=Path, help="Write the audit plan as CSV")
    args = parser.parse_args()

    if args.delete_source and not args.execute:
        parser.error("--delete-source requires --execute")
    if not args.scan_all and not args.mapping and not args.plan and not args.analyze_conflicts:
        parser.error("provide --scan-all, --plan, --analyze-conflicts, or a mapping")
    if args.mapping and args.plan:
        parser.error("--mapping and --plan are mutually exclusive")
    mappings = list(MIGRATION_PLANS[args.plan]) if args.plan else args.mapping
    if args.execute and not mappings:
        parser.error("--execute requires --plan or at least one explicit --mapping")
    if args.all_exchanges and not args.scan_all:
        parser.error("--all-exchanges requires --scan-all")
    if args.all_recorders and not args.scan_all:
        parser.error("--all-recorders requires --scan-all")
    if args.all_exchanges and mappings:
        parser.error("global validation and migration mappings must be separate runs")
    if args.all_recorders and mappings:
        parser.error("all-recorder validation and migration mappings must be separate runs")
    if args.analyze_conflicts and (args.scan_all or mappings or args.execute or args.delete_source):
        parser.error("--analyze-conflicts is read-only and cannot be combined with other modes")

    if args.scan_all or args.mapping:
        if not args.start or not args.end:
            parser.error("--start and --end are required for audits and ad hoc mappings")
        start = pd.Timestamp(args.start)
        end = pd.Timestamp(args.end)
        if end <= start:
            parser.error("--end must be after --start")
    else:
        # Built-in plan entries carry their own reviewed windows.
        start = end = pd.NaT
    if args.max_workers < 1:
        parser.error("--max-workers must be positive")
    if args.samples_per_pair < 0:
        parser.error("--samples-per-pair cannot be negative")

    cfg = load_config()
    bucket = cfg["S3_BUCKET"]
    fs = s3_client()
    source_recorder_id = RECORDER_REGISTRY.get(args.source_recorder.upper(), args.source_recorder)
    compare_recorder_id = RECORDER_REGISTRY.get(args.compare_recorder.upper(), args.compare_recorder)

    if args.analyze_conflicts:
        analysis = _analyze_conflicts(args.analyze_conflicts, args.max_workers)
        if analysis.empty:
            print("No ERROR_DESTINATION_CONFLICT rows found.")
        else:
            print("\nConflict classification")
            print(analysis["conflict_classification"].value_counts().to_string())
            print("\nBy conversion")
            print(
                analysis.groupby(
                    ["source_instrument", "target_instrument", "conflict_classification"]
                ).size().rename("chunks").to_string()
            )
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            analysis.to_csv(args.report, index=False)
            print(f"\nWrote conflict analysis: {args.report}")
        return 0

    if args.scan_all:
        exchanges = (
            list_dirs(bucket, "market_data") if args.all_exchanges else [args.exchange]
        )
        audit_frames: list[pd.DataFrame] = []
        for exchange_number, exchange in enumerate(exchanges, start=1):
            print(f"\n[exchange {exchange_number}/{len(exchanges)}] {exchange}")
            if args.all_recorders:
                frame = _audit_all_recorders(
                    bucket=bucket,
                    exchange=exchange,
                    start=start,
                    end=end,
                    divergence=args.divergence,
                    max_workers=args.max_workers,
                    samples_per_pair=args.samples_per_pair,
                )
            else:
                frame = _audit_all_symbols(
                    bucket=bucket,
                    exchange=exchange,
                    source_recorder_id=source_recorder_id,
                    compare_recorder_id=compare_recorder_id,
                    start=start,
                    end=end,
                    divergence=args.divergence,
                )
            if not frame.empty:
                audit_frames.append(frame)
        audit = pd.concat(audit_frames, ignore_index=True) if audit_frames else pd.DataFrame()
        print("\nFull cross-recorder audit")
        if audit.empty:
            print("No source-recorder objects found in the requested window.")
        else:
            print(audit["status"].value_counts().sort_index().to_string())
            suspicious = audit[audit["status"].isin({
                "RECORDER_DIVERGENCE", "NO_OVERLAP",
                "SUSPECT_GAP_IN_DIVERGENT_REGIME",
            })]
            if not suspicious.empty:
                group_columns = ["exchange", "instrument"]
                if args.all_recorders:
                    group_columns.extend(["left_recorder", "right_recorder"])
                group_columns.append("status")
                by_symbol = suspicious.groupby(group_columns).size().rename("chunks")
                print("\nSuspicious symbols")
                print(by_symbol.to_string())
        if args.audit_report:
            args.audit_report.parent.mkdir(parents=True, exist_ok=True)
            audit.to_csv(args.audit_report, index=False)
            print(f"\nWrote full audit: {args.audit_report}")

    if not mappings:
        return 0

    records: list[dict[str, object]] = []

    for mapping in mappings:
        mapping_start = pd.Timestamp(mapping.start) if mapping.start else start
        mapping_end = pd.Timestamp(mapping.end) if mapping.end else end
        print(
            f"\n[{mapping.source} -> {mapping.target}] "
            f"{mapping_start:%Y-%m-%d}..{mapping_end:%Y-%m-%d}"
        )
        source_keys = _keys_for_window(
            bucket, args.exchange, mapping.source, mapping_start, mapping_end
        )
        destination_keys = set(_keys_for_window(
            bucket, args.exchange, mapping.target, mapping_start, mapping_end
        ))
        source_candidates: list[tuple[str, object]] = []
        available_source_keys = set(source_keys)
        for source_key in source_keys:
            info = parse_chunk_name(Path(source_key).name)
            if info is None or info.recorder_id != source_recorder_id:
                continue
            chunk_start = info.chunk_start
            if not (mapping_start <= chunk_start < mapping_end):
                continue
            source_candidates.append((source_key, info))

        def validate_candidate(candidate: tuple[str, object]) -> dict[str, object]:
            source_key, raw_info = candidate
            info = raw_info
            chunk_start = info.chunk_start  # type: ignore[attr-defined]
            comparison_key = _key(
                bucket, args.exchange, mapping.source,
                info.date, info.hhmm, compare_recorder_id,  # type: ignore[attr-defined]
            )
            destination_key = _key(
                bucket, args.exchange, mapping.target,
                info.date, info.hhmm, source_recorder_id,  # type: ignore[attr-defined]
            )
            row: dict[str, object] = {
                "chunk_start": chunk_start,
                "source_instrument": mapping.source,
                "target_instrument": mapping.target,
                "mapping_start": mapping_start,
                "mapping_end": mapping_end,
                "source_key": source_key,
                "destination_key": destination_key,
                "ratio": None,
                "status": "UNVERIFIED_NO_COMPARISON",
            }
            source_frame: pd.DataFrame | None = None

            # All source-recorder filenames were obtained in one listing, so
            # avoid a remote HEAD request for every comparison object.
            if comparison_key in available_source_keys:
                # Existing destinations require a logical content comparison.
                # Read the complete source once and reuse it for its median,
                # avoiding a second S3 GET for the common conflict case.
                if destination_key in destination_keys:
                    # Dry runs and cleanup never construct a merged object, so
                    # project only the columns required for logical validation.
                    # A copy/merge execution retains full frames and schemas.
                    validation_columns = (
                        None if args.execute and not args.delete_source
                        else _BOOKTOP_VALIDATION_COLUMNS
                    )
                    source_frame = read_parquet_key(
                        source_key, columns=validation_columns
                    )
                    source_mid = _median_mid_frame(source_frame)
                else:
                    source_mid = _median_mid(source_key)
                comparison_mid = _median_mid(comparison_key)
                if pd.notna(source_mid) and pd.notna(comparison_mid) and comparison_mid != 0:
                    ratio = source_mid / comparison_mid
                    row["ratio"] = ratio
                    if mapping.min_ratio <= ratio <= mapping.max_ratio:
                        row["status"] = "VERIFIED"
                    else:
                        row["status"] = "SKIP_RATIO_MISMATCH"
                else:
                    row["status"] = "UNVERIFIED_INVALID_MID"

            # Existing destinations may have been independently encoded, so
            # size/ETag inequality is not proof of a data conflict. Compare
            # logical booktop rows and merge only complementary observations.
            if row["status"] == "VERIFIED" and destination_key in destination_keys:
                if _same_object(fs, source_key, destination_key):
                    row["status"] = "ALREADY_MIGRATED"
                else:
                    try:
                        if source_frame is None:
                            source_frame = read_parquet_key(source_key)
                        validation_columns = (
                            None if args.execute and not args.delete_source
                            else _BOOKTOP_VALIDATION_COLUMNS
                        )
                        destination_frame = read_parquet_key(
                            destination_key, columns=validation_columns
                        )
                        row["status"], _ = _destination_relationship(
                            source_frame,
                            destination_frame,
                            mapping.normalized_destination_max_bps,
                        )
                    except Exception as exc:
                        row["status"] = "ERROR_DESTINATION_VALIDATION"
                        row["error"] = f"{type(exc).__name__}: {exc}"

            # Cleanup is deliberately a separate pass. It may remove a source
            # only when a verified destination existed before this invocation;
            # it cannot create a destination and delete its source in one run.
            if args.delete_source and row["status"] == "VERIFIED":
                row["status"] = "ERROR_CLEANUP_DESTINATION_MISSING"

            if args.execute and not args.delete_source and row["status"] == "VERIFIED":
                fs.copy(source_key, destination_key)
                if not _same_object(fs, source_key, destination_key):
                    row["status"] = "ERROR_COPY_VERIFICATION"
                else:
                    row["status"] = "COPIED"

            if args.delete_source and row["status"] in {
                "VERIFIED_NEEDS_MERGE", "VERIFIED_NEEDS_NORMALIZED_MERGE"
            }:
                row["status"] = "ERROR_CLEANUP_DESTINATION_UNRESOLVED"

            if args.execute and not args.delete_source and row["status"] in {
                "VERIFIED_NEEDS_MERGE", "VERIFIED_NEEDS_NORMALIZED_MERGE"
            }:
                try:
                    row["status"] = (
                        "MERGED"
                        if _merge_destination(
                            fs, bucket, source_key, destination_key, mapping.target,
                            mapping.normalized_destination_max_bps,
                        )
                        else "ERROR_MERGE_VERIFICATION"
                    )
                except Exception as exc:
                    row["status"] = "ERROR_MERGE"
                    row["error"] = f"{type(exc).__name__}: {exc}"

            if args.delete_source and row["status"] in {
                "ALREADY_MIGRATED", "ALREADY_EQUIVALENT",
                "DESTINATION_SUPERSET", "DESTINATION_NORMALIZED_EQUIVALENT",
            }:
                row["pre_delete_status"] = row["status"]
                try:
                    row["status"] = (
                        "MOVED"
                        if _backup_and_delete_source(fs, bucket, source_key)
                        else "ERROR_SOURCE_BACKUP_VERIFICATION"
                    )
                except Exception as exc:
                    row["status"] = "ERROR_SOURCE_DELETE"
                    row["error"] = f"{type(exc).__name__}: {exc}"

            return row

        def safely_validate_candidate(candidate: tuple[str, object]) -> dict[str, object]:
            """Keep a complete report even when one remote object operation fails."""
            try:
                return validate_candidate(candidate)
            except Exception as exc:
                source_key, raw_info = candidate
                return {
                    "chunk_start": raw_info.chunk_start,  # type: ignore[attr-defined]
                    "source_instrument": mapping.source,
                    "target_instrument": mapping.target,
                    "mapping_start": mapping_start,
                    "mapping_end": mapping_end,
                    "source_key": source_key,
                    "status": "ERROR_PROCESSING",
                    "error": f"{type(exc).__name__}: {exc}",
                }

        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            validated = track(
                executor.map(safely_validate_candidate, source_candidates),
                len(source_candidates),
                f"Validating {mapping.source}",
                unit="chunks",
            )
            records.extend(validated)

    report = pd.DataFrame(records)
    if report.empty:
        print("No matching source objects found.")
        return 0

    counts = report["status"].value_counts().sort_index()
    mode = "EXECUTE" if args.execute else "DRY RUN"
    print(f"{mode}: {len(report)} candidate objects")
    print(counts.to_string())
    print("\nSample")
    print(
        report[["chunk_start", "source_instrument", "target_instrument", "ratio", "status"]]
        .head(30)
        .to_string(index=False)
    )
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        report.to_csv(args.report, index=False)
        print(f"\nWrote report: {args.report}")

    statuses = report["status"].astype(str)
    blocking = statuses.str.startswith(("ERROR", "UNVERIFIED")).sum()
    return 1 if blocking else 0


if __name__ == "__main__":
    raise SystemExit(main())
