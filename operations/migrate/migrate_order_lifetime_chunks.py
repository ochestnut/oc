#!/usr/bin/env python3
"""One-time S3 migration from daily order lifetime to 15-minute chunks."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import os
import re
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

_JST_ROOT = Path(os.environ.get("JST_ROOT") or Path(__file__).resolve().parents[3])
_UMM_SRC = next(
    (path for path in (_JST_ROOT / "umm" / "src", _JST_ROOT / "src") if path.exists()),
    _JST_ROOT / "umm" / "src",
)
sys.path.insert(0, str(_UMM_SRC))

from umm.analytics.schemas import ORDER_LIFETIME_COLUMNS
from umm.analytics.trading_data._common import (
    _cache_path,
    _order_lifetime_chunk_path,
    _order_lifetime_chunk_paths,
)
from umm.tools.s3.client import s3_client


@dataclass(frozen=True)
class LegacyObject:
    key: str
    book: str
    day: pd.Timestamp


def discover_legacy_objects(
    keys: Iterable[str],
    *,
    bucket: str,
    prefix: str,
    books: set[str] | None = None,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
) -> list[LegacyObject]:
    """Parse and filter legacy daily objects from an S3 recursive listing."""
    pattern = re.compile(
        rf"^{re.escape(bucket)}/{re.escape(prefix.strip('/'))}/"
        r"([^/]+)/order_lifetime/(\d{4}-\d{2}-\d{2})/"
        r"order_lifetime_(\d{8})\.parquet$"
    )
    found: list[LegacyObject] = []
    for key in keys:
        match = pattern.match(key)
        if not match:
            continue
        book, day_text, compact_day = match.groups()
        book = book.upper()
        day = pd.Timestamp(day_text)
        if compact_day != day.strftime("%Y%m%d"):
            raise ValueError(f"date mismatch in legacy key: {key}")
        if books and book not in books:
            continue
        if start is not None and day < start:
            continue
        if end is not None and day >= end:
            continue
        found.append(LegacyObject(key=key, book=book, day=day))
    return sorted(found, key=lambda item: (item.book, item.day))


def discover_s3_legacy_objects(
    fs: Any,
    *,
    bucket: str,
    prefix: str,
    books: set[str] | None = None,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
) -> list[LegacyObject]:
    """List only order-lifetime prefixes instead of walking all trading data."""
    root = f"{bucket}/{prefix.strip('/')}"
    if books is None:
        entries = fs.ls(root, detail=True)
        books = {
            str(entry["name"]).rstrip("/").rsplit("/", 1)[-1].upper()
            for entry in entries
            if entry.get("type") == "directory"
        }

    keys: list[str] = []
    for book in sorted(books):
        order_lifetime_prefix = f"{root}/{book}/order_lifetime"
        try:
            keys.extend(fs.find(order_lifetime_prefix))
        except FileNotFoundError:
            continue
    return discover_legacy_objects(
        keys,
        bucket=bucket,
        prefix=prefix,
        books=books,
        start=start,
        end=end,
    )


def migrate_day(
    data_root: Path,
    book: str,
    day: pd.Timestamp,
    *,
    batch_rows: int = 100_000,
) -> tuple[int, int]:
    """Stream-convert one local daily file; return its row and chunk counts."""
    source = _cache_path(data_root, book, day, "order_lifetime")
    if not source.exists():
        raise FileNotFoundError(source)

    staging = source.parent / ".order_lifetime_chunk_staging"
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    writers: dict[pd.Timestamp, pq.ParquetWriter] = {}
    staged_paths: dict[pd.Timestamp, Path] = {}
    source_rows = pq.ParquetFile(source).metadata.num_rows

    try:
        parquet = pq.ParquetFile(source)
        for batch in parquet.iter_batches(
            batch_size=batch_rows,
            columns=ORDER_LIFETIME_COLUMNS,
        ):
            frame = batch.to_pandas()
            timestamps = pd.to_datetime(frame["timestamp"], errors="raise")
            if getattr(timestamps.dt, "tz", None) is not None:
                timestamps = timestamps.dt.tz_convert("UTC").dt.tz_localize(None)
            frame["timestamp"] = timestamps
            frame["_chunk"] = timestamps.dt.floor("15min")
            for chunk_start, group in frame.groupby("_chunk", sort=True):
                chunk_start = pd.Timestamp(chunk_start)
                if chunk_start.normalize() != day.normalize():
                    raise ValueError(
                        f"{source} contains {chunk_start} outside {day.date()}"
                    )
                output = group.drop(columns="_chunk")[ORDER_LIFETIME_COLUMNS]
                table = pa.Table.from_pandas(output, preserve_index=False)
                if chunk_start not in writers:
                    staged = staging / _order_lifetime_chunk_path(
                        data_root, book, chunk_start,
                    ).name
                    staged_paths[chunk_start] = staged
                    writers[chunk_start] = pq.ParquetWriter(
                        staged, table.schema, compression="snappy",
                    )
                writers[chunk_start].write_table(table)
    finally:
        for writer in writers.values():
            writer.close()

    converted_rows = sum(
        pq.ParquetFile(path).metadata.num_rows for path in staged_paths.values()
    )
    if converted_rows != source_rows:
        shutil.rmtree(staging, ignore_errors=True)
        raise RuntimeError(
            f"row-count mismatch for {book} {day.date()}: "
            f"source={source_rows} chunks={converted_rows}"
        )

    for chunk_start, staged in staged_paths.items():
        final = _order_lifetime_chunk_path(data_root, book, chunk_start)
        final.parent.mkdir(parents=True, exist_ok=True)
        staged.replace(final)
    shutil.rmtree(staging, ignore_errors=True)
    source.unlink()
    return source_rows, len(staged_paths)


def _remote_parquet_rows(fs: Any, key: str) -> int:
    with fs.open(key, "rb") as stream:
        return pq.ParquetFile(stream).metadata.num_rows


def migrate_s3_object(
    fs: Any,
    item: LegacyObject,
    *,
    bucket: str,
    prefix: str,
    data_root: Path,
    batch_rows: int,
    s3_workers: int = 4,
) -> tuple[int, int]:
    """Migrate, upload, verify, and retire one S3 legacy object."""
    source = _cache_path(data_root, item.book, item.day, "order_lifetime")
    source.parent.mkdir(parents=True, exist_ok=True)
    fs.get_file(item.key, str(source))
    source_rows, chunk_count = migrate_day(
        data_root, item.book, item.day, batch_rows=batch_rows,
    )
    chunks = _order_lifetime_chunk_paths(data_root, item.book, item.day)
    if len(chunks) != chunk_count:
        raise RuntimeError(
            f"local chunk-count mismatch for {item.book} {item.day.date()}"
        )

    day_prefix = (
        f"{bucket}/{prefix.strip('/')}/{item.book}/order_lifetime/"
        f"{item.day:%Y-%m-%d}"
    )
    uploads: dict[str, Path] = {}
    for chunk in chunks:
        key = f"{day_prefix}/{chunk.name}"
        uploads[key] = chunk

    # Book-days remain sequential. Only independent transfers within the
    # current day overlap, bounding memory while hiding S3 request latency.
    with ThreadPoolExecutor(max_workers=s3_workers) as executor:
        futures = [
            executor.submit(fs.put_file, str(chunk), key)
            for key, chunk in uploads.items()
        ]
        for future in futures:
            future.result()

    expected_keys = set(uploads)
    with ThreadPoolExecutor(max_workers=s3_workers) as executor:
        uploaded_rows = sum(executor.map(
            lambda key: _remote_parquet_rows(fs, key),
            sorted(expected_keys),
        ))
    if uploaded_rows != source_rows:
        raise RuntimeError(
            f"S3 row-count mismatch for {item.book} {item.day.date()}: "
            f"source={source_rows} chunks={uploaded_rows}"
        )

    # A retry may have left obsolete chunks. Remove them only after the complete
    # expected set has uploaded and passed row-count validation.
    remote_chunks = {
        key for key in fs.find(day_prefix)
        if re.search(r"order_lifetime_\d{8}_\d{4}\.parquet$", key)
    }
    for stale_key in sorted(remote_chunks - expected_keys):
        fs.rm(stale_key)
    fs.rm(item.key)
    shutil.rmtree(source.parent, ignore_errors=True)
    return source_rows, chunk_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Discover and sequentially migrate legacy order-lifetime objects in S3. "
            "Without --apply, only the migration plan is printed."
        )
    )
    parser.add_argument("--bucket", help="S3 bucket; defaults to analytics config")
    parser.add_argument("--prefix", default="trading_data")
    parser.add_argument(
        "--book", action="append", dest="books",
        help="limit to a book; repeat as needed (default: every discovered book)",
    )
    parser.add_argument("--start", help="optional first UTC day, inclusive")
    parser.add_argument("--end", help="optional last UTC day, exclusive")
    parser.add_argument("--batch-rows", type=int, default=100_000)
    parser.add_argument(
        "--s3-workers", type=int, default=4,
        help="concurrent chunk uploads/verifications within one day (default: 4)",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="perform uploads and delete each verified legacy S3 object",
    )
    args = parser.parse_args()
    if args.batch_rows < 1:
        parser.error("--batch-rows must be positive")
    if args.s3_workers < 1:
        parser.error("--s3-workers must be positive")

    from umm.analytics.config import load_config
    bucket = args.bucket or load_config()["S3_BUCKET"]
    start = pd.Timestamp(args.start).normalize() if args.start else None
    end = pd.Timestamp(args.end).normalize() if args.end else None
    if start is not None and end is not None and start >= end:
        parser.error("--start must be before --end")
    books = {book.upper() for book in args.books} if args.books else None

    fs = s3_client()
    root_prefix = f"{bucket}/{args.prefix.strip('/')}"
    print(f"Discovering legacy order-lifetime objects under s3://{root_prefix}")
    objects = discover_s3_legacy_objects(
        fs, bucket=bucket, prefix=args.prefix,
        books=books, start=start, end=end,
    )
    counts: dict[str, int] = {}
    for item in objects:
        counts[item.book] = counts.get(item.book, 0) + 1
    print(f"Found {len(objects)} legacy day(s) across {len(counts)} book(s)")
    for book, count in sorted(counts.items()):
        print(f"  {book}: {count} day(s)")
    if not args.apply:
        print("Plan only; rerun with --apply to perform the migration.")
        return

    with tempfile.TemporaryDirectory(prefix="order-lifetime-migration-") as tmp:
        data_root = Path(tmp)
        for index, item in enumerate(objects, start=1):
            started = time.monotonic()
            print(
                f"[{index}/{len(objects)}] START "
                f"{item.book} {item.day:%Y-%m-%d}"
            )
            rows, chunks = migrate_s3_object(
                fs, item, bucket=bucket, prefix=args.prefix,
                data_root=data_root, batch_rows=args.batch_rows,
                s3_workers=args.s3_workers,
            )
            elapsed = time.monotonic() - started
            print(
                f"[{index}/{len(objects)}] OK {item.book} "
                f"{item.day:%Y-%m-%d} rows={rows} chunks={chunks} "
                f"elapsed={elapsed:.1f}s"
            )
    print(f"DONE migrated={len(objects)}")


if __name__ == "__main__":
    main()
