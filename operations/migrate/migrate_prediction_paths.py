#!/usr/bin/env python3
"""One-time migration of prediction objects to the date-first S3 layout.

The default is a read-only plan. With --apply, each object is copied to its
date-first key, verified, and only then deleted from its old key.

Legacy: market_data/EXCHANGE/TYPE/predictions/INSTRUMENT/YYYY-MM-DD/file.parquet
New:    market_data/EXCHANGE/TYPE/predictions/YYYY-MM-DD/INSTRUMENT/file.parquet
"""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Iterator

from botocore.exceptions import ClientError
from umm.analytics.config import load_config
from umm.analytics.market_data._common import prediction_path


LEGACY_KEY = re.compile(
    r"^market_data/(?P<exchange>[^/]+)/(?P<data_type>booktop|trades)/"
    r"predictions/(?P<instrument>[^/]+)/(?P<day>\d{4}-\d{2}-\d{2})/"
    r"(?P<filename>[^/]+\.parquet)$"
)


@dataclass(frozen=True)
class Move:
    source: str
    destination: str
    exchange: str
    data_type: str
    instrument: str
    day: str
    size: int


def parse_move(key: str, size: int = 0) -> Move | None:
    match = LEGACY_KEY.match(key)
    if match is None:
        return None
    values = match.groupdict()
    exchange = values["exchange"]
    data_type = values["data_type"]
    instrument = values["instrument"]
    day = values["day"]
    destination = str(
        prediction_path(
            Path("market_data"), exchange, data_type, instrument, day,
        ) / values["filename"]
    )
    return Move(
        key, destination, exchange, data_type, instrument, day, size,
    )


def discover_exchanges(client: Any, bucket: str) -> list[str]:
    """List only immediate market_data exchange prefixes."""
    paginator = client.get_paginator("list_objects_v2")
    exchanges: set[str] = set()
    for page in paginator.paginate(
        Bucket=bucket, Prefix="market_data/", Delimiter="/",
    ):
        for item in page.get("CommonPrefixes", []):
            parts = item["Prefix"].strip("/").split("/")
            if len(parts) == 2:
                exchanges.add(parts[1])
    return sorted(exchanges)


def discover(client: Any, bucket: str, prefixes: list[str]) -> Iterator[Move]:
    """List only explicit prediction prefixes, reporting pagination progress."""
    paginator = client.get_paginator("list_objects_v2")
    for prefix in prefixes:
        print(f"Scanning s3://{bucket}/{prefix}", flush=True)
        objects = 0
        for page_number, page in enumerate(
            paginator.paginate(Bucket=bucket, Prefix=prefix), start=1,
        ):
            objects += len(page.get("Contents", []))
            if page_number % 100 == 0:
                print(f"  listed {objects:,} objects", flush=True)
            for item in page.get("Contents", []):
                move = parse_move(item["Key"], int(item.get("Size", 0)))
                if move is not None:
                    yield move
        print(f"  listed {objects:,} objects", flush=True)


def _head(client: Any, bucket: str, key: str) -> dict | None:
    try:
        return client.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
            return None
        raise


def _identity(head: dict) -> tuple[int, str]:
    """Stable content identity: strongest available S3 checksum, then ETag."""
    checksum = next((
        str(head[name]) for name in (
            "ChecksumSHA256", "ChecksumSHA1", "ChecksumCRC64NVME",
            "ChecksumCRC32C", "ChecksumCRC32",
        ) if head.get(name)
    ), str(head.get("ETag", "")))
    return int(head["ContentLength"]), checksum


def migrate_one(client: Any, bucket: str, move: Move) -> str:
    """Copy, verify, and retire one object. Safe to retry after interruption."""
    source = _head(client, bucket, move.source)
    if source is None:
        raise FileNotFoundError(move.source)
    destination = _head(client, bucket, move.destination)

    copied = destination is None
    if copied:
        client.copy_object(
            Bucket=bucket,
            Key=move.destination,
            CopySource={"Bucket": bucket, "Key": move.source},
            CopySourceIfMatch=source["ETag"],
        )
        destination = _head(client, bucket, move.destination)
    if destination is None or _identity(destination) != _identity(source):
        raise RuntimeError(f"content verification failed for {move.source}")
    client.delete_object(Bucket=bucket, Key=move.source)
    return "copied" if copied else "already-copied"


def migrate_all(
    client: Any, bucket: str, moves: list[Move], workers: int,
) -> Iterator[str]:
    """Migrate objects concurrently without queuing the full inventory."""
    move_iter = iter(moves)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = {
            pool.submit(migrate_one, client, bucket, move)
            for move in (next(move_iter, None) for _ in range(workers * 2))
            if move is not None
        }
        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                yield future.result()
                move = next(move_iter, None)
                if move is not None:
                    pending.add(pool.submit(migrate_one, client, bucket, move))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", help="default: configured S3 bucket")
    parser.add_argument("--exchange", action="append", help="limit exchange; repeatable")
    parser.add_argument("--data-type", choices=("booktop", "trades"))
    parser.add_argument("--start", help="first date, inclusive")
    parser.add_argument("--end", help="last date, exclusive")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument(
        "--apply", action="store_true",
        help="copy, verify, and delete each old key",
    )
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be positive")
    if args.start and args.end and args.start >= args.end:
        parser.error("--start must be before --end")

    import boto3

    cfg = load_config()
    bucket = args.bucket or cfg["S3_BUCKET"]
    client = boto3.client("s3")
    exchanges = sorted({value.upper() for value in args.exchange or []})
    if not exchanges:
        exchanges = discover_exchanges(client, bucket)
    data_types = [args.data_type] if args.data_type else ["booktop", "trades"]
    prefixes = [
        f"market_data/{exchange}/{data_type}/predictions/"
        for exchange in exchanges for data_type in data_types
    ]
    moves = [
        move for move in discover(client, bucket, prefixes)
        if (not args.start or move.day >= args.start)
        and (not args.end or move.day < args.end)
    ]

    total_bytes = sum(move.size for move in moves)
    print(
        f"Found {len(moves):,} legacy prediction object(s) "
        f"({total_bytes / 1024 ** 3:.2f} GiB) in s3://{bucket}/market_data"
    )
    by_scope: dict[tuple[str, str], int] = {}
    for move in moves:
        scope = (move.exchange, move.data_type)
        by_scope[scope] = by_scope.get(scope, 0) + 1
    for (exchange, data_type), count in sorted(by_scope.items()):
        print(f"  {exchange}/{data_type}: {count:,}")
    if not args.apply:
        print("Plan only; rerun with --apply to copy, verify, and retire old keys.")
        return

    if not moves:
        print("DONE migrated=0")
        return

    completed = 0
    for completed, _ in enumerate(
        migrate_all(client, bucket, moves, args.workers), start=1,
    ):
        if completed % 1000 == 0 or completed == len(moves):
            print(f"Migrated {completed:,}/{len(moves):,}")
    print(f"DONE migrated={completed:,}")


if __name__ == "__main__":
    main()
