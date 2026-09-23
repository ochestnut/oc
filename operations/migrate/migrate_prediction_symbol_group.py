#!/usr/bin/env python3
"""Migrate legacy prediction Parquet objects and add symbol/group columns.

The default mode is a read-only inventory. The phases are intentionally
separate:

  --migrate   preserve, enrich, validate, and retire each object in one process
  --apply     read a legacy object, enrich it, and write the date-first key
  --validate  verify destination schema and row content without changing S3
  --retire    validate again, then delete the legacy source key

Legacy: market_data/EXCHANGE/TYPE/predictions/INSTRUMENT/DATE/file.parquet
New:    market_data/EXCHANGE/TYPE/predictions/DATE/INSTRUMENT/file.parquet

For already date-first objects, ``--apply`` first copies the original object to
``migration_backups/prediction_symbol_group/...`` and verifies that copy before
rewriting the canonical key. This makes interrupted runs safe to retry.

An enriched canonical object with no backup is already complete. The phased
modes skip it after checking its identity columns; content is compared with the
original whenever a backup is available.
"""

from __future__ import annotations

import argparse
import base64
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
import hashlib
from io import BytesIO
from pathlib import Path
import re
import time
from typing import Any, Callable, Iterator

import pandas as pd
from botocore.exceptions import ClientError

from umm.analytics.config import load_config
from umm.analytics.market_data._common import prediction_group, prediction_path
from umm.tools.s3.io import write_parquet


LEGACY_KEY = re.compile(
    r"^market_data/(?P<exchange>[^/]+)/(?P<data_type>booktop|trades)/"
    r"predictions/(?P<instrument>[^/]+)/(?P<day>\d{4}-\d{2}-\d{2})/"
    r"(?P<filename>[^/]+\.parquet)$"
)
CANONICAL_KEY = re.compile(
    r"^market_data/(?P<exchange>[^/]+)/(?P<data_type>booktop|trades)/"
    r"predictions/(?P<day>\d{4}-\d{2}-\d{2})/(?P<instrument>[^/]+)/"
    r"(?P<filename>[^/]+\.parquet)$"
)
BACKUP_PREFIX = "migration_backups/prediction_symbol_group"


@dataclass(frozen=True)
class Migration:
    source: str
    destination: str
    exchange: str
    data_type: str
    instrument: str
    group: str
    day: str
    legacy: bool
    backup: str | None = None
    size: int = 0


def parse_migration(key: str, size: int = 0) -> Migration | None:
    """Parse a legacy symbol-first or canonical date-first prediction key."""
    legacy_match = LEGACY_KEY.fullmatch(key)
    canonical_match = CANONICAL_KEY.fullmatch(key)
    match = legacy_match or canonical_match
    if match is None:
        return None
    values = match.groupdict()
    group = prediction_group(values["instrument"])
    if group is None:
        raise ValueError(f"cannot derive prediction group from {values['instrument']!r}")
    canonical = str(prediction_path(
        Path("market_data"), values["exchange"], values["data_type"],
        values["instrument"], values["day"],
    ) / values["filename"])
    legacy = legacy_match is not None
    return Migration(
        source=key,
        destination=canonical,
        exchange=values["exchange"],
        data_type=values["data_type"],
        instrument=values["instrument"],
        group=group,
        day=values["day"],
        legacy=legacy,
        backup=None if legacy else f"{BACKUP_PREFIX}/{key}",
        size=size,
    )


def discover_exchanges(client: Any, bucket: str) -> list[str]:
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


def discover(client: Any, bucket: str, prefixes: list[str]) -> Iterator[Migration]:
    paginator = client.get_paginator("list_objects_v2")
    for prefix in prefixes:
        print(f"Scanning s3://{bucket}/{prefix}", flush=True)
        count = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            count += len(page.get("Contents", []))
            for item in page.get("Contents", []):
                migration = parse_migration(item["Key"], int(item.get("Size", 0)))
                if migration is not None:
                    yield migration
        print(f"  listed {count:,} object(s)", flush=True)


def discovery_prefixes(
    exchanges: list[str], data_types: list[str], instruments: set[str],
    start: str | None, end: str | None, layout: str,
) -> list[str]:
    """Build the narrowest prefixes possible for the requested scope."""
    bases = [
        f"market_data/{exchange}/{data_type}/predictions/"
        for exchange in exchanges for data_type in data_types
    ]
    days: list[str] = []
    if start and end:
        days = [
            day.strftime("%Y-%m-%d")
            for day in pd.date_range(
                pd.Timestamp(start), pd.Timestamp(end) - pd.Timedelta("1ns"), freq="D",
            )
        ]

    # Without enough filters, one recursive base listing discovers both layouts
    # more cheaply than listing the same base twice.
    if layout == "both" and (not instruments or not days):
        return bases

    prefixes: list[str] = []
    for base in bases:
        if layout in {"canonical", "both"}:
            if days and instruments:
                prefixes.extend(
                    f"{base}{day}/{instrument}/"
                    for day in days for instrument in sorted(instruments)
                )
            elif days:
                prefixes.extend(f"{base}{day}/" for day in days)
            else:
                prefixes.append(base)
        if layout in {"legacy", "both"}:
            if instruments:
                prefixes.extend(f"{base}{instrument}/" for instrument in sorted(instruments))
            else:
                prefixes.append(base)
    return list(dict.fromkeys(prefixes))


def _head(client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
            return None
        raise


def _read(client: Any, bucket: str, key: str) -> pd.DataFrame:
    response = client.get_object(Bucket=bucket, Key=key)
    # botocore.response.StreamingBody is readable but not seekable; PyArrow
    # needs random access to the Parquet footer. Prediction chunks are small,
    # so buffer exactly one object per worker.
    return pd.read_parquet(BytesIO(response["Body"].read()), engine="pyarrow")


def _identity(head: dict[str, Any]) -> tuple[int, str]:
    return int(head["ContentLength"]), str(head.get("ETag", ""))


def _copy_original(client: Any, bucket: str, migration: Migration) -> None:
    """Create and verify the immutable backup for a canonical rewrite."""
    assert migration.backup is not None
    source = _head(client, bucket, migration.source)
    if source is None:
        raise FileNotFoundError(migration.source)
    backup = _head(client, bucket, migration.backup)
    if backup is None:
        client.copy_object(
            Bucket=bucket,
            Key=migration.backup,
            CopySource={"Bucket": bucket, "Key": migration.source},
            CopySourceIfMatch=source.get("ETag", ""),
        )
        backup = _head(client, bucket, migration.backup)
    if backup is None or _identity(backup) != _identity(source):
        raise RuntimeError(f"unverified backup for {migration.source}")


def enrich(frame: pd.DataFrame, migration: Migration) -> pd.DataFrame:
    """Add constant identity columns, rejecting conflicting existing values."""
    if "timestamp" not in frame.columns:
        raise ValueError(f"{migration.source} has no timestamp column")
    result = frame.copy()
    expected = {"symbol": migration.instrument, "group": migration.group}
    for column, value in expected.items():
        if column in result.columns:
            actual = set(result[column].dropna().astype(str).unique())
            if actual and actual != {value}:
                raise RuntimeError(
                    f"{migration.source} has conflicting {column}: {sorted(actual)!r}"
                )
        result[column] = value
    return result


def _is_enriched(frame: pd.DataFrame, migration: Migration) -> bool:
    expected = {"symbol": migration.instrument, "group": migration.group}
    return "timestamp" in frame.columns and all(
        column in frame.columns
        and frame[column].notna().all()
        and frame[column].astype(str).eq(value).all()
        for column, value in expected.items()
    )


def _validate_frames(
    source: pd.DataFrame, destination: pd.DataFrame, migration: Migration,
) -> None:
    expected = enrich(source, migration)
    if list(destination.columns) != list(expected.columns):
        raise RuntimeError(
            f"column mismatch for {migration.destination}: "
            f"expected={list(expected.columns)!r} actual={list(destination.columns)!r}"
        )
    try:
        pd.testing.assert_frame_equal(
            destination.reset_index(drop=True), expected.reset_index(drop=True),
            check_dtype=True, check_like=False,
        )
    except AssertionError as exc:
        raise RuntimeError(f"content mismatch for {migration.destination}: {exc}") from exc


def validate_one(client: Any, bucket: str, migration: Migration) -> str:
    comparison_key = migration.source
    if not migration.legacy:
        assert migration.backup is not None
        comparison_key = migration.backup
    if _head(client, bucket, comparison_key) is None:
        if not migration.legacy and _is_enriched(
            _read(client, bucket, migration.destination), migration,
        ):
            return "already-enriched"
        raise FileNotFoundError(comparison_key)
    if _head(client, bucket, migration.destination) is None:
        raise FileNotFoundError(migration.destination)
    source = _read(client, bucket, comparison_key)
    destination = _read(client, bucket, migration.destination)
    _validate_frames(source, destination, migration)
    return "validated"


def apply_one(client: Any, bucket: str, migration: Migration) -> str:
    """Create and validate one enriched destination; retain the source."""
    if _head(client, bucket, migration.source) is None:
        raise FileNotFoundError(migration.source)
    destination_head = _head(client, bucket, migration.destination)
    if migration.legacy and destination_head is not None:
        validate_one(client, bucket, migration)
        return "already-applied"

    if not migration.legacy:
        current = _read(client, bucket, migration.source)
        if _is_enriched(current, migration):
            assert migration.backup is not None
            if _head(client, bucket, migration.backup) is not None:
                _validate_frames(_read(client, bucket, migration.backup), current, migration)
            return "already-enriched"
        # Fail before the backup/write if existing non-null identity conflicts.
        enrich(current, migration)
        _copy_original(client, bucket, migration)
        assert migration.backup is not None
        source = _read(client, bucket, migration.backup)
    else:
        source = _read(client, bucket, migration.source)
    enriched = enrich(source, migration)
    payload = BytesIO()
    write_parquet(enriched, payload)
    payload.seek(0)
    client.put_object(
        Bucket=bucket,
        Key=migration.destination,
        Body=payload.getvalue(),
        ContentType="application/vnd.apache.parquet",
    )
    try:
        validate_one(client, bucket, migration)
    except Exception:
        # Legacy sources and canonical backups remain available for recovery.
        if migration.legacy:
            client.delete_object(Bucket=bucket, Key=migration.destination)
        raise
    return "applied"


def retire_one(client: Any, bucket: str, migration: Migration) -> str:
    if validate_one(client, bucket, migration) == "already-enriched":
        return "already-retired"
    retire_key = migration.source if migration.legacy else migration.backup
    assert retire_key is not None
    client.delete_object(Bucket=bucket, Key=retire_key)
    return "retired"


def _restore_canonical(client: Any, bucket: str, migration: Migration) -> None:
    """Restore a canonical object from its verified backup after a failed write."""
    assert migration.backup is not None
    backup = _head(client, bucket, migration.backup)
    if backup is None:
        raise RuntimeError(f"cannot restore {migration.source}: backup is missing")
    client.copy_object(
        Bucket=bucket,
        Key=migration.source,
        CopySource={"Bucket": bucket, "Key": migration.backup},
        CopySourceIfMatch=backup.get("ETag", ""),
    )
    restored = _head(client, bucket, migration.source)
    if restored is None or _identity(restored) != _identity(backup):
        raise RuntimeError(f"failed to restore {migration.source} from backup")


def migrate_one(client: Any, bucket: str, migration: Migration) -> str:
    """Efficient combined migration with one source and one verification read."""
    original = _read(client, bucket, migration.source)

    if not migration.legacy and _is_enriched(original, migration):
        assert migration.backup is not None
        if _head(client, bucket, migration.backup) is None:
            return "already-enriched"
        # Resume an interrupted run that completed the rewrite but retained its
        # backup. validate_one compares the backup with the enriched canonical.
        retire_one(client, bucket, migration)
        return "migrated"

    enriched = enrich(original, migration)
    destination_exists = False
    write_attempted = False
    try:
        destination_exists = _head(client, bucket, migration.destination) is not None
        if not migration.legacy:
            _copy_original(client, bucket, migration)
        if not destination_exists or not migration.legacy:
            payload = BytesIO()
            write_parquet(enriched, payload)
            write_attempted = True
            client.put_object(
                Bucket=bucket,
                Key=migration.destination,
                Body=payload.getvalue(),
                ContentType="application/vnd.apache.parquet",
            )

        destination = _read(client, bucket, migration.destination)
        _validate_frames(original, destination, migration)
    except Exception:
        if write_attempted and migration.legacy and not destination_exists:
            client.delete_object(Bucket=bucket, Key=migration.destination)
        if (
            write_attempted
            and not migration.legacy
            and migration.backup is not None
            and _head(client, bucket, migration.backup) is not None
        ):
            _restore_canonical(client, bucket, migration)
        raise

    # A DELETE can succeed remotely even when its response is lost. Once the
    # destination is validated, retirement errors must never roll it back.
    retire_key = migration.source if migration.legacy else migration.backup
    assert retire_key is not None
    client.delete_object(Bucket=bucket, Key=retire_key)
    return "migrated"


def fast_migrate_one(client: Any, bucket: str, migration: Migration) -> str:
    """Validate locally and atomically replace an object with one GET and PUT.

    S3 PutObject is atomic, and ChecksumSHA256 makes S3 reject a corrupted
    transfer. The original remains intact unless the complete validated PUT
    succeeds. This mode intentionally avoids persistent backup objects.
    """
    if migration.legacy:
        raise ValueError("--fast-migrate supports canonical objects only")
    original = _read(client, bucket, migration.source)
    if _is_enriched(original, migration):
        return "already-enriched"

    enriched = enrich(original, migration)
    payload = BytesIO()
    write_parquet(enriched, payload)
    contents = payload.getvalue()

    # Verify the exact bytes that will be sent, rather than downloading them
    # again after S3 has accepted a checksum-validated atomic PUT.
    encoded = pd.read_parquet(BytesIO(contents), engine="pyarrow")
    _validate_frames(original, encoded, migration)
    checksum = base64.b64encode(hashlib.sha256(contents).digest()).decode("ascii")
    client.put_object(
        Bucket=bucket,
        Key=migration.destination,
        Body=contents,
        ContentType="application/vnd.apache.parquet",
        ChecksumSHA256=checksum,
    )
    return "migrated"


def run_all(
    operation: Callable[[Any, str, Migration], str], client: Any, bucket: str,
    migrations: list[Migration], workers: int,
) -> Iterator[str]:
    """Run with a bounded future queue so inventories do not double memory use."""
    destinations: set[str] = set()
    for item in migrations:
        if item.destination in destinations:
            raise ValueError(
                f"multiple sources target {item.destination}; "
                "run --layout canonical first, then --layout legacy"
            )
        destinations.add(item.destination)
    migration_iter = iter(migrations)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = set()
        try:
            for _ in range(workers * 2):
                migration = next(migration_iter, None)
                if migration is None:
                    break
                pending.add(pool.submit(operation, client, bucket, migration))
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                # Check the entire batch before yielding or replenishing work.
                results = [future.result() for future in done]
                yield from results
                for _ in results:
                    migration = next(migration_iter, None)
                    if migration is not None:
                        pending.add(pool.submit(operation, client, bucket, migration))
        except BaseException:
            for future in pending:
                future.cancel()
            raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", help="default: configured S3 bucket")
    parser.add_argument("--exchange", action="append", help="limit exchange; repeatable")
    parser.add_argument("--data-type", choices=("booktop", "trades"))
    parser.add_argument("--instrument", action="append", help="limit exact instrument; repeatable")
    parser.add_argument(
        "--layout", choices=("canonical", "legacy", "both"), default="both",
        help="layout to scan; canonical is fastest for the current date-first dataset",
    )
    parser.add_argument("--start", help="first date, inclusive (YYYY-MM-DD)")
    parser.add_argument("--end", help="last date, exclusive (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=4)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--fast-migrate", action="store_true",
        help="canonical only: local validation plus one checksummed atomic PUT",
    )
    mode.add_argument(
        "--migrate", action="store_true",
        help="preserve, enrich, validate, and retire each object",
    )
    mode.add_argument("--apply", action="store_true", help="write and validate; retain sources")
    mode.add_argument("--validate", action="store_true", help="validate destinations only")
    mode.add_argument("--retire", action="store_true", help="validate, then delete legacy sources")
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be positive")
    if args.start and args.end and args.start >= args.end:
        parser.error("--start must be before --end")

    import boto3

    config = load_config()
    bucket = args.bucket or config["S3_BUCKET"]
    client = boto3.client("s3")
    exchanges = sorted({value.upper() for value in args.exchange or []})
    if not exchanges:
        exchanges = discover_exchanges(client, bucket)
    data_types = [args.data_type] if args.data_type else ["booktop", "trades"]
    instruments = set(args.instrument or [])
    prefixes = discovery_prefixes(
        exchanges, data_types, instruments, args.start, args.end, args.layout,
    )
    migrations = [
        item for item in discover(client, bucket, prefixes)
        if (not args.start or item.day >= args.start)
        and (not args.end or item.day < args.end)
        and (not instruments or item.instrument in instruments)
        and (args.layout == "both" or item.legacy == (args.layout == "legacy"))
    ]

    total_bytes = sum(item.size for item in migrations)
    print(
        f"Found {len(migrations):,} prediction object(s) "
        f"({total_bytes / 1024 ** 3:.2f} GiB) in s3://{bucket}"
    )
    scopes: dict[tuple[str, str], int] = {}
    for item in migrations:
        scope = (item.exchange, item.data_type)
        scopes[scope] = scopes.get(scope, 0) + 1
    for (exchange, data_type), count in sorted(scopes.items()):
        print(f"  {exchange}/{data_type}: {count:,}")
    legacy_count = sum(item.legacy for item in migrations)
    print(
        f"  layouts: legacy={legacy_count:,} canonical={len(migrations) - legacy_count:,}"
    )

    if not any((args.fast_migrate, args.migrate, args.apply, args.validate, args.retire)):
        print("Plan only; use --fast-migrate for canonical bulk migration.")
        return

    if args.fast_migrate:
        if args.layout != "canonical":
            parser.error("--fast-migrate requires --layout canonical")
        operation, verb = fast_migrate_one, "Migrated"
    elif args.migrate:
        operation, verb = migrate_one, "Migrated"
    elif args.apply:
        operation, verb = apply_one, "Applied"
    elif args.validate:
        operation, verb = validate_one, "Validated"
    else:
        operation, verb = retire_one, "Retired"
    completed = 0
    started = time.monotonic()
    for completed, _ in enumerate(
        run_all(operation, client, bucket, migrations, args.workers), start=1,
    ):
        if completed % 100 == 0 or completed == len(migrations):
            elapsed = time.monotonic() - started
            rate = completed / elapsed if elapsed else 0.0
            remaining = (len(migrations) - completed) / rate if rate else 0.0
            print(
                f"{verb} {completed:,}/{len(migrations):,} "
                f"rate={rate:.1f}/s eta={remaining / 60:.1f}m",
                flush=True,
            )
    print(f"DONE {verb.lower()}={completed:,}")


if __name__ == "__main__":
    main()
