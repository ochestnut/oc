#!/usr/bin/env python3
"""migrate legacy funding parquet symbols into the typed expected catalog."""

from __future__ import annotations

import argparse
from datetime import date
import re

from umm.analytics.config import load_config
from umm.analytics.storage.catalog import Catalogs, market_scope
from umm.operations.s3_catalog import write_catalogs
from umm.tools.s3.client import s3_client
from umm.tools.s3.io import list_dirs, list_parquet_keys, read_parquet_key


DATE_PART = re.compile(r"\d{4}-\d{2}-\d{2}")


def _valid_day(value: str) -> bool:
    if DATE_PART.fullmatch(value) is None:
        return False
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def discover_legacy_expected(bucket: str) -> tuple[Catalogs, set[str]]:
    catalogs: Catalogs = {}
    exchanges: set[str] = set()
    for exchange in list_dirs(bucket, "market_data"):
        prefix = f"market_data/{exchange}/funding_rates"
        legacy_days = [day for day in list_dirs(bucket, prefix) if _valid_day(day)]
        if not legacy_days:
            continue
        exchanges.add(exchange)
        scope = market_scope(exchange, "funding_rates", funding_type="expected")
        symbols = catalogs.setdefault(scope, {})
        for day in legacy_days:
            for key in list_parquet_keys(bucket, f"{prefix}/{day}"):
                frame = read_parquet_key(key, columns=["symbol"])
                for symbol in frame["symbol"].dropna().unique():
                    name = str(symbol)
                    if not name:
                        continue
                    first, last = symbols.get(name, (None, None))
                    symbols[name] = (
                        day if first is None else min(first, day),
                        day if last is None else max(last, day),
                    )
    return catalogs, exchanges


def discover_legacy_catalog_keys(bucket: str) -> list[str]:
    """find combined funding catalogs superseded by typed catalogs."""
    filesystem = s3_client()
    legacy_pattern = (
        f"{bucket}/market_data/catalog/symbols/*/funding_rates/standard.parquet"
    )
    keys: list[str] = []
    for key in sorted(filesystem.glob(legacy_pattern)):
        exchange = key.split("/")[-3]
        typed_prefix = f"{bucket}/market_data/catalog/symbols/{exchange}/funding_rates"
        if filesystem.exists(f"{typed_prefix}/expected/standard.parquet") or filesystem.exists(
            f"{typed_prefix}/realized/standard.parquet"
        ):
            keys.append(key)
    return keys


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", help="default: configured s3 bucket")
    parser.add_argument("--apply", action="store_true", help="write the expected catalog shards")
    parser.add_argument(
        "--delete-legacy-catalog", action="store_true",
        help="plan combined catalog deletion; delete them only with --apply",
    )
    args = parser.parse_args()

    cfg = load_config()
    bucket = args.bucket or cfg["S3_BUCKET"]
    catalogs, _ = discover_legacy_expected(bucket)
    legacy_catalog_keys = discover_legacy_catalog_keys(bucket)
    total = sum(len(symbols) for symbols in catalogs.values())
    print(f"[funding_catalog_migration] discovered symbols={total:,} shards={len(catalogs):,}")
    for scope, symbols in sorted(catalogs.items()):
        print(f"  {scope.key}: {len(symbols):,} symbols")
    if args.delete_legacy_catalog:
        for key in legacy_catalog_keys:
            print(f"  legacy catalog: s3://{key}")
    if not args.apply:
        print("[funding_catalog_migration] plan only; rerun with --apply to write catalogs.")
        return

    stats = write_catalogs(bucket, catalogs)
    print(
        f"[funding_catalog_migration] wrote={stats.written:,} "
        f"unchanged={stats.unchanged:,}"
    )
    if args.delete_legacy_catalog:
        filesystem = s3_client()
        for key in legacy_catalog_keys:
            filesystem.rm(key)
            print(f"[funding_catalog_migration] deleted s3://{key}")


if __name__ == "__main__":
    main()
