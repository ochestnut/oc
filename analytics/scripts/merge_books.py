#!/usr/bin/env python3
"""Merge venue-specific book history into canonical local cache paths.

The merge is idempotent and local-only; --delete-members moves source folders
to a recoverable backup. Use --help for book, data-type, and dry-run options.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "umm" / "src"))

from umm.analytics.trading_data._common import (
    merge_frames, all_members, canonical_book, _cache_path, partition_days, _atomic_write_parquet,
)
from umm.analytics.trading_data.fetchers import _load_env_data_root

ALL_TYPES = ("position", "fills", "pnl")


def _registry_pairs() -> dict[str, list[str]]:
    """{canonical: [member, ...]} for every venue-split book in the registry (e.g. {'JDXM': ['JDXM_BITSO'], ...})."""
    pairs: dict[str, list[str]] = {}
    for m in all_members():
        pairs.setdefault(canonical_book(m), []).append(m)
    return pairs


def _read_day(root: Path, book: str, data_type: str, date: str) -> pd.DataFrame | None:
    """Concat every parquet in one book/type/date partition, or None if the partition is absent/empty."""
    d = root / "trading_data" / book / data_type / date
    frames = [pd.read_parquet(p, engine="pyarrow") for p in sorted(d.glob("*.parquet"))] if d.exists() else []
    return pd.concat(frames, ignore_index=True) if frames else None


def _write_day(root: Path, canonical: str, data_type: str, date: str, df: pd.DataFrame) -> Path:
    """Atomically write the merged day to the canonical book's cache path."""
    path = _cache_path(root, canonical, pd.Timestamp(date), data_type)
    _atomic_write_parquet(df, path)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge venue-split member books into their canonical book (history migration).")
    parser.add_argument("--into", help="canonical book (with --from); default: every registry pair")
    parser.add_argument("--from", dest="members", nargs="*", help="member book(s) to merge into --into")
    parser.add_argument("--types", nargs="*", default=list(ALL_TYPES), choices=ALL_TYPES)
    parser.add_argument("--dry-run", action="store_true", help="show the plan, write nothing")
    parser.add_argument("--delete-members", action="store_true",
                        help="after merging, move each member dir to trading_data/_merged_backup/ (reversible)")
    args = parser.parse_args()

    root = _load_env_data_root()

    if args.into:
        if not args.members:
            parser.error("--into requires --from <member> [...]")
        pairs = {args.into.upper(): [m.upper() for m in args.members]}
    else:
        pairs = _registry_pairs()

    if not pairs:
        print("No venue-split books in the registry — nothing to merge.")
        return

    print(f"Data root: {root}")
    print("DRY RUN — no files written\n" if args.dry_run else "")
    total_written = 0

    for canonical, members in pairs.items():
        print(f"═══ {canonical}  ⇐  {', '.join(members)} ═══")
        for data_type in args.types:
            dates = set()
            for m in members:
                dates |= partition_days(root, m, data_type)
            if not dates:
                print(f"  {data_type:<9} no member data — skip")
                continue

            n_days = 0
            n_member_rows = 0
            n_skipped = 0
            for date in sorted(dates):
                frames = []
                existing = _read_day(root, canonical, data_type, date)   # canonical's own current rows (kept)
                if existing is not None:
                    frames.append(existing)
                for m in members:
                    mf = _read_day(root, m, data_type, date)
                    if mf is not None:
                        frames.append(mf)
                        n_member_rows += len(mf)
                if not frames:
                    continue
                # Old pnl files predate the exchange column (Phase 1); merging exchange-less pnl would let two venues of
                # the same coin collapse as duplicates. Skip those days — re-sweep pnl --force to label them first.
                if data_type == "pnl" and any("exchange" not in f.columns for f in frames):
                    n_skipped += 1
                    continue
                merged = merge_frames(data_type, frames)
                n_days += 1
                if not args.dry_run:
                    _write_day(root, canonical, data_type, date, merged)
                    total_written += 1
            verb = "would merge" if args.dry_run else "merged"
            skip_note = f"   ⚠ skipped {n_skipped} day(s) missing exchange — re-sweep pnl --force first" if n_skipped else ""
            print(f"  {data_type:<9} {verb} {n_member_rows:,} member rows across {n_days} day(s) → {canonical}/{data_type}/{skip_note}")

        if args.delete_members and not args.dry_run:
            backup = root / "_merged_backup"   # sibling of trading_data, so sync.sh --path trading_data never pushes it
            backup.mkdir(parents=True, exist_ok=True)
            for m in members:
                src = root / "trading_data" / m
                if src.exists():
                    dest = backup / m
                    if dest.exists():
                        print(f"  ⚠ {dest} already exists — leaving {src} in place")
                        continue
                    shutil.move(str(src), str(dest))
                    print(f"  moved {m}/ → _merged_backup/{m}/")
        print()

    if args.dry_run:
        print("Dry run complete. Re-run without --dry-run to write the merges.")
    else:
        print(f"Done — wrote {total_written} merged day-files.\n")
        print("Next steps:")
        print("  1. Push the merged canonical dirs to S3:")
        print("       ./scripts/influx_to_s3/sync.sh --from-earliest --path trading_data")
        print("  2. Remove the now-stale member prefixes from S3 (verify first), e.g.:")
        for _canon, members in pairs.items():
            for m in members:
                print(f"       aws s3 rm --recursive s3://jstdata/trading_data/{m}/")
    sys.exit(0)


if __name__ == "__main__":
    main()
