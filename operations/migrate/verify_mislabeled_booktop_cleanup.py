#!/usr/bin/env python3
"""Read-only verification of the completed TY04 output-name cleanup."""

from __future__ import annotations

import argparse
import importlib.util
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys

import pandas as pd


_JST_ROOT = Path(__file__).resolve().parents[3]
_UMM_SRC = _JST_ROOT / "umm" / "src"
if str(_UMM_SRC) not in sys.path:
    sys.path.insert(0, str(_UMM_SRC))

from umm.analytics.config import load_config
from umm.tools.s3.client import s3_client
from umm.tools.s3.io import read_parquet_key


MIGRATION_SCRIPT = Path(__file__).with_name("migrate_mislabeled_booktop.py")
SPEC = importlib.util.spec_from_file_location("mislabeled_migration", MIGRATION_SCRIPT)
assert SPEC and SPEC.loader
MIGRATION = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MIGRATION
SPEC.loader.exec_module(MIGRATION)

TOLERANCE_BY_TARGET = {
    mapping.target: mapping.normalized_destination_max_bps
    for mapping in MIGRATION.MIGRATION_PLANS["historical-ty04-output-names"]
}


def _source_backup(bucket: str, source_key: str) -> str:
    relative = source_key.removeprefix(f"{bucket}/")
    return f"{bucket}/migration_backups/historical_ty04_output_names/sources/{relative}"


def _even_sample(frame: pd.DataFrame, count: int) -> pd.DataFrame:
    if count == 0 or len(frame) <= count:
        return frame
    if count == 1:
        return frame.iloc[[len(frame) // 2]]
    indexes = sorted({round(i * (len(frame) - 1) / (count - 1)) for i in range(count)})
    return frame.iloc[indexes]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cleanup-report", type=Path, required=True)
    parser.add_argument("--samples-per-mapping", type=int, default=3)
    parser.add_argument("--max-workers", type=int, default=64)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.samples_per_mapping < 0 or args.max_workers < 1:
        parser.error("sample count must be nonnegative and workers positive")

    bucket = load_config()["S3_BUCKET"]
    fs = s3_client()
    cleanup = pd.read_csv(args.cleanup_report)
    required = {"status", "source_key", "destination_key", "source_instrument", "target_instrument"}
    missing = required.difference(cleanup.columns)
    if missing:
        parser.error(f"cleanup report missing columns: {sorted(missing)}")

    def inspect(row: object) -> dict[str, object]:
        record = row._asdict()  # type: ignore[attr-defined]
        source = record["source_key"]
        destination = record["destination_key"]
        backup = _source_backup(bucket, source)
        source_exists = fs.exists(source)
        destination_exists = fs.exists(destination)
        backup_exists = fs.exists(backup)
        status = record["status"]
        if status == "MOVED":
            ok = not source_exists and destination_exists and backup_exists
        elif status == "SKIP_RATIO_MISMATCH":
            ok = source_exists
        elif status == "ERROR_DESTINATION_CONFLICT":
            # These are the two separately reviewed malformed sources. A final
            # cleanup is valid only after they are absent, backed up, and their
            # repaired destinations remain present.
            ok = not source_exists and destination_exists and backup_exists
        else:
            ok = False
        return {
            **record,
            "source_exists": source_exists,
            "destination_exists": destination_exists,
            "source_backup_exists": backup_exists,
            "layout_status": "OK" if ok else "ERROR_LAYOUT",
        }

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        rows = list(executor.map(inspect, cleanup.itertuples(index=False)))
    audit = pd.DataFrame(rows)

    content_rows = []
    moved = audit[audit.status.eq("MOVED")]
    for (_, target), group in moved.groupby(["source_instrument", "target_instrument"]):
        for row in _even_sample(group.sort_values("chunk_start"), args.samples_per_mapping).itertuples(index=False):
            backup = _source_backup(bucket, row.source_key)
            try:
                source = read_parquet_key(backup, columns=MIGRATION._BOOKTOP_VALIDATION_COLUMNS)
                destination = read_parquet_key(row.destination_key, columns=MIGRATION._BOOKTOP_VALIDATION_COLUMNS)
                relationship, _ = MIGRATION._destination_relationship(
                    source, destination, TOLERANCE_BY_TARGET[row.target_instrument]
                )
                ok = relationship in {
                    "ALREADY_EQUIVALENT", "DESTINATION_SUPERSET",
                    "DESTINATION_NORMALIZED_EQUIVALENT",
                }
                content_rows.append({"source_instrument": row.source_instrument,
                                     "target_instrument": row.target_instrument,
                                     "chunk_start": row.chunk_start,
                                     "relationship": relationship,
                                     "content_status": "OK" if ok else "ERROR_CONTENT"})
            except Exception as exc:
                content_rows.append({"source_instrument": row.source_instrument,
                                     "target_instrument": row.target_instrument,
                                     "chunk_start": row.chunk_start,
                                     "relationship": f"{type(exc).__name__}: {exc}",
                                     "content_status": "ERROR_CONTENT"})

    content = pd.DataFrame(content_rows)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    audit.to_csv(args.report, index=False)
    content_path = args.report.with_name(f"{args.report.stem}.content-samples.csv")
    content.to_csv(content_path, index=False)
    print("Layout verification")
    print(audit.groupby(["status", "layout_status"]).size().to_string())
    print("\nContent samples")
    print(content.groupby(["content_status", "relationship"]).size().to_string())
    print(f"\nWrote: {args.report}")
    print(f"Wrote: {content_path}")
    return 1 if audit.layout_status.eq("ERROR_LAYOUT").any() or content.content_status.eq("ERROR_CONTENT").any() else 0


if __name__ == "__main__":
    raise SystemExit(main())
