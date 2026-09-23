#!/usr/bin/env python3
"""compare production and direct booktop archives without changing source objects."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import json
from pathlib import Path
from typing import Callable

import pandas as pd

from umm.analytics.market_data._common import parse_chunk_name
from umm.analytics.market_data.recorders import recorder_id
from umm.tools.s3.client import s3_client

VALUES = ["bid_price", "ask_price", "bid_qty", "ask_qty"]
CORE = ["timestamp", *VALUES]
Window = tuple[str, str, str, pd.Timestamp]  # exchange, symbol, production id, start


def inventory(keys: list[str], start: pd.Timestamp, end: pd.Timestamp,
              recorders: set[str]) -> dict[Window, dict[str, list[str]]]:
    """Pair suffix-D ids with their exact production id, retaining ambiguous keys."""
    result: dict[Window, dict[str, list[str]]] = {}
    for key in sorted(keys):
        parts = key.split("/")
        index = next((i + 2 for i, part in enumerate(parts[:-2])
                      if part == "market_data" and parts[i + 2] == "booktop"), None)
        if index is None:
            continue
        info = parse_chunk_name(parts[-1])
        if not info or not start <= info.chunk_start < end:
            continue
        if index < 1 or len(parts) <= index + 3:
            continue
        # Prediction layout: booktop/predictions/date/symbol/file.
        symbol = parts[-2] if parts[index + 1] == "predictions" else parts[index + 1]
        direct = info.recorder_id.endswith("D")
        production = info.recorder_id[:-1] if direct else info.recorder_id
        if recorders and production not in recorders:
            continue
        window = (parts[index - 1], symbol, production, info.chunk_start)
        result.setdefault(window, {"production": [], "direct": []})[
            "direct" if direct else "production"].append(key)
    return result


def _canonical(frame: pd.DataFrame, window: pd.Timestamp) -> pd.DataFrame:
    missing = set(CORE) - set(frame.columns)
    if missing:
        raise ValueError(f"missing required columns: {sorted(missing)}")
    if frame.empty:
        raise ValueError("empty parquet is not usable coverage")
    out = frame[CORE + (["recv"] if "recv" in frame else [])].copy()
    for column in ("timestamp", "recv"):
        if column in out:
            out[column] = pd.to_datetime(out[column], utc=True, errors="raise").dt.tz_localize(None).astype("datetime64[ns]")
    if out.timestamp.isna().any() or not (
        (out.timestamp >= window) & (out.timestamp < window + pd.Timedelta(minutes=15))
    ).all():
        raise ValueError("null or out-of-window exchange timestamps")
    for column in VALUES:
        out[column] = pd.to_numeric(out[column], errors="raise").astype("float64")
    if out[VALUES].isin([float("inf"), float("-inf")]).any().any():
        raise ValueError("nonfinite prices or quantities")
    if out[["bid_price", "ask_price"]].isna().any().any():
        raise ValueError("null booktop prices")
    return out.sort_values(list(out.columns), kind="stable").reset_index(drop=True)


def _changes(frame: pd.DataFrame) -> pd.DataFrame:
    # Keep timestamp ties visible; never pick an arbitrary winner at one timestamp.
    out = frame[CORE].drop_duplicates().sort_values(CORE, kind="stable")
    previous = out[VALUES].shift()
    same = (out[VALUES].eq(previous) | (out[VALUES].isna() & previous.isna())).all(axis=1)
    return out.loc[~same].reset_index(drop=True)


def _difference(left: pd.DataFrame, right: pd.DataFrame) -> tuple[int, int, list[dict]]:
    columns = list(left.columns)
    # Group counts preserve duplicate multiplicity, unlike a set comparison.
    a = left.groupby(columns, dropna=False).size().rename("production_count")
    b = right.groupby(columns, dropna=False).size().rename("direct_count")
    diff = pd.concat([a, b], axis=1).fillna(0).astype(int)
    delta = diff.production_count - diff.direct_count
    samples = diff.loc[delta.ne(0)].reset_index().head(5)
    return int(delta.clip(lower=0).sum()), int((-delta).clip(lower=0).sum()), json.loads(
        samples.to_json(orient="records", date_format="iso", date_unit="ns"))


def compare(window: Window, files: dict[str, list[str]], read: Callable[[str], pd.DataFrame]) -> dict:
    exchange, symbol, recorder, start = window
    row = dict(exchange=exchange, symbol=symbol, recorder=recorder, window=str(start),
               production_keys=files["production"], direct_keys=files["direct"])
    if any(len(keys) > 1 for keys in files.values()):
        return dict(row, status="error", error="multiple objects for the same recorder/feed/window")
    if not files["direct"]:
        return dict(row, status="missing_direct")
    if not files["production"]:
        return dict(row, status="direct_only")
    try:
        production = _canonical(read(files["production"][0]), start)
        direct = _canonical(read(files["direct"][0]), start)
        same_schema = list(production.columns) == list(direct.columns)
        common = [column for column in production if column in direct]
        missing, extra, samples = _difference(production[common], direct[common])
        equal = same_schema and missing == 0 and extra == 0
        return dict(row, status="equal" if equal else "different",
                    production_rows=len(production), direct_rows=len(direct),
                    production_columns=list(production), direct_columns=list(direct),
                    records_equal=equal, state_changes_equal=_changes(production).equals(_changes(direct)),
                    missing_records=missing, extra_records=extra, difference_samples=samples)
    except Exception as exc:
        return dict(row, status="error", error=f"{type(exc).__name__}: {exc}")


def s3_keys(fs, bucket: str, start: pd.Timestamp, end: pd.Timestamp, workers: int = 4) -> list[str]:
    """List only requested day partitions, across all booktop exchanges/symbols."""
    def dirs(prefix: str) -> list[str]:
        try:
            return [item["name"].rstrip("/") for item in fs.ls(prefix, detail=True)
                    if item["type"] == "directory"]
        except FileNotFoundError:
            return []

    days = pd.date_range(start.normalize(), (end - pd.Timedelta(nanoseconds=1)).normalize())
    prefixes: list[str] = []
    for exchange in dirs(f"{bucket}/market_data"):
        symbols = dirs(f"{exchange}/booktop")
        prefixes.extend(f"{symbol}/{day:%Y-%m-%d}" for symbol in symbols for day in days)
        print(f"discovered {exchange.rsplit('/', 1)[-1]}: {len(symbols)} booktop directories", flush=True)

    def list_partition(prefix: str) -> list[str]:
        try:
            return [key for key in fs.find(prefix) if key.endswith(".parquet")]
        except FileNotFoundError:
            return []

    keys: list[str] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = {pool.submit(list_partition, prefix) for prefix in prefixes}
        completed = 0
        while pending:
            done, pending = wait(pending, timeout=10, return_when=FIRST_COMPLETED)
            for future in done:
                keys.extend(future.result())
                completed += 1
            if not done or completed % 100 == 0 or not pending:
                print(f"listed {completed}/{len(prefixes)} partitions; {len(keys)} parquet objects", flush=True)
    return keys


def audit(windows: dict[Window, dict[str, list[str]]], read: Callable[[str], pd.DataFrame],
          output: Path, workers: int) -> tuple[dict, int]:
    output.mkdir(parents=True, exist_ok=True)
    counts: Counter = Counter()
    feeds: dict[tuple[str, str, str], Counter] = {}
    state_matches = 0
    ordered = sorted(windows)
    with (output / "windows.jsonl").open("w") as report, ThreadPoolExecutor(max_workers=workers) as pool:
        # executor.map is ordered; at most workers parquet pairs are decoded concurrently.
        for number, row in enumerate(pool.map(lambda key: compare(key, windows[key], read), ordered), 1):
            report.write(json.dumps(row) + "\n")
            counts[row["status"]] += 1
            feeds.setdefault((row["exchange"], row["symbol"], row["recorder"]), Counter())[row["status"]] += 1
            state_matches += bool(row.get("state_changes_equal"))
            if number % 100 == 0:
                print(f"checked {number}/{len(windows)} windows", flush=True)
    baseline = sum(bool(files["production"]) for files in windows.values())
    missing = sum(bool(files["production"]) and not files["direct"] for files in windows.values())
    summary = dict(completed=True, production_windows=baseline, missing_direct_windows=missing,
                   object_coverage_complete=baseline > 0 and missing == 0,
                   exact_match=baseline > 0 and counts["equal"] == len(windows),
                   state_change_matches=state_matches, statuses=dict(counts),
                   feeds=[dict(exchange=key[0], symbol=key[1], recorder=key[2], statuses=dict(value))
                          for key, value in sorted(feeds.items())])
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    exit_code = 2 if not baseline or counts["error"] else 0 if summary["exact_match"] else 1
    return summary, exit_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="inclusive utc 15-minute boundary")
    parser.add_argument("--end", required=True, help="exclusive utc 15-minute boundary; closed windows only")
    parser.add_argument("--bucket", default="jstdata")
    parser.add_argument("--local-root", type=Path, help="offline archive root containing market_data/")
    parser.add_argument("--recorder", action="append", help="production alias/full id; repeat to limit scope; default all")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--inventory", type=Path, help="reuse a saved listing snapshot; does not discover new objects")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        start, end = (pd.Timestamp(value).tz_localize("UTC") if pd.Timestamp(value).tzinfo is None
                      else pd.Timestamp(value).tz_convert("UTC") for value in (args.start, args.end))
        start, end = start.tz_localize(None), end.tz_localize(None)
        if start >= end or start != start.floor("15min") or end != end.floor("15min"):
            raise ValueError("start/end must be ordered 15-minute boundaries")
        closed = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(seconds=60)).tz_localize(None).floor("15min")
        if end > closed:
            raise ValueError("end must allow at least 60 seconds after the final window closes")
        if args.workers < 1:
            raise ValueError("workers must be positive")
        selected = {recorder_id(value) for value in args.recorder or []}
        if any(value == "any" or value.endswith("D") for value in selected):
            raise ValueError("--recorder takes production ids, without the D suffix")
    except (ValueError, TypeError) as exc:
        parser.error(str(exc))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    # A failed or interrupted rerun must not leave a previous success as its summary.
    (args.output_dir / "summary.json").write_text(json.dumps(dict(completed=False, start=str(start), end=str(end))) + "\n")
    print(f"listing booktop archives: {start} to {end} utc", flush=True)
    scope = dict(start=str(start), end=str(end), bucket=None if args.local_root else args.bucket,
                 local_root=str(args.local_root.resolve()) if args.local_root else None)
    try:
        saved = json.loads(args.inventory.read_text()) if args.inventory else None
        if saved is not None and saved["scope"] != scope:
            raise ValueError("saved inventory scope does not match this run")
        if args.local_root:
            keys = saved["keys"] if saved is not None else [str(path) for path in (args.local_root / "market_data").rglob("*.parquet")]
            def read(key: str) -> pd.DataFrame:
                path = Path(key)
                before = path.stat()
                frame = pd.read_parquet(path)
                after = path.stat()
                if (before.st_mtime_ns, before.st_size) != (after.st_mtime_ns, after.st_size):
                    raise RuntimeError("object changed during read; rerun the check")
                return frame
        else:
            fs = s3_client(creds_file_fallback=True)
            keys = saved["keys"] if saved is not None else s3_keys(fs, args.bucket, start, end, args.workers)
            def read(key: str) -> pd.DataFrame:
                fs.invalidate_cache(key)
                before = fs.info(key)
                with fs.open(key, "rb") as stream:
                    frame = pd.read_parquet(stream)
                fs.invalidate_cache(key)
                after = fs.info(key)
                if any(before.get(field) != after.get(field) for field in ("ETag", "size", "LastModified")):
                    raise RuntimeError("object changed during read; rerun the check")
                return frame
    except Exception as exc:
        failure = dict(completed=False, error=f"{type(exc).__name__}: {exc}", start=str(start), end=str(end))
        (args.output_dir / "summary.json").write_text(json.dumps(failure, indent=2) + "\n")
        print(json.dumps(failure))
        return 2
    (args.output_dir / "inventory.json").write_text(json.dumps(dict(scope=scope, keys=keys)) + "\n")
    windows = inventory(keys, start, end, selected)
    present = sum(bool(files["production"]) for files in windows.values())
    absent = sum(bool(files["production"]) and not files["direct"] for files in windows.values())
    print(f"inventory: {len(windows)} windows; {present} production; {absent} missing direct", flush=True)
    summary, exit_code = audit(windows, read, args.output_dir, args.workers)
    summary.update(start=str(start), end=str(end), bucket=None if args.local_root else args.bucket,
                   local_root=str(args.local_root) if args.local_root else None, recorders=sorted(selected))
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({key: value for key, value in summary.items() if key != "feeds"}, indent=2))
    print(f"reports: {args.output_dir.resolve()}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
