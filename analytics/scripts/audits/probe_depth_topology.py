#!/usr/bin/env python3
"""
Read-only probe of market-data recording topology across every recorder node.

For each recorder in RECORDER_REGISTRY, connects to that node's InfluxDB and asks
which exchanges have each measurement (booktop, trades, bookdepth). Nothing is
written — the only queries issued are SHOW TAG VALUES catalog reads.

- answers "which recorders hold md_bookdepth for which exchanges" against reality,
  rather than inferring it from BOOK_RECORDER_MAP / EXCHANGE_RECORDER_MAP
- also probes booktop/trades so the depth gap (exchanges with booktop but no depth)
  is visible in the same table
- a node that is unreachable is reported as such and skipped, never fatal

Usage:
    python probe_depth_topology.py [--out /path/to/topology.csv]
"""

import sys as _path_sys
from pathlib import Path as _Path
_path_sys.path.insert(0, str(_Path(__file__).resolve().parents[3] / "analytics/scripts"))
from analytics_paths import output_path

import argparse
import re
import sys
from collections import defaultdict

import pandas as pd

from umm.analytics.config import load_config
from umm.analytics.storage.influx import query_influx_raw
from umm.analytics.market_data import MarketData
from umm.analytics.market_data._common import RECORDER_REGISTRY, EXCHANGE_ALIAS_MAP

# measurement label → InfluxDB measurement name (bookdepth included directly so the
# probe does not depend on _MD_MEASUREMENTS being updated first).
MEASUREMENTS: dict[str, str] = {
    "booktop":   "md_booktop",
    "trades":    "md_trades",
    "bookdepth": "md_bookdepth",
}


def _exchanges_on_node(conn_args: dict, measurement: str) -> set[str]:
    """Canonical exchange ids present for one measurement on one node (aliases collapsed)."""
    q = f'SHOW TAG VALUES FROM {measurement} WITH KEY = "exchange_id"'
    rs = query_influx_raw(conn_args, q)
    return {EXCHANGE_ALIAS_MAP.get(p["value"], p["value"]) for p in rs.get_points()}


def _measurements_on_node(conn_args: dict) -> list[str]:
    """Every measurement name the node reports (raw SHOW MEASUREMENTS)."""
    rs = query_influx_raw(conn_args, "SHOW MEASUREMENTS")
    return sorted(p["name"] for p in rs.get_points())


def _tag_keys_on_node(conn_args: dict, measurement: str) -> list[str]:
    """Tag keys for a measurement (empty if the measurement is absent)."""
    rs = query_influx_raw(conn_args, f"SHOW TAG KEYS FROM {measurement}")
    return sorted(p["tagKey"] for p in rs.get_points())


def diagnose() -> None:
    """Per-node ground-truth dump to expose silent gaps the name-based probe can miss.
    - lists every measurement whose name hints at book/depth/trade, so depth recorded
      under a different name than md_bookdepth is not missed
    - shows md_bookdepth's actual tag keys, so a depth feed tagged by something other
      than exchange_id is not misread as "no depth"
    - reachability is explicit per node; nothing is swallowed
    """
    cfg = load_config()
    hint = re.compile(r"book|depth|trade", re.IGNORECASE)
    for recorder in sorted(RECORDER_REGISTRY):
        conn_args = MarketData._catalog_conn_args(recorder, cfg, timeout=30)
        print(f"\n=== {recorder} ({conn_args['host']}) ===")
        try:
            measurements = _measurements_on_node(conn_args)
        except Exception as exc:
            print(f"  UNREACHABLE: {exc}")
            continue
        matching = [m for m in measurements if hint.search(m)]
        print(f"  total measurements: {len(measurements)}")
        print(f"  book/depth/trade-like: {matching}")
        if "md_bookdepth" in measurements:
            print(f"  md_bookdepth tag keys: {_tag_keys_on_node(conn_args, 'md_bookdepth')}")
        else:
            print("  md_bookdepth: NOT PRESENT on this node")


def probe() -> pd.DataFrame:
    """Long-format presence table: one row per (exchange, data_type, recorder) found.
    - iterates every recorder in the registry and every measurement
    - a per-node failure is printed to stderr and the node is skipped
    """
    cfg = load_config()
    rows: list[dict] = []
    for recorder in sorted(RECORDER_REGISTRY):
        conn_args = MarketData._catalog_conn_args(recorder, cfg, timeout=30)
        for data_type, measurement in MEASUREMENTS.items():
            try:
                exchanges = _exchanges_on_node(conn_args, measurement)
            except Exception as exc:
                print(f"[skip] {recorder} / {measurement}: {exc}", file=sys.stderr)
                continue
            for exchange in exchanges:
                rows.append({"exchange": exchange, "data_type": data_type, "recorder": recorder})
    return pd.DataFrame(rows, columns=["exchange", "data_type", "recorder"])


def summarize(df: pd.DataFrame) -> None:
    """Print a per-data-type exchange→recorders map and flag the depth gap."""
    if df.empty:
        print("No data returned from any recorder (all nodes unreachable?).")
        return

    for data_type in MEASUREMENTS:
        sub = df[df["data_type"] == data_type]
        by_exchange: dict[str, list[str]] = defaultdict(list)
        for _, r in sub.iterrows():
            by_exchange[r["exchange"]].append(r["recorder"])
        print(f"\n=== {data_type} ({sub['exchange'].nunique()} exchanges) ===")
        for exchange in sorted(by_exchange):
            print(f"  {exchange:<40} {', '.join(sorted(by_exchange[exchange]))}")

    has_booktop = set(df[df["data_type"] == "booktop"]["exchange"])
    has_depth = set(df[df["data_type"] == "bookdepth"]["exchange"])
    gap = sorted(has_booktop - has_depth)
    print(f"\n=== depth gap: booktop present but NO bookdepth ({len(gap)}) ===")
    for exchange in gap:
        print(f"  {exchange}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=str, default=str(output_path("depth_topology.csv", "audits")), help="CSV output path for the long presence table")
    parser.add_argument("--diagnose", action="store_true", help="Dump per-node measurements + md_bookdepth tag keys, then exit")
    args = parser.parse_args()

    if args.diagnose:
        diagnose()
        return

    df = probe()
    summarize(df)
    df.to_csv(args.out, index=False)
    print(f"\nWrote {len(df)} presence rows to {args.out}")


if __name__ == "__main__":
    main()
