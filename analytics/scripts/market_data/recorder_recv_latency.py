#!/usr/bin/env python3
"""
recorder_recv_latency.py — compare receive-timestamp latency of shared feeds across recorders.

For each exchange recorded on two or more recorders, computes recv_latency = local_recv_time - exchange_ts
per instrument straight off each recorder's InfluxDB host. It goes direct to each host on purpose: the
high-level read path routes an exchange to a single recorder (and would raise for the others) even when
those others physically record the same feed, so load_best_levels can't compare them. Lower / tighter
median recv latency = the more accurate receive timestamp on that box.

Usage (JST_ROOT must be set):
    python analytics/scripts/recorder_recv_latency.py
    python analytics/scripts/recorder_recv_latency.py --recorders TY03 TY04 --exchanges OKX HYPERLIQUID
    python analytics/scripts/recorder_recv_latency.py --data-type trades --minutes 60 --max-syms 20
    python analytics/scripts/recorder_recv_latency.py --csv /tmp/recv_latency.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "umm" / "src"))

import numpy as np
import pandas as pd

from umm.analytics.market_data import api as mdapi, fetchers as mdf
from umm.analytics.market_data._common import _MD_MEASUREMENTS
from umm.analytics.market_data.api import MarketData

DEFAULT_EXCHANGES = ["BNBFUT", "ONDO", "OKX", "HYPERLIQUID", "BNBSPOT"]  # recorded on both TY03 & TY04


def recv_latency_ms(conn_args: dict, measurement: str, exchange: str, symbol: str,
                    start: pd.Timestamp, end: pd.Timestamp) -> np.ndarray:
    """recv - exchange_ts (ms) samples for one feed, read straight off a recorder's Influx host."""
    out: list[float] = []
    for name in mdf._influx_names_for(exchange)[1]:  # every alias spelling of the exchange tag
        q = (f'SELECT "local_recv_time" FROM {measurement} '
             'WHERE time >= $s AND time < $e AND "exchange_id" = $x AND "instrument_id" = $i')
        params = {"s": mdf.to_rfc3339(start), "e": mdf.to_rfc3339(end), "x": name, "i": symbol}
        rs = mdf.query_influx_raw(conn_args, q, params, epoch="ns")  # {(measurement, tags): DataFrame indexed by ns time}
        for df in rs.values():
            if df is None or df.empty or "local_recv_time" not in df.columns:
                continue
            d = df[["local_recv_time"]].copy()
            d["recv"] = pd.to_numeric(d["local_recv_time"], errors="coerce")
            d = d.dropna(subset=["recv"])
            if d.empty:
                continue
            t = d.index.values.astype("int64")           # exchange ts, ns since epoch
            recv = d["recv"].astype("int64").to_numpy()  # recv ts, ns since epoch
            out.extend(((recv - t) / 1e6).tolist())      # → ms
    return np.asarray(out, dtype=float)


def compare(recorders: list[str], exchanges: list[str], data_type: str,
            minutes: int, max_syms: int) -> pd.DataFrame:
    """One row per shared (exchange, symbol): median/p95 recv latency on each recorder."""
    cfg = mdapi._load_config()
    end = pd.Timestamp.utcnow().replace(tzinfo=None)
    start = end - pd.Timedelta(minutes=minutes)
    measurement = _MD_MEASUREMENTS[data_type][0]
    conn = {r: MarketData._catalog_conn_args(r, cfg, timeout=120) for r in recorders}  # per-host, routes via RECORDER_HOSTS

    rows = []
    for exchange in exchanges:
        symbol_sets = [set(MarketData.available_symbols(exchange, data_type=data_type, recorder=r, source="influx"))
                       for r in recorders]
        shared = sorted(set.intersection(*symbol_sets)) if symbol_sets else []
        if not shared:
            print(f"  {exchange}: no symbols shared across {', '.join(recorders)}", file=sys.stderr)
            continue
        for symbol in shared[:max_syms]:
            lat = {r: recv_latency_ms(conn[r], measurement, exchange, symbol, start, end) for r in recorders}
            if any(lat[r].size == 0 for r in recorders):
                continue
            row = {"exchange": exchange, "symbol": symbol}
            for r in recorders:
                row[f"{r}_med_ms"] = round(float(np.median(lat[r])), 2)
                row[f"{r}_p95_ms"] = round(float(np.percentile(lat[r], 95)), 2)
                row[f"{r}_n"] = int(lat[r].size)
            row["tighter"] = min(recorders, key=lambda r: np.median(lat[r]))
            rows.append(row)
    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare recv-timestamp latency of shared feeds across recorders (InfluxDB).")
    parser.add_argument("--recorders", nargs="+", default=["TY03", "TY04"], help="Recorder aliases to compare (default: TY03 TY04).")
    parser.add_argument("--exchanges", nargs="+", default=DEFAULT_EXCHANGES, help="Exchanges to check (default: the TY03/TY04 overlap).")
    parser.add_argument("--data-type", default="booktop", choices=("booktop", "trades", "bookdepth"), help="Measurement to read (default: booktop).")
    parser.add_argument("--minutes", type=int, default=30, help="Lookback window in minutes (default: 30).")
    parser.add_argument("--max-syms", type=int, default=10, help="Max instruments per exchange (default: 10).")
    parser.add_argument("--csv", default=None, help="Optional path to also write the table as CSV.")
    args = parser.parse_args()

    res = compare(args.recorders, args.exchanges, args.data_type, args.minutes, args.max_syms)
    if res.empty:
        print("No overlapping feeds returned data in the window.")
        return 0

    res = res.sort_values(["exchange", "symbol"]).reset_index(drop=True)
    print(res.to_string(index=False))
    print("\nLower median recv-latency (per feed):")
    print(res["tighter"].value_counts().to_string())
    if args.csv:
        res.to_csv(args.csv, index=False)
        print(f"\nWrote {args.csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
