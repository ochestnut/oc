#!/usr/bin/env python3
"""Detect unsafe TY04 backfills into TY03 booktop data.

The diagnostic compares TY03, TY04, and recorder="any" in fixed time chunks.
It first estimates the TY04/TY03 price ratio from chunks where both recorders
have data.  It then identifies TY03 gaps that are present in TY04 and checks
whether the merged ("any") result appears to have used TY04 for the gap.

Example:
    python analytics/scripts/audits/check_cross_recorder_booktop.py \
        --start '2026-07-08 12:00' --end '2026-07-08 18:00' \
        --symbols PERP-XAU-USDT
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd


_JST_ROOT = Path(os.environ.get("JST_ROOT") or Path(__file__).resolve().parents[4])
_UMM_SRC = next(
    (path for path in (_JST_ROOT / "umm" / "src", _JST_ROOT / "src") if path.exists()),
    _JST_ROOT / "umm" / "src",
)
if str(_UMM_SRC) not in sys.path:
    sys.path.insert(0, str(_UMM_SRC))

from umm.analytics.market_data.api import MarketData


DEFAULT_SYMBOLS = (
    "PERP-XAU-USDT",
    "PERP-XAG-USDT",
    "PERP-SPY-USDT",
    "PERP-QQQ-USDT",
    "PERP-CL-USDT",
)


def _load(exchange: str, symbol: str, start: str, end: str, recorder: str, force: bool) -> pd.DataFrame:
    try:
        return MarketData.load_best_levels(
            exchange=exchange,
            symbol=symbol,
            start=start,
            end=end,
            recorder=recorder,
            source="s3",
            stats=True,
            silent=True,
            force=force,
        )
    except ValueError:
        return pd.DataFrame()


def _chunk_summary(frame: pd.DataFrame, chunk: str, label: str) -> pd.DataFrame:
    columns = [f"{label}_rows", f"{label}_mid"]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    data = frame.copy()
    if "mid_price" not in data:
        data["mid_price"] = (data["bid_price"] + data["ask_price"]) / 2

    timestamps = pd.to_datetime(data.index, utc=True, errors="coerce")
    valid = ~timestamps.isna()
    data = data.loc[valid].copy()
    data["chunk_start"] = timestamps[valid].floor(chunk)

    return data.groupby("chunk_start").agg(
        **{
            f"{label}_rows": ("mid_price", "size"),
            f"{label}_mid": ("mid_price", "median"),
        }
    )


def inspect_symbol(
    *,
    exchange: str,
    symbol: str,
    start: str,
    end: str,
    chunk: str = "15min",
    divergence: float = 0.05,
    force: bool = False,
) -> tuple[pd.DataFrame, dict[str, object]]:
    frames = {
        recorder: _load(exchange, symbol, start, end, recorder, force)
        for recorder in ("TY03", "TY04", "any")
    }
    table = pd.concat(
        [_chunk_summary(frames[name], chunk, name.lower()) for name in frames],
        axis=1,
    ).sort_index()

    for recorder in ("ty03", "ty04", "any"):
        rows = f"{recorder}_rows"
        if rows not in table:
            table[rows] = 0
        table[rows] = table[rows].fillna(0).astype(int)

    table["ty04_to_ty03"] = table["ty04_mid"] / table["ty03_mid"]
    overlap = table["ty04_to_ty03"].replace([np.inf, -np.inf], np.nan).dropna()
    baseline_ratio = float(overlap.median()) if not overlap.empty else np.nan

    table["any_to_ty04"] = table["any_mid"] / table["ty04_mid"]
    table["any_to_ty03"] = table["any_mid"] / table["ty03_mid"]
    table["ty03_gap"] = (table["ty03_rows"] == 0) & (table["ty04_rows"] > 0)
    table["recorders_diverge"] = (
        table["ty04_to_ty03"].notna()
        & ((table["ty04_to_ty03"] - 1.0).abs() > divergence)
    )
    table["any_matches_ty04"] = (
        table["any_to_ty04"].notna()
        & ((table["any_to_ty04"] - 1.0).abs() <= 0.001)
    )
    table["any_matches_ty03"] = (
        table["any_to_ty03"].notna()
        & ((table["any_to_ty03"] - 1.0).abs() <= 0.001)
    )

    # For a complete TY03 gap there is no same-chunk ratio. Use the nearest
    # observed overlap as evidence of whether TY04 represents an incompatible
    # price regime around that gap.
    nearby_ratio = table["ty04_to_ty03"].ffill().bfill()
    unsafe_near_gap = nearby_ratio.notna() & ((nearby_ratio - 1.0).abs() > divergence)
    incompatible_overlap_selected = table["recorders_diverge"] & table["any_matches_ty04"]
    incompatible_gap_backfill = table["ty03_gap"] & table["any_matches_ty04"] & unsafe_near_gap
    table["suspected_corruption"] = incompatible_overlap_selected | incompatible_gap_backfill

    def status(row: pd.Series) -> str:
        if row["suspected_corruption"] and row["ty03_gap"]:
            return "SUSPECT_TY04_BACKFILL"
        if row["suspected_corruption"]:
            return "SUSPECT_TY04_SELECTED"
        if row["ty03_gap"] and row["any_matches_ty04"]:
            return "TY04_BACKFILL_ALIGNED"
        if row["ty03_gap"]:
            return "TY03_GAP_TY04_PRESENT"
        ratio = row["ty04_to_ty03"]
        if pd.notna(ratio) and abs(ratio - 1.0) > divergence:
            return "RECORDER_PRICE_DIVERGENCE"
        return "OK"

    table["status"] = table.apply(status, axis=1)
    summary = {
        "symbol": symbol,
        "ty03_rows": len(frames["TY03"]),
        "ty04_rows": len(frames["TY04"]),
        "any_rows": len(frames["any"]),
        "overlap_chunks": len(overlap),
        "median_ty04_to_ty03": baseline_ratio,
        "divergent_overlap_chunks": int(table["recorders_diverge"].sum()),
        "ty03_gap_chunks_with_ty04": int(table["ty03_gap"].sum()),
        "any_ty04_gap_backfills": int((table["ty03_gap"] & table["any_matches_ty04"]).sum()),
        "suspected_corrupt_chunks": int(table["suspected_corruption"].sum()),
    }
    return table, summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="UTC start, e.g. '2026-07-08 12:00'")
    parser.add_argument("--end", required=True, help="UTC end (exclusive)")
    parser.add_argument("--exchange", default="BNBFUT")
    parser.add_argument("--symbols", nargs="+", default=list(DEFAULT_SYMBOLS))
    parser.add_argument("--chunk", default="15min")
    parser.add_argument(
        "--divergence",
        type=float,
        default=0.05,
        help="Fractional median-price divergence considered unsafe (default: 0.05)",
    )
    parser.add_argument("--force", action="store_true", help="Refresh cached S3 downloads")
    parser.add_argument("--csv-dir", type=Path, help="Write one detailed CSV per symbol")
    args = parser.parse_args()

    summaries: list[dict[str, object]] = []
    for symbol in args.symbols:
        detail, summary = inspect_symbol(
            exchange=args.exchange,
            symbol=symbol,
            start=args.start,
            end=args.end,
            chunk=args.chunk,
            divergence=args.divergence,
            force=args.force,
        )
        summaries.append(summary)

        flagged = detail[detail["status"] != "OK"]
        print(f"\n{args.exchange}/{symbol}")
        if flagged.empty:
            print("  No gaps or recorder-price divergence found in this window.")
        else:
            shown = [
                "ty03_rows", "ty04_rows", "any_rows", "ty03_mid", "ty04_mid",
                "any_mid", "ty04_to_ty03", "status",
            ]
            print(flagged[shown].to_string(float_format=lambda value: f"{value:.6g}"))

        if args.csv_dir:
            args.csv_dir.mkdir(parents=True, exist_ok=True)
            detail.to_csv(args.csv_dir / f"{args.exchange}_{symbol}.csv")

    summary_frame = pd.DataFrame(summaries).set_index("symbol")
    print("\nSummary")
    print(summary_frame.to_string(float_format=lambda value: f"{value:.6g}"))

    if int(summary_frame["suspected_corrupt_chunks"].sum()) > 0:
        print("\nRESULT: recorder='any' selected suspected incompatible TY04 data.")
        return 2
    print("\nRESULT: no incompatible TY04 backfill was proven in the requested window.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
