"""PnL report with JST data loaders."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from jst_trading.data import load_fills, load_position
from jst_trading.view import LOCATION_MAP
from utils import to_naive_utc
from tca import TCAResult, decompose_pnl, compare_tca


def _to_naive(ts):
    """Convert timestamps to naive UTC."""
    ts = pd.to_datetime(ts)
    if hasattr(ts, 'dt'):
        return ts.dt.tz_convert('UTC').dt.tz_localize(None) if ts.dt.tz is not None else ts
    elif hasattr(ts, 'tz'):
        return ts.tz_convert('UTC').tz_localize(None) if ts.tz is not None else ts
    return ts


def pnl_report(
    symbol: str,
    exchange: str,
    start: datetime,
    end: datetime,
    book: str,
    ref_mid: pd.Series,
    jst_sym: Optional[str] = None,
    freq: Optional[str] = None,
    markout_horizons: Optional[list[str]] = None,
    chunk_minutes: int = 360,
) -> TCAResult:
    """Load data and compute PnL decomposition."""
    start = to_naive_utc(start)
    end = to_naive_utc(end)
    location = LOCATION_MAP.get(exchange, exchange)
    asset = symbol.split("-")[-2]

    ref_mid = ref_mid.copy()
    ref_mid.index = _to_naive(ref_mid.index)

    fills = load_fills(book, start, end, chunk_minutes=chunk_minutes)
    fills["timestamp"] = _to_naive(fills["timestamp"])
    fills = fills[fills["location"] == location]
    if jst_sym:
        fills = fills[fills["jst_sym"] == jst_sym]

    if fills.empty:
        raise ValueError("No fills found for the specified parameters.")

    position = load_position(book, start, end, chunk_minutes=chunk_minutes)
    position = position[
        (position["location"] == location)
        & (position["sub_location"] == "ALL_SUB_LOCATIONS")
        & (position["jst_sym"] == asset)
    ]
    position["timestamp"] = _to_naive(position["timestamp"])
    position = position.sort_values("timestamp")

    pos_before_start = position[position["timestamp"] <= start]
    initial_position = float(pos_before_start.iloc[-1]["position"]) if not pos_before_start.empty else 0.0

    return decompose_pnl(
        fills,
        ref_mid,
        initial_position,
        freq=freq,
        markout_horizons=markout_horizons,
    )