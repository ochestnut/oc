"""Bitstamp public-trade / JST-fill markouts against Bitstamp and Coinbase mids.

Examples (run through run_bitstamp_markouts.sh):
  --refs BITSTAMP COINBS
  --mode overlay --books JDBP BBMM --refs BITSTAMP COINBS
  --mode fills --books BBMM --assets SOL --refs COINBS

Default invocation retains the original public-trade, Bitstamp-local-mid report.
"""

from analytics_paths import output_path
import argparse
from io import BytesIO
from pathlib import Path
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "umm" / "src"))
from umm.analytics.compute.markout import compute_overlay, signed_horizons_ns
from umm.analytics.market_data import MarketData
from umm.analytics.trading_data import BotData

REF_LABELS = {"BITSTAMP": "Bitstamp local mid", "COINBS": "Coinbase reference mid"}


def ref_exchange(value):
    value = value.upper()
    if value == "COINBASE":
        value = "COINBS"
    if value not in REF_LABELS:
        raise argparse.ArgumentTypeError("reference must be BITSTAMP, COINBS, or COINBASE")
    return value


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    today = pd.Timestamp.now(tz="UTC").normalize()
    parser.add_argument("--start", default=str(today - pd.Timedelta(days=1)), help="UTC start, default yesterday")
    parser.add_argument("--end", default=str(today), help="UTC end, default today")
    parser.add_argument("--horizon", type=float, default=60, help="seconds each side of trade")
    parser.add_argument("--source", choices=("influx", "s3"), default="influx")
    parser.add_argument("--recorder", default="TY03", help="recorder for public trades and both mids")
    parser.add_argument("--mode", choices=("market", "fills", "overlay"), default="market")
    parser.add_argument("--books", nargs="+", type=str.upper, help="exact JST books, kept separate")
    parser.add_argument("--assets", nargs="+", type=str.upper, choices=("BTC", "ETH", "SOL"), default=["BTC", "ETH", "SOL"])
    parser.add_argument("--refs", nargs="+", type=ref_exchange, default=["BITSTAMP"])
    parser.set_defaults(timestamp="exchange")
    parser.add_argument("--output", type=Path, default=output_path("bitstamp_market_markouts.pdf", "reports"))
    args = parser.parse_args(argv)
    try:
        args.start, args.end = (pd.to_datetime(value, utc=True) for value in (args.start, args.end))
    except (ValueError, TypeError) as error:
        parser.error(str(error))
    if pd.isna(args.start) or pd.isna(args.end) or args.end <= args.start or not np.isfinite(args.horizon) or args.horizon <= 0:
        parser.error("end must follow start and horizon must be finite and positive")
    if args.mode != "market" and not args.books:
        parser.error("--books is required for fills and overlay modes")
    if args.mode == "market" and args.books:
        parser.error("--books requires --mode fills or --mode overlay")
    if args.output.suffix.lower() != ".pdf":
        parser.error("output must have a .pdf extension")
    for key in ("refs", "assets", "books"):
        values = getattr(args, key)
        if values:
            setattr(args, key, list(dict.fromkeys(values)))
    return args


def normalize_frame(frame, label):
    if frame.empty:
        raise RuntimeError(f"No data for {label}; report not written")
    frame = frame.copy()
    if "timestamp" not in frame.columns:
        frame = frame.reset_index()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="raise").dt.tz_localize(None)
    if frame["timestamp"].isna().any():
        raise ValueError(f"Missing timestamps in {label}")
    return frame.sort_values("timestamp", kind="stable").reset_index(drop=True)


def load_asset(args, asset):
    symbol = f"PAIR-{asset}-USD"
    trades = {}
    missing = []
    if args.mode in ("market", "overlay"):
        taker = normalize_frame(MarketData.load_trades(
            exchange="BITSTAMP", symbol=symbol, start=args.start, end=args.end,
            recorder=args.recorder, source=args.source, timestamp=args.timestamp,
            mm_perspective=False, force=True,
        ), f"Bitstamp public trades / {symbol}")
        maker = taker.copy()
        maker["side"] = maker["side"].map({"buy": "sell", "sell": "buy"})
        if maker["side"].isna().any():
            raise ValueError(f"Unknown public trade sides for {symbol}")
        trades.update({"Public maker": maker, "Public taker": taker})
    if args.mode in ("fills", "overlay"):
        for book in args.books:
            fills = BotData.load_fills(book, args.start, args.end, exchange="BITSTAMP",
                                       symbol=symbol, source=args.source, force=True)
            if fills.empty:
                missing.append(f"{book}: no Bitstamp fills")
                continue
            trades[f"JST {book}"] = normalize_frame(fills, f"{book} / {symbol}")
        if not any(label.startswith("JST ") for label in trades):
            raise RuntimeError(f"No Bitstamp fills for requested books / {symbol}; report not written")
    refs = {}
    padding = pd.Timedelta(seconds=args.horizon + 1)
    for exchange in args.refs:
        refs[exchange] = normalize_frame(MarketData.load_mid_price(
            exchange=exchange, symbol=symbol, start=args.start-padding, end=args.end+padding,
            recorder=args.recorder, source=args.source, timestamp=args.timestamp, force=True,
        ), f"{exchange} mid / {symbol}")
    return trades, refs, missing


def calculate(args, trades, refs):
    results = {}
    for exchange, mid in refs.items():
        result = compute_overlay(trades, mid, horizons_ns=signed_horizons_ns(args.horizon),
                                 by_side=True, mark_to="fill_price", volume_weighted=False,
                                 alignment="backward").df
        if not result["n_trades"].gt(0).any():
            raise RuntimeError(f"No timestamp overlap with {exchange}; report not written")
        results[exchange] = result
    return results


def make_figure(args, asset, trades, refs, results, missing, page_number):
    fig, axes = plt.subplots(1, len(refs), figsize=(16 if len(refs) > 1 else 11.7, 8.3),
                             squeeze=False, sharey=True)
    colors = {}
    palette = plt.get_cmap("tab10")
    for ax, (exchange, result) in zip(axes[0], results.items()):
        keys = ["ref_market"] + (["liquidity"] if "liquidity" in result else []) + ["side"]
        for values, group in result.groupby(keys, dropna=False):
            label, side = values[0], values[-1]
            liquidity = values[1] if len(values) == 3 else "market"
            name = label if label.startswith("Public ") else f"{label} {liquidity}"
            if name not in colors:
                colors[name] = palette(len(colors) % 10)
            group = group.sort_values("horizon_sec")
            ax.plot(group["horizon_sec"], group["mean_markout_bps"], color=colors[name],
                    linestyle="-" if side == "buy" else "--", label=f"{name} {side}")
        ax.set(xlabel="Seconds relative to trade (T0)", title=REF_LABELS[exchange])
        ax.set_xscale("symlog", linthresh=0.002)
        ax.axhline(0, color="grey", linewidth=0.6)
        ax.axvline(0, color="grey", linewidth=0.6)
        ax.grid(alpha=0.2)
    axes[0, 0].set_ylabel("Mean markout (bps)")
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", bbox_to_anchor=(0.5, 0.12), ncol=4, fontsize=8)
    fig.suptitle(f"Bitstamp {asset}/USD - {args.mode} markouts", fontsize=18, y=0.98)
    counts = [f"{label}: {len(frame):,}" for label, frame in trades.items() if label != "Public taker"]
    counts += [f"{exchange} mid ticks: {len(frame):,}" for exchange, frame in refs.items()]
    fig.text(0.06, 0.915, f"{args.start} to {args.end}\nSource: {args.source} / {args.recorder} | " + " | ".join(counts), fontsize=9)
    note = (f"Public/mid timestamps: {args.timestamp}; JST fills: stored fill timestamp. Backward as-of alignment.\n"
            "Equal trade weighting | Trade-price baseline | Before fees | Solid: buy; dashed: sell\n"
            "Public maker/taker are opposite sides of the same tape. Counts may vary by horizon/reference.")
    if missing:
        note += "\n" + "; ".join(missing)
    fig.text(0.06, 0.025, note, fontsize=8)
    fig.text(0.96, 0.025, f"{page_number} / {len(args.assets)}", ha="right", fontsize=9)
    legend_rows = (len(labels) + 3) // 4
    fig.tight_layout(rect=(0.025, 0.17 + legend_rows * 0.022, 0.99, 0.88))
    return fig


def main(argv=None):
    args = parse_args(argv)
    pages = []
    try:
        for asset in args.assets:
            print(f"Loading Bitstamp {asset}/USD ({args.mode}) against {', '.join(args.refs)}...", flush=True)
            trades, refs, missing = load_asset(args, asset)
            for note in missing:
                print(f"{asset}: {note}", flush=True)
            results = calculate(args, trades, refs)
            pages.append(make_figure(args, asset, trades, refs, results, missing, len(pages)+1))
        # Complete every requested asset/reference before replacing the output.
        buffer = BytesIO()
        with PdfPages(buffer) as pdf:
            for fig in pages:
                pdf.savefig(fig)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(buffer.getvalue())
        print(f"Saved {args.output.resolve()}")
    finally:
        for fig in pages:
            plt.close(fig)


if __name__ == "__main__":
    main()
