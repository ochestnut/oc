"""Read-only api validation. Run with %run in a notebook; results remain in discovery/reads.

Default: every configured/discovered symbol, both sources, one closed 15-minute
window. --limit N is a smoke check only. Empty results are not a pass.
"""
import argparse
import os

import pandas as pd
from umm.analytics.market_data import MarketData

# Snapshot of migrated instrument lists in ondo_ty03_migrate_ty04.
EXPECTED = {'ARCUS': ['PERP-AAPL-USDG',
           'PERP-AMD-USDG',
           'PERP-AMZN-USDG',
           'PERP-BABA-USDG',
           'PERP-BAC-USDG',
           'PERP-BE-USDG',
           'PERP-BTC-USDG',
           'PERP-CCL-USDG',
           'PERP-COIN-USDG',
           'PERP-CRCL-USDG',
           'PERP-CRWV-USDG',
           'PERP-DRAM-USDG',
           'PERP-DYDX-USDG',
           'PERP-ETH-USDG',
           'PERP-F-USDG',
           'PERP-GLD-USDG',
           'PERP-GOOGL-USDG',
           'PERP-HOOD-USDG',
           'PERP-HYPE-USDG',
           'PERP-INTC-USDG',
           'PERP-LIT-USDG',
           'PERP-META-USDG',
           'PERP-MSFT-USDG',
           'PERP-MU-USDG',
           'PERP-NBIS-USDG',
           'PERP-NVDA-USDG',
           'PERP-ORCL-USDG',
           'PERP-PLTR-USDG',
           'PERP-QQQ-USDG',
           'PERP-RVI-USDG',
           'PERP-SGOV-USDG',
           'PERP-SKHY-USDG',
           'PERP-SLV-USDG',
           'PERP-SNDK-USDG',
           'PERP-SOL-USDG',
           'PERP-SPCX-USDG',
           'PERP-SPY-USDG',
           'PERP-TSLA-USDG',
           'PERP-USAR-USDG',
           'PERP-USO-USDG',
           'PERP-VT-USDG',
           'PERP-XRP-USDG',
           'PERP-ZEC-USDG'],
 'BITMEX': ['PERP-BTC-USD',
            'PERP-BTC-USDT',
            'PERP-ETH-USD',
            'PERP-ETH-USDT',
            'PERP-SOL-USD',
            'PERP-SOL-USDT'],
 'GATEIO': ['PAIR-AERO-USDT', 'PAIR-BTC-USDT', 'PAIR-DOLO-USDT', 'PAIR-IO-USDT'],
 'NADO': ['PERP-BTC-USDT0', 'PERP-ETH-USDT0', 'PERP-SOL-USDT0'],
 'ONDO-SPOT': ['PAIR-GOOGL-USDC',
               'PAIR-SPY-USDC',
               'PAIR-QQQ-USDC',
               'PAIR-GLD-USDC',
               'PAIR-SLV-USDC',
               'PAIR-CRCL-USDC',
               'PAIR-SPCX-USDC',
               'PAIR-SNDK-USDC',
               'PAIR-TSLA-USDC',
               'PAIR-NVDA-USDC',
               'PAIR-MU-USDC',
               'PAIR-DRAM-USDC']}

# exchange, influx host, instrument namespace, symbol prefix, recorded types
SCOPES = [
    (ex, "TY04", "standard", "", ("booktop", "bookdepth", "trades"))
    for ex in ("ARCUS", "GATEIO", "BITMEX", "NADO")
] + [
    ("ONDO-SPOT", "TY04", "standard", "PAIR-", ("booktop",)),
    ("KALSHI", "TY03", "prediction", "PRED-", ("booktop",)),
    ("KALSHI", "TY03", "standard", "PERP-", ("booktop",)),
    ("DERIBIT", "TY03", "standard", "OPT-", ("booktop",)),
] + [
    (ex, host, "standard", "", ("booktop",)) for ex, host in [
        ("DERIVED_ARCUS_BNBFUT", "TY04"),
        ("DERIVED_BNBFUT_NASDAQ", "TY04"),
        ("DERIVED_ONDO_NASDAQ", "TY04"),
        ("DERIVED_ONDO_NYSE", "TY04"),
        ("DERIVED_ONETRADING_BNBFUT", "TY04"),
        ("DERIVED_BITSTAMPPERP_BNBFUT", "FR01"),
        ("DERIVED_BITSTAMP_BNBFUT", "FR01"),
    ]
]


def run_validation(
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    limit: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # One chunk and sequential calls keep depth reads and query load bounded.
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("use explicit UTC bounds, e.g. 2026-09-21T20:00:00Z")
    if not start < end or end - start > pd.Timedelta(minutes=15):
        raise ValueError("choose a positive window of at most 15 minutes")
    os.environ["JST_MAX_WORKERS"] = "1"
    discovery_rows, read_rows, exchanges_cache = [], [], {}
    print(f"window: {start} -> {end}; limit per scope/type: {limit or 'all'}", flush=True)
    for exchange, host, namespace, prefix, types in SCOPES:
        for data_type in types:
            print(f"checking {exchange}/{namespace}/{data_type}", flush=True)
            listed = {}
            for source in ("influx", "s3"):
                row = dict(exchange=exchange, namespace=namespace, data_type=data_type,
                           source=source, host=host, exchange_listed=False,
                           symbols=0, status="ok", error="")
                try:
                    key = (source, host, namespace, data_type)
                    if key not in exchanges_cache:
                        exchanges_cache[key] = MarketData.available_exchanges(
                            source=source, recorder=host, data_type=data_type,
                            instrument_type=namespace)
                    row["exchange_listed"] = exchange in exchanges_cache[key]
                    # any exercises the updated default influx routing.
                    symbols = MarketData.available_symbols(
                        exchange, source=source, recorder="any", data_type=data_type,
                        instrument_type=namespace, start=start, end=end)
                    listed[source] = {s for s in symbols if s.startswith(prefix)}
                    row["symbols"] = len(listed[source])
                    if not row["exchange_listed"] or not listed[source]:
                        row["status"] = "review: missing discovery"
                except Exception as exc:
                    listed[source] = set()
                    row.update(status="error", error=str(exc))
                discovery_rows.append(row)
            # Include expected symbols even if BOTH discovery paths omit them.
            symbols = sorted(set(EXPECTED.get(exchange, [])) | listed["influx"] | listed["s3"])
            for symbol in symbols[:limit]:
                for source in ("influx", "s3"):
                    # Explicit archives prevent old ty03 data masking migration gaps.
                    recorder = "any" if source == "influx" else host
                    if source == "s3" and (namespace == "prediction" or exchange == "DERIBIT"):
                        recorder = "TY03D"
                    elif source == "s3" and exchange == "ONDO-SPOT":
                        recorder = "TY04D"
                    row = dict(exchange=exchange, namespace=namespace, data_type=data_type,
                               symbol=symbol, source=source, recorder=recorder,
                               discovered=symbol in listed[source], rows=0, status="empty",
                               first=None, last=None, error="")
                    try:
                        if data_type == "bookdepth":
                            df = MarketData.load_book(
                                exchange=exchange, symbol=symbol, start=start, end=end,
                                source=source, recorder=recorder, force=True, silent=True,
                                pivot=False)
                        elif data_type == "trades":
                            df = MarketData.load_trades(
                                exchange=exchange, symbol=symbol, start=start, end=end,
                                source=source, recorder=recorder, force=True, silent=True,
                                max_workers=1)
                        else:
                            df = MarketData.load_best_levels(
                                exchange=exchange, symbol=symbol, start=start, end=end,
                                source=source, recorder=recorder, force=True, silent=True,
                                max_workers=1)
                        row["rows"] = len(df)
                        if not df.empty:
                            row.update(status="read ok", first=df.index.min(), last=df.index.max())
                            fields = {"booktop": ["bid_price", "ask_price"],
                                      "bookdepth": ["price"], "trades": ["price"]}[data_type]
                            if not df[fields].apply(pd.to_numeric, errors="coerce").gt(0).any().any():
                                row["status"] = "review: no positive prices"
                    except Exception as exc:
                        row.update(status="error", error=str(exc))
                    read_rows.append(row)
            if limit and len(symbols) > limit:
                print(f"  sampled {limit}/{len(symbols)} symbols; not full coverage", flush=True)
    discovery = pd.DataFrame(discovery_rows)
    reads = pd.DataFrame(read_rows)
    print("\ndiscovery:\n" + discovery.to_string(index=False))
    if not reads.empty:
        print("\nreads:\n" + reads.groupby(
            ["exchange", "namespace", "data_type", "source", "status"]
        ).size().rename("symbols").to_string())
        attention = reads[(reads.status != "read ok") | ~reads.discovered]
        print("\nreads needing review:\n" + attention.to_string(index=False))
    print("\nempty/zero quotes are inconclusive; read ok is a load check, not completeness or parity.")
    print("s3 discovery is day-based; reads use the exact window and recorder. export/catalog lag may differ.")
    return discovery, reads


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--limit", type=int, help="optional smoke-check symbol cap per scope/type")
    args = parser.parse_args()
    if bool(args.start) != bool(args.end):
        parser.error("provide both --start and --end")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be positive")
    end = pd.Timestamp(args.end) if args.end else pd.Timestamp.now(tz="UTC").floor("15min") - pd.Timedelta(minutes=15)
    start = pd.Timestamp(args.start) if args.start else end - pd.Timedelta(minutes=15)
    discovery, reads = run_validation(start, end, args.limit)
