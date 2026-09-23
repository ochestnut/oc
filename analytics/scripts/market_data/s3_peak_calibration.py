"""S3 peak VERIFICATION (READ-ONLY, writes nothing): confirm the S3 peak — now computed inside
activity_s3 and reached via MarketData.activity_summary(source="s3", peak=True), the same call shape as the
Influx peak — matches Influx's peak_per_bucket, so the S3 metric needs no fudge factor.

For a liquidity-spread sample of feeds in BOTH Influx and S3 it prints s3_peak / influx_peak; aligned means
the ratios cluster at ~1.0. No estimator logic lives here — it only calls the facade both ways and compares.

Needs Influx access (the truth), so run where Influx is reachable (e.g. the gemini-test box). Run as a FILE:
  python s3_peak_calibration.py [DAYS=2] [PER_TYPE=20]
"""

import sys

import pandas as pd

from umm.analytics.market_data.api import MarketData

DATA_TYPES = ("booktop", "trades", "bookdepth")


def _influx_truth(data_type, days, per_type):
    """Sampled (exch, sym, influx_peak) across the liquidity range, from the Influx 1-min peak scan."""
    rows = []
    for exch in MarketData.available_exchanges(data_type=data_type, source="s3"):
        try:
            inf = MarketData.activity_summary(exch, data_type=data_type, days=days, source="influx",
                                              peak=True, peak_bucket="1m", min_per_day=None)
        except Exception:
            continue
        if inf.empty or "peak_per_bucket" not in inf.columns:
            continue
        inf = inf.sort_values("peak_per_bucket", ascending=False).reset_index(drop=True)
        take = inf.iloc[:: max(len(inf) // 6, 1)].head(6)  # spread: top..low, not just the whales
        rows += [(exch, str(r["symbol"]), float(r["peak_per_bucket"])) for _, r in take.iterrows()]
    return sorted(rows, key=lambda t: -t[2])[:per_type]


def _s3_peak(exch, sym, data_type, days):
    """S3 peak for one feed via the same facade, source='s3'."""
    try:
        df = MarketData.activity_summary(exch, symbol=sym, data_type=data_type, days=days,
                                         source="s3", peak=True, peak_bucket="1m", min_per_day=None)
    except Exception:
        return None
    if df.empty or "peak_per_bucket" not in df.columns:
        return None
    v = float(df["peak_per_bucket"].iloc[0])
    return v if v > 0 else None


def _report(data_type, df):
    print(f"\n=== {data_type}: {len(df)} feeds  (s3_peak / influx_peak — aligned == ~1.0) ===")
    if df.empty:
        print("  no overlapping feeds with both signals")
        return
    q = df["ratio"].quantile([0.05, 0.5, 0.95])
    within10 = int(df["ratio"].between(0.9, 1.1).sum())
    print(f"  ratio p05={q[0.05]:.2f} p50={q[0.5]:.2f} p95={q[0.95]:.2f} | within +/-10% of 1.0: {within10}/{len(df)}")
    off = df.assign(dev=(df["ratio"] - 1.0).abs()).sort_values("dev", ascending=False).head(6)
    print("  furthest from 1.0:")
    for _, r in off.iterrows():
        print(f"    {r['exch']:16} {r['sym']:18} influx={r['influx_peak']:8.0f}/min s3={r['s3_peak']:8.0f}/min ratio={r['ratio']:.2f}")


def main(days, per_type):
    for data_type in DATA_TYPES:
        recs = []
        for exch, sym, inf_peak in _influx_truth(data_type, days, per_type):
            s3p = _s3_peak(exch, sym, data_type, days)
            if s3p and inf_peak > 0:
                recs.append({"exch": exch, "sym": sym, "influx_peak": inf_peak,
                             "s3_peak": s3p, "ratio": s3p / inf_peak})
        _report(data_type, pd.DataFrame(recs))


if __name__ == "__main__":
    d = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    main(d, n)
