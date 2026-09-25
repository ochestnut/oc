"""
influx_survey.py — deep survey of UMM InfluxDB instances.

Shows per-exchange instrument coverage and time ranges so we can determine
whether exchange tags like COINBASE vs COINBS are true duplicates/aliases
or genuinely different data sources.

Usage:
    python influx_survey.py
    python influx_survey.py --host 172.33.10.230 --coins PAIR-AAVE-USD PAIR-BTC-USD
    python influx_survey.py --compare-exchanges COINBASE COINBS --coin PAIR-AAVE-USD
"""

from __future__ import annotations

import argparse
from pathlib import Path
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from influxdb import InfluxDBClient

# ── connection ──────────────────────────────────────────────────────────────

TOKYO_HOST     = "umm2prod-03.jstcapdev.com"
FRANKFURT_HOST = "172.33.10.230"
USER           = "umm"
PASS           = "LUKUZMR2HNOPNRVISYUR"
DATABASE       = "UMM_MD"

HOSTS = {
    "tokyo":     TOKYO_HOST,
    "frankfurt": FRANKFURT_HOST,
}


def _client(host: str) -> InfluxDBClient:
    return InfluxDBClient(host=host, port=8086, username=USER, password=PASS, database=DATABASE)


# ── helpers ─────────────────────────────────────────────────────────────────

def _tag_values(client: InfluxDBClient, measurement: str, key: str) -> list[str]:
    q = f'SHOW TAG VALUES FROM {measurement} WITH KEY = "{key}"'
    return [v["value"] for v in client.query(q).get_points()]


def _time_range_for_exchange(client: InfluxDBClient, measurement: str,
                              price_field: str, exchange: str) -> dict:
    """Return first/last timestamp and row count for a single exchange."""
    q = (
        f'SELECT FIRST({price_field}), LAST({price_field}), COUNT({price_field}) '
        f'FROM {measurement} WHERE "exchange_id" = $exchange'
    )
    pts = list(client.query(q, bind_params={"exchange": exchange}).get_points())
    if not pts:
        return {"first": None, "last": None, "count": 0}
    row = pts[0]
    return {
        "first": row.get("time"),
        "last":  row.get("last"),          # LAST() returns the value, not a time — see note below
        "count": row.get("count", 0),
    }


def _coins_for_exchange(client: InfluxDBClient, measurement: str, exchange: str) -> list[str]:
    """List instruments available for a specific exchange tag, excluding PRED-* contracts."""
    q = (
        f'SHOW TAG VALUES FROM {measurement} WITH KEY = "instrument_id" '
        f'WHERE "exchange_id" = $exchange'
    )
    return sorted(
        v["value"] for v in client.query(q, bind_params={"exchange": exchange}).get_points()
        if not v["value"].startswith("PRED-")
    )


def _time_range_for_exchange_instrument(client: InfluxDBClient, measurement: str,
                                         price_field: str, exchange: str,
                                         instrument: str) -> tuple[str | None, str | None, int]:
    """Return (first_time, last_time, count) for a specific exchange+instrument pair."""
    q = (
        f'SELECT FIRST({price_field}), LAST({price_field}), COUNT({price_field}) '
        f'FROM {measurement} '
        f'WHERE "exchange_id" = $exchange AND "instrument_id" = $instrument'
    )
    pts = list(client.query(q, bind_params={"exchange": exchange, "instrument": instrument}).get_points())
    if not pts:
        return None, None, 0
    row = pts[0]
    return row.get("time"), row.get("last"), int(row.get("count", 0))


# ── main survey ─────────────────────────────────────────────────────────────

def survey_exchange_coverage(host: str, measurement: str = "md_booktop",
                              price_field: str = "bid_price",
                              filter_coins: list[str] | None = None,
                              max_workers: int = 8) -> pd.DataFrame:
    """
    For every exchange in `measurement`, list its instruments and time range.

    Returns a DataFrame with columns:
        exchange, instrument, first, count

    Each thread creates its own InfluxDBClient — InfluxDBClient is not thread-safe
    and sharing one across threads causes deadlocks.
    """
    # Use a dedicated client just for the initial exchange list, then close it.
    init_client = _client(host)
    exchanges = _tag_values(init_client, measurement, "exchange_id")
    init_client.close()
    print(f"  Found {len(exchanges)} exchanges in {measurement} on {host}")

    rows = []
    done = 0

    def _fetch(exchange: str):
        # Each worker gets its own client to avoid thread-safety issues.
        c = _client(host)
        try:
            coins = _coins_for_exchange(c, measurement, exchange)
            if filter_coins:
                coins = [coin for coin in coins if coin in filter_coins]
            result = []
            for coin in coins:
                first, _, count = _time_range_for_exchange_instrument(
                    c, measurement, price_field, exchange, coin
                )
                result.append((exchange, coin, first, count))
            return result
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futs = {exe.submit(_fetch, exch): exch for exch in exchanges}
        for fut in as_completed(futs):
            exch = futs[fut]
            done += 1
            try:
                batch = fut.result()
                for exchange, coin, first, count in batch:
                    rows.append({"exchange": exchange, "instrument": coin,
                                 "first": first, "count": count})
                print(f"  [{done}/{len(exchanges)}] {exch} — {len(batch)} instruments")
            except Exception as e:
                print(f"  ⚠️  [{done}/{len(exchanges)}] {exch}: {e}")

    df = pd.DataFrame(rows, columns=["exchange", "instrument", "first", "count"])
    return df.sort_values(["exchange", "instrument"]).reset_index(drop=True)


def compare_exchanges(host: str, exchanges: list[str], coin: str,
                      measurement: str = "md_booktop",
                      price_field: str = "bid_price") -> None:
    """
    Print a side-by-side comparison of two or more exchange tags for a single instrument.
    Useful for determining whether e.g. COINBASE and COINBS are aliases or different feeds.
    """
    print(f"\n{'='*60}")
    print(f"Comparing exchanges for {coin} on {host} / {measurement}")
    print(f"{'='*60}")

    for exch in exchanges:
        c = _client(host)
        try:
            first, _, count = _time_range_for_exchange_instrument(
                c, measurement, price_field, exch, coin
            )
            print(f"  {exch:<45}  first={first}  count={count:,}")

            q = (
                f'SELECT {price_field} FROM {measurement} '
                f'WHERE "exchange_id" = $exchange AND "instrument_id" = $instrument '
                f'ORDER BY time DESC LIMIT 5'
            )
            pts = list(c.query(q, bind_params={"exchange": exch, "instrument": coin}).get_points())
            times = [p["time"] for p in pts]
            print(f"  {'':45}  last 5 timestamps: {times}")
        finally:
            c.close()


def full_survey(host: str) -> None:
    """Quick top-level survey — checks md_booktop and both known trades measurements."""
    client = _client(host)

    # ── booktop ──
    exchanges_bt = _tag_values(client, "md_booktop", "exchange_id")
    coins_bt     = [c for c in _tag_values(client, "md_booktop", "instrument_id") if not c.startswith("PRED-")]
    time_bt      = pd.DataFrame(client.query(
        "SELECT FIRST(bid_price), LAST(bid_price) FROM md_booktop"
    ).get_points())

    # ── trades — try both known measurement names ──
    trades_measurement = None
    for candidate in ("md_trades", "trades"):
        try:
            exch = _tag_values(client, candidate, "exchange_id")
            if exch:
                trades_measurement = candidate
                break
        except Exception:
            pass

    if trades_measurement:
        exchanges_tr = _tag_values(client, trades_measurement, "exchange_id")
        coins_tr     = [c for c in _tag_values(client, trades_measurement, "instrument_id") if not c.startswith("PRED-")]
        time_tr      = pd.DataFrame(client.query(
            f"SELECT FIRST(price), LAST(price) FROM {trades_measurement}"
        ).get_points())
    else:
        exchanges_tr, coins_tr, time_tr = [], [], pd.DataFrame()

    client.close()

    print(f"\n=== {host} / {DATABASE} ===")
    print(f"md_booktop exchanges : {exchanges_bt}")
    print(f"md_booktop coins     : {coins_bt}")
    print(f"md_booktop time range: "
          f"{time_bt[['time','last']].to_string(index=False) if not time_bt.empty else 'N/A'}")
    tr_label = trades_measurement or "trades (not found)"
    print(f"{tr_label} exchanges  : {exchanges_tr}")
    print(f"{tr_label} coins      : {coins_tr}")
    print(f"{tr_label} time range : "
          f"{time_tr[['time','last']].to_string(index=False) if not time_tr.empty else 'N/A'}")


def print_coverage(df: pd.DataFrame, host: str) -> None:
    print(f"\n{'='*70}")
    print(f"Per-exchange instrument coverage — {host}")
    print(f"{'='*70}")
    for exch, group in df.groupby("exchange"):
        print(f"\n  {exch}  ({len(group)} instruments)")
        for _, row in group.iterrows():
            print(f"    {row['instrument']:<40}  first={row['first']}  count={row['count']:,}")


# ── Exchange canonical audit ─────────────────────────────────────────────────

def audit_exchange_canonical(coverage_csvs: list[str], books: list[str] | None = None) -> None:
    """
    For each book+coin, compare the config's ref_exchange+ref_symbol against
    what actually exists in InfluxDB (from pre-generated coverage CSVs).

    This tells you: "BBMM uses DERIVED_BITSTAMP_BNBSPOT_COINBS for PAIR-LTC-USD,
    but that (exchange, instrument) pair isn't in InfluxDB — it lives under
    DERIVED_BITSTAMP_COINBS_BNBSPOT instead."

    Uses the CSVs already in the project (tests/influx_audit/):
        influx_coverage_frankfurt_md_booktop.csv
        influx_coverage_frankfurt_md_trades.csv
        influx_coverage_tokyo_md_booktop.csv
        influx_coverage_tokyo_md_trades.csv

    Requires JST_ROOT to be set so load_instrument_config can find cfg files.
    """
    import os, sys
    sys.path.insert(0, os.path.join(os.environ.get("JST_ROOT", ""), "umm", "src"))
    from umm.analytics.jst_trading.fills import load_instrument_config

    # Load coverage CSVs into a set of (exchange, instrument) pairs we know exist.
    coverage_pairs: set[tuple[str, str]] = set()
    all_coverage: pd.DataFrame = pd.DataFrame()
    for path in coverage_csvs:
        try:
            df = pd.read_csv(path)
            for _, row in df.iterrows():
                coverage_pairs.add((row["exchange"], row["instrument"]))
            all_coverage = pd.concat([all_coverage, df], ignore_index=True)
        except FileNotFoundError:
            print(f"  ⚠️  Coverage file not found: {path}")
            return

    print(f"  Loaded {len(coverage_pairs):,} (exchange, instrument) pairs from InfluxDB coverage")

    # Discover books if not specified.
    if not books:
        jst_root = os.environ.get("JST_ROOT", "")
        cfg_dir  = os.path.join(jst_root, "umm_config")
        if not os.path.isdir(cfg_dir):
            print(f"  ⚠️  JST_ROOT not set or umm_config not found at {cfg_dir}")
            return
        books = sorted(
            d.upper() for d in os.listdir(cfg_dir)
            if os.path.isdir(os.path.join(cfg_dir, d))
        )
        print(f"  Discovered {len(books)} books in umm_config")

    # For each book+coin, check if (ref_exchange, ref_symbol) exists in coverage.
    rows = []
    failed_books = []
    for book in books:
        try:
            cfg = load_instrument_config([book])
            if cfg.empty:
                continue
            for _, row in cfg.iterrows():
                ref_exch   = str(row.get("ref_exchange", "")).strip()
                ref_sym    = str(row.get("ref_symbol",   "")).strip()
                jst_sym    = str(row.get("jst_sym",      "")).strip()
                if not ref_exch or not ref_sym:
                    continue

                in_influx = (ref_exch, ref_sym) in coverage_pairs

                # If not found, look for any exchange in InfluxDB that HAS this instrument.
                if not in_influx:
                    matches = all_coverage[all_coverage["instrument"] == ref_sym]["exchange"].tolist()
                else:
                    matches = []

                rows.append({
                    "book":       book,
                    "jst_sym":    jst_sym,
                    "ref_symbol": ref_sym,
                    "ref_exchange": ref_exch,
                    "in_influx":  in_influx,
                    "alternatives": ", ".join(sorted(set(matches))),
                })
        except Exception as e:
            failed_books.append((book, str(e)))

    if failed_books:
        print(f"\n  ⚠️  Could not load {len(failed_books)} books:")
        for b, err in failed_books[:10]:
            print(f"     {b}: {err}")

    df_out = pd.DataFrame(rows)
    if df_out.empty:
        print("  No config data found.")
        return

    ok      = df_out[df_out["in_influx"]]
    missing = df_out[~df_out["in_influx"]]

    print(f"\n{'='*80}")
    print(f"Exchange canonical audit — {len(df_out)} book+coin entries across {df_out['book'].nunique()} books")
    print(f"{'='*80}")
    print(f"\n  ✅  {len(ok)} (exchange, instrument) pairs found in InfluxDB")
    print(f"  ❌  {len(missing)} NOT found\n")

    if not missing.empty:
        print(f"  {'BOOK':<10} {'JST_SYM':<15} {'REF_SYMBOL':<25} {'CONFIG REF_EXCHANGE':<45} {'ALTERNATIVES IN INFLUXDB'}")
        print(f"  {'-'*10} {'-'*15} {'-'*25} {'-'*45} {'-'*40}")
        for _, row in missing.sort_values(["ref_exchange", "book"]).iterrows():
            alts = row["alternatives"] or "— not found anywhere"
            print(f"  {row['book']:<10} {row['jst_sym']:<15} {row['ref_symbol']:<25} {row['ref_exchange']:<45} {alts}")

        # Derive suggested EXCHANGE_ALIAS_MAP entries.
        # Group by ref_exchange: if all its missing instruments live under one alternative, it's a clean mapping.
        print(f"\n  Suggested EXCHANGE_ALIAS_MAP entries:")
        print(f"  EXCHANGE_ALIAS_MAP = {{")
        for ref_exch, grp in missing.groupby("ref_exchange"):
            alt_sets = [set(r.split(", ")) for r in grp["alternatives"] if r]
            if not alt_sets:
                print(f'      # "{ref_exch}" → no alternative found in InfluxDB')
                continue
            common = alt_sets[0].intersection(*alt_sets[1:]) if len(alt_sets) > 1 else alt_sets[0]
            if len(common) == 1:
                print(f'      "{ref_exch}": "{common.pop()}",')
            else:
                print(f'      # "{ref_exch}" → ambiguous alternatives: {sorted(common or alt_sets[0])}')
        print(f"  }}")

    # Save full results for inspection.
    here = Path(os.environ.get("JST_ANALYTICS_DIR", Path.home() / "Documents/jst/analytics")).expanduser() / "audits"
    here.mkdir(parents=True, exist_ok=True)
    out = here / "audit_exchange_canonical.csv"
    df_out.to_csv(out, index=False)
    print(f"\n  → full results saved to {out}")


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Survey UMM InfluxDB instances")
    parser.add_argument("--host",     default=None,
                        help="Single host to survey (tokyo|frankfurt or raw hostname). "
                             "Default: both.")
    parser.add_argument("--coins",    nargs="*", default=None,
                        help="Filter coverage table to these instrument IDs. "
                             "Default: all.")
    parser.add_argument("--compare-exchanges", nargs="+", dest="compare", default=None,
                        help="Exchange tags to compare side-by-side (requires --coin).")
    parser.add_argument("--coin",     default="PAIR-AAVE-USD",
                        help="Instrument for --compare-exchanges. Default: PAIR-AAVE-USD")
    parser.add_argument("--measurement", default=None,
                        help="Measurement to survey (md_booktop, md_trades, or both). "
                             "Default: both.")
    parser.add_argument("--quick",    action="store_true",
                        help="Only run the quick top-level survey, skip per-exchange breakdown.")
    parser.add_argument("--audit",    action="store_true",
                        help="Audit ref_exchange values from all book configs against InfluxDB coverage CSVs.")
    parser.add_argument("--books",    nargs="*", default=None,
                        help="Books to audit (default: all books in umm_config). Used with --audit.")
    parser.add_argument("--coverage", nargs="*", default=None,
                        help="Coverage CSV files to use for audit. "
                             "Default: influx_coverage_frankfurt_md_booktop.csv + influx_coverage_tokyo_md_booktop.csv")
    args = parser.parse_args()

    # ── exchange canonical audit (standalone — no survey needed) ──
    if args.audit:
        here = Path(os.environ.get("JST_ANALYTICS_DIR", Path.home() / "Documents/jst/analytics")).expanduser() / "audits"
        csvs = args.coverage or [
            os.path.join(here, "influx_coverage_frankfurt_md_booktop.csv"),
            os.path.join(here, "influx_coverage_tokyo_md_booktop.csv"),
            os.path.join(here, "influx_coverage_frankfurt_md_trades.csv"),
            os.path.join(here, "influx_coverage_tokyo_md_trades.csv"),
        ]
        audit_exchange_canonical(csvs, books=args.books)
        raise SystemExit(0)

    hosts_to_survey = {}
    if args.host:
        h = HOSTS.get(args.host, args.host)
        hosts_to_survey = {args.host: h}
    else:
        hosts_to_survey = HOSTS

    # ── quick top-level survey ──
    for label, host in hosts_to_survey.items():
        full_survey(host)

    if args.quick:
        raise SystemExit(0)

    # ── per-exchange coverage breakdown ──
    measurements = (
        [args.measurement] if args.measurement
        else ["md_booktop", "md_trades"]
    )
    price_fields = {"md_booktop": "bid_price", "md_trades": "price"}

    for label, host in hosts_to_survey.items():
        for meas in measurements:
            price_field = price_fields.get(meas, "price")
            print(f"\nBuilding per-exchange coverage for {label} ({host}) / {meas} ...")
            df = survey_exchange_coverage(
                host,
                measurement=meas,
                price_field=price_field,
                filter_coins=args.coins,
            )
            if df.empty:
                print(f"  (no data found in {meas})")
                continue
            print_coverage(df, host)

            out = Path(os.environ.get("JST_ANALYTICS_DIR", Path.home() / "Documents/jst/analytics")).expanduser() / "audits" / f"influx_coverage_{label}_{meas}.csv"
            out.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(out, index=False)
            print(f"\n  → saved to {out}")

    # ── optional side-by-side comparison ──
    if args.compare:
        for label, host in hosts_to_survey.items():
            compare_exchanges(host, args.compare, args.coin, measurement=args.measurement)
