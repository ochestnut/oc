#!/usr/bin/env python3
"""read-only contract freshness monitor; uses existing influx credentials."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from functools import lru_cache
import importlib.util
import json
import math
from pathlib import Path
import sys
import time
from zoneinfo import ZoneInfo


SCHEMA_PATH = Path("src/umm/exchanges/kalshi/pred/schema.py")


def default_umm_repo():
    """prefer the sibling checkout when running from oc."""
    sibling = Path(__file__).resolve().parents[3] / "umm"
    return sibling if (sibling / SCHEMA_PATH).is_file() else Path.home() / "umm"


@lru_cache(maxsize=4)
def load_schema(repo=None):
    """load the recorder's symbol rules without changing the python import path."""
    path = (repo or default_umm_repo()).expanduser().resolve() / SCHEMA_PATH
    if not path.is_file():
        raise FileNotFoundError(f"kalshi schema not found: {path}; set --repo to an umm checkout")
    name = "_kalshi_freshness_schema"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    # dataclasses resolves the defining module through sys.modules.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def utc_seconds(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def summarize(latest, expected, previous, now, stale_seconds):
    """latest points are sampled observations, not a complete message stream."""
    rows = []
    for symbol in sorted(set(latest) | set(expected)):
        contracts = expected.get(symbol, [])
        point = latest.get(symbol)
        row = dict(symbol=symbol, tickers=[m['ticker'] for m in contracts],
                   expected_open=bool(contracts), collision=len(contracts) > 1,
                   event_age_s=None, receive_age_s=None, receipt_delay_ms=None,
                   observed_after_receive_s=None)
        if point is None:
            row['status'] = 'no_recent_record'
        else:
            event = utc_seconds(point['time'])
            received = int(point['recv']) / 1e9
            row.update(event_age_s=round(now-event, 3), receive_age_s=round(now-received, 3),
                       receipt_delay_ms=round((received-event)*1000, 3))
            row['status'] = 'old_or_quiet' if now-event > stale_seconds else 'recent'
            if received < event or event > now or received > now:
                row['status'] = 'clock_or_timestamp_check'
            # The initial snapshot is a baseline, not evidence of newly visible data.
            identity = (point['time'], point['recv'])
            if previous is not None and previous.get(symbol) != identity:
                row['observed_after_receive_s'] = round(now-received, 3)
        rows.append(row)
    return rows


def discover(session, underlyings, repo=None):
    schema = load_schema(repo)
    markets = []
    errors = []
    for series, underlying in schema.SERIES_TO_UNDERLYING.items():
        if underlying not in underlyings:
            continue
        cursor = None
        try:
            for _ in range(50):
                params = dict(series_ticker=series, status='open', limit=200)
                if cursor:
                    params['cursor'] = cursor
                response = session.get('https://api.elections.kalshi.com/trade-api/v2/markets',
                                       params=params, timeout=10)
                response.raise_for_status()
                page = response.json()
                markets.extend(page['markets'])
                cursor = page.get('cursor')
                if not cursor:
                    break
            else:
                errors.append(series + ': pagination limit reached')
        except Exception as exc:
            errors.append(series + ': ' + type(exc).__name__)
    return markets, errors


def open_contracts(markets, now, repo=None):
    schema = load_schema(repo)
    expected = defaultdict(list)
    errors = []
    for market in markets:
        try:
            if not utc_seconds(market['open_time']) <= now < utc_seconds(market['close_time']):
                continue
            underlying, expiry, strike, *_ = schema.kalshi_breakdown_exchange_symbol(market['ticker'])
            symbol = schema.make_unified_jst_symbol(underlying, expiry.replace(tzinfo=ZoneInfo('America/New_York')), strike)
            expected[symbol].append(market)
        except (KeyError, ValueError) as exc:
            errors.append(str(market.get('ticker', '?')) + ': ' + type(exc).__name__)
    return expected, errors


def read_latest(client, lookback_minutes, underlyings):
    # Underlyings are validated CLI choices, not arbitrary query fragments.
    pattern = '^PRED-(' + '|'.join(underlyings) + ')_'
    result = client.query(
        "SELECT LAST(local_recv_time) AS recv FROM md_booktop_pred "
        "WHERE exchange_id='KALSHI' AND instrument_id =~ /" + pattern + '/ '
        f'AND time > now()-{lookback_minutes}m GROUP BY instrument_id')
    latest = {}
    for (_, tags), points in result.items():
        for point in points:
            if point.get('recv') is not None:
                latest[tags['instrument_id']] = point
    return latest


def print_report(report, limit):
    rows = report['contracts']
    print('\n' + report['sampled_utc'] + '  ' + str(dict(Counter(r['status'] for r in rows))), flush=True)
    print(f"open markets={report['open_markets']} query_seconds={report['query_seconds']:.2f} "
          f"discovery_errors={len(report['discovery_errors'])}")
    for error in report['discovery_errors']:
        print('discovery incomplete: ' + error)
    print('symbol                                            open status                    event_age_s recv_delay_ms observed_after_recv_s')
    ordered = sorted(rows, key=lambda r: (not r['expected_open'], r['status'] != 'no_recent_record', -(r['event_age_s'] or 0)))
    for row in ordered[:limit]:
        print(f"{row['symbol']:<49} {'yes' if row['expected_open'] else 'no':<4} {row['status']:<25} {str(row['event_age_s']):>11} "
              f"{str(row['receipt_delay_ms']):>13} {str(row['observed_after_receive_s']):>21}"
              + (' collision' if row['collision'] else ''))
    print(f'showing {min(limit, len(rows))}/{len(rows)} contracts; full rows available with --json-output', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--credentials', type=Path, default=Path.home()/'.creds')
    parser.add_argument('--repo', type=Path, default=default_umm_repo(),
                        help='umm checkout containing the recorder symbol schema (default: sibling umm or ~/umm)')
    parser.add_argument('--underlying', nargs='+', choices=['BTC', 'ETH', 'SOL', 'XRP'], default=['BTC', 'ETH', 'SOL', 'XRP'])
    parser.add_argument('--interval', type=float, default=15)
    parser.add_argument('--stale-seconds', type=float, default=120)
    parser.add_argument('--lookback-minutes', type=int, default=30)
    parser.add_argument('--rows', type=int, default=30)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--json-output', type=Path, help='append complete snapshots as json lines')
    args = parser.parse_args()
    if any(not math.isfinite(v) or v <= 0 for v in [args.interval, args.stale_seconds]) or min(args.rows, args.lookback_minutes) <= 0:
        parser.error('intervals and sizes must be positive and finite')
    try:
        load_schema(args.repo)
    except (FileNotFoundError, ImportError) as exc:
        parser.error(str(exc))
    import hjson
    import requests
    from influxdb import InfluxDBClient
    creds = hjson.loads(args.credentials.read_text())['INFLUX']
    client = InfluxDBClient(host=args.host, database='UMM_MD', username=creds['username'],
                           password=creds['password'], timeout=15, retries=0)
    session = requests.Session()
    previous = None
    markets, errors = [], []
    refreshed = float('-inf')
    print('read-only monitor; old/absent records can mean quiet markets. latency requires synchronized clocks.')
    print('observed_after_recv is an upper bound including polling/query delay, not database write latency.')
    try:
        while True:
            if time.monotonic()-refreshed >= 60:
                markets, errors = discover(session, args.underlying, args.repo)
                refreshed = time.monotonic()
            start = time.monotonic()
            try:
                latest = read_latest(client, args.lookback_minutes, args.underlying)
            except Exception as exc:
                print('influx query failed: ' + type(exc).__name__, flush=True)
                if args.once:
                    raise SystemExit(1)
                time.sleep(args.interval)
                continue
            now = time.time()
            expected, parse_errors = open_contracts(markets, now, args.repo)
            rows = summarize(latest, expected, previous, now, args.stale_seconds)
            report = dict(sampled_utc=datetime.fromtimestamp(now, timezone.utc).isoformat(),
                          host=args.host, query_seconds=round(time.monotonic()-start, 3),
                          discovery_errors=errors+parse_errors, open_markets=sum(map(len, expected.values())),
                          discovery_age_s=round(time.monotonic()-refreshed, 3), contracts=rows)
            print_report(report, args.rows)
            if args.json_output:
                with args.json_output.open('a') as out:
                    out.write(json.dumps(report)+'\n')
            previous = {s: (p['time'], p['recv']) for s, p in latest.items()}
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        client.close()
        session.close()


if __name__ == '__main__':
    main()
