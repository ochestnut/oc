#!/usr/bin/env python3
"""plan, verify archived quote changes, and drop old prediction booktop series.

no writes by default. see cleanup_prediction_series.md for the operator workflow.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Any

MEASUREMENT = "md_booktop_pred"
EXCHANGES = {"KALSHI", "GEMINI"}
CHUNK_NS = 15 * 60 * 1_000_000_000
QUOTE_FIELDS = ("bid_price", "ask_price", "bid_quantity", "ask_quantity")


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def ident(value: str) -> str:
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'


def literal(value: str) -> str:
    return "'" + value.replace('\\', '\\\\').replace("'", "\\'") + "'"


def predicate(exchange: str, symbol: str) -> str:
    if exchange not in EXCHANGES or not symbol or any(ord(c) < 32 for c in symbol):
        raise ValueError("invalid candidate identity")
    return f'"exchange_id" = {literal(exchange)} AND "symbol" = {literal(symbol)}'


def cutoff_ns(value: str) -> int:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("--before must include a timezone, e.g. 2026-08-01T00:00:00Z")
    if dt > datetime.now(timezone.utc) - timedelta(days=7):
        raise ValueError("cutoff must be at least seven days ago")
    return int(dt.timestamp()) * 1_000_000_000


def snapshot(client: Any, source: dict, exchange: str, symbol: str, max_rows: int) -> dict:
    """read ALL retained timestamps in ALL policies; DROP SERIES has no time scope."""
    where = predicate(exchange, symbol)
    policies = sorted(p['name'] for p in client.get_list_retention_policies(source['database']))
    if not policies:
        raise ValueError("no retention policies returned")
    records = []
    for policy in policies:
        query = (f'SELECT * FROM {ident(policy)}.{ident(MEASUREMENT)} WHERE {where} '
                 'AND time >= -9223372036854775806 AND time <= 9223372036854775806 '
                 f'ORDER BY time ASC LIMIT {max_rows + 1}')
        result = client.query(query, epoch="ns")
        if any(series.get('partial') for series in getattr(result, 'raw', {}).get('series', [])):
            raise ValueError('partial influx result cannot authorize deletion')
        for point in result.get_points():
            if point.get('exchange_id') != exchange or point.get('symbol') != symbol:
                raise ValueError("unexpected source identity")
            if not isinstance(point.get('time'), int):
                raise ValueError("source timestamp is not integer nanoseconds")
            records.append({'retention_policy': policy, 'point': point})
            if len(records) > max_rows:
                raise ValueError("candidate exceeds --max-rows; nothing may be deleted")
    records.sort(key=canonical)
    return {'version': 1, 'source': source, 'measurement': MEASUREMENT,
            'exchange': exchange, 'symbol': symbol, 'policies': policies, 'records': records}


def is_old(data: dict, before: int) -> bool:
    return bool(data['records']) and all(r['point']['time'] < before for r in data['records'])


def batch_predicate(exchange: str, symbols: list[str]) -> str:
    if not symbols or len(symbols) > 50 or len(set(symbols)) != len(symbols):
        raise ValueError('batch must contain 1–50 distinct symbols')
    return '(' + ' OR '.join(f'({predicate(exchange, symbol)})' for symbol in symbols) + ')'


def snapshot_batch(client: Any, source: dict, exchange: str,
                   symbols: list[str], max_rows: int) -> dict[str, dict]:
    where = batch_predicate(exchange, symbols)
    policies = sorted(p['name'] for p in client.get_list_retention_policies(source['database']))
    if not policies:
        raise ValueError('no retention policies returned')
    result = {symbol: {'version': 1, 'source': source, 'measurement': MEASUREMENT,
                      'exchange': exchange, 'symbol': symbol, 'policies': policies, 'records': []}
              for symbol in symbols}
    count = 0
    for policy in policies:
        response = client.query(
            f'SELECT * FROM {ident(policy)}.{ident(MEASUREMENT)} WHERE {where} '
            'AND time >= -9223372036854775806 AND time <= 9223372036854775806 '
            f'ORDER BY time ASC LIMIT {max_rows + 1}', epoch='ns')
        if any(s.get('partial') for s in getattr(response, 'raw', {}).get('series', [])):
            raise ValueError('partial batch query')
        for point in response.get_points():
            symbol = point.get('symbol')
            if (symbol not in result or point.get('exchange_id') != exchange
                    or not isinstance(point.get('time'), int)):
                raise ValueError('unexpected batch source identity/timestamp')
            count += 1
            if count > max_rows:
                raise ValueError('batch exceeds --max-rows; use a smaller --batch-size')
            result[symbol]['records'].append({'retention_policy': policy, 'point': point})
    for data in result.values():
        data['records'].sort(key=canonical)
    return result


def apply_batch(client: Any, fs: Any, source: dict, bucket: str, candidates: list[dict],
                before: int, max_rows: int, journal: Any, recorder: str, workers: int) -> None:
    exchange = candidates[0]['exchange']
    if any(c['exchange'] != exchange for c in candidates):
        raise ValueError('batch must contain one exchange')
    symbols = [c['symbol'] for c in candidates]
    data = snapshot_batch(client, source, exchange, symbols, max_rows)
    for candidate in candidates:
        current = data[candidate['symbol']]
        if not is_old(current, before) or digest(current) != candidate['sha256']:
            raise ValueError('batch source changed, is empty, or recent; regenerate plan')
    with ThreadPoolExecutor(max_workers=workers) as executor:
        evidence = list(executor.map(lambda symbol: verify_archive(fs, bucket, data[symbol], recorder), symbols))
    if snapshot_batch(client, source, exchange, symbols, max_rows) != data:
        raise ValueError('batch source changed during verification')
    where = batch_predicate(exchange, symbols)
    query = f'DROP SERIES FROM {ident(MEASUREMENT)} WHERE {where}'
    audit(journal, {'status': 'verified_before_batch_drop', 'exchange': exchange,
                    'symbols': symbols, 'archives': evidence, 'query': query})
    client.query(query)
    remaining = client.query(f'SHOW SERIES FROM {ident(MEASUREMENT)} WHERE {where} LIMIT 1')
    if list(remaining.get_points()):
        raise ValueError('series still indexed after batch drop; stop and investigate')
    after = snapshot_batch(client, source, exchange, symbols, max_rows)
    if any(item['records'] for item in after.values()):
        raise ValueError('points remain after batch drop; stop and investigate')
    audit(journal, {'status': 'batch_dropped', 'exchange': exchange, 'symbols': symbols})
    print(f'dropped and verified {len(symbols)} contracts', flush=True)


def quote(point: dict) -> tuple:
    values = tuple(point.get(field) for field in QUOTE_FIELDS)
    if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
           for v in values):
        raise ValueError("null/nonfinite quote fields cannot be verified")
    return values


def changes(points: list[dict]) -> list[tuple]:
    """same quote compression as the exporter, but reject timestamp conflicts."""
    by_time = {}
    for point in points:
        stamp, values = point['time'], quote(point)
        if stamp in by_time and by_time[stamp] != values:
            raise ValueError("conflicting source quotes at one timestamp")
        by_time[stamp] = values
    result, previous = [], None
    for stamp, values in sorted(by_time.items()):
        if values != previous:
            result.append((stamp, *values))
        previous = values
    return result


def verify_quotes(expected: list[tuple], archived: list[tuple]) -> None:
    by_time = {}
    for row in archived:
        stamp, values = row[0], row[1:]
        if stamp in by_time and by_time[stamp] != values:
            raise ValueError("conflicting s3 quotes at one timestamp")
        by_time[stamp] = values
    for row in expected:
        if by_time.get(row[0]) != row[1:]:
            raise ValueError("s3 is missing or differs at a retained quote change")


def verify_archive(fs: Any, bucket: str, data: dict, recorder: str) -> list[dict]:
    """verify canonical 15-minute parquet content, scoped to this recorder."""
    import pandas as pd
    from umm.analytics.market_data._common import local_path
    windows = {}
    for record in data['records']:
        point = record['point']
        instrument = point.get('instrument_id')
        if not isinstance(instrument, str) or not instrument.startswith('PRED-'):
            raise ValueError("missing prediction instrument identity")
        start = (point['time'] // CHUNK_NS) * CHUNK_NS
        windows.setdefault((record['retention_policy'], instrument, start), []).append(point)
    evidence = []
    for (_, instrument, start), points in sorted(windows.items()):
        dt = pd.Timestamp(start, unit='ns').to_pydatetime()
        relative = local_path(Path('market_data'), data['exchange'], 'booktop',
                              instrument, recorder, dt).as_posix()
        key = f'{bucket}/{relative}'
        fs.invalidate_cache(key)
        # missing files, missing columns, unreadable objects all block deletion.
        with fs.open(key, 'rb') as handle:
            frame = pd.read_parquet(handle)
        required = {'timestamp', 'bid_price', 'ask_price', 'bid_qty', 'ask_qty'}
        if not required.issubset(frame.columns):
            raise ValueError(f"s3 columns missing: {sorted(required - set(frame.columns))}")
        if 'symbol' in frame and not frame['symbol'].eq(instrument).all():
            raise ValueError('s3 symbol column does not match the source instrument')
        timestamps = pd.to_datetime(frame['timestamp'], utc=True, errors='raise')
        archived = []
        for stamp, (_, row) in zip(timestamps, frame.iterrows()):
            if pd.isna(stamp) or not start <= stamp.value < start + CHUNK_NS:
                raise ValueError('s3 timestamp outside expected chunk')
            values = quote({'bid_price': row['bid_price'], 'ask_price': row['ask_price'],
                            'bid_quantity': row['bid_qty'], 'ask_quantity': row['ask_qty']})
            archived.append((stamp.value, *values))
        expected = changes(points)
        verify_quotes(expected, archived)
        evidence.append({'key': key, 'source_rows': len(points),
                         'quote_changes': len(expected), 'quote_sha256': digest(expected)})
    if not evidence:
        raise ValueError('no archive evidence')
    return evidence


def audit(handle: Any, event: dict) -> None:
    handle.write(json.dumps({'at': datetime.now(timezone.utc).isoformat(), **event}) + '\n')
    handle.flush()
    os.fsync(handle.fileno())


def apply_candidate(client: Any, fs: Any, source: dict, bucket: str, candidate: dict,
                    before: int, max_rows: int, journal: Any, recorder: str = "TY03") -> None:
    exchange, symbol = candidate['exchange'], candidate['symbol']
    data = snapshot(client, source, exchange, symbol, max_rows)
    if not is_old(data, before) or digest(data) != candidate['sha256']:
        raise ValueError("source changed, is empty, or is too recent; regenerate the plan")
    evidence = verify_archive(fs, bucket, data, recorder)
    # catch changes during archive verification; concurrent replay still has a race.
    if snapshot(client, source, exchange, symbol, max_rows) != data:
        raise ValueError("source changed during verification")
    query = f'DROP SERIES FROM {ident(MEASUREMENT)} WHERE {predicate(exchange, symbol)}'
    audit(journal, {'status': 'verified_before_drop', 'exchange': exchange,
                    'symbol': symbol, 'archives': evidence, 'sha256': digest(data), 'query': query})
    client.query(query)  # never automatically retry a destructive request
    remaining = client.query(f'SHOW SERIES FROM {ident(MEASUREMENT)} '
                             f'WHERE {predicate(exchange, symbol)} LIMIT 1')
    if list(remaining.get_points()):
        raise ValueError("series still indexed after drop; stop and investigate")
    if snapshot(client, source, exchange, symbol, max_rows)['records']:
        raise ValueError("points still present after drop; stop and investigate")
    audit(journal, {'status': 'dropped', 'exchange': exchange, 'symbol': symbol,
                    'archives': evidence, 'sha256': digest(data)})


def dependencies(recorder: str) -> tuple[Any, Any, dict, str]:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'umm' / 'src'))
    from influxdb import InfluxDBClient
    from umm.analytics.config import load_config
    from umm.analytics.market_data.api import MarketData
    from umm.tools.s3.client import s3_client
    cfg = load_config()
    conn = MarketData._catalog_conn_args(recorder, cfg, timeout=60)
    # influxdb-python uses 1 for one attempt; 0 means unlimited retries.
    # do not let its default retries repeat a DROP after an ambiguous timeout.
    conn['retries'] = 1
    source = {k: conn[k] for k in ('host', 'port', 'database')}
    client = InfluxDBClient(**conn)
    client.cleanup_connection_args = conn
    return client, s3_client(), source, cfg['S3_BUCKET']


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--recorder', default='TY03')
    parser.add_argument('--max-rows', type=int, default=250000)
    sub = parser.add_subparsers(dest='mode', required=True)
    plan = sub.add_parser('plan', help='read-only verification of existing s3 parquet')
    plan.add_argument('--before', required=True)
    plan.add_argument('--exchange', choices=sorted(EXCHANGES), required=True)
    plan.add_argument('--symbol', action='append', help='exact venue ticker; repeatable')
    plan.add_argument('--limit', type=int, default=100, help='maximum symbols to examine')
    plan.add_argument('--offset', type=int, default=0, help='discovery page offset')
    plan.add_argument('--workers', type=int, default=1, choices=range(1, 5),
                      help='independent read-only verification workers (maximum four)')
    plan.add_argument('--out', type=Path, required=True)
    apply = sub.add_parser('apply', help='reverify an existing plan and remove only verified series')
    apply.add_argument('--plan', type=Path, required=True)
    apply.add_argument('--journal', type=Path, required=True)
    apply.add_argument('--batch-size', type=int, default=1, choices=range(1, 51))
    apply.add_argument('--workers', type=int, default=1, choices=range(1, 5))
    concurrency = apply.add_mutually_exclusive_group(required=True)
    concurrency.add_argument('--writers-paused', action='store_true')
    concurrency.add_argument('--allow-live-writers', action='store_true',
                             help='accept the concurrent-write race for confirmed expired contracts')
    apply.add_argument('--confirmed-expired', action='store_true', required=True)
    args = parser.parse_args()
    if args.max_rows < 1:
        parser.error('--max-rows must be positive')
    if args.mode == 'plan':
        if args.limit < 1 or args.offset < 0:
            parser.error('limit must be positive and offset nonnegative')
        before = cutoff_ns(args.before)
    else:
        saved = json.loads(args.plan.read_text())
        if saved.get('version') != 1 or saved.get('measurement') != MEASUREMENT:
            raise ValueError('unsupported plan')
        before = cutoff_ns(saved['before'])
    client, fs, source, bucket = dependencies(args.recorder)
    try:
        if args.mode == 'apply':
            if saved['source'] != source or saved['bucket'] != bucket or saved['recorder'] != args.recorder:
                raise ValueError('plan source/bucket does not match current configuration')
            if not saved['candidates']:
                raise ValueError('plan has no verified candidates')
            # exclusive creation prevents accidental reuse/overwrite of an audit log.
            with args.journal.open('x') as journal:
                audit(journal, {'status': 'started', 'allow_live_writers': args.allow_live_writers,
                                'confirmed_expired': args.confirmed_expired})
                for offset in range(0, len(saved['candidates']), args.batch_size):
                    batch = saved['candidates'][offset:offset + args.batch_size]
                    try:
                        if args.batch_size == 1:
                            apply_candidate(client, fs, source, bucket, batch[0],
                                            before, args.max_rows, journal, args.recorder)
                        else:
                            apply_batch(client, fs, source, bucket, batch,
                                        before, args.max_rows, journal, args.recorder, args.workers)
                    except Exception as exc:
                        audit(journal, {'status': 'stopped', 'candidates': batch,
                                        'error': str(exc)})
                        raise
            return 0
        if args.symbol:
            symbols = list(dict.fromkeys(args.symbol))[:args.limit]
        else:
            result = client.query(f'SHOW TAG VALUES FROM {ident(MEASUREMENT)} WITH KEY = "symbol" '
                                  f'WHERE "exchange_id" = {literal(args.exchange)} '
                                  f'LIMIT {args.limit} OFFSET {args.offset}')
            symbols = sorted({p['value'] for p in result.get_points()})
        report = {'version': 1, 'source': source, 'bucket': bucket,
                  'measurement': MEASUREMENT, 'before': args.before, 'recorder': args.recorder,
                  'candidates': [], 'checks': []}
        def inspect_symbol(symbol: str) -> dict:
            # a separate requests session for each task; never share the influx client.
            worker_client = client.__class__(**client.cleanup_connection_args)
            check = {'exchange': args.exchange, 'symbol': symbol}
            try:
                data = snapshot(worker_client, source, args.exchange, symbol, args.max_rows)
                check['rows'] = len(data['records'])
                if not is_old(data, before):
                    check['status'] = 'skip_empty_or_recent'
                else:
                    evidence = verify_archive(fs, bucket, data, args.recorder)
                    check.update(status='verified', sha256=digest(data), archives=evidence)
            except Exception as exc:
                check.update(status='blocked', error=str(exc))
            finally:
                worker_client.close()
            return check

        # exclusive output creation avoids replacing a previously reviewed plan.
        with args.out.open('x') as handle:
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                for check in executor.map(inspect_symbol, symbols):
                    if check['status'] == 'verified':
                        report['candidates'].append(dict(check))
                    report['checks'].append(check)
                    print(json.dumps(check), flush=True)
            json.dump(report, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        print(f"verified {len(report['candidates'])}/{len(symbols)} candidates; no influx data deleted")
        return 1 if any(c['status'] == 'blocked' for c in report['checks']) else 0
    finally:
        client.close()


if __name__ == '__main__':
    raise SystemExit(main())
