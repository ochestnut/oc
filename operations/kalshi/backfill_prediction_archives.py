#!/usr/bin/env python3
"""fill missing kalshi booktop archives from retained ty03 history."""
from __future__ import annotations

import argparse
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import closing, nullcontext
from datetime import datetime, timezone
import fcntl
import importlib.util
from io import BytesIO
from hashlib import sha256
import json
import os
import runpy
from pathlib import Path
import sys
import sqlite3
from tempfile import TemporaryDirectory
from time import monotonic, sleep

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from influxdb.exceptions import InfluxDBServerError
import pandas as pd
from requests.exceptions import ConnectionError, Timeout

CATALOG = 'market_data/catalog/symbols/KALSHI/booktop/prediction.parquet'
PREFIX = 'market_data/KALSHI/booktop/predictions/'


def emit(status, **values):
    from umm.tools.progress import clear_progress_line
    clear_progress_line()
    fields = ' '.join(f'{key}={value}' for key, value in values.items()).replace('\n', ' ')
    stamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    print(f'{stamp} [kalshi_backfill] {status} {fields}'.rstrip(), flush=True)


def retry_read(operation, read, **context):
    """three attempts for transient read failures; never wrap a database write."""
    for attempt in range(1, 4):
        try:
            return read()
        except (ConnectionError, Timeout, InfluxDBServerError) as exc:
            if attempt == 3:
                emit('read_failed', operation=operation, attempts=attempt,
                     error_type=type(exc).__name__, error=str(exc), **context)
                raise
            delay = (5, 15)[attempt - 1]
            emit('read_retry', operation=operation, attempt=attempt, wait_seconds=delay,
                 error_type=type(exc).__name__, error=str(exc), **context)
            sleep(delay)


def utc(value):
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        raise ValueError('timestamps must include timezone')
    stamp = stamp.tz_convert('UTC').tz_localize(None)
    if stamp != stamp.floor('15min'):
        raise ValueError('timestamps must align to 15-minute boundaries')
    return stamp


def load_sweep(repo):
    sys.path.insert(0, str(repo / 'src'))
    spec = importlib.util.spec_from_file_location('prediction_sweep', repo / 'scripts/influx_to_s3/sweep/prediction_market_data.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DayCache:
    """run-local disk index; only complete day listings become visible."""
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA cache_size = -4096')
        self.db.execute('CREATE TABLE days (day TEXT PRIMARY KEY)')
        self.db.execute('CREATE TABLE objects (key TEXT PRIMARY KEY, size INTEGER NOT NULL)')

    def __contains__(self, day):
        return self.db.execute('SELECT 1 FROM days WHERE day = ?', (day,)).fetchone() is not None

    def __setitem__(self, day, objects):
        entries = objects.items() if isinstance(objects, dict) else objects
        with self.db:
            self.db.executemany('INSERT OR REPLACE INTO objects VALUES (?, ?)', entries)
            self.db.execute('INSERT INTO days VALUES (?)', (day,))

    def __getitem__(self, day):
        return self

    def size(self, key):
        row = self.db.execute('SELECT size FROM objects WHERE key = ?', (key,)).fetchone()
        return None if row is None else row[0]

    def record(self, key, size):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO objects VALUES (?, ?)', (key, size))

    def close(self):
        self.db.close()


def list_day(s3, bucket, day, instruments=None, progress_interval=30, report=None):
    """stream pages into the disk cache instead of retaining all keys in memory."""
    from umm.tools.progress import Progress, clear_progress_line
    count = 0
    prefixes = [PREFIX + day + '/' + name + '/' for name in instruments] if instruments else [PREFIX + day + '/']
    if report is None:
        clear_progress_line()
    context = (nullcontext() if report else
               Progress(None, 'objects', lambda message: emit('listing', message=message),
                        interval=progress_interval, label=f'[s3 {day}]'))
    with context as progress:
        def listing(force=False):
            if report:
                report(phase='listing', day=day, listed_objects=count, force=force)
            else:
                progress.report(count, active=1, force=force)
        listing(force=True)
        for prefix in prefixes:
            for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    count += 1
                    yield obj['Key'], obj['Size']
                listing()
        if progress is not None:
            progress.complete_progress(count)
        else:
            listing(force=True)


def upload_missing(s3, bucket, key, path):
    """a conditional put prevents overwrites even when another writer wins the race."""
    try:
        with path.open('rb') as handle:
            s3.put_object(Bucket=bucket, Key=key, Body=handle, IfNoneMatch='*')
        return True
    except ClientError as exc:
        if exc.response['ResponseMetadata']['HTTPStatusCode'] != 412:
            raise
        if s3.head_object(Bucket=bucket, Key=key)['ContentLength'] <= 0:
            raise ValueError(f'existing archive is empty: {key}')
        return False


def merge_catalog(s3, bucket, observed):
    """merge only this run's coverage, using compare-and-swap against other writers."""
    if not observed:
        return
    for attempt in range(5):
        try:
            response = s3.get_object(Bucket=bucket, Key=CATALOG)
            body = response['Body']
            try:
                frame = pd.read_parquet(BytesIO(body.read()))
            finally:
                body.close()
            condition = {'IfMatch': response['ETag']}
        except ClientError as exc:
            if exc.response['ResponseMetadata']['HTTPStatusCode'] != 404:
                raise
            frame = pd.DataFrame(columns=['symbol', 'first_seen', 'last_seen'])
            condition = {'IfNoneMatch': '*'}
        if not frame['symbol'].is_unique:
            raise ValueError('duplicate catalog symbols')
        frame = frame.set_index('symbol')
        changed = False
        for symbol, (first, last) in observed.items():
            if symbol not in frame.index:
                frame.loc[symbol, ['first_seen', 'last_seen']] = [first, last]
                changed = True
                continue
            for col, value, select in [('first_seen', first, min), ('last_seen', last, max)]:
                old = frame.at[symbol, col]
                merged = value if pd.isna(old) else select(str(old)[:10], value)
                if pd.isna(old) or str(old)[:10] != merged:
                    frame.at[symbol, col] = merged
                    changed = True
        if not changed:
            return
        payload = BytesIO()
        frame.reset_index().to_parquet(payload, index=False)
        try:
            s3.put_object(Bucket=bucket, Key=CATALOG, Body=payload.getvalue(), **condition)
            return
        except ClientError as exc:
            if exc.response['ResponseMetadata']['HTTPStatusCode'] not in (409, 412):
                raise
    raise RuntimeError('catalog changed repeatedly; checkpoint not advanced')


class TooManyRows(ValueError):
    pass


def read_window(client, policies, start, end, max_rows, instruments):
    """read exact contracts serially across policies; oversized reads are split."""
    from umm.analytics.storage.influx import coalesce_influx_columns
    frames = defaultdict(list)
    count = 0
    ids = ' OR '.join('"instrument_id" = ' + "'" + name.replace('\\', '\\\\').replace("'", "\\'") + "'" for name in instruments)
    for policy in policies:
        escaped = policy.replace('\\', '\\\\').replace('"', '\\"')
        query = (
            f'SELECT * FROM "{escaped}"."md_booktop_pred" '
            f"WHERE \"exchange_id\" = 'KALSHI' AND ({ids}) AND time >= {start.value} AND time < {end.value} "
            f'LIMIT {max_rows + 1}')
        result = retry_read('source', lambda: client.query(query, epoch='ns'),
                            policy=policy, start=start.isoformat(), end=end.isoformat(),
                            contracts=len(instruments), first=instruments[0], last=instruments[-1])
        if result.raw.get('partial') or any(series.get('partial') for series in result.raw.get('series', [])):
            raise ValueError('partial influx response; batch not archived')
        rows = list(result.get_points())
        count += len(rows)
        if count > max_rows:
            raise TooManyRows('source slice exceeds --max-rows')
        if rows:
            frame = coalesce_influx_columns(pd.DataFrame(rows)).rename(columns={'time': 'timestamp'})
            if not frame['instrument_id'].isin(instruments).all() or not frame['exchange_id'].eq('KALSHI').all():
                raise ValueError('unexpected source identity')
            if not frame['timestamp'].map(lambda t: type(t) is int and start.value <= t < end.value).all():
                raise ValueError('unexpected source timestamp')
            frame['chunk'] = pd.to_datetime(frame['timestamp'], unit='ns').dt.floor('15min')
            for (symbol, chunk), group in frame.groupby(['instrument_id', 'chunk']):
                frames[(symbol, chunk)].append(group)
    return frames, count


def source_slices(client, policies, start, end, max_rows, instruments):
    try:
        result = read_window(client, policies, start, end, max_rows, instruments)
    except TooManyRows:
        pass
    else:
        yield result
        return
    # Release the oversized response's exception traceback before recursive reads.
    if len(instruments) > 1:
        half = len(instruments) // 2
        yield from source_slices(client, policies, start, end, max_rows, instruments[:half])
        yield from source_slices(client, policies, start, end, max_rows, instruments[half:])
    elif end - start > pd.Timedelta(minutes=15):
        middle = (start + (end-start)/2).floor('15min')
        yield from source_slices(client, policies, start, middle, max_rows, instruments)
        yield from source_slices(client, policies, middle, end, max_rows, instruments)
    else:
        raise ValueError('one contract/chunk exceeds --max-rows; no checkpoint advance')


def archive_frames(frames, *, s3, bucket, fetcher, pool, temporary, day_cache,
                   instruments, apply, workers, counts, observed, progress_interval=30, report=None):
    from umm.analytics.market_data._common import local_path
    pending = deque()
    completed_files = 0

    def activity(phase='archiving', **details):
        if report:
            report(phase=phase, slice_files=f'{completed_files}/{len(frames)}', **details)

    def file_done():
        nonlocal completed_files
        completed_files += 1
        activity()

    activity(force=True)

    def coverage(symbol, day):
        first, last = observed.get(symbol, (day, day))
        observed[symbol] = (min(first, day), max(last, day))

    def finish_one():
        future, path, existing, key, symbol, day = pending.popleft()
        if report:
            while not wait([future], timeout=progress_interval).done:
                activity(phase='uploading')
        counts['uploaded'] += int(future.result())
        existing.record(key, path.stat().st_size)
        path.unlink()
        coverage(symbol, day)
        file_done()

    for (symbol, chunk), raw in frames.items():
        day = chunk.strftime('%Y-%m-%d')
        if day not in day_cache:
            day_cache[day] = list_day(s3, bucket, day, instruments, progress_interval,
                                      activity if report else None)
            activity(force=True)
        existing = day_cache[day]
        key = local_path(Path('market_data'), 'KALSHI', 'booktop', symbol, 'TY03', chunk).as_posix()
        size = existing.size(key)
        if size is not None:
            if size <= 0:
                counts['invalid'] += 1
                file_done()
                continue
            counts['existing'] += 1
            coverage(symbol, day)
            file_done()
            continue
        counts['missing'] += 1
        path = Path(temporary) / (sha256(key.encode()).hexdigest() + '.parquet')
        if not fetcher._write_chunk_frames(raw, path, 'KALSHI', symbol, chunk,
                                          chunk + pd.Timedelta(minutes=15)):
            counts['invalid'] += 1
            path.unlink(missing_ok=True)
            file_done()
            continue
        if not apply:
            path.unlink(missing_ok=True)
            file_done()
            continue
        pending.append((pool.submit(upload_missing, s3, bucket, key, path),
                        path, existing, key, symbol, day))
        if len(pending) >= workers:
            finish_one()
    while pending:
        finish_one()


def save_state(path, scope, after):
    temporary = path.with_suffix('.tmp')
    with temporary.open('w') as handle:
        json.dump(dict(scope=scope, after=after), handle)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[3] / 'umm')
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--max-rows', type=int, default=500000)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--batch-size', type=int, default=50, choices=range(1, 51))
    parser.add_argument('--s3-workers', type=int, default=8, choices=range(1, 17))
    parser.add_argument('--catalog-every', type=int, default=5, choices=range(1, 21),
                        help='merge catalog and checkpoint every N batches (default: 5)')
    parser.add_argument('--progress-interval', type=float, default=30,
                        help='seconds between progress reports (default: 30)')
    parser.add_argument('--instruments', nargs='+', help='optional exact contract ids for a trial')
    args = parser.parse_args()
    start, end = utc(args.start), utc(args.end)
    if start >= end or end > pd.Timestamp.now(tz='UTC').tz_localize(None) - pd.Timedelta(days=7):
        parser.error('range must be positive and end at least seven days ago')
    if not 0 < args.progress_interval < float('inf'):
        parser.error('--progress-interval must be positive and finite')
    if args.max_rows < 1:
        parser.error('--max-rows must be positive')
    sweep = load_sweep(args.repo.resolve())
    from umm.tools.progress import Progress
    from influxdb import InfluxDBClient
    from umm.analytics.config import load_config
    from umm.analytics.market_data.api import MarketData
    cfg = load_config()
    conn = MarketData._catalog_conn_args('TY03', cfg, timeout=60)
    conn['retries'] = 1
    s3 = boto3.client('s3', config=Config(max_pool_connections=max(10, args.s3_workers),
                      connect_timeout=10, read_timeout=60,
                      retries={'mode': 'standard', 'total_max_attempts': 3}))
    # fail before reading history if the installed sdk lacks conditional puts.
    members = s3.meta.service_model.operation_model('PutObject').input_shape.members
    if not {'IfMatch', 'IfNoneMatch'} <= members.keys():
        raise RuntimeError('boto3 must support conditional PutObject')
    scope = dict(source={k: conn[k] for k in ('host', 'port', 'database')}, bucket=cfg['S3_BUCKET'],
                 exchange='KALSHI', recorder='TY03', start=start.isoformat(), end=end.isoformat(),
                 instruments=sorted(args.instruments or []))
    args.state.parent.mkdir(parents=True, exist_ok=True)
    with args.state.with_suffix('.lock').open('a') as lock, TemporaryDirectory(prefix='kalshi_backfill_') as temporary, \
            closing(DayCache(Path(temporary) / 'day_cache.sqlite')) as day_cache, \
            ThreadPoolExecutor(max_workers=args.s3_workers) as pool:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        after = ''
        if args.state.exists():
            saved = json.loads(args.state.read_text())
            if saved['scope'] != scope:
                raise ValueError('state belongs to a different range/source')
            after = saved['after']
        fetcher = sweep._make_fetcher('booktop', 'md_booktop_pred', 1)
        client = InfluxDBClient(**conn)
        progress = None
        try:
            policies = [p['name'] for p in retry_read('retention_policies',
                        lambda: client.get_list_retention_policies(conn['database']))]
            if not policies:
                raise ValueError('no retention policies')
            emit('started', source=f"{conn['host']}/{conn['database']}",
                 start=start.isoformat(), end=end.isoformat(), resume_after=after or 'none',
                 apply=args.apply, s3_workers=args.s3_workers,
                 batch_size=args.batch_size, catalog_every=args.catalog_every)
            cleanup = runpy.run_path(str(args.repo / 'scripts/s3/cleanup_prediction_series.py'))
            result = retry_read('discovery', lambda: client.query('SHOW TAG VALUES FROM "md_booktop_pred" WITH KEY = "instrument_id" '
                                  "WHERE \"exchange_id\" = 'KALSHI'"))
            points = cleanup['points'](result)
            instruments = sorted({r['value'] for r in points
                                  if (expiry := cleanup['expiry_ns'](r['value'])) is not None
                                  and expiry < end.value and r['value'] > after
                                  and (not args.instruments or r['value'] in args.instruments)})
            emit('discovered', contracts=len(instruments))
            totals = dict(rows=0, existing=0, missing=0, uploaded=0, invalid=0)
            progress = Progress(len(instruments), 'contracts', lambda message: emit('progress', message=message),
                                interval=args.progress_interval)
            progress.report(0, active=int(bool(instruments)), force=True, uploaded=0, saved=0)
            saved_contracts = 0
            observed = {}
            pending_batches = 0
            for offset in range(0, len(instruments), args.batch_size):
                batch = instruments[offset:offset+args.batch_size]
                clock = monotonic()
                counts = dict(rows=0, existing=0, missing=0, uploaded=0, invalid=0)
                def report_activity(*, phase, force=False, **details):
                    # Only completed batches count as processed contracts. File activity
                    # is scoped to a source slice because oversized batches can split.
                    progress.report(offset, active=1, force=force, phase=phase,
                                    batch=f'{offset + 1}-{offset + len(batch)}',
                                    uploaded=totals['uploaded'] + counts['uploaded'],
                                    saved=saved_contracts, pending_batches=pending_batches,
                                    **details)

                source_seconds = archive_seconds = catalog_seconds = 0.0
                slices = iter(source_slices(client, policies, start, end, args.max_rows, batch))
                while True:
                    report_activity(phase='reading', force=True)
                    stage = monotonic()
                    try:
                        frames, rows = next(slices)
                    except StopIteration:
                        break
                    source_seconds += monotonic() - stage
                    counts['rows'] += rows
                    stage = monotonic()
                    archive_frames(frames, s3=s3, bucket=cfg['S3_BUCKET'], fetcher=fetcher,
                                   pool=pool, temporary=temporary, day_cache=day_cache,
                                   instruments=args.instruments, apply=args.apply,
                                   workers=args.s3_workers, counts=counts, observed=observed,
                                   progress_interval=args.progress_interval, report=report_activity)
                    archive_seconds += monotonic() - stage
                pending_batches += 1
                checkpointed = False
                if args.apply and (pending_batches >= args.catalog_every or
                                   offset + len(batch) == len(instruments)):
                    stage = monotonic()
                    report_activity(phase='catalog', force=True)
                    merge_catalog(s3, cfg['S3_BUCKET'], observed)
                    catalog_seconds = monotonic() - stage
                    save_state(args.state, scope, batch[-1])
                    saved_contracts = offset + len(batch)
                    checkpointed = True
                    observed.clear()
                    pending_batches = 0
                elif not args.apply:
                    observed.clear()
                    pending_batches = 0
                for name, value in counts.items():
                    totals[name] += value
                progress.report(offset + len(batch), active=int(offset + len(batch) < len(instruments)),
                                force=checkpointed, uploaded=totals['uploaded'], existing=totals['existing'],
                                invalid=totals['invalid'], saved=saved_contracts,
                                pending_batches=pending_batches,
                                batch_s=round(monotonic()-clock, 1), source_s=round(source_seconds, 1),
                                archive_s=round(archive_seconds, 1), catalog_s=round(catalog_seconds, 1))
            progress.finish(persist=True)
            progress = None
            emit('complete', apply=args.apply, contracts=len(instruments), saved=saved_contracts, **totals)
        finally:
            if progress is not None:
                progress.finish(persist=False)
            client.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        emit('stopped', error=str(exc))
        raise
