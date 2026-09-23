#!/usr/bin/env python3
"""Preview or repair verified historical EMA archive prefixes, preserving all observations."""
from __future__ import annotations
import argparse
import hashlib
import io
import json
from itertools import count
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config
import pandas as pd
from botocore.exceptions import ClientError
from umm.tools.s3.client import s3_client
from umm.analytics.market_data._common import parse_chunk_name

BUCKET = 'jstdata'
# Explicit mappings preserve producer output identities, before pipeline alias normalization.
SOURCES = {
    'EMA_BCH_BITSTAMP': ('DERIVED_BITSTAMP_BNBSPOT', {'PAIR-BCH-USD'}),
    'EMA_LTC_BITSTAMP': ('DERIVED_BITSTAMP_BNBSPOT', {'PAIR-LTC-USD'}),
    'EMA_10SEC_AAVE_BITSTAMP': ('DERIVED_BITSTAMP_BNBSPOT', {'PAIR-AAVE-USD'}),
    'EMA_ALTS_BITSTAMP': ('DERIVED_BITSTAMP_BNBSPOT', {'PAIR-COMP-USD'}),
    'EMA_ALTS_COINBS': ('DERIVED_COINBS_BNBSPOT', {'PAIR-DOGE-USD'}),
    'EMA_ASTER_BITSTAMP': ('DERIVED_BITSTAMP_BNBSPOT', {'PAIR-ASTER-USD'}),
    'EMA_BTC_COINBS': ('DERIVED_COINBS_BNBSPOT', {'PAIR-BTC-USD'}),
    'EMA_ETH_COINBS': ('DERIVED_COINBS_BNBSPOT', {'PAIR-ETH-USD'}),
    'EMA_LUKASZ_BITSTAMP': ('DERIVED_BITSTAMP_BNBSPOT', {'PAIR-BNB-USD', 'PAIR-ETC-USD', 'PAIR-PNUT-USD'}),
    'EMA_UNI_BITSTAMP': ('DERIVED_BITSTAMP_BNBSPOT', {'PAIR-UNI-USD'}),
}
SCOPES = {'bch-ltc': {'EMA_BCH_BITSTAMP', 'EMA_LTC_BITSTAMP'},
          'remaining': set(SOURCES) - {'EMA_BCH_BITSTAMP', 'EMA_LTC_BITSTAMP'}}
COLUMNS = ['timestamp', 'bid_price', 'ask_price', 'bid_qty', 'ask_qty', 'recv']


def client():
    fs = s3_client(creds_file_fallback=True)
    kwargs = dict(aws_access_key_id=fs.key, aws_secret_access_key=fs.secret,
                  aws_session_token=fs.token) if fs.key else {}
    return boto3.client('s3', config=Config(max_pool_connections=64), **kwargs)


def read_object(s3, key):
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
    except ClientError as exc:
        if exc.response['Error']['Code'] in ('NoSuchKey', '404'):
            return None
        raise
    body = response['Body']
    try:
        data = body.read()
    finally:
        body.close()
    return data, response['ETag']


def frame(data, window):
    df = pd.read_parquet(io.BytesIO(data))
    if set(df.columns) != set(COLUMNS) or df.empty:
        raise ValueError('unexpected columns or empty source')
    df = df[COLUMNS].copy()
    for col in ('timestamp', 'recv'):
        df[col] = pd.to_datetime(df[col], utc=True)
    if df.isna().any().any():
        raise ValueError('null values')
    start = pd.Timestamp(window, tz='UTC')
    if not ((df.timestamp >= start) & (df.timestamp < start + pd.Timedelta(minutes=15))).all():
        raise ValueError('records outside filename window')
    return df


def merge(source, destination):
    return pd.concat([source, destination], ignore_index=True).drop_duplicates().sort_values(
        ['timestamp', 'recv'], kind='stable').reset_index(drop=True)


def contains(actual, expected):
    return len(merge(actual, expected)) == len(actual.drop_duplicates())


def target_key(key):
    parts = key.split('/')
    if len(parts) != 6 or parts[0] != 'market_data' or parts[1] not in SOURCES:
        raise ValueError('outside permitted source prefixes')
    alias = parts[1]
    info = parse_chunk_name(parts[-1])
    target, symbols = SOURCES[alias]
    if parts[2] != 'booktop' or parts[3] not in symbols or not info:
        raise ValueError('unexpected source layout')
    if info.recorder_id != 'eu-central-1a_FR01D':
        raise ValueError('unexpected source recorder')
    parts[1] = target
    if not parts[-1].startswith(alias + '_'):
        raise ValueError('unexpected filename')
    parts[-1] = target + parts[-1][len(alias):]
    return '/'.join(parts), str(info.chunk_start)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report-dir', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--scope', choices=sorted(SCOPES), default='bch-ltc')
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--apply', action='store_true')
    modes.add_argument('--delete-verified', action='store_true')
    args = parser.parse_args()
    if not 1 <= args.workers <= 64:
        parser.error('workers must be between 1 and 64')
    args.report_dir.mkdir(parents=True, exist_ok=True)
    s3 = client()
    plan_path = args.report_dir / 'plan.json'
    if not args.apply and not args.delete_verified:
        if plan_path.exists():
            raise ValueError('existing plan: use a new report directory to preserve backups')
        keys = []
        for alias in sorted(SCOPES[args.scope]):
            pages = s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=f'market_data/{alias}/')
            keys.extend(obj['Key'] for page in pages for obj in page.get('Contents', []))
        def preview(key):
            destination, window = target_key(key)
            source = read_object(s3, key)
            if source is None:
                raise ValueError('source disappeared')
            existing = read_object(s3, destination)
            src = frame(source[0], window)
            dst = frame(existing[0], window) if existing else src.iloc[:0]
            backup = args.report_dir / 'backup' / key
            backup.parent.mkdir(parents=True, exist_ok=True)
            backup.write_bytes(source[0])
            if existing:
                old = args.report_dir / 'destination-backup' / destination
                old.parent.mkdir(parents=True, exist_ok=True)
                old.write_bytes(existing[0])
            return dict(source=key, destination=destination, window=window,
                        source_etag=source[1], source_sha256=hashlib.sha256(source[0]).hexdigest(),
                        destination_etag=existing[1] if existing else None,
                        source_rows=len(src), destination_rows=len(dst), merged_rows=len(merge(src, dst)))
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            plan = list(pool.map(preview, sorted(keys)))
        plan_path.write_text(json.dumps(plan, indent=2))
        print(json.dumps({'objects': len(plan), 'destination_conflicts': sum(bool(r['destination_etag']) for r in plan),
                          'source_rows': sum(r['source_rows'] for r in plan), 'plan': str(plan_path)}), flush=True)
        return
    plan = json.loads(plan_path.read_text())
    # Validate every source/destination mapping before performing any writes.
    for row in plan:
        if row['source'].split('/')[1] not in SCOPES[args.scope]:
            raise ValueError('source outside selected scope')
        if target_key(row['source']) != (row['destination'], row['window']):
            raise ValueError('invalid manifest mapping')
    def execute(row):
        backup = (args.report_dir / 'backup' / row['source']).read_bytes()
        if hashlib.sha256(backup).hexdigest() != row['source_sha256']:
            raise ValueError('backup checksum mismatch')
        source = read_object(s3, row['source'])
        if source is None:
            if not args.delete_verified:
                raise ValueError('source disappeared')
        elif source[1] != row['source_etag'] or hashlib.sha256(source[0]).hexdigest() != row['source_sha256']:
            raise ValueError('source changed since preview')
        src = frame(backup, row['window'])
        existing = read_object(s3, row['destination'])
        dst = frame(existing[0], row['window']) if existing else src.iloc[:0]
        if args.apply and not contains(dst, src):
            merged = merge(src, dst)
            output = io.BytesIO()
            # Match the archive's naive-utc timestamp representation.
            for col in ('timestamp', 'recv'):
                merged[col] = merged[col].dt.tz_localize(None)
            merged.to_parquet(output, index=False, compression='snappy')
            condition = {'IfMatch': existing[1]} if existing else {'IfNoneMatch': '*'}
            s3.put_object(Bucket=BUCKET, Key=row['destination'], Body=output.getvalue(), **condition)
        verified = existing if args.delete_verified else read_object(s3, row['destination'])
        if not verified:
            raise ValueError('destination missing')
        actual = frame(verified[0], row['window'])
        previous_path = args.report_dir / 'destination-backup' / row['destination']
        previous = frame(previous_path.read_bytes(), row['window']) if row['destination_etag'] else src.iloc[:0]
        if not contains(actual, src) or not contains(actual, dst) or not contains(actual, previous):
            raise ValueError('destination does not preserve all observations')
        if args.delete_verified and source:
            # Recheck destination identity before conditional source deletion.
            current = s3.head_object(Bucket=BUCKET, Key=row['destination'])
            if current['ETag'] != verified[1]:
                raise ValueError('destination changed during verification')
            s3.delete_object(Bucket=BUCKET, Key=row['source'], IfMatch=row['source_etag'])
            if read_object(s3, row['source']) is not None:
                raise ValueError('source still exists after deletion')
        return dict(source=row['source'], destination=row['destination'], verified_rows=len(actual),
                    status='deleted_verified_source' if args.delete_verified else 'verified_destination')
    # Report every outcome even when some objects fail. Successful objects are safe to rerun.
    completed = count(1)
    def guarded(row):
        try:
            result = execute(row)
        except Exception as exc:
            result = dict(source=row['source'], status='error', error=type(exc).__name__, detail=str(exc))
        n = next(completed)
        if n % 25 == 0 or n == len(plan):
            print(f'checked {n}/{len(plan)} objects', flush=True)
        return result
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(guarded, plan))
    report = args.report_dir / ('deletion.json' if args.delete_verified else 'migration.json')
    report.write_text(json.dumps(results, indent=2))
    errors = sum(r['status'] == 'error' for r in results)
    print(json.dumps({'objects':len(results), 'errors':errors, 'report':str(report)}), flush=True)
    if errors:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
