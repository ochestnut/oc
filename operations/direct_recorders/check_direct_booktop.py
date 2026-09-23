#!/usr/bin/env python3
"""Read-only structural, value, and coverage audit of direct booktop archives."""
import argparse,io,json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import boto3
from botocore.config import Config
import pandas as pd
from umm.tools.s3.client import s3_client
from check_booktop_archives import s3_keys,inventory,compare,CORE,VALUES


def inspect_frame(df,window):
    required=[*CORE,'recv']
    if set(required)-set(df.columns):raise ValueError('missing required columns: '+str(sorted(set(required)-set(df.columns))))
    if df.empty:raise ValueError('empty parquet')
    a=df[required].copy()
    for col in ['timestamp','recv']:
        if not pd.api.types.is_datetime64_any_dtype(a[col]):raise ValueError('timestamp column is not datetime: '+col)
        a[col]=pd.to_datetime(a[col],utc=True,errors='raise').dt.tz_localize(None)
        if a[col].isna().any():raise ValueError('null '+col)
    if not ((a.timestamp>=window)&(a.timestamp<window+pd.Timedelta(minutes=15))).all():raise ValueError('exchange timestamps outside filename window')
    for col in VALUES:a[col]=pd.to_numeric(a[col],errors='raise')
    if a[VALUES].isin([float('inf'),-float('inf')]).any().any():raise ValueError('nonfinite value')
    if a[['bid_price','ask_price']].isna().any().any():raise ValueError('null price')
    latency=(a.recv-a.timestamp).dt.total_seconds()
    return dict(rows=len(a),exact_duplicates=int(a.duplicated().sum()),timestamp_sorted=bool(a.timestamp.is_monotonic_increasing),crossed_quotes=int((a.bid_price>a.ask_price).sum()),locked_quotes=int((a.bid_price==a.ask_price).sum()),nonpositive_price_rows=int((a[['bid_price','ask_price']]<=0).any(axis=1).sum()),negative_quantity_rows=int((a[['bid_qty','ask_qty']]<0).any(axis=1).sum()),null_quantity_rows=int(a[['bid_qty','ask_qty']].isna().any(axis=1).sum()),negative_latency_rows=int((latency<0).sum()),latency_over_5min_rows=int((latency>300).sum()),latency_min_seconds=float(latency.min()),latency_max_seconds=float(latency.max()),first_timestamp=str(a.timestamp.min()),last_timestamp=str(a.timestamp.max()))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--start',required=True);p.add_argument('--end',required=True)
    p.add_argument('--output-dir',type=Path,required=True);p.add_argument('--workers',type=int,default=16)
    p.add_argument('--compare-production',action='store_true');args=p.parse_args()
    def utc(value):
        stamp = pd.Timestamp(value)
        return (stamp.tz_localize('UTC') if stamp.tzinfo is None else stamp.tz_convert('UTC')).tz_localize(None)
    start=utc(args.start);end=utc(args.end)
    if not start<end or start!=start.floor('15min') or end!=end.floor('15min'):p.error('ordered utc quarter-hour boundaries required')
    if end>pd.Timestamp.now(tz='UTC').tz_localize(None)-pd.Timedelta(minutes=1):p.error('use settled windows')
    if not 1<=args.workers<=32:p.error('workers must be 1–32')
    out=args.output_dir;out.mkdir(parents=True,exist_ok=True)
    if (out/'summary.json').exists():p.error('use a fresh report directory')
    scope=dict(start=str(start),end=str(end),bucket='jstdata',compare_production=args.compare_production)
    (out/'summary.json').write_text(json.dumps(dict(completed=False,scope=scope)))
    fs=s3_client(creds_file_fallback=True)
    kwargs=dict(aws_access_key_id=fs.key,aws_secret_access_key=fs.secret,aws_session_token=fs.token) if fs.key else {}
    s3=boto3.client('s3',config=Config(max_pool_connections=64),**kwargs)
    keys=s3_keys(fs,'jstdata',start,end,args.workers)
    (out/'inventory.json').write_text(json.dumps(dict(scope=scope,keys=keys)))
    windows=inventory(keys,start,end,set())
    direct={(e,s,t) for (e,s,r,t),v in windows.items() if v['direct']}
    gaps=[dict(exchange=e,symbol=s,recorder=r,window=str(t),covered_elsewhere=(e,s,t) in direct) for (e,s,r,t),v in windows.items() if v['production'] and not v['direct']]
    (out/'coverage_gaps.json').write_text(json.dumps(gaps,indent=2))
    selected=[(w,v) for w,v in sorted(windows.items()) if v['direct']]
    print('direct windows to inspect',len(selected),flush=True)
    def read(key):
        bucket,k=key.split('/',1);resp=s3.get_object(Bucket=bucket,Key=k);body=resp['Body']
        try:data=body.read()
        finally:body.close()
        after=s3.head_object(Bucket=bucket,Key=k)
        if after['ETag']!=resp['ETag'] or after['ContentLength']!=len(data):raise RuntimeError('object changed while reading')
        return pd.read_parquet(io.BytesIO(data))
    def check(item):
        w,v=item;e,s,r,t=w
        row=dict(exchange=e,symbol=s,recorder=r+'D',window=str(t),keys=v['direct'],pipeline_present=bool(v['production']))
        try:
            if len(v['direct'])!=1:raise ValueError('multiple direct objects for same window')
            frame=read(v['direct'][0]);row.update(inspect_frame(frame,t));row['status']='valid'
            if args.compare_production and v['production']:
                def cached_read(key):return frame if key==v['direct'][0] else read(key)
                c=compare(w,v,cached_read)
                row['comparison']={k:value for k,value in c.items() if k not in ['production_keys','direct_keys','difference_samples']}
        except Exception as exc:row.update(status='error',error=type(exc).__name__+': '+str(exc))
        return row
    results=[]
    with (out/'files.jsonl').open('w') as log,ThreadPoolExecutor(max_workers=args.workers) as pool:
        for i,row in enumerate(pool.map(check,selected),1):
            results.append(row);log.write(json.dumps(row)+'\n');log.flush()
            if i%100==0 or i==len(selected):print('checked',i,'/',len(selected),flush=True)
    metrics=['rows','exact_duplicates','crossed_quotes','locked_quotes','nonpositive_price_rows','negative_quantity_rows','null_quantity_rows','negative_latency_rows','latency_over_5min_rows']
    summary=dict(completed=True,scope=scope,direct_files=len(results),direct_only_files=sum(not r['pipeline_present'] for r in results),statuses=dict(Counter(r['status'] for r in results)),totals={k:sum(r.get(k,0) for r in results) for k in metrics},unsorted_files=sum(not r.get('timestamp_sorted',True) for r in results),by_recorder=dict(Counter(r['recorder'] for r in results)),comparison_statuses=dict(Counter(r['comparison']['status'] for r in results if 'comparison' in r)),missing_pipeline_records=sum(r.get('comparison',{}).get('missing_records',0) for r in results),extra_direct_records=sum(r.get('comparison',{}).get('extra_records',0) for r in results),missing_exact_recorder_windows=len(gaps),missing_anywhere_windows=sum(not r['covered_elsewhere'] for r in gaps))
    (out/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2),flush=True)
    if not results or summary['statuses'].get('error') or summary['comparison_statuses'].get('error'):raise SystemExit(2)

if __name__=='__main__':main()
