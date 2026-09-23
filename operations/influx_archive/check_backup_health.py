#!/usr/bin/env python3
"""Read-only Linux host and Influx booktop freshness checks during backups.

Run on the Influx server with its existing Python environment (requires hjson).
This samples symptoms, not causality. Compare with a run after backups finish.
"""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import shlex
import sys
import socket
import subprocess
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import hjson

GIB = 1024 ** 3


def host_sample():
    cpu = [int(n) for n in Path('/proc/stat').read_text().splitlines()[0].split()[1:9]]
    memory = {}
    for line in Path('/proc/meminfo').read_text().splitlines():
        key, value = line.split(':', 1)
        memory[key] = int(value.split()[0]) * 1024
    vm = dict(line.split() for line in Path('/proc/vmstat').read_text().splitlines())
    disks = {}
    for line in Path('/proc/diskstats').read_text().splitlines():
        fields = line.split()
        name = fields[2]
        if name.startswith(('loop', 'ram')) or Path('/sys/class/block', name, 'partition').exists():
            continue
        if len(fields) >= 14:
            disks[name] = {'read_sectors': int(fields[5]), 'write_sectors': int(fields[9]),
                           'busy_ms': int(fields[12])}
    return {'monotonic': time.monotonic(), 'cpu': cpu, 'memory': memory,
            'swap_in_pages': int(vm['pswpin']), 'swap_out_pages': int(vm['pswpout']),
            'disks': disks}


def host_metrics(first, last, stage_dir):
    elapsed = last['monotonic'] - first['monotonic']
    delta = [b - a for a, b in zip(first['cpu'], last['cpu'])]
    total = sum(delta)
    memory = last['memory']
    disk = shutil.disk_usage(stage_dir)
    return {
        'sample_seconds': round(elapsed, 2),
        'cpu_busy_pct': round(100 * (total - delta[3] - delta[4]) / total, 1) if total else None,
        'cpu_iowait_pct': round(100 * delta[4] / total, 1) if total else None,
        'load_1m': os.getloadavg()[0], 'logical_cpus': os.cpu_count(),
        'memory_available_pct': round(100 * memory['MemAvailable'] / memory['MemTotal'], 1),
        'swap_used_gib': round((memory['SwapTotal'] - memory['SwapFree']) / GIB, 2),
        'swap_in_pages_per_second': (last['swap_in_pages'] - first['swap_in_pages']) / elapsed,
        'swap_out_pages_per_second': (last['swap_out_pages'] - first['swap_out_pages']) / elapsed,
        'staging_free_gib': round(disk.free / GIB, 2),
        'disks': {name: {
            'read_mib_per_second': round((values['read_sectors'] - first['disks'][name]['read_sectors']) * 512 / elapsed / 1024**2, 2),
            'write_mib_per_second': round((values['write_sectors'] - first['disks'][name]['write_sectors']) * 512 / elapsed / 1024**2, 2),
            'busy_pct': round((values['busy_ms'] - first['disks'][name]['busy_ms']) / elapsed / 10, 1),
        } for name, values in last['disks'].items() if name in first['disks']},
    }


def literal(value):
    return "'" + value.replace('\\', '\\\\').replace("'", "\\'") + "'"


def fetch_feeds(args):
    query = ('SELECT LAST("local_recv_time") AS "recv" FROM "md_booktop" '
             f'WHERE time >= now() - {args.lookback_minutes}m AND time <= now()')
    if args.feed:
        filters = []
        for exchange, instrument in args.feed:
            filters.append(f'("exchange_id" = {literal(exchange)} AND "instrument_id" = {literal(instrument)})')
        query += ' AND (' + ' OR '.join(filters) + ')'
    query += ' GROUP BY "exchange_id", "instrument_id"'
    with args.creds.open() as stream:
        credentials = hjson.load(stream)['INFLUX']
    request = Request('http://127.0.0.1:8086/query?' + urlencode({'db': args.database, 'q': query, 'epoch': 'ns'}))
    token = base64.b64encode((credentials['username'] + ':' + credentials['password']).encode()).decode()
    request.add_header('Authorization', 'Basic ' + token)
    started = time.monotonic()
    with urlopen(request, timeout=15) as response:
        payload = json.load(response)
    seconds = time.monotonic() - started
    if payload.get('error'):
        raise RuntimeError('Influx query rejected')
    feeds = {}
    for result in payload.get('results', []):
        if result.get('error'):
            raise RuntimeError('Influx query rejected')
        for series in result.get('series', []):
            tags = series.get('tags', {})
            for values in series.get('values', []):
                row = dict(zip(series['columns'], values))
                if row.get('recv') is not None:
                    key = (tags.get('exchange_id', ''), tags.get('instrument_id', ''))
                    feeds[key] = int(row['recv'])
    return feeds, seconds


def feed_metrics(first, last, expected, now_ns, stale_seconds):
    rows = []
    for key in sorted(set(first) | set(last) | set(expected)):
        current = last.get(key)
        age = (now_ns - current) / 1e9 if current is not None else None
        advanced = current > first[key] if current is not None and key in first else None
        status = 'observed'
        if current is None:
            status = 'missing'
        elif age < -1:
            status = 'future_timestamp'
        elif age > stale_seconds:
            status = 'stale'
        elif advanced is False:
            status = 'not_advancing'
        rows.append({'exchange': key[0], 'instrument': key[1], 'receive_age_seconds': age,
                     'advanced': advanced, 'status': status})
    return rows


def remote_checks(args):
    targets = args.ssh or [
        "ubuntu@172.33.10.230", "ubuntu@172.30.72.219",
        "ubuntu@10.8.64.14", "umm2prod@umm2prod-03.jstcapdev.com",
    ]
    reports = []
    failed = False
    source = Path(__file__).read_text()
    for target in targets:
        if target.startswith("-") or any(c.isspace() for c in target):
            raise ValueError("Invalid SSH destination")
        options = ["--seconds", str(args.seconds), "--stale-seconds", str(args.stale_seconds),
                   "--lookback-minutes", str(args.lookback_minutes), "--database", args.database,
                   "--stage-dir", str(args.stage_dir)]
        for exchange, instrument in args.feed:
            options.extend(["--feed", exchange + ":" + instrument])
        command = '"$HOME/umm/venv/bin/python" - ' + shlex.join(options)
        print(f"\n=== {target} ===", flush=True)
        try:
            result = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                 "-o", "ConnectTimeout=10", target, command],
                input=source, text=True, capture_output=True,
                timeout=args.seconds + 90,
            )
            print(result.stdout, end="", flush=True)
            if result.stderr:
                print(result.stderr, end="", file=sys.stderr)
            reports.append({"target": target, "exit_code": result.returncode,
                            "output": result.stdout, "stderr": result.stderr})
            failed |= result.returncode != 0
        except (subprocess.TimeoutExpired, OSError) as exc:
            message = f"SSH check failed: {type(exc).__name__}"
            print(message, flush=True)
            reports.append({"target": target, "error": message})
            failed = True
    if args.output:
        args.output.write_text(json.dumps(reports, indent=2) + "\n")
        print("Combined report:", args.output)
    return int(failed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--all-servers', action='store_true', help='SSH to FR01, TY04, NY01 and TY03')
    parser.add_argument('--ssh', action='append', help='SSH destination; repeat to select servers')
    parser.add_argument('--seconds', type=int, default=10)
    parser.add_argument('--stale-seconds', type=float, default=60)
    parser.add_argument('--lookback-minutes', type=int, default=15)
    parser.add_argument('--feed', action='append', default=[], help='EXCHANGE:INSTRUMENT; repeat to limit query to known active feeds')
    parser.add_argument('--database', default='UMM_MD')
    parser.add_argument('--creds', type=Path, default=Path.home() / '.creds')
    parser.add_argument('--stage-dir', type=Path, default=Path('/tmp'))
    parser.add_argument('--output', type=Path, help='Optional single JSON report')
    args = parser.parse_args()
    if not 1 <= args.seconds <= 60 or not 1 <= args.lookback_minutes <= 60 or not 0 < args.stale_seconds < float('inf'):
        parser.error('Use seconds/lookback in 1..60 and a finite positive stale threshold')
    if any(':' not in feed or not all(feed.split(':', 1)) for feed in args.feed):
        parser.error('--feed must be EXCHANGE:INSTRUMENT')
    args.feed = [tuple(feed.split(':', 1)) for feed in args.feed]
    if args.all_servers or args.ssh:
        return remote_checks(args)
    print(f'Sampling {socket.gethostname()} for {args.seconds}s; two read-only Influx queries...', flush=True)
    issues = []
    samples = []
    timings = []
    for number in range(2):
        try:
            rows, elapsed = fetch_feeds(args)
            samples.append(rows)
            timings.append(elapsed)
        except Exception as exc:
            samples.append({})
            timings.append(None)
            issues.append(f'Influx sample {number + 1} failed ({type(exc).__name__}); freshness is unassessed')
        if number == 0:
            first_host = host_sample()
            time.sleep(args.seconds)
            last_host = host_sample()
    metrics = host_metrics(first_host, last_host, args.stage_dir)
    feeds = feed_metrics(*samples, args.feed, time.time_ns(), args.stale_seconds)
    if not feeds:
        issues.append('No booktop feeds observed in query window')
    if metrics['staging_free_gib'] < 20:
        issues.append('Less than 20 GiB free on staging filesystem')
    if metrics['memory_available_pct'] < 10:
        issues.append('Less than 10% memory available')
    if metrics['cpu_iowait_pct'] is not None and metrics['cpu_iowait_pct'] > 10:
        issues.append('CPU I/O wait above 10%')
    if metrics['cpu_busy_pct'] is not None and metrics['cpu_busy_pct'] > 90:
        issues.append('CPU busy above 90%')
    if metrics['swap_out_pages_per_second'] > 0:
        issues.append('Swap-out activity during sample')
    flagged = [row for row in feeds if row['status'] != 'observed']
    if flagged:
        issues.append(f'{len(flagged)} feeds need review; quiet feeds may legitimately not advance')
    try:
        processes = subprocess.check_output(['ps', '-eo', 'pid,comm,pcpu,pmem,args'], text=True, timeout=5)
        processes = [' '.join(line.split()[:4]) for line in processes.splitlines()
                     if 'backup_influx_to_s3.py' in line or 'influxd' in line or 'log_to_influx' in line]
    except Exception:
        processes = []
    report = {'host': socket.gethostname(), 'time_utc': datetime.now(timezone.utc).isoformat(),
              'status': 'REVIEW' if issues else 'NO_FLAGS_IN_SAMPLE', 'host_metrics': metrics,
              'query_seconds': timings, 'feeds': feeds, 'issues': issues, 'processes_pid_name_cpu_mem': processes,
              'limitations': 'Booktop only. Queries cover exchange timestamps within lookback; absent unconfigured feeds are not discoverable. Receive time may be a source-time fallback for some writers. Fresh data does not establish completeness or lack of backup impact. Compare with a sample after backups finish.'}
    print(json.dumps(metrics, indent=2))
    print(f'Influx query seconds: {timings}; observed feeds: {len(feeds)}')
    for row in flagged[:30]:
        print(json.dumps(row))
    for issue in issues:
        print('REVIEW:', issue)
    print(report['status'])
    print(report['limitations'])
    if args.output:
        args.output.write_text(json.dumps(report, indent=2) + '\n')
        print('Report:', args.output)
    return int(bool(issues))


if __name__ == '__main__':
    raise SystemExit(main())
