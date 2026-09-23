#!/usr/bin/env python3
"""passively measure the running jdbp fix feed's published l1 update rate.

linux, python 3.8+, standard library only. reads existing /dev/shm files with
read-only mmap; never connects to fix, creates shared memory, or unlinks it.
preserves the original measure_bitstamp_fix_l1.py sampling and equations.
"""

import argparse
import csv
import json
import math
import mmap
import os
from pathlib import Path
import struct
import sys
import time
import zlib
from collections import Counter
from datetime import datetime, timezone


HEADER = struct.Struct("iIII")  # heartbeat, status, length, crc32
RECORD = struct.Struct("9Q")  # sequence, bid px/qty/ts, ask px/qty/ts, recv/parse ts


class Reader:
    def __init__(self, exchange, instrument):
        self.path = Path('/dev/shm') / ('BOOKTOP:%s:%s' % (exchange, instrument))
        self.file = self.path.open('rb')
        try:
            self.identity = os.fstat(self.file.fileno()).st_ino
            self.mem = mmap.mmap(self.file.fileno(), 0, access=mmap.ACCESS_READ)
        except BaseException:
            self.file.close()
            raise

    def read(self):
        if self.path.stat().st_ino != self.identity:
            raise RuntimeError('%s was replaced; rerun after feed restart' % self.path)
        for _ in range(20):
            header = self.mem[:HEADER.size]
            heartbeat, status, length, checksum = HEADER.unpack(header)
            if time.time() - heartbeat > 3:
                raise RuntimeError('%s writer heartbeat is stale' % self.path)
            if status != 0:
                raise RuntimeError('%s writer status is %s, expected connected=0' %
                                   (self.path, status))
            if length != RECORD.size:
                raise RuntimeError('%s unexpected record size %s; expected %s' %
                                   (self.path, length, RECORD.size))
            payload = self.mem[HEADER.size:HEADER.size + length]
            # check the header again in case the writer raced our read.
            if (header[8:] == self.mem[8:HEADER.size]
                    and zlib.crc32(payload) == checksum):
                return RECORD.unpack(payload)
        raise RuntimeError('%s failed consistent-read checks' % self.path)

    def close(self):
        self.mem.close()
        self.file.close()


def stats(values):
    values = sorted(values)
    if not values:
        return {'n': 0}

    def percentile(p):
        position = (len(values) - 1) * p
        low = int(position)
        high = min(low + 1, len(values) - 1)
        return values[low] + (values[high] - values[low]) * (position - low)

    result = dict(n=len(values), mean=sum(values) / len(values), min=values[0],
                p50=percentile(.5), p90=percentile(.9), p95=percentile(.95),
                p99=percentile(.99), max=values[-1])
    result['percentiles'] = {'p%d' % p: percentile(p / 100) for p in range(101)}
    return result


def write_csv(path, fields, rows):
    with path.open('w', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=int, default=600)
    parser.add_argument('--poll-ms', type=float, default=100.0)
    parser.add_argument('--exchange', default='BITSTAMP')
    parser.add_argument('--instruments', default=
                        'PAIR-BTC-USD,PAIR-ETH-USD,PAIR-SOL-USD,PAIR-DOGE-USD,PAIR-XRP-USD')
    parser.add_argument('--output', type=Path, required=True,
                        help='new directory for summary and distributions')
    args = parser.parse_args()
    instruments = list(dict.fromkeys(args.instruments.split(',')))
    if args.seconds < 1 or not math.isfinite(args.poll_ms) or args.poll_ms <= 0:
        parser.error('seconds and poll-ms must be positive')
    if any(not s or '/' in s or s.strip() != s for s in instruments + [args.exchange]):
        parser.error('exchange/instruments must be nonempty names without slashes/spaces')
    args.output.mkdir(parents=True, exist_ok=False)
    readers, state, rows = {}, {}, []
    error = None
    started = datetime.now(timezone.utc).isoformat()
    try:
        for instrument in instruments:
            reader = Reader(args.exchange, instrument)
            readers[instrument] = reader
            baseline = reader.read()
            state[instrument] = dict(previous=baseline, boundary_seq=baseline[0],
                                     boundary_time=time.monotonic(), observed=0,
                                     skipped=0, quote_changes=0, gaps=[],
                                     timestamp_regressions=0, total=0)
        start = time.monotonic()
        completed = 0
        print('reading existing booktop shared memory for %ss; no fix login' % args.seconds,
              flush=True)
        while completed < args.seconds:
            # nominal one-second windows; use actual read times for rates.
            boundary = time.monotonic() >= start + completed + 1
            for instrument, reader in readers.items():
                current = reader.read()
                now = time.monotonic()
                item = state[instrument]
                previous = item['previous']
                delta = current[0] - previous[0]
                if delta < 0 or (delta == 0 and current != previous):
                    raise RuntimeError('%s sequence reset/inconsistent record; rerun' % instrument)
                if delta:
                    item['total'] += delta
                    item['observed'] += 1
                    item['skipped'] += delta - 1
                    if (current[1], current[2], current[4], current[5]) != (
                            previous[1], previous[2], previous[4], previous[5]):
                        item['quote_changes'] += 1
                    # Only consecutive records have a known single-update interval.
                    if delta == 1 and previous[7] > 0 and current[7] >= previous[7]:
                        item['gaps'].append((current[7] - previous[7]) / 1e6)
                    elif current[7] < previous[7]:
                        item['timestamp_regressions'] += 1
                    item['previous'] = current
                if boundary:
                    elapsed = now - item['boundary_time']
                    count = current[0] - item['boundary_seq']
                    rows.append(dict(instrument=instrument, window=completed + 1,
                                     elapsed_seconds=elapsed, updates=count,
                                     updates_per_second=count / elapsed))
                    item['boundary_seq'] = current[0]
                    item['boundary_time'] = now
            if boundary:
                completed += 1
                # Do not invent empty seconds if the reader was descheduled.
                if time.monotonic() >= start + completed + 1:
                    raise RuntimeError('reader fell more than one window behind; distribution incomplete')
            remaining = start + completed + 1 - time.monotonic()
            if completed < args.seconds and remaining > 0:
                time.sleep(min(args.poll_ms / 1000, remaining))
    except KeyboardInterrupt:
        error = 'interrupted; completed windows only, capture incomplete'
    except (OSError, RuntimeError, ValueError, struct.error) as exc:
        error = str(exc)
    finally:
        for reader in readers.values():
            reader.close()

    summary = dict(started_utc=started, finished_utc=datetime.now(timezone.utc).isoformat(),
                   complete=error is None, error=error, requested_seconds=args.seconds,
                   poll_ms=args.poll_ms, exchange=args.exchange,
                   measurement='published l1 sequence increments, not raw fix messages',
                   instruments={})
    histogram = []
    for instrument, item in state.items():
        windows = [row for row in rows if row['instrument'] == instrument]
        counts = [row['updates'] for row in windows]
        rates = [row['updates_per_second'] for row in windows]
        elapsed = sum(row['elapsed_seconds'] for row in windows)
        summary['instruments'][instrument] = dict(
            completed_windows=len(windows), measured_seconds=elapsed,
            updates_in_completed_windows=sum(counts),
            mean_updates_per_second=sum(counts) / elapsed if elapsed else None,
            updates_per_second=stats(rates), updates_per_window=stats(counts),
            zero_update_windows=counts.count(0),
            observed_records=item['observed'], unobserved_records=item['skipped'],
            observed_quote_changes=item['quote_changes'],
            consecutive_receive_interarrival_ms=stats(item['gaps']),
            receive_timestamp_regressions=item['timestamp_regressions'])
        for count, frequency in sorted(Counter(counts).items()):
            histogram.append(dict(instrument=instrument, updates_per_window=count,
                                  windows=frequency, fraction=frequency / len(counts)))
    write_csv(args.output / 'per_second.csv',
              ['instrument', 'window', 'elapsed_seconds', 'updates', 'updates_per_second'], rows)
    write_csv(args.output / 'distribution.csv',
              ['instrument', 'updates_per_window', 'windows', 'fraction'], histogram)
    (args.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    lines = ['bitstamp fix l1 published update frequency',
             'capture: ' + ('complete' if error is None else 'incomplete: ' + error),
             'instrument | mean/s | p50/s | p95/s | p99/s | max/s | zero windows | skipped observations']
    for instrument, item in summary['instruments'].items():
        rate = item['updates_per_second']
        if rate['n']:
            lines.append('%s | %.2f | %.2f | %.2f | %.2f | %.2f | %s | %s' % (
                instrument, item['mean_updates_per_second'], rate['p50'], rate['p95'],
                rate['p99'], rate['max'], item['zero_update_windows'], item['unobserved_records']))
    percentile_rows = []
    for instrument, item in summary['instruments'].items():
        rate = item['updates_per_second']
        if not rate['n']:
            continue
        lines.extend(['', instrument + ' full updates/sec distribution', 'percentile | updates/s'])
        for p in range(101):
            value = rate['percentiles']['p%d' % p]
            lines.append('p%d | %.6f' % (p, value))
            percentile_rows.append(dict(instrument=instrument, percentile=p, updates_per_second=value))
        lines.extend(['', 'updates/window | windows | percent | cumulative percent'])
        cumulative = 0.0
        for row in histogram:
            if row['instrument'] == instrument:
                cumulative += row['fraction']
                lines.append('%s | %s | %.3f | %.3f' % (
                    row['updates_per_window'], row['windows'], 100 * row['fraction'], 100 * cumulative))
    write_csv(args.output / 'percentiles.csv',
              ['instrument', 'percentile', 'updates_per_second'], percentile_rows)
    lines.extend(['', 'counts use sequence deltas, including updates overwritten between reads.',
                  'windows are approximately one second; rates use actual elapsed time.',
                  'interarrival statistics use consecutive observed records only; gaps can bias this sample.',
                  'quote-change counts are observed lower bounds, not raw fix message counts.'])
    report = '\n'.join(lines) + '\n'
    (args.output / 'summary.txt').write_text(report)
    compact = [lines[0], lines[1],
               'instrument | mean/s | min | p5 | p10 | p25 | p50 | p75 | p90 | p95 | p99 | max | zero windows | skipped observations']
    for instrument, item in summary['instruments'].items():
        rate = item['updates_per_second']
        if rate['n']:
            values = [item['mean_updates_per_second']] + [
                rate['percentiles']['p%d' % p]
                for p in (0, 5, 10, 25, 50, 75, 90, 95, 99, 100)]
            compact.append(instrument + ' | ' + ' | '.join('%.2f' % v for v in values)
                           + ' | %s | %s' % (item['zero_update_windows'], item['unobserved_records']))
    compact.extend(['', 'rates are updates/sec using actual window durations; sequence gaps remain counted.',
                    'full p0-p100, histogram, and per-window observations saved in: %s' % args.output])
    compact_report = '\n'.join(compact) + '\n'
    (args.output / 'compact.txt').write_text(compact_report)
    print(compact_report)
    return 0 if error is None else 2


if __name__ == '__main__':
    sys.exit(main())
