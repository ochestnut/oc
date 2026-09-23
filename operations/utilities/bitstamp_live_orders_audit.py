"""Audit a saved public capture without silently repairing or reordering its events."""
import argparse
import bisect
import collections
import json
from decimal import Decimal as D
from pathlib import Path

class Book:
    def __init__(self, snapshot):
        self.orders = {}
        self.levels = [collections.defaultdict(D), collections.defaultdict(D)]
        for side, name in enumerate(('bids', 'asks')):
            for p, q, oid in snapshot[name]:
                self.put(str(oid), side, D(p), D(q))
    def remove(self, oid):
        old = self.orders.pop(oid, None)
        if old:
            side, p, q = old
            self.levels[side][p] -= q
            if not self.levels[side][p]: del self.levels[side][p]
        return old
    def put(self, oid, side, p, q):
        old = self.remove(oid)
        if p > 0 and q > 0:
            self.orders[oid] = (side, p, q)
            self.levels[side][p] += q
        return old
    def top(self):
        if not all(self.levels): return None
        b, a = max(self.levels[0]), min(self.levels[1])
        return (b, self.levels[0][b], a, self.levels[1][a])
    def apply(self, msg):
        d = msg['data']; oid = str(d['id_str']); action = msg['event']
        if action == 'order_deleted': return self.remove(oid)
        assert int(d['order_type']) in (0, 1)
        return self.put(oid, int(d['order_type']), D(d['price_str']), D(d['amount_str']))

def reference_top(data):
    levels = []
    for side, name in enumerate(('bids', 'asks')):
        total = collections.defaultdict(D)
        for row in data[name]: total[D(row[0])] += D(row[1])
        p = (max if side == 0 else min)(total)
        levels.extend((p, total[p]))
    return tuple(levels)

def valid(top): return top is not None and top[0] < top[2]

def audit(raw):
    start, end = raw['start_ns'], raw['end_ns']
    report = {}
    for market, seed in raw['seeds'].items():
        events = [x for x in raw['messages'] if x['message'].get('channel') == 'live_orders_' + market and x['message'].get('event') in ('order_created','order_deleted','order_changed')]
        c = collections.Counter({key: 0 for key in (
            'order_events', 'valid_per_event_changes', 'valid_per_event_price_changes',
            'valid_per_event_size_only_changes', 'timestamp_grouped_changes',
            'timestamp_grouped_price_changes', 'timestamp_grouped_size_only_changes',
            'order_book_messages', 'order_book_changes')})
        ids = set(); prev = None; prev_ts = None
        for x in events:
            msg = x['message']; eid = msg.get('event_id'); ts = int(msg['data']['microtimestamp'])
            c['chain_breaks'] += prev is not None and msg.get('pre_event_id') != prev
            c['missing_event_ids'] += not eid or not msg.get('pre_event_id')
            c['duplicate_ids'] += eid in ids
            c['timestamp_regressions'] += prev_ts is not None and ts < prev_ts
            c['non_orderbook_events'] += msg.get('order_source') != 'orderbook'
            ids.add(eid); prev = eid; prev_ts = ts
        book = Book(seed)
        seed_ts = int(seed['microtimestamp'])
        replay = [x for x in events if int(x['message']['data']['microtimestamp']) > seed_ts]
        states = [(seed_ts, book.top())]
        last_valid = book.top(); last_grouped = book.top()
        checkpoints = sorted(raw.get('checkpoints', {}).get(market, []), key=lambda d: int(d['microtimestamp']))
        checkpoint_results = []
        checkpoint_index = 0
        def compare_checkpoint(data):
            other = Book(data)
            return {'microtimestamp': data['microtimestamp'], 'full_order_map_match': book.orders == other.orders,
                    'order_id_differences': len(book.orders.keys() ^ other.orders.keys()),
                    'order_value_differences': sum(book.orders[k] != other.orders[k] for k in book.orders.keys() & other.orders.keys())}
        final = raw['final_snapshots'][market]; final_ts = int(final['microtimestamp'])
        final_book = Book(final)
        final_checked = False
        def compare_final():
            c['final_order_id_symmetric_difference'] = len(book.orders.keys() ^ final_book.orders.keys())
            c['final_order_value_differences'] = sum(book.orders[k] != final_book.orders[k] for k in book.orders.keys() & final_book.orders.keys())
            c['final_full_order_map_match'] = int(book.orders == final_book.orders)
        for i, x in enumerate(replay):
            msg=x['message']; ts=int(msg['data']['microtimestamp']); inside = start <= x['recv_ns'] < end
            while checkpoint_index < len(checkpoints) and int(checkpoints[checkpoint_index]['microtimestamp']) < ts:
                checkpoint_results.append(compare_checkpoint(checkpoints[checkpoint_index]))
                checkpoint_index += 1
            if not final_checked and ts > final_ts:
                compare_final(); final_checked=True
            old=book.top(); prior=book.apply(msg); new=book.top()
            if inside:
                c['order_events'] += 1
                c['unknown_delete_or_change'] += prior is None and msg['event'] != 'order_created'
                c['create_existing_id'] += prior is not None and msg['event'] == 'order_created'
                c['per_event_changes_including_invalid'] += old != new
                c['invalid_after_event'] += not valid(new)
            states.append((ts,new))
            if valid(new):
                if inside and new != last_valid:
                    c['valid_per_event_changes'] += 1
                    c['valid_per_event_price_changes' if new[::2] != last_valid[::2] else 'valid_per_event_size_only_changes'] += 1
                last_valid=new
            last_in_ms = i+1 == len(replay) or int(replay[i+1]['message']['data']['microtimestamp']) != ts
            if last_in_ms:
                if inside: c['invalid_after_timestamp_group'] += not valid(new)
                if valid(new):
                    if inside and new != last_grouped:
                        c['timestamp_grouped_changes'] += 1
                        c['timestamp_grouped_price_changes' if new[::2] != last_grouped[::2] else 'timestamp_grouped_size_only_changes'] += 1
                    last_grouped = new
        while checkpoint_index < len(checkpoints):
            checkpoint_results.append(compare_checkpoint(checkpoints[checkpoint_index])); checkpoint_index += 1
        if not final_checked: compare_final()
        times=[s[0] for s in states]
        previous_top = None
        for x in raw['messages']:
            msg = x['message']
            if msg.get('channel') != 'order_book_' + market or msg.get('event') != 'data': continue
            current = reference_top(msg['data'])
            if start <= x['recv_ns'] < end:
                c['order_book_messages'] += 1
                c['order_book_changes'] += previous_top is not None and previous_top != current
            previous_top = current
        checks=[x for x in raw['messages'] if x['message'].get('channel')=='order_book_'+market and x['message'].get('event')=='data' and start <= x['recv_ns'] < end]
        mismatches=[]
        for x in checks:
            d=x['message']['data']; ts=int(d['microtimestamp']); ref=reference_top(d)
            idx=bisect.bisect_right(times,ts)-1
            c['snapshot_checks'] += 1
            if states[idx][1] == ref:
                c['exact_timestamp_snapshot_matches'] += 1
                continue
            # Diagnostic only: do not change the replay or claim nearby matches are exact.
            candidates=[]
            for j in range(max(0,bisect.bisect_left(times,ts-100000)-1),min(len(states),bisect.bisect_right(times,ts+100000)+1)):
                if states[j][1]!=ref: continue
                begin=times[j]; finish=times[j+1] if j+1<len(states) else begin
                distance=begin-ts if ts<begin else finish-ts if ts>finish else 0
                candidates.append((abs(distance),distance,j))
            near=min(candidates) if candidates else None
            mismatches.append({'snapshot_us':ts,'asof_top':states[idx][1],'snapshot_top':ref,'nearest_matching_state_boundary_offset_us':near[1] if near else None})
        report[market]={'matching_engine_update_count_verified':False,
                        'capture_integrity_passed': all(c[k] == 0 for k in ('chain_breaks','missing_event_ids','duplicate_ids','timestamp_regressions','unknown_delete_or_change','create_existing_id')) and c['final_full_order_map_match'] == 1,
                        'duration_seconds':(end-start)/1e9, 'counts':dict(c),'rest_checkpoints':checkpoint_results,'mismatches':mismatches, 'event_timestamp_remainders_us':sorted({int(x['message']['data']['microtimestamp'])%1000 for x in events})}
    return report

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('capture');args=parser.parse_args()
    p=Path(args.capture); result=audit(json.loads(p.read_text()))
    out=p.with_name(p.stem+'-audit.json');out.write_text(json.dumps(result,indent=2,default=str))
    for market,r in result.items():
        print(market,json.dumps(r['counts']))
        print('mismatch nearest-state offsets (us):',[x['nearest_matching_state_boundary_offset_us'] for x in r['mismatches']])
    print(out)
