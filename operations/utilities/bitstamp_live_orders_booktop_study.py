"""Read-only public capture; offline reconstruction, with timestamp-aligned snapshot checks."""
import asyncio, datetime, json, pathlib, time
import requests
import websockets
from bitstamp_live_orders_audit import audit

MARKETS = ['btcusd-perp', 'ethusd-perp', 'solusd-perp']
ROOT = pathlib.Path('/Users/owenchestnut/Desktop/jst/oc/operations/utilities')

def snapshot(market):
    r = requests.get(f'https://www.bitstamp.net/api/v2/order_book/{market}/', params={'group': 2}, timeout=20)
    r.raise_for_status()
    data = r.json()
    assert data.get('bids') and data.get('asks'), data
    return data

async def main():
    raw = {'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(), 'messages':[], 'seeds':{}, 'final_snapshots':{}, 'checkpoints':{m: [] for m in MARKETS}}
    async with websockets.connect('wss://ws.bitstamp.net', max_queue=None, ping_interval=20) as ws:
        async def receive():
            async for payload in ws:
                raw['messages'].append({'recv_ns':time.time_ns(),'message':json.loads(payload)})
        reader = asyncio.create_task(receive())
        for market in MARKETS:
            for prefix in ('live_orders', 'order_book'):
                await ws.send(json.dumps({'event':'bts:subscribe','data':{'channel':f'{prefix}_{market}'}}))
        await asyncio.sleep(2)
        for market in MARKETS:
            assert any(x['message'].get('event') == 'bts:subscription_succeeded' and x['message'].get('channel') == f'live_orders_{market}' for x in raw['messages']), f'subscription failed: {market}'
        seeds = await asyncio.gather(*(asyncio.to_thread(snapshot, m) for m in MARKETS))
        raw['seeds'] = dict(zip(MARKETS,seeds))
        await asyncio.sleep(2)
        async def checkpoint_capture():
            for _ in range(5):
                await asyncio.sleep(10)
                snapshots = await asyncio.gather(*(asyncio.to_thread(snapshot, m) for m in MARKETS))
                for m, data in zip(MARKETS, snapshots): raw['checkpoints'][m].append(data)
        checkpoints_task = asyncio.create_task(checkpoint_capture())
        raw['start_ns'] = time.time_ns()
        print('seeded all three markets; starting 60-second measurement', flush=True)
        await asyncio.sleep(60)
        raw['end_ns'] = raw['start_ns'] + 60_000_000_000
        assert not reader.done(), 'websocket disconnected during test'
        finals = await asyncio.gather(*(asyncio.to_thread(snapshot, m) for m in MARKETS))
        raw['final_snapshots'] = dict(zip(MARKETS, finals))
        await asyncio.sleep(2)
        await checkpoints_task
        reader.cancel()
        try: await reader
        except asyncio.CancelledError: pass
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d-%H%M%S')
    raw_path = ROOT / f'bitstamp-live-orders-{stamp}.json'
    raw_path.write_text(json.dumps(raw))
    result = audit(raw)
    report = {'raw_file':str(raw_path),'utc':raw['utc'],'method':'seed group=2 REST snapshot; replay order events by exchange microtimestamp, exclude events at/before seed timestamp; audit original event-id chain and replay in received order; report valid per-event and millisecond-grouped changes separately; check full order maps against periodic REST snapshots and diagnose websocket snapshot alignment; no resync; counts are observed reconstructed changes, not certified matching-engine updates', 'results':result}
    report_path = ROOT / f'bitstamp-live-orders-{stamp}-summary.json'
    report_path.write_text(json.dumps(report, indent=2, default=str))
    print('market | valid per-event changes | millisecond-grouped changes | order_book changes')
    for market, result in report['results'].items():
        c = result['counts']
        print(f"{market} | {c['valid_per_event_changes']} | {c['timestamp_grouped_changes']} | {c['order_book_changes']}")
        print(f"  chain breaks: {c['chain_breaks']}; unknown updates: {c['unknown_delete_or_change']}; final full book match: {bool(c['final_full_order_map_match'])}")
        print(f"  exact snapshot matches: {c.get('exact_timestamp_snapshot_matches',0)}/{c.get('snapshot_checks',0)}; checkpoint matches: {sum(x['full_order_map_match'] for x in result['rest_checkpoints'])}/{len(result['rest_checkpoints'])}")
    print('these are reconstructed changes under two counting rules, not a certified matching-engine update count.')
    print('summary:', report_path, flush=True)

if __name__ == '__main__': asyncio.run(main())
