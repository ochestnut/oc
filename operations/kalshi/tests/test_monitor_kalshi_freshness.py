"""offline checks for freshness interpretation and coverage discovery."""
from pathlib import Path
import importlib.util
from io import BytesIO
import pandas as pd
from botocore.exceptions import ClientError
import sys
from unittest.mock import Mock
import json
import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))
from monitor_kalshi_freshness import summarize, read_latest, open_contracts, discover


def test_explicit_schema_checkout_does_not_change_import_path(tmp_path):
    import monitor_kalshi_freshness as monitor

    schema = tmp_path / monitor.SCHEMA_PATH
    schema.parent.mkdir(parents=True)
    schema.write_text("SERIES_TO_UNDERLYING = {'TEST': 'BTC'}\n")
    before = list(sys.path)
    assert monitor.load_schema(tmp_path).SERIES_TO_UNDERLYING == {'TEST': 'BTC'}
    assert sys.path == before


def test_missing_schema_gives_actionable_error(tmp_path):
    import monitor_kalshi_freshness as monitor

    with pytest.raises(FileNotFoundError, match='set --repo'):
        monitor.load_schema(tmp_path)


def point(event='2026-09-22T17:50:53+00:00', recv=1790099453137000000):
    return {'time': event, 'recv': recv}


def test_receipt_latency_and_initial_baseline():
    rows = summarize({'a': point()}, {'a': [{'ticker': 'a'}]}, None, 1790099455, 120)
    assert rows[0]['status'] == 'recent'
    assert rows[0]['event_age_s'] == 2
    assert rows[0]['receipt_delay_ms'] == 137
    assert rows[0]['observed_after_receive_s'] is None


def test_repeated_snapshot_is_not_a_new_visibility_sample():
    latest = {'a': point()}
    previous = {'a': (latest['a']['time'], latest['a']['recv'])}
    row = summarize(latest, {}, previous, 1790099460, 120)[0]
    assert row['observed_after_receive_s'] is None
    row = summarize(latest, {}, {}, 1790099460, 120)[0]
    assert row['observed_after_receive_s'] == 6.863


def test_quiet_missing_and_collision_are_distinct():
    expected = {'a': [{'ticker': 'first'}, {'ticker': 'second'}], 'missing': [{'ticker': 'third'}]}
    rows = {r['symbol']: r for r in summarize({'a': point()}, expected, None, 1790100000, 120)}
    assert rows['a']['status'] == 'old_or_quiet'
    assert rows['a']['collision']
    assert rows['missing']['status'] == 'no_recent_record'
    assert rows['missing']['receipt_delay_ms'] is None


def test_bad_clock_is_flagged_not_clamped():
    row = summarize({'a': point(recv=1790099452000000000)}, {}, None, 1790099455, 120)[0]
    assert row['status'] == 'clock_or_timestamp_check'
    assert row['receipt_delay_ms'] == -1000


def test_exchange_discovery_excludes_future_and_expired_contracts():
    market = dict(ticker='KXBTC15M-26SEP221400-00', open_time='2026-09-22T17:45:00Z', close_time='2026-09-22T18:00:00Z')
    expected, errors = open_contracts([market], 1790099455)
    assert not errors
    assert list(expected) == ['PRED-BTC_2609221800_UPDOWN-USD']
    assert not open_contracts([market], 1790098200)[0]
    assert not open_contracts([market], 1790100000)[0]


def test_database_query_is_bounded_and_grouped():
    client = Mock()
    client.query.return_value.items.return_value = [(('md_booktop_pred', {'instrument_id': 'a'}), [point()])]
    assert read_latest(client, 30, ['BTC']) == {'a': point()}
    query = client.query.call_args.args[0]
    assert 'now()-30m' in query
    assert 'GROUP BY instrument_id' in query
    assert '/^PRED-(BTC)_/' in query


def test_discovery_failure_remains_visible():
    session = Mock()
    session.get.side_effect = TimeoutError()
    markets, errors = discover(session, ['BTC'])
    assert not markets
    assert len(errors) == 3
    assert all('TimeoutError' in error for error in errors)


def test_cli_snapshot_and_report_do_not_expose_credentials(tmp_path, monkeypatch, capsys):
    import monitor_kalshi_freshness as monitor
    import influxdb
    creds = tmp_path/'creds.json'
    creds.write_text(json.dumps({'INFLUX': {'username': 'test_user', 'password': 'test_secret'}}))
    output = tmp_path/'report.jsonl'
    client = Mock()
    monkeypatch.setattr(influxdb, 'InfluxDBClient', Mock(return_value=client))
    monkeypatch.setattr(monitor, 'discover', lambda *a: ([], []))
    monkeypatch.setattr(monitor, 'read_latest', lambda *a: {'a': point()})
    monkeypatch.setattr(monitor.time, 'time', lambda: 1790099455)
    monkeypatch.setattr(sys, 'argv', ['monitor', '--once', '--credentials', str(creds), '--json-output', str(output)])
    monitor.main()
    report = json.loads(output.read_text())
    assert report['contracts'][0]['receipt_delay_ms'] == 137
    assert report['contracts'][0]['observed_after_receive_s'] is None
    assert 'test_secret' not in capsys.readouterr().out + output.read_text()
    client.close.assert_called_once()


def test_cli_database_failure_is_not_reported_as_missing_data(tmp_path, monkeypatch, capsys):
    import monitor_kalshi_freshness as monitor
    import influxdb
    creds = tmp_path/'creds.json'
    creds.write_text(json.dumps({'INFLUX': {'username': 'test_user', 'password': 'test_secret'}}))
    client = Mock()
    monkeypatch.setattr(influxdb, 'InfluxDBClient', Mock(return_value=client))
    monkeypatch.setattr(monitor, 'discover', lambda *a: ([], []))
    monkeypatch.setattr(monitor, 'read_latest', Mock(side_effect=TimeoutError()))
    monkeypatch.setattr(sys, 'argv', ['monitor', '--once', '--credentials', str(creds)])
    with pytest.raises(SystemExit) as exc:
        monitor.main()
    assert exc.value.code == 1
    text = capsys.readouterr().out
    assert 'influx query failed: TimeoutError' in text
    assert 'no_recent_record' not in text
    client.close.assert_called_once()


# missing-only prediction archive backfill

spec=importlib.util.spec_from_file_location('backfill', Path(__file__).parents[1] / 'backfill_prediction_archives.py')
backfill=importlib.util.module_from_spec(spec)
spec.loader.exec_module(backfill)

def error(code):
    return ClientError({'Error':{'Code':str(code)},'ResponseMetadata':{'HTTPStatusCode':code}}, 'PutObject')

def test_upload_never_overwrites(tmp_path):
    path=tmp_path/'chunk'
    path.write_bytes(b'parquet')
    s3=Mock()
    assert backfill.upload_missing(s3,'bucket','key',path)
    assert s3.put_object.call_args.kwargs['IfNoneMatch']=='*'
    s3.put_object.side_effect=error(412)
    s3.head_object.return_value={'ContentLength':10}
    assert not backfill.upload_missing(s3,'bucket','key',path)
    s3.head_object.return_value={'ContentLength':0}
    with pytest.raises(ValueError): backfill.upload_missing(s3,'bucket','key',path)
    s3.put_object.side_effect=error(403)
    with pytest.raises(ClientError): backfill.upload_missing(s3,'bucket','key',path)

def test_catalog_retry_preserves_concurrent_addition():
    s3=Mock()
    def response(symbols,etag):
        buf=BytesIO()
        pd.DataFrame({'symbol':symbols,'first_seen':['2026-08-01']*len(symbols),'last_seen':['2026-08-01']*len(symbols)}).to_parquet(buf,index=False)
        return {'Body':BytesIO(buf.getvalue()),'ETag':etag}
    s3.get_object.side_effect=[response(['old'],'one'),response(['old','concurrent'],'two')]
    s3.put_object.side_effect=[error(412),{}]
    backfill.merge_catalog(s3,'bucket',{'new':('2026-08-18','2026-08-18')})
    call=s3.put_object.call_args.kwargs
    assert call['IfMatch']=='two'
    assert set(pd.read_parquet(BytesIO(call['Body']))['symbol'])=={'old','new','concurrent'}

def test_catalog_auth_failure_is_fatal():
    s3=Mock()
    s3.get_object.side_effect=error(403)
    with pytest.raises(ClientError): backfill.merge_catalog(s3,'bucket',{'new':('2026-08-18','2026-08-18')})
    s3.put_object.assert_not_called()

@pytest.mark.parametrize('partial,limit',[(False,10),(True,10),(False,1)])
def test_read_all_policies_and_fail_closed(partial,limit):
    start=pd.Timestamp('2026-08-18T18:30:00')
    client=Mock()
    def query(sql,**kw):
        result=Mock()
        result.raw={'series':[{'partial':partial}]}
        result.get_points.return_value=iter([{'time':start.value,'exchange_id':'KALSHI','instrument_id':'PRED-BTC_2608181900_53700-USD','bid_price':1,'ask_price':2}])
        assert "\"exchange_id\" = 'KALSHI'" in sql
        return result
    client.query.side_effect=query
    if partial or limit==1:
        with pytest.raises(ValueError): backfill.read_window(client,['a','b'],start,start+pd.Timedelta(minutes=1),limit,['PRED-BTC_2608181900_53700-USD'])
    else:
        frames,count=backfill.read_window(client,['a','b'],start,start+pd.Timedelta(minutes=1),limit,['PRED-BTC_2608181900_53700-USD'])
        assert count==2 and len(frames)==1
        assert client.query.call_count==2
        assert '"a".' in client.query.call_args_list[0].args[0]
        assert '"b".' in client.query.call_args_list[1].args[0]

def test_time_and_checkpoint(tmp_path):
    with pytest.raises(ValueError): backfill.utc('2026-08-18')
    with pytest.raises(ValueError): backfill.utc('2026-08-18T00:01:00Z')
    t=backfill.utc('2026-08-18T00:00:00Z')
    state=tmp_path/'state.json'
    backfill.save_state(state,{'source':'ty03'},'last_contract')
    import json
    assert json.loads(state.read_text())['after']=='last_contract'


def test_backfill_splits_large_reads_without_losing_contracts():
    from unittest.mock import patch
    start=pd.Timestamp('2026-08-18T00:00:00')
    def read(client, policies, a, b, maximum, instruments):
        if len(instruments)>1 or b-a>pd.Timedelta(minutes=15):
            raise backfill.TooManyRows()
        return {(instruments[0],a): []}, 1
    with patch.object(backfill,'read_window',side_effect=read):
        results=list(backfill.source_slices(Mock(),['rp'],start,start+pd.Timedelta(minutes=30),10,['a','b']))
    assert sum(count for _,count in results)==4
    assert {key for frames,_ in results for key in frames}=={
        (symbol,stamp) for symbol in ['a','b'] for stamp in [start,start+pd.Timedelta(minutes=15)]}


def test_backfill_retries_only_transient_reads(monkeypatch):
    pause = Mock()
    monkeypatch.setattr(backfill, 'sleep', pause)
    read = Mock(side_effect=[backfill.Timeout('slow'), backfill.ConnectionError('disconnected'), 'ok'])
    assert backfill.retry_read('source', read) == 'ok'
    assert [call.args[0] for call in pause.call_args_list] == [5, 15]
    read = Mock(side_effect=backfill.Timeout('still slow'))
    with pytest.raises(backfill.Timeout):
        backfill.retry_read('source', read)
    assert read.call_count == 3
    from influxdb.exceptions import InfluxDBClientError
    read = Mock(side_effect=InfluxDBClientError('unauthorized', 401))
    with pytest.raises(InfluxDBClientError):
        backfill.retry_read('source', read)
    assert read.call_count == 1


def test_backfill_uploads_concurrently_with_distinct_files(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    barrier = Barrier(2)
    monkeypatch.setattr(backfill, 'list_day', lambda *args: {})
    def write(raw, path, exchange, symbol, *args):
        path.write_text(symbol)
        return True
    def upload(s3, bucket, key, path):
        barrier.wait(timeout=5)
        assert path.read_text() in key
        return True
    monkeypatch.setattr(backfill, 'upload_missing', upload)
    frames = {(symbol, pd.Timestamp('2026-08-18')): [] for symbol in ['a', 'b', 'c', 'd']}
    counts = dict(existing=0, missing=0, uploaded=0, invalid=0)
    observed = {}
    with ThreadPoolExecutor(max_workers=2) as pool:
        backfill.archive_frames(frames, s3=Mock(), bucket='bucket', fetcher=Mock(_write_chunk_frames=write),
                                pool=pool, temporary=tmp_path, day_cache=backfill.DayCache(tmp_path / "cache.sqlite"), instruments=None,
                                apply=True, workers=2, counts=counts, observed=observed)
    assert counts == dict(existing=0, missing=4, uploaded=4, invalid=0)
    assert set(observed) == {'a', 'b', 'c', 'd'}
    assert not list(tmp_path.glob("*.parquet"))


def test_backfill_failed_upload_cannot_finish_batch(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    monkeypatch.setattr(backfill, 'list_day', lambda *args: {})
    def write(raw, path, *args):
        path.write_bytes(b'parquet')
        return True
    monkeypatch.setattr(backfill, 'upload_missing', Mock(side_effect=error(403)))
    observed = {}
    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(ClientError):
            backfill.archive_frames({('a', pd.Timestamp('2026-08-18')): []}, s3=Mock(),
                                    bucket='bucket', fetcher=Mock(_write_chunk_frames=write),
                                    pool=pool, temporary=tmp_path, day_cache=backfill.DayCache(tmp_path / "cache.sqlite"), instruments=None,
                                    apply=True, workers=1,
                                    counts=dict(existing=0, missing=0, uploaded=0, invalid=0), observed=observed)
    assert not observed


@pytest.mark.parametrize('mode', ['apply', 'dry', 'catalog_failure'])
def test_grouped_catalog_checkpoint_and_final_flush(tmp_path, monkeypatch, mode, capsys):
    import influxdb
    import umm.analytics.config as config
    from umm.analytics.market_data.api import MarketData
    monkeypatch.setattr(backfill, 'load_sweep', Mock(return_value=Mock()))
    monkeypatch.setattr(config, 'load_config', lambda: {'S3_BUCKET': 'bucket'})
    monkeypatch.setattr(MarketData, '_catalog_conn_args', lambda *a, **k:
                        dict(host='test', port=8086, database='db'))
    client = Mock()
    client.get_list_retention_policies.return_value = [{'name': 'rp'}]
    monkeypatch.setattr(influxdb, 'InfluxDBClient', Mock(return_value=client))
    s3 = Mock()
    s3.meta.service_model.operation_model.return_value.input_shape.members = {'IfMatch': None, 'IfNoneMatch': None}
    monkeypatch.setattr(backfill.boto3, 'client', Mock(return_value=s3))
    names = [f'contract_{n:03}' for n in range(275)]
    monkeypatch.setattr(backfill.runpy, 'run_path', lambda *a: {
        'points': lambda result: [{'value': name} for name in names],
        'expiry_ns': lambda name: 1})
    batches = []
    def slices(client, policies, start, end, maximum, batch):
        batches.append(batch)
        yield batch, len(batch)
    monkeypatch.setattr(backfill, 'source_slices', slices)
    def archive(frames, **kw):
        assert kw['workers'] == 8
        kw['report'](phase='uploading', slice_files='1/50', force=True)
        for name in frames:
            kw['observed'][name] = ('2020-01-01', '2020-01-01')
    monkeypatch.setattr(backfill, 'archive_frames', archive)
    state = tmp_path / 'state.json'
    merged = []
    def merge(s3, bucket, observed):
        # The first merge must precede any checkpoint; the final merge sees the previous one.
        if not merged:
            assert not state.exists()
        else:
            assert json.loads(state.read_text())['after'] == names[249]
        merged.append(dict(observed))
        if mode == 'catalog_failure':
            raise RuntimeError('catalog unavailable')
    monkeypatch.setattr(backfill, 'merge_catalog', merge)
    argv = ['backfill', '--start', '2020-01-01T00:00:00Z', '--end', '2020-01-02T00:00:00Z',
            '--state', str(state)]
    if mode != 'dry':
        argv.append('--apply')
    monkeypatch.setattr(sys, 'argv', argv)
    if mode == 'catalog_failure':
        with pytest.raises(RuntimeError, match='catalog unavailable'):
            backfill.main()
        assert not state.exists()
        assert len(batches) == 5
    else:
        backfill.main()
        assert [len(batch) for batch in batches] == [50, 50, 50, 50, 50, 25]
        if mode == 'apply':
            assert [set(group) for group in merged] == [set(names[:250]), set(names[250:])]
            assert json.loads(state.read_text())['after'] == names[-1]
        else:
            assert not merged and not state.exists()
    output = capsys.readouterr().out
    assert 'phase=uploading' in output and 'slice_files=1/50' in output
    first_upload = next(line for line in output.splitlines() if 'phase=uploading' in line)
    assert '0/275 contracts' in first_upload and 'saved=0' in first_upload
    assert '[kalshi_backfill] progress' in output
    assert 'eta=' in output and 'rate=' in output and 'elapsed=' in output
    assert '"status": "batch"' not in output
    if mode == 'apply':
        assert 'saved=250' in output and 'saved=275' in output
        assert '275/275 contracts' in output
    elif mode == 'catalog_failure':
        assert 'saved=250' not in output and '] complete' not in output
    else:
        assert 'saved=250' not in output and 'saved=275' not in output
    client.close.assert_called_once()


def test_day_cache_keeps_four_days_across_contracts_and_batches(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from contextlib import closing
    from umm.analytics.market_data._common import local_path
    days = pd.date_range('2026-08-18', periods=4)
    def listing(s3, bucket, day, instruments, progress_interval, report=None):
        for symbol in ['a', 'b', 'c', 'd']:
            yield local_path(Path('market_data'), 'KALSHI', 'booktop', symbol,
                             'TY03', pd.Timestamp(day)).as_posix(), 10
    listed = Mock(side_effect=listing)
    monkeypatch.setattr(backfill, 'list_day', listed)
    counts = dict(existing=0, missing=0, uploaded=0, invalid=0)
    fetcher = Mock()
    with closing(backfill.DayCache(tmp_path / 'cache.sqlite')) as cache, ThreadPoolExecutor(2) as pool:
        for symbols in [['a', 'b'], ['c', 'd']]:
            frames = {(symbol, day): [] for symbol in symbols for day in days}
            backfill.archive_frames(frames, s3=Mock(), bucket='bucket', fetcher=fetcher,
                                    pool=pool, temporary=tmp_path, day_cache=cache, instruments=None,
                                    apply=True, workers=2, counts=counts, observed={})
        assert listed.call_count == 4
        assert counts == dict(existing=16, missing=0, uploaded=0, invalid=0)
        fetcher._write_chunk_frames.assert_not_called()


def test_incomplete_listing_is_rolled_back_and_can_retry(tmp_path):
    from contextlib import closing
    def failed_listing():
        yield 'first', 10
        raise RuntimeError('listing interrupted')
    with closing(backfill.DayCache(tmp_path / 'cache.sqlite')) as cache:
        with pytest.raises(RuntimeError):
            cache['2026-08-18'] = failed_listing()
        assert '2026-08-18' not in cache
        assert cache.size('first') is None
        cache['2026-08-18'] = iter([('first', 10), ('empty', 0)])
        assert '2026-08-18' in cache
        assert cache.size('first') == 10
        assert cache.size('empty') == 0
        cache.record('uploaded', 20)
        assert cache.size('uploaded') == 20


@pytest.mark.parametrize('fails', [False, True])
def test_listing_shared_progress_does_not_claim_failed_completion(fails, capsys):
    s3 = Mock()
    def pages(**kwargs):
        yield {'Contents': [{'Key': 'one', 'Size': 10}]}
        if fails:
            raise RuntimeError('s3 unavailable')
        yield {'Contents': [{'Key': 'two', 'Size': 20}]}
    s3.get_paginator.return_value.paginate.side_effect = pages
    if fails:
        with pytest.raises(RuntimeError):
            list(backfill.list_day(s3, 'bucket', '2026-08-18'))
    else:
        assert list(backfill.list_day(s3, 'bucket', '2026-08-18')) == [('one', 10), ('two', 20)]
    output = capsys.readouterr().out
    assert '[s3 2026-08-18]' in output and 'elapsed=' in output
    assert ('100.0%' in output) is (not fails)


def test_backfill_reports_files_before_batch_finishes(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    s3 = Mock()
    s3.get_paginator.return_value.paginate.return_value = [
        {'Contents': [{'Key': 'unrelated', 'Size': 1}]},
        {'Contents': []},
    ]
    events = []
    counts = dict(existing=0, missing=0, uploaded=0, invalid=0)
    def report(**values):
        events.append(dict(values, uploaded=counts['uploaded']))
    def write(raw, path, *args):
        path.write_text('data')
        return True
    def upload(*args):
        # Before the second upload, the first one has already been reported.
        if counts['uploaded']:
            assert any(e['slice_files'] == '1/2' for e in events)
        return True
    monkeypatch.setattr(backfill, 'upload_missing', upload)
    cache = backfill.DayCache(tmp_path / 'cache.sqlite')
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            backfill.archive_frames(
                {(name, pd.Timestamp('2026-08-18')): [] for name in ['a', 'b']},
                s3=s3, bucket='bucket', fetcher=Mock(_write_chunk_frames=write),
                pool=pool, temporary=tmp_path, day_cache=cache, instruments=None,
                apply=True, workers=1, counts=counts, observed={}, report=report)
    finally:
        cache.close()
    listing = [e for e in events if e['phase'] == 'listing']
    assert [e['listed_objects'] for e in listing] == [0, 1, 1, 1]
    assert all(e['slice_files'] == '0/2' for e in listing)
    assert events[-1]['slice_files'] == '2/2'
    assert events[-1]['uploaded'] == 2
