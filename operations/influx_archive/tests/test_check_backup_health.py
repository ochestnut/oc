import importlib.util
import io
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import mock_open, patch

import pytest

SPEC = importlib.util.spec_from_file_location('health', Path(__file__).parents[1] / 'check_backup_health.py')
health = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(health)


def test_fresh_stale_stalled_missing_and_future_feeds():
    now = 1000 * 10**9
    keys = [(name, 'BTC') for name in ('fresh', 'stale', 'stalled', 'missing', 'future')]
    before = {keys[0]: now - 5 * 10**9, keys[1]: now - 100 * 10**9, keys[2]: now - 2 * 10**9}
    after = {keys[0]: now - 10**9, keys[1]: now - 90 * 10**9, keys[2]: before[keys[2]], keys[4]: now + 5 * 10**9}
    rows = health.feed_metrics(before, after, keys, now, 60)
    assert {row['exchange']: row['status'] for row in rows} == {
        'fresh': 'observed', 'stale': 'stale', 'stalled': 'not_advancing', 'missing': 'missing', 'future': 'future_timestamp'}


def test_bounded_authenticated_query():
    args = SimpleNamespace(lookback_minutes=15, feed=[('BNBSPOT', 'PAIR-BTC-USDT')], creds=Path('/fake'), database='UMM_MD')
    payload = {'results': [{'series': [{'tags': {'exchange_id': 'BNBSPOT', 'instrument_id': 'PAIR-BTC-USDT'},
               'columns': ['time', 'recv'], 'values': [[1, 123456789]]}]}]}
    with patch.object(Path, 'open', mock_open(read_data='{"INFLUX":{"username":"fake","password":"secret"}}')), \
         patch.object(health, 'urlopen', return_value=io.StringIO(json.dumps(payload))) as url:
        feeds, elapsed = health.fetch_feeds(args)
    assert feeds == {('BNBSPOT', 'PAIR-BTC-USDT'): 123456789}
    assert elapsed >= 0
    assert 'secret' not in url.call_args.args[0].full_url
    assert '15m' in url.call_args.args[0].full_url


def test_query_error_is_not_empty_success():
    args = SimpleNamespace(lookback_minutes=15, feed=[], creds=Path('/fake'), database='UMM_MD')
    with patch.object(Path, 'open', mock_open(read_data='{"INFLUX":{"username":"fake","password":"secret"}}')), \
         patch.object(health, 'urlopen', return_value=io.StringIO('{"results":[{"error":"denied"}]}')):
        with pytest.raises(RuntimeError):
            health.fetch_feeds(args)


def test_remote_checks_continue_after_failed_server(tmp_path):
    args = SimpleNamespace(ssh=['user@first', 'user@second'], seconds=10, stale_seconds=60,
                           lookback_minutes=15, database='UMM_MD', stage_dir=Path('/tmp'),
                           feed=[], output=tmp_path / 'report.json')
    with patch.object(health.subprocess, 'run', side_effect=[
        SimpleNamespace(returncode=255, stdout='', stderr='connection failed'),
        SimpleNamespace(returncode=0, stdout='NO_FLAGS_IN_SAMPLE\n', stderr=''),
    ]) as run:
        assert health.remote_checks(args) == 1
    assert run.call_count == 2
    assert 'StrictHostKeyChecking=yes' in run.call_args.args[0]
    assert 'def host_sample' in run.call_args.kwargs['input']
    assert len(json.loads(args.output.read_text())) == 2
