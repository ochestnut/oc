"""offline safety tests; no network, credentials, or production mutation."""
import copy
import importlib.util
import io
import json
import sys
import types
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    'cleanup_prediction_series', Path(__file__).resolve().parents[1] / 'cleanup_prediction_series.py')
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)
SOURCE = {'host': 'test', 'port': 8086, 'database': 'UMM_MD'}


def point(time=100, bid=1):
    return {'time': time, 'exchange_id': 'KALSHI', 'symbol': 'venue-contract',
            'instrument_id': 'PRED-BTC_2608010000_100-USD',
            'bid_price': bid, 'ask_price': 2, 'bid_quantity': 3, 'ask_quantity': 4,
            'local_recv_time': time + 10}


class Result:
    def __init__(self, points):
        self.points = points
    def get_points(self):
        return iter(copy.deepcopy(self.points))


class Client:
    def __init__(self):
        self.rows = {'autogen': [point()], 'other': [point(200)]}
        self.queries = []
        self.after_read = None
        self.reads = 0
    def get_list_retention_policies(self, database):
        return [{'name': name} for name in self.rows]
    def query(self, query, **kwargs):
        self.queries.append(query)
        if query.startswith('DROP'):
            self.rows = {key: [] for key in self.rows}
            return Result([])
        if query.startswith('SHOW SERIES'):
            return Result([{'key': 'remaining'}] if any(self.rows.values()) else [])
        self.reads += 1
        if self.after_read:
            self.after_read(self)
        policy = next(p for p in self.rows if f'"{p}".' in query)
        return Result(self.rows[policy])


class SafetyTests(unittest.TestCase):
    def test_unchanged_quotes_compress_but_size_changes_survive(self):
        rows = [point(100), point(200), point(300)]
        rows[2]['bid_quantity'] = 5
        expected = mod.changes(rows)
        self.assertEqual([r[0] for r in expected], [100, 300])
        mod.verify_quotes(expected, expected)
        with self.assertRaises(ValueError):
            mod.verify_quotes(expected, expected[:1])

    def test_price_or_timestamp_mismatch_blocks(self):
        expected = mod.changes([point()])
        for wrong in [(100, 9, 2, 3, 4), (101, 1, 2, 3, 4)]:
            with self.assertRaises(ValueError):
                mod.verify_quotes(expected, [wrong])

    def test_conflicting_duplicate_timestamp_blocks(self):
        with self.assertRaises(ValueError):
            mod.changes([point(), point(bid=9)])
        with self.assertRaises(ValueError):
            mod.verify_quotes([], [(100, 1), (100, 2)])

    def test_null_and_nan_block(self):
        for value in (None, float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                mod.changes([point(bid=value)])

    def test_all_retention_policies_and_recent_rows(self):
        client = Client()
        data = mod.snapshot(client, SOURCE, 'KALSHI', 'venue-contract', 10)
        self.assertEqual(len(data['records']), 2)
        self.assertFalse(mod.is_old(data, 150))
        self.assertTrue(mod.is_old(data, 201))
        self.assertFalse(mod.is_old({'records': []}, 201))
        self.assertTrue(all('time <= 9223372036854775806' in q for q in client.queries))

    def test_row_limit_blocks_partial_verification(self):
        with self.assertRaises(ValueError):
            mod.snapshot(Client(), SOURCE, 'KALSHI', 'venue-contract', 1)

    def test_bad_identity_blocks(self):
        client = Client()
        client.rows['other'][0]['symbol'] = 'unexpected'
        with self.assertRaises(ValueError):
            mod.snapshot(client, SOURCE, 'KALSHI', 'venue-contract', 10)
        with self.assertRaises(ValueError):
            mod.predicate('other', 'x')
        self.assertEqual(mod.literal("a'b\\c"), "'a\\'b\\\\c'")

    def candidate(self, client):
        return {'exchange': 'KALSHI', 'symbol': 'venue-contract',
                'sha256': mod.digest(mod.snapshot(client, SOURCE, 'KALSHI', 'venue-contract', 10))}

    def test_success_reverifies_then_drops_exact_scope(self):
        client = Client()
        candidate = self.candidate(client)
        with tempfile.TemporaryFile(mode='w+') as journal, patch.object(mod, 'verify_archive', return_value=[]) as verify:
            mod.apply_candidate(client, object(), SOURCE, 'bucket', candidate, 300, 10, journal)
            verify.assert_called_once()
            journal.seek(0)
            events = [json.loads(line) for line in journal]
        self.assertEqual([e['status'] for e in events], ['verified_before_drop', 'dropped'])
        drops = [q for q in client.queries if q.startswith('DROP')]
        self.assertEqual(drops, ['DROP SERIES FROM "md_booktop_pred" WHERE "exchange_id" = \'KALSHI\' AND "symbol" = \'venue-contract\''])

    def test_changed_source_prevents_drop(self):
        client = Client()
        candidate = self.candidate(client)
        client.rows['autogen'].append(point(250))
        with tempfile.TemporaryFile(mode='w+') as journal:
            with self.assertRaises(ValueError):
                mod.apply_candidate(client, None, SOURCE, 'bucket', candidate, 300, 10, journal)
        self.assertFalse(any(q.startswith('DROP') for q in client.queries))

    def test_missing_archive_prevents_drop(self):
        client = Client()
        candidate = self.candidate(client)
        with tempfile.TemporaryFile(mode='w+') as journal, patch.object(mod, 'verify_archive', side_effect=FileNotFoundError):
            with self.assertRaises(FileNotFoundError):
                mod.apply_candidate(client, None, SOURCE, 'bucket', candidate, 300, 10, journal)
        self.assertFalse(any(q.startswith('DROP') for q in client.queries))

    def test_write_during_verification_prevents_drop(self):
        client = Client()
        candidate = self.candidate(client)
        def mutate(*args):
            client.rows['autogen'].append(point(250))
            return []
        with tempfile.TemporaryFile(mode='w+') as journal, patch.object(mod, 'verify_archive', side_effect=mutate):
            with self.assertRaises(ValueError):
                mod.apply_candidate(client, None, SOURCE, 'bucket', candidate, 300, 10, journal)
        self.assertFalse(any(q.startswith('DROP') for q in client.queries))

    def test_apply_requires_operator_flags(self):
        with patch('sys.argv', ['cleanup', 'apply', '--plan', 'x', '--journal', 'y']), patch('sys.stderr', io.StringIO()):
            with self.assertRaises(SystemExit) as exc:
                mod.main()
            self.assertEqual(exc.exception.code, 2)

    def test_batch_predicate_is_exact_and_bounded(self):
        self.assertIn("'a\\'b'", mod.batch_predicate('KALSHI', ["a'b", 'other']))
        for symbols in ([], ['a', 'a'], [str(i) for i in range(51)]):
            with self.assertRaises(ValueError):
                mod.batch_predicate('KALSHI', symbols)

    def test_batch_snapshot_matches_single_snapshot(self):
        client = Client()
        one = mod.snapshot(client, SOURCE, 'KALSHI', 'venue-contract', 10)
        many = mod.snapshot_batch(client, SOURCE, 'KALSHI', ['venue-contract'], 10)
        self.assertEqual(one, many['venue-contract'])
        with self.assertRaises(ValueError):
            mod.snapshot_batch(Client(), SOURCE, 'KALSHI', ['venue-contract'], 1)

    def test_batch_success(self):
        client = Client()
        candidate = self.candidate(client)
        with tempfile.TemporaryFile(mode='w+') as journal, patch.object(mod, 'verify_archive', return_value=[]):
            mod.apply_batch(client, None, SOURCE, 'bucket', [candidate], 300, 10, journal, 'TY03', 2)
            journal.seek(0)
            self.assertEqual([json.loads(l)['status'] for l in journal],
                             ['verified_before_batch_drop', 'batch_dropped'])
        self.assertEqual(sum(q.startswith('DROP') for q in client.queries), 1)

    def test_batch_archive_failure_and_race_prevent_drop(self):
        for race in (False, True):
            client = Client()
            candidate = self.candidate(client)
            def verify(*args):
                if race:
                    client.rows['other'].append(point(250))
                    return []
                raise ValueError('missing archive')
            with tempfile.TemporaryFile(mode='w+') as journal, patch.object(mod, 'verify_archive', side_effect=verify):
                with self.assertRaises(ValueError):
                    mod.apply_batch(client, None, SOURCE, 'bucket', [candidate], 300, 10, journal, 'TY03', 2)
            self.assertFalse(any(q.startswith('DROP') for q in client.queries))


class ParquetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import pandas as pd
            import pyarrow
        except ImportError:
            raise unittest.SkipTest('pandas/pyarrow unavailable in this interpreter')
        cls.pd = pd

    def run_archive(self, points, archived, symbol=None):
        pd = self.pd
        frame = pd.DataFrame(archived)
        frame['timestamp'] = pd.to_datetime(frame.pop('time'), unit='ns')
        frame = frame.rename(columns={'bid_quantity': 'bid_qty', 'ask_quantity': 'ask_qty'})
        frame['symbol'] = symbol or points[0]['instrument_id']
        stream = io.BytesIO()
        frame.to_parquet(stream, index=False)
        content = stream.getvalue()
        class FS:
            def invalidate_cache(self, key):
                pass
            def open(self, key, mode):
                self.key = key
                return io.BytesIO(content)
        helper = types.ModuleType('umm.analytics.market_data._common')
        helper.local_path = lambda root, exchange, kind, instrument, recorder, dt: root / recorder / instrument / 'chunk.parquet'
        data = {'exchange': 'KALSHI', 'records': [
            {'retention_policy': 'autogen', 'point': p} for p in points]}
        with patch.dict(sys.modules, {'umm.analytics.market_data._common': helper}):
            return mod.verify_archive(FS(), 'bucket', data, 'TY03')

    def test_real_parquet_accepts_compressed_quotes(self):
        rows = [point(100), point(200), point(300, bid=1.5)]
        evidence = self.run_archive(rows, [rows[0], rows[2]])
        self.assertEqual(evidence[0]['quote_changes'], 2)
        self.assertIn('/TY03/', evidence[0]['key'])

    def test_real_parquet_rejects_missing_change(self):
        rows = [point(100), point(300, bid=1.5)]
        with self.assertRaises(ValueError):
            self.run_archive(rows, rows[:1])

    def test_real_parquet_rejects_wrong_contract(self):
        with self.assertRaises(ValueError):
            self.run_archive([point()], [point()], symbol='PRED-WRONG-USD')

    def test_real_parquet_rejects_outside_window(self):
        with self.assertRaises(ValueError):
            self.run_archive([point()], [point(mod.CHUNK_NS)])


if __name__ == '__main__':
    unittest.main()
