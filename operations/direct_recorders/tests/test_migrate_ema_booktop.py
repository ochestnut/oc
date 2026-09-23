import importlib.util
from pathlib import Path
import pandas as pd
import pytest

spec = importlib.util.spec_from_file_location('migration', Path(__file__).parents[1] / 'migrate_ema_booktop.py')
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


def test_exact_duplicates_removed_but_receive_observations_preserved():
    src = pd.DataFrame({'timestamp': [1, 1], 'recv': [2, 3], 'bid_price': [10, 10]})
    dst = pd.DataFrame({'timestamp': [1, 2], 'recv': [2, 4], 'bid_price': [10, 11]})
    merged = migration.merge(src, dst)
    assert len(merged) == 3
    assert migration.contains(merged, src)
    assert migration.contains(merged, dst)
    assert not migration.contains(dst, src)


def test_destination_preserves_symbol_window_and_recorder():
    key = 'market_data/EMA_BCH_BITSTAMP/booktop/PAIR-BCH-USD/2026-09-10/EMA_BCH_BITSTAMP_PAIR-BCH-USD_booktop_2026-09-10_eu-central-1a_FR01D_0000.parquet'
    dest, window = migration.target_key(key)
    assert dest == key.replace('EMA_BCH_BITSTAMP', 'DERIVED_BITSTAMP_BNBSPOT')
    assert window == '2026-09-10 00:00:00'
    with pytest.raises(ValueError):
        migration.target_key(key.replace('FR01D', 'FR01'))
    with pytest.raises(ValueError):
        migration.target_key(key.replace('PAIR-BCH-USD', 'PAIR-LTC-USD'))


def test_bad_schema_and_wrong_window_are_rejected():
    import io
    data = pd.DataFrame({'timestamp': [pd.Timestamp('2026-09-10')],
                         'recv': [pd.Timestamp('2026-09-10')],
                         'bid_price': [1.0], 'ask_price': [2.0],
                         'bid_qty': [1.0], 'ask_qty': [1.0]})
    def encoded(df):
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        return buf.getvalue()
    assert len(migration.frame(encoded(data), '2026-09-10 00:00:00')) == 1
    with pytest.raises(ValueError, match='outside'):
        migration.frame(encoded(data), '2026-09-10 00:15:00')
    with pytest.raises(ValueError, match='columns'):
        migration.frame(encoded(data.drop(columns='recv')), '2026-09-10 00:00:00')


def test_remaining_aliases_keep_symbol_and_direct_recorder():
    for alias in migration.SCOPES['remaining']:
        target, symbols = migration.SOURCES[alias]
        for symbol in symbols:
            key = f'market_data/{alias}/booktop/{symbol}/2026-09-10/{alias}_{symbol}_booktop_2026-09-10_eu-central-1a_FR01D_0000.parquet'
            destination, _ = migration.target_key(key)
            assert destination == key.replace(alias, target)
            with pytest.raises(ValueError):
                migration.target_key(key.replace(symbol, 'PAIR-UNEXPECTED-USD'))
    with pytest.raises(ValueError):
        migration.target_key('market_data/DERIVED_BTC_BITSTAMP/booktop/PAIR-BTC-USD/2026-09-10/DERIVED_BTC_BITSTAMP_PAIR-BTC-USD_booktop_2026-09-10_eu-central-1a_FR01D_0000.parquet')


def test_same_timestamp_different_quantities_are_preserved():
    source = pd.DataFrame({'timestamp': [1], 'recv': [2], 'ask_qty': [3.22]})
    destination = pd.DataFrame({'timestamp': [1], 'recv': [3], 'ask_qty': [4.305]})
    merged = migration.merge(source, destination)
    assert len(merged) == 2
    assert migration.contains(merged, source)
    assert migration.contains(merged, destination)


def test_coinbase_ema_keeps_producer_identity_before_pipeline_aliases():
    for alias in ('EMA_ALTS_COINBS', 'EMA_BTC_COINBS', 'EMA_ETH_COINBS'):
        assert migration.SOURCES[alias][0] == 'DERIVED_COINBS_BNBSPOT'
