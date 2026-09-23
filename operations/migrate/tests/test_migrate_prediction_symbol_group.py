from __future__ import annotations

import importlib.util
from concurrent.futures import Future
from io import BytesIO
from pathlib import Path
import sys
from typing import Any
from unittest.mock import patch

from botocore.exceptions import ClientError
import pandas as pd
import pytest


SCRIPT = Path(__file__).parents[1] / "migrate_prediction_symbol_group.py"
SPEC = importlib.util.spec_from_file_location("migrate_prediction_symbol_group", SCRIPT)
assert SPEC and SPEC.loader
MIGRATE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MIGRATE
SPEC.loader.exec_module(MIGRATE)


def _parquet(frame: pd.DataFrame) -> bytes:
    payload = BytesIO()
    frame.to_parquet(payload, index=False)
    return payload.getvalue()


class FakeS3:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = dict(objects)
        self.deleted: list[str] = []
        self.reads: list[str] = []

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[Key])}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.reads.append(Key)
        return {"Body": NonSeekableBody(self.objects[Key])}

    def put_object(
        self, *, Bucket: str, Key: str, Body: bytes, ContentType: str,
        ChecksumSHA256: str | None = None,
    ) -> None:
        self.objects[Key] = Body

    def copy_object(
        self, *, Bucket: str, Key: str, CopySource: dict[str, str],
        CopySourceIfMatch: str,
    ) -> None:
        self.objects[Key] = self.objects[CopySource["Key"]]

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        self.deleted.append(Key)
        del self.objects[Key]


class NonSeekableBody:
    def __init__(self, value: bytes) -> None:
        self.value = value

    def read(self) -> bytes:
        return self.value

    def seek(self, *args: object) -> None:
        raise OSError("seek is unsupported")


def test_parse_migration_reorders_legacy_key_and_derives_group() -> None:
    migration = MIGRATE.parse_migration(
        "market_data/KALSHI/booktop/predictions/"
        "PRED-BTC_2608241200_70000-USD/2026-08-24/chunk.parquet",
        123,
    )
    assert migration is not None
    assert migration.destination == (
        "market_data/KALSHI/booktop/predictions/2026-08-24/"
        "PRED-BTC_2608241200_70000-USD/chunk.parquet"
    )
    assert migration.group == "PRED-BTC-USD"
    assert migration.legacy
    assert migration.size == 123
    canonical = MIGRATE.parse_migration(migration.destination)
    assert canonical is not None and not canonical.legacy


def test_parse_canonical_migration_has_backup() -> None:
    key = (
        "market_data/KALSHI/booktop/predictions/2026-08-24/"
        "PRED-BTC_2608241200_70000-USD/chunk.parquet"
    )
    migration = MIGRATE.parse_migration(key)
    assert migration is not None
    assert not migration.legacy
    assert migration.destination == key
    assert migration.backup == f"migration_backups/prediction_symbol_group/{key}"


def test_parse_migration_rejects_unknown_contract_shape() -> None:
    with pytest.raises(ValueError, match="cannot derive prediction group"):
        MIGRATE.parse_migration(
            "market_data/KALSHI/trades/predictions/PRED-ODD/"
            "2026-08-24/chunk.parquet"
        )


def test_apply_enriches_and_retains_source_then_retire_revalidates() -> None:
    migration = MIGRATE.parse_migration(
        "market_data/KALSHI/trades/predictions/"
        "PRED-ETH_2608241200_4000-USD/2026-08-24/chunk.parquet"
    )
    assert migration is not None
    source = pd.DataFrame({"timestamp": [pd.Timestamp("2026-08-24")], "price": [0.5]})
    client = FakeS3({migration.source: _parquet(source)})

    assert MIGRATE.apply_one(client, "bucket", migration) == "applied"
    assert migration.source in client.objects
    migrated = pd.read_parquet(BytesIO(client.objects[migration.destination]))
    assert migrated["symbol"].tolist() == [migration.instrument]
    assert migrated["group"].tolist() == ["PRED-ETH-USD"]

    assert MIGRATE.apply_one(client, "bucket", migration) == "already-applied"
    assert MIGRATE.retire_one(client, "bucket", migration) == "retired"
    assert migration.source not in client.objects
    assert migration.destination in client.objects


def test_enrich_rejects_conflicting_identity() -> None:
    migration = MIGRATE.parse_migration(
        "market_data/GEMINI/booktop/predictions/"
        "PRED-BTC_2608241200_70000-USD/2026-08-24/chunk.parquet"
    )
    assert migration is not None
    frame = pd.DataFrame({"timestamp": [pd.Timestamp("2026-08-24")],
                          "symbol": ["PRED-WRONG_1_2-USD"]})
    with pytest.raises(RuntimeError, match="conflicting symbol"):
        MIGRATE.enrich(frame, migration)


def test_apply_canonical_backs_up_before_rewrite() -> None:
    key = (
        "market_data/KALSHI/booktop/predictions/2026-08-24/"
        "PRED-SOL_2608241200_200-USD/chunk.parquet"
    )
    migration = MIGRATE.parse_migration(key)
    assert migration is not None and migration.backup is not None
    source = pd.DataFrame({"timestamp": [pd.Timestamp("2026-08-24")], "bid_price": [0.4]})
    original = _parquet(source)
    client = FakeS3({key: original})

    assert MIGRATE.apply_one(client, "bucket", migration) == "applied"
    assert client.objects[migration.backup] == original
    migrated = pd.read_parquet(BytesIO(client.objects[key]))
    assert migrated["symbol"].tolist() == [migration.instrument]
    assert migrated["group"].tolist() == ["PRED-SOL-USD"]
    assert MIGRATE.apply_one(client, "bucket", migration) == "already-enriched"

    assert MIGRATE.retire_one(client, "bucket", migration) == "retired"
    assert migration.backup not in client.objects
    assert key in client.objects


def test_discovery_prefixes_narrows_bounded_canonical_instrument() -> None:
    assert MIGRATE.discovery_prefixes(
        ["KALSHI"], ["booktop"], {"PRED-BTC_1_2-USD"},
        "2026-08-18", "2026-08-19", "canonical",
    ) == [
        "market_data/KALSHI/booktop/predictions/2026-08-18/PRED-BTC_1_2-USD/"
    ]


def test_discovery_prefixes_targets_both_layouts_when_bounded() -> None:
    assert MIGRATE.discovery_prefixes(
        ["KALSHI"], ["trades"], {"PRED-ETH_1_2-USD"},
        "2026-08-18", "2026-08-19", "both",
    ) == [
        "market_data/KALSHI/trades/predictions/2026-08-18/PRED-ETH_1_2-USD/",
        "market_data/KALSHI/trades/predictions/PRED-ETH_1_2-USD/",
    ]


def test_migrate_canonical_validates_and_removes_backup() -> None:
    key = (
        "market_data/KALSHI/booktop/predictions/2026-08-24/"
        "PRED-XRP_2608241200_1.25-USD/chunk.parquet"
    )
    migration = MIGRATE.parse_migration(key)
    assert migration is not None and migration.backup is not None
    source = pd.DataFrame({"timestamp": [pd.Timestamp("2026-08-24")], "bid_price": [0.3]})
    client = FakeS3({key: _parquet(source)})

    assert MIGRATE.migrate_one(client, "bucket", migration) == "migrated"
    assert migration.backup not in client.objects
    migrated = pd.read_parquet(BytesIO(client.objects[key]))
    assert migrated["symbol"].tolist() == [migration.instrument]
    assert migrated["group"].tolist() == ["PRED-XRP-USD"]
    assert client.reads == [key, key]


def test_migrate_legacy_validates_and_removes_source() -> None:
    source_key = (
        "market_data/KALSHI/trades/predictions/"
        "PRED-BTC_2608241200_70000-USD/2026-08-24/chunk.parquet"
    )
    migration = MIGRATE.parse_migration(source_key)
    assert migration is not None
    source = pd.DataFrame({"timestamp": [pd.Timestamp("2026-08-24")], "price": [0.6]})
    client = FakeS3({source_key: _parquet(source)})

    assert MIGRATE.migrate_one(client, "bucket", migration) == "migrated"
    assert source_key not in client.objects
    assert migration.destination in client.objects
    assert client.reads == [source_key, migration.destination]


def test_fast_migrate_uses_one_read_and_atomic_put() -> None:
    key = (
        "market_data/KALSHI/booktop/predictions/2026-08-24/"
        "PRED-BTC_2608241200_70000-USD/chunk.parquet"
    )
    migration = MIGRATE.parse_migration(key)
    assert migration is not None
    source = pd.DataFrame({"timestamp": [pd.Timestamp("2026-08-24")], "bid_price": [0.2]})
    client = FakeS3({key: _parquet(source)})

    assert MIGRATE.fast_migrate_one(client, "bucket", migration) == "migrated"
    assert client.reads == [key]
    migrated = pd.read_parquet(BytesIO(client.objects[key]))
    assert migrated["symbol"].tolist() == [migration.instrument]
    assert migrated["group"].tolist() == ["PRED-BTC-USD"]


def test_migration_rejects_timestamp_index_before_writing() -> None:
    migration = MIGRATE.parse_migration(
        "market_data/GEMINI/booktop/predictions/2026-08-24/"
        "PRED-BTC_1_2-USD/chunk.parquet"
    )
    source = pd.DataFrame({"price": [0.5]},
                          index=pd.DatetimeIndex(["2026-08-24"], name="timestamp"))
    with pytest.raises(ValueError, match="no timestamp column"):
        MIGRATE.enrich(source, migration)


def test_overlapping_layouts_fail_before_any_operation() -> None:
    canonical = MIGRATE.parse_migration(
        "market_data/GEMINI/booktop/predictions/2026-08-24/"
        "PRED-BTC_1_2-USD/chunk.parquet"
    )
    legacy = MIGRATE.parse_migration(
        "market_data/GEMINI/booktop/predictions/"
        "PRED-BTC_1_2-USD/2026-08-24/chunk.parquet"
    )
    calls = []
    with pytest.raises(ValueError, match="multiple sources target"):
        list(MIGRATE.run_all(lambda *args: calls.append(args), None, "bucket",
                             [canonical, legacy], 2))
    assert calls == []


def test_migrate_does_not_restore_unverified_backup() -> None:
    migration = MIGRATE.parse_migration(
        "market_data/GEMINI/booktop/predictions/2026-08-24/PRED-BTC_1_2-USD/chunk.parquet"
    )
    source = _parquet(pd.DataFrame({"timestamp": [pd.Timestamp("2026-08-24")], "price": [0.5]}))
    stale_backup = b"unverified backup from an earlier attempt"
    client = FakeS3({migration.source: source, migration.backup: stale_backup})
    with pytest.raises(RuntimeError, match="unverified backup"):
        MIGRATE.migrate_one(client, "bucket", migration)
    assert client.objects[migration.source] == source
    assert client.objects[migration.backup] == stale_backup
    assert client.deleted == []


def test_migrate_restores_verified_backup_after_failed_write() -> None:
    migration = MIGRATE.parse_migration(
        "market_data/GEMINI/booktop/predictions/2026-08-24/PRED-BTC_1_2-USD/chunk.parquet"
    )
    source = _parquet(pd.DataFrame({"timestamp": [pd.Timestamp("2026-08-24")], "price": [0.5]}))

    class CorruptingS3(FakeS3):
        def put_object(self, *, Key: str, **kwargs: Any) -> None:
            self.objects[Key] = b"bad parquet"

    client = CorruptingS3({migration.source: source})
    with pytest.raises(Exception, match="Parquet"):
        MIGRATE.migrate_one(client, "bucket", migration)
    assert client.objects[migration.source] == source
    assert client.objects[migration.backup] == source
    assert client.deleted == []


@pytest.mark.parametrize("operation", ["apply_one", "migrate_one", "fast_migrate_one"])
def test_enriched_identity_does_not_bypass_timestamp_validation(operation: str) -> None:
    migration = MIGRATE.parse_migration(
        "market_data/GEMINI/booktop/predictions/2026-08-24/PRED-BTC_1_2-USD/chunk.parquet"
    )
    source = _parquet(pd.DataFrame({"symbol": [migration.instrument], "group": [migration.group]}))
    client = FakeS3({migration.source: source})
    with pytest.raises(ValueError, match="no timestamp column"):
        getattr(MIGRATE, operation)(client, "bucket", migration)
    assert client.objects == {migration.source: source}
    assert client.deleted == []


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("deleted_remotely", [False, True])
def test_retirement_failure_preserves_validated_destination(
    legacy: bool, deleted_remotely: bool,
) -> None:
    partition = (
        "PRED-BTC_1_2-USD/2026-08-24" if legacy
        else "2026-08-24/PRED-BTC_1_2-USD"
    )
    migration = MIGRATE.parse_migration(
        f"market_data/GEMINI/booktop/predictions/{partition}/chunk.parquet"
    )
    source = pd.DataFrame({"timestamp": [pd.Timestamp("2026-08-24")], "price": [0.5]})
    retire_key = migration.source if legacy else migration.backup

    class FailingDeleteS3(FakeS3):
        def delete_object(self, *, Bucket: str, Key: str) -> None:
            assert Key == retire_key, "must not delete the validated destination"
            if deleted_remotely:
                super().delete_object(Bucket=Bucket, Key=Key)
            raise TimeoutError("retirement response lost")

    client = FailingDeleteS3({migration.source: _parquet(source)})
    with pytest.raises(TimeoutError, match="retirement response lost"):
        MIGRATE.migrate_one(client, "bucket", migration)

    destination = pd.read_parquet(BytesIO(client.objects[migration.destination]))
    pd.testing.assert_frame_equal(destination, MIGRATE.enrich(source, migration))
    assert (retire_key in client.objects) == (not deleted_remotely)


@pytest.mark.parametrize("previous_migration", [False, True])
def test_phased_modes_accept_complete_canonical_objects(previous_migration: bool) -> None:
    migration = MIGRATE.parse_migration(
        "market_data/GEMINI/booktop/predictions/2026-08-24/PRED-BTC_1_2-USD/chunk.parquet"
    )
    source = pd.DataFrame({"timestamp": [pd.Timestamp("2026-08-24")], "price": [0.5]})
    if previous_migration:
        client = FakeS3({migration.source: _parquet(source)})
        assert MIGRATE.migrate_one(client, "bucket", migration) == "migrated"
    else:
        client = FakeS3({migration.source: _parquet(MIGRATE.enrich(source, migration))})
    objects = dict(client.objects)
    deleted = list(client.deleted)

    assert MIGRATE.apply_one(client, "bucket", migration) == "already-enriched"
    assert MIGRATE.validate_one(client, "bucket", migration) == "already-enriched"
    assert MIGRATE.retire_one(client, "bucket", migration) == "already-retired"
    assert client.objects == objects
    assert client.deleted == deleted


@pytest.mark.parametrize("operation", ["validate_one", "retire_one"])
def test_missing_backup_does_not_skip_unenriched_object(operation: str) -> None:
    migration = MIGRATE.parse_migration(
        "market_data/GEMINI/booktop/predictions/2026-08-24/PRED-BTC_1_2-USD/chunk.parquet"
    )
    original = _parquet(pd.DataFrame({"timestamp": [pd.Timestamp("2026-08-24")], "price": [0.5]}))
    client = FakeS3({migration.source: original})
    with pytest.raises(FileNotFoundError):
        getattr(MIGRATE, operation)(client, "bucket", migration)
    assert client.objects == {migration.source: original}
    assert client.deleted == []


@pytest.mark.parametrize("operation", ["apply_one", "validate_one", "retire_one"])
def test_enriched_object_still_compared_with_existing_backup(operation: str) -> None:
    migration = MIGRATE.parse_migration(
        "market_data/GEMINI/booktop/predictions/2026-08-24/PRED-BTC_1_2-USD/chunk.parquet"
    )
    source = pd.DataFrame({"timestamp": [pd.Timestamp("2026-08-24")], "price": [0.5]})
    corrupted = MIGRATE.enrich(source, migration)
    corrupted["price"] = 0.9
    objects = {migration.source: _parquet(corrupted), migration.backup: _parquet(source)}
    client = FakeS3(objects)
    with pytest.raises(RuntimeError, match="content mismatch"):
        getattr(MIGRATE, operation)(client, "bucket", migration)
    assert client.objects == objects
    assert client.deleted == []


@pytest.mark.parametrize("mode", ["success", "failure", "close"])
def test_migration_queue_stops_pending_work(mode: str) -> None:
    migrations = [MIGRATE.parse_migration(
        f"market_data/GEMINI/booktop/predictions/2026-08-24/PRED-BTC_1_{i}-USD/chunk.parquet"
    ) for i in range(20)]
    submitted: list[Future[str]] = []

    def submit(*args: Any) -> Future[str]:
        future: Future[str] = Future()
        submitted.append(future)
        return future

    def complete(pending: set[Future[str]], **kwargs: Any) -> tuple:
        assert 0 < len(pending) <= 8
        # Return a successful result before the failure to exercise whole-batch
        # checking, independently of thread scheduling and set iteration order.
        done = [future for future in submitted if future in pending][:2]
        for index, future in enumerate(done):
            if mode == "failure" and index == 1:
                future.set_exception(RuntimeError("migration failed"))
            else:
                future.set_result("migrated")
        return done, pending - set(done)

    with patch.object(MIGRATE, "ThreadPoolExecutor") as executor, \
         patch.object(MIGRATE, "wait", side_effect=complete):
        executor.return_value.__enter__.return_value.submit.side_effect = submit
        results = MIGRATE.run_all(lambda *args: "migrated", None, "bucket", migrations, 4)
        if mode == "success":
            assert list(results) == ["migrated"] * len(migrations)
            assert len(submitted) == len(migrations)
        else:
            if mode == "failure":
                with pytest.raises(RuntimeError, match="migration failed"):
                    list(results)
            else:
                assert next(results) == "migrated"
                results.close()
            assert len(submitted) == 8
            assert sum(future.cancelled() for future in submitted) == 6
