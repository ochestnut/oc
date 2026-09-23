from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from typing import Any

from botocore.exceptions import ClientError
import pytest


SCRIPT = Path(__file__).parents[1] / "migrate_prediction_paths.py"
SPEC = importlib.util.spec_from_file_location("migrate_prediction_paths", SCRIPT)
assert SPEC and SPEC.loader
MIGRATE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MIGRATE
SPEC.loader.exec_module(MIGRATE)


def test_parse_move_reorders_only_legacy_prediction_keys() -> None:
    move = MIGRATE.parse_move(
        "market_data/KALSHI/booktop/predictions/PRED-ABC/2026-08-24/chunk.parquet",
        123,
    )
    assert move is not None
    assert move.destination == (
        "market_data/KALSHI/booktop/predictions/2026-08-24/PRED-ABC/chunk.parquet"
    )
    assert move.size == 123
    assert MIGRATE.parse_move(move.destination) is None
    assert MIGRATE.parse_move(
        "market_data/KALSHI/booktop/BTC-USD/2026-08-24/chunk.parquet"
    ) is None


class FakeS3:
    def __init__(self, source: str, size: int = 123) -> None:
        self.objects = {source: size}
        self.deleted: list[str] = []

    def head_object(
        self, *, Bucket: str, Key: str, ChecksumMode: str,
    ) -> dict[str, Any]:
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": self.objects[Key], "ETag": '"etag"'}

    def copy_object(
        self, *, Bucket: str, Key: str, CopySource: dict[str, str],
        CopySourceIfMatch: str,
    ) -> None:
        self.objects[Key] = self.objects[CopySource["Key"]]

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        self.deleted.append(Key)
        del self.objects[Key]


def test_apply_verifies_then_retires_source() -> None:
    move = MIGRATE.parse_move(
        "market_data/KALSHI/trades/predictions/PRED-ABC/2026-08-24/chunk.parquet"
    )
    assert move is not None
    client = FakeS3(move.source)

    assert MIGRATE.migrate_one(client, "bucket", move) == "copied"
    assert move.source not in client.objects
    assert move.destination in client.objects
    assert client.deleted == [move.source]


def test_migrate_all_processes_every_move(monkeypatch: pytest.MonkeyPatch) -> None:
    moves = [MIGRATE.Move(str(i), str(i), "X", "booktop", "P", "2026-08-24", 1)
             for i in range(10)]
    seen: list[str] = []

    def migrate_one(client: Any, bucket: str, move: Any) -> str:
        seen.append(move.source)
        return move.source

    monkeypatch.setattr(MIGRATE, "migrate_one", migrate_one)

    assert sorted(MIGRATE.migrate_all(None, "bucket", moves, 2), key=int) == [
        str(i) for i in range(10)
    ]
    assert sorted(seen, key=int) == [str(i) for i in range(10)]
