"""Backup safety contracts using synthetic shards and an in-memory S3 double."""
import base64
import hashlib
import importlib.util
from pathlib import Path
import subprocess
import sys
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

PATH = Path(__file__).parents[1] / 'backup_influx_to_s3.py'
SPEC = importlib.util.spec_from_file_location('backup_influx_to_s3', PATH)
assert SPEC and SPEC.loader
backup = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(backup)


class S3:
    def __init__(self, corrupt: bool = False) -> None:
        self.parts: dict[int, bytes] = {}
        self.corrupt = corrupt
        self.aborted = False

    def create_multipart_upload(self, **kwargs: Any) -> dict[str, str]:
        self.parts = {}
        return {'UploadId': 'test'}

    def upload_part(self, **kwargs: Any) -> dict[str, str]:
        body = kwargs['Body']
        assert kwargs['ChecksumSHA256'] == base64.b64encode(hashlib.sha256(body).digest()).decode()
        self.parts[kwargs['PartNumber']] = body
        return {'ETag': str(kwargs['PartNumber'])}

    def complete_multipart_upload(self, **kwargs: Any) -> None:
        assert len(kwargs['MultipartUpload']['Parts']) == len(self.parts)

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        digest = hashlib.sha256(b''.join(hashlib.sha256(p).digest() for _, p in sorted(self.parts.items()))).digest()
        checksum = base64.b64encode(digest).decode() + f'-{len(self.parts)}'
        return {'ContentLength': sum(map(len, self.parts.values())), 'ChecksumSHA256': 'wrong' if self.corrupt else checksum}

    def abort_multipart_upload(self, **kwargs: Any) -> None:
        self.aborted = True


@pytest.mark.parametrize('size', [1, 10, 11, 25])
def test_upload_verifies_single_and_multiple_parts(tmp_path: Path, size: int) -> None:
    file = tmp_path / 'backup'
    data = bytes(range(size))
    file.write_bytes(data)
    with patch.object(backup, 'PART_SIZE', 10):
        result = backup.upload_verified(S3(), 'bucket', 'key', file)
    assert result['sha256'] == hashlib.sha256(data).hexdigest()
    assert result['bytes'] == size


def test_corruption_stops_upload_verification(tmp_path: Path) -> None:
    file = tmp_path / 'backup'
    file.write_bytes(b'data')
    with pytest.raises(RuntimeError, match='verification failed'):
        backup.upload_verified(S3(corrupt=True), 'bucket', 'key', file)
    assert file.exists()


def test_failed_upload_aborts_multipart(tmp_path: Path) -> None:
    file = tmp_path / 'backup'
    file.write_bytes(b'data')
    s3 = S3()
    with patch.object(s3, 'upload_part', side_effect=RuntimeError('network')):
        with pytest.raises(RuntimeError, match='network'):
            backup.upload_verified(s3, 'bucket', 'key', file)
    assert s3.aborted


def test_headroom_requires_both_disks() -> None:
    backup.check_space(20, 55, 35, 10)
    with pytest.raises(RuntimeError, match='remote'):
        backup.check_space(20, 53, 35, 10)
    with pytest.raises(RuntimeError, match='local'):
        backup.check_space(20, 55, 33, 10)


def test_failed_backup_never_deletes_staging_or_marks_complete(tmp_path: Path) -> None:
    calls: list[tuple[str, ...]] = []
    def ssh(target: str, *command: str) -> str:
        calls.append(command)
        if backup.BACKUP in command:
            raise subprocess.CalledProcessError(1, command)
        if 'du' in command:
            return '100 /test/shard'
        return 'InfluxDB 1.11.8'
    argv = ['backup', '--ssh', 'ubuntu@fr01', '--recorder', 'FR01', '--local-dir', str(tmp_path)]
    with patch.object(sys, 'argv', argv), patch.object(backup, 'ssh', side_effect=ssh), \
         patch.object(backup, 'inventory', return_value=[{'id': 426, 'rp': 'autogen', 'bytes': 100}]), \
         patch.object(backup, 'remote_free', return_value=100 * backup.GIB), \
         patch.dict(backup.os.environ, {'AWS_ACCESS_KEY_ID': 'test'}), patch.object(backup, 'check_space'), patch.object(backup.boto3, 'Session', return_value=MagicMock()), \
         patch.object(backup, 'upload_verified') as upload:
        with pytest.raises(subprocess.CalledProcessError):
            backup.main()
    assert len(upload.call_args_list) == 1  # only the initial permission probe
    assert not any(c[0] == 'rm' for c in calls)
    assert list(tmp_path.glob('*/shard-426-*'))


def test_remote_programs_compile() -> None:
    compile(backup.BACKUP, '<remote-backup>', 'exec')
    compile(backup.INVENTORY, '<remote-inventory>', 'exec')


def test_resume_reverifies_and_does_not_repeat_backup(tmp_path: Path) -> None:
    events: list[str] = []
    def ssh(target: str, *command: str) -> str:
        if backup.BACKUP in command:
            events.append('backup')
        if command[0] == 'rm':
            events.append('cleanup')
        if 'du' in command:
            return '100 /test/shard'
        return 'InfluxDB 1.11.8'
    def scp(command: list[str], check: bool) -> None:
        local = Path(command[-1])
        (local/'test.manifest').write_text('{}')
        (local/'test.s426.tar.gz').write_bytes(b'shard')
        (local/'test.meta').write_bytes(b'meta')
    def upload(s3: Any, bucket: str, key: str, path: Path) -> dict[str, Any]:
        events.append('upload')
        return {'key': key, 'bytes': path.stat().st_size, 's3_checksum': 'verified'}
    argv = ['backup', '--ssh', 'ubuntu@fr01', '--recorder', 'FR01', '--local-dir', str(tmp_path)]
    with patch.object(sys, 'argv', argv), patch.object(backup, 'ssh', side_effect=ssh), \
         patch.object(backup, 'inventory', return_value=[{'id': 426, 'rp': 'autogen', 'bytes': 100}]), \
         patch.object(backup, 'remote_free', return_value=100 * backup.GIB), \
         patch.dict(backup.os.environ, {'AWS_ACCESS_KEY_ID': 'test'}), patch.object(backup, 'check_space'), patch.object(backup.boto3, 'Session', return_value=MagicMock()), \
         patch.object(backup, 'upload_verified', side_effect=upload), \
         patch.object(backup.subprocess, 'run', side_effect=scp), \
         patch.object(backup, 'verify', side_effect=lambda *a: events.append('verify')):
        backup.main()
        assert events.count('backup') == 1
        assert events.index('cleanup') > events.index('upload')
        state = next(tmp_path.glob('*/run.json'))
        with patch.object(sys, 'argv', argv + ['--resume', str(state)]):
            backup.main()
        assert events.count('backup') == 1
        assert events.count('verify') == 3
        assert events[-2] == 'cleanup'


def test_four_parts_upload_concurrently(tmp_path: Path) -> None:
    from threading import Barrier

    barrier = Barrier(4, timeout=5)

    class ConcurrentS3(S3):
        def upload_part(self, **kwargs: Any) -> dict[str, str]:
            barrier.wait()
            return super().upload_part(**kwargs)

    file = tmp_path / 'backup'
    data = bytes(range(40))
    file.write_bytes(data)
    with patch.object(backup, 'PART_SIZE', 10):
        result = backup.upload_verified(ConcurrentS3(), 'bucket', 'key', file)
    assert result['sha256'] == hashlib.sha256(data).hexdigest()


def test_api_inventory_filters_database_and_keeps_policy():
    import io
    import json
    from unittest.mock import mock_open
    payload = {"results": [{"series": [{"columns": ["id", "database", "retention_policy"],
        "values": [[9, "_internal", "monitor"], [2212, "UMM_MD", "two_months"]]}]}]}
    with patch.object(backup.Path, "open", mock_open(read_data='{"INFLUX":{"username":"test","password":"test"}}')), \
         patch.object(backup, "urlopen", return_value=io.StringIO(json.dumps(payload))) as request:
        assert backup.api_inventory("UMM_MD") == [{"id": 2212, "rp": "two_months", "bytes": None}]
    assert request.call_args.args[0].full_url.startswith("http://127.0.0.1:8086/query?")
    assert "test" not in request.call_args.args[0].full_url


def test_api_inventory_rejects_query_errors():
    import io
    from unittest.mock import mock_open
    with patch.object(backup.Path, "open", mock_open(read_data='{"INFLUX":{"username":"test","password":"test"}}')), \
         patch.object(backup, "urlopen", return_value=io.StringIO('{"results":[{"error":"denied"}]}')):
        with pytest.raises(RuntimeError, match="query failed"):
            backup.api_inventory("UMM_MD")


def test_unknown_shard_size_still_requires_disk_reserve():
    backup.check_space(None, 100, 100, 10)
    with pytest.raises(RuntimeError, match="reserve"):
        backup.check_space(None, 9, 100, 10)


def test_capacity_gate_waits_until_both_limits_pass():
    args = type('Args', (), {'max_load_per_cpu': 1.0, 'max_iowait_pct': 8.0,
                             'health_retry_seconds': 30})()
    with patch.object(backup, 'host_pressure', side_effect=[(1.2, 3.0), (0.7, 12.0), (0.8, 4.0)]), \
         patch.object(backup.time, 'sleep') as sleep:
        backup.wait_for_capacity(args)
    assert sleep.call_count == 2
    sleep.assert_called_with(30)


def test_backup_monitor_stops_if_controller_exits():
    assert "os.getppid() != expected_parent" in backup.BACKUP
    assert "Backup controller stopped" in backup.BACKUP
