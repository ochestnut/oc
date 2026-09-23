import importlib.util
from pathlib import Path
from types import SimpleNamespace


spec = importlib.util.spec_from_file_location(
    "runtime", Path(__file__).parents[1] / "check_booktop_runtime.py")
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


def test_log_sample_validates_format_and_checkpoint(tmp_path):
    log = tmp_path / "producer.log"
    log.write_text("prefix MD|B|BNBFUT|PERP-X-USDT|1|2|3|4|5|6\n"
                   "prefix MD|B|PERP-X-USDT|1|2|3|4|5|6\n")
    Path(str(log) + ".s3.position").write_text("5")
    evidence = runtime.log_evidence(log, 1024, log.stat().st_mtime)
    assert evidence["current_format_pairs"] == [("BNBFUT", "PERP-X-USDT")]
    assert evidence["legacy_or_malformed_records_sampled"] == 1
    assert evidence["unread_bytes"] == log.stat().st_size - 5


def test_tail_discards_partial_record(tmp_path):
    log = tmp_path / "producer.log"
    log.write_text("MD|B|BNBFUT|PERP-X-USDT|1|2|3|4|5|6\nend\n")
    assert runtime.tail(log, 20) == "end\n"


def test_snapshot_and_stall_detection(tmp_path, monkeypatch):
    config = tmp_path / "config.json5"
    config.write_text("{}")
    log = tmp_path / "producer.log"
    log.write_text("MD|B|ONDO|PERP-X-USDC|1|2|3|4|5|6\n")
    checkpoint = Path(str(log) + ".s3.position")
    checkpoint.write_text("0")
    monkeypatch.setattr(runtime, "supervisor_status", lambda *args: ({"feed": "RUNNING"}, None))
    manifest = {"producers": [{"name": "feed", "config": str(config),
                               "sha256": runtime.digest(config), "log_pattern": str(log),
                               "expected_pairs": [["ONDO", "PERP-X-USDC"]]}], "uploaders": []}
    args = SimpleNamespace(supervisorctl="unused", supervisor_config=None, tail_bytes=1024, max_logs=2)
    first = runtime.snapshot(manifest, args)
    assert first["producers"][0]["config_matches_baseline"]
    assert first["producers"][0]["expected_pairs_not_sampled"] == []
    assert any("no checkpoint progress" in note for note in runtime.findings(first, first, 1800))
    checkpoint.write_text(str(log.stat().st_size))
    assert runtime.findings(first, runtime.snapshot(manifest, args), 1800) == []
    config.write_text('{"changed": true}')
    assert any("deployed config differs" in note for note in
               runtime.findings(first, runtime.snapshot(manifest, args), 1800))


def test_supervisor_failure_is_unverified(monkeypatch):
    def fail(*args, **kwargs):
        raise FileNotFoundError()
    monkeypatch.setattr(runtime.subprocess, "run", fail)
    states, error = runtime.supervisor_status("absent", None)
    assert states == {}
    assert error
