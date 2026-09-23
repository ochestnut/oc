#!/usr/bin/env python3
"""Build a checked-in baseline, then collect read-only evidence on a producer host."""
from __future__ import annotations

import argparse
import configparser
import fnmatch
import glob
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess
import time


PROFILES = {
    "bvod": ("bvod_md_test", {"log_to_s3_ty04d_bnbfut", "log_to_s3_ty04d_derived"}),
    "central-ondo": ("timescaledb/umm2prod-03", {"log_to_s3_ty03d_ondo"}),
}


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def options(command, name):
    words = shlex.split(command)
    return [words[i + 1] for i, word in enumerate(words[:-1]) if word == name]


def build_manifest(root, profile):
    # Only baseline construction needs hjson; the live probe uses the standard library.
    import hjson

    relative, selected = PROFILES[profile]
    folder = Path(root) / relative
    parser = configparser.ConfigParser(interpolation=None, strict=False)
    parser.read(folder / "supervisord.conf")
    uploaders = []
    for name in sorted(selected):
        section = parser["program:" + name]
        command = section["command"]
        uploaders.append({
            "name": name, "patterns": options(command, "--pattern"),
            "log_dir": options(command, "--log-dir")[0],
            "tmp_dir": options(command, "--tmp-dir")[0],
            "recorder": options(command, "--recorder")[0],
            "logs": [section[key].replace("%(program_name)s", name)
                     for key in ("stdout_logfile", "stderr_logfile") if key in section],
        })
    producers = []
    for section_name in parser.sections():
        if not section_name.startswith("program:"):
            continue
        command = parser[section_name].get("command", "")
        if not any(writer in command for writer in
                   ("shmem_writer_booktop.py", "shmem_writer_derived_booktop.py")):
            continue
        configs = options(command, "--config")
        if not configs:
            continue
        source = folder / "config" / Path(configs[0]).name
        config = hjson.loads(source.read_text())
        log = config.get("log_file", "")
        matched = [u["name"] for u in uploaders if
                   str(Path(log).parent) == u["log_dir"] and any(
                       fnmatch.fnmatch(Path(log).name.replace("%T", "20260912_000000"), p)
                       for p in u["patterns"])]
        if matched:
            exchange = config.get("output", {}).get("exchange_name") or config.get("ref", {}).get("exchange")
            symbols = [item.get("output", item.get("instrument")) if isinstance(item, dict) else item
                       for item in config.get("instruments", [])]
            symbols += [item.get("output_instrument") for item in config.get("pairs", [])]
            producers.append({"name": section_name.split(":", 1)[1],
                              "config": configs[0], "sha256": digest(source),
                              "structured_expected": config.get("structured_recorder_mode", False),
                              "expected_pairs": [[exchange, symbol] for symbol in symbols if symbol and exchange],
                              "log_pattern": log.replace("%T", "*"), "uploaders": matched})
    if not producers:
        raise ValueError("no matching producers discovered")
    return {"version": 1, "profile": profile, "producers": producers, "uploaders": uploaders}


def tail(path, limit):
    with open(path, "rb") as stream:
        size = stream.seek(0, os.SEEK_END)
        start = max(0, size - limit)
        stream.seek(start)
        data = stream.read(limit)
    if start:
        data = data.partition(b"\n")[2]  # discard partial first record
    return data.decode("utf-8", errors="replace")


def log_evidence(path, limit, now):
    result = {"path": str(path)}
    try:
        stat = Path(path).stat()
        content = tail(path, limit)
        result.update(size=stat.st_size, age_seconds=round(now - stat.st_mtime, 1))
        records = re.findall(r"MD\|B\|([^\r\n]+)", content)
        pairs = set()
        malformed = 0
        for record in records:
            fields = record.split("|")
            try:
                if len(fields) < 8:
                    raise ValueError()
                for value in fields[2:8]:
                    int(value)
                pairs.add((fields[0], fields[1]))
            except ValueError:
                malformed += 1
        result.update(structured_records_sampled=len(records),
                      current_format_pairs=sorted(pairs),
                      legacy_or_malformed_records_sampled=malformed,
                      error_lines_sampled=sum(bool(re.search(r"error|exception|traceback", line, re.I))
                                              for line in content.splitlines()),
                      successful_upload_lines_sampled=content.count("S3 →"))
        checkpoint = Path(str(path) + ".s3.position")
        if checkpoint.exists():
            position = int(checkpoint.read_text().strip())
            result.update(checkpoint=position, unread_bytes=max(0, stat.st_size - position),
                          checkpoint_out_of_range=position < 0 or position > stat.st_size)
        else:
            result["checkpoint"] = None
    except (OSError, ValueError) as exc:
        result["error"] = type(exc).__name__
    return result


def supervisor_status(executable, config):
    command = [executable] + (["-c", config] if config else []) + ["status"]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=15)
        states = {}
        for line in result.stdout.splitlines():
            match = re.match(r"^(\S+)\s+(RUNNING|STOPPED|STARTING|BACKOFF|STOPPING|EXITED|FATAL|UNKNOWN)\b", line)
            if match:
                states[match[1]] = match[2]
        return states, None if states else "supervisor status unavailable; check executable, config, and permissions"
    except (OSError, subprocess.TimeoutExpired):
        return {}, "supervisor status unavailable; check executable, config, and permissions"


def snapshot(manifest, args):
    now = time.time()
    states, error = supervisor_status(args.supervisorctl, args.supervisor_config)
    result = {"time": now, "supervisor_error": error, "producers": [], "uploaders": []}
    def state(name):
        matches = [value for key, value in states.items() if key == name or key.endswith(":" + name)]
        return matches[0] if len(matches) == 1 else "unverified"
    for producer in manifest["producers"]:
        row = dict(producer, state=state(producer["name"]))
        try:
            row["config_matches_baseline"] = digest(producer["config"]) == producer["sha256"]
        except OSError as exc:
            row["config_error"] = type(exc).__name__
        try:
            paths = sorted(glob.glob(producer["log_pattern"]), key=os.path.getmtime, reverse=True)
            row["matching_files"] = len(paths)
            row["logs"] = [log_evidence(p, args.tail_bytes, now) for p in paths[:args.max_logs]]
            observed = {tuple(pair) for log in row["logs"] for pair in log.get("current_format_pairs", [])}
            row["expected_pairs_not_sampled"] = [pair for pair in producer.get("expected_pairs", [])
                                                if tuple(pair) not in observed]
        except OSError as exc:
            row.update(logs=[], log_error=type(exc).__name__)
        result["producers"].append(row)
    for uploader in manifest["uploaders"]:
        row = dict(uploader, state=state(uploader["name"]))
        row["logs"] = [log_evidence(p, args.tail_bytes, now) for p in uploader["logs"]]
        buffer = Path(uploader["tmp_dir"]) / uploader["recorder"] / "s3"
        row["buffer_exists"] = buffer.exists()
        try:
            batches = list(buffer.glob("*/*/*/*/batch_*.parquet"))
            stats = [p.stat() for p in batches]
            row.update(pending_batches=len(stats), pending_bytes=sum(s.st_size for s in stats),
                       oldest_batch_age_seconds=round(max((now - s.st_mtime for s in stats), default=0), 1))
        except OSError as exc:
            row["buffer_error"] = type(exc).__name__
        result["uploaders"].append(row)
    return result


def findings(before, after, stale):
    notes = []
    for group in ("producers", "uploaders"):
        previous = {row["name"]: row for row in before[group]}
        for row in after[group]:
            name = row["name"]
            if row["state"] != "RUNNING":
                notes.append(f"{name}: process state {row['state']}")
            if group == "producers":
                if not row.get("config_matches_baseline"):
                    notes.append(f"{name}: deployed config differs or cannot be read")
                if not row["logs"]:
                    notes.append(f"{name}: no readable matching producer log; check deployed log path")
                elif not any(log.get("age_seconds", float('inf')) <= stale for log in row["logs"]):
                    notes.append(f"{name}: sampled logs stale or unreadable")
                if row["logs"] and not any(log.get("current_format_pairs") for log in row["logs"]):
                    notes.append(f"{name}: no current-format structured booktops in bounded log sample")
                old_logs = {log["path"]: log for log in previous[name]["logs"]}
                for log in row["logs"]:
                    old = old_logs.get(log["path"], {})
                    if log.get("checkpoint_out_of_range"):
                        notes.append(f"{name}: checkpoint outside file bounds")
                    elif log.get("unread_bytes", 0) > 0 and log.get("checkpoint") == old.get("checkpoint"):
                        notes.append(f"{name}: unread bytes with no checkpoint progress during sample; possible backlog")
                    elif log.get("checkpoint") is None:
                        notes.append(f"{name}: s3 checkpoint missing or unreadable")
            else:
                if row.get("oldest_batch_age_seconds", 0) > stale:
                    notes.append(f"{name}: old pending batches; investigate upload backlog/errors")
                if any(log.get("error_lines_sampled", 0) for log in row["logs"]):
                    notes.append(f"{name}: error markers in sampled uploader logs; inspect locally")
    return notes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--build-manifest", metavar="CONFIG_REPO")
    mode.add_argument("--manifest", help="baseline json; run this mode on the target host")
    parser.add_argument("--profile", choices=PROFILES)
    parser.add_argument("--supervisorctl", default="supervisorctl")
    parser.add_argument("--supervisor-config")
    parser.add_argument("--interval", type=float, default=45)
    parser.add_argument("--stale-seconds", type=float, default=1800)
    parser.add_argument("--tail-bytes", type=int, default=262144)
    parser.add_argument("--max-logs", type=int, default=2)
    args = parser.parse_args()
    if args.interval <= 0 or args.stale_seconds <= 0 or args.tail_bytes <= 0 or args.max_logs <= 0:
        parser.error("sampling limits must be positive")
    if args.build_manifest:
        if not args.profile:
            parser.error("--profile is required with --build-manifest")
        print(json.dumps(build_manifest(args.build_manifest, args.profile), indent=2))
        return
    manifest = json.loads(Path(args.manifest).read_text())
    before = snapshot(manifest, args)
    time.sleep(args.interval)
    after = snapshot(manifest, args)
    print(json.dumps({"host": socket.gethostname(), "profile": manifest["profile"],
                      "limitations": "bounded samples; disk config is not proof of loaded config; no s3 verification; current evidence cannot prove the historical gap cause",
                      "findings": findings(before, after, args.stale_seconds),
                      "before": before, "after": after}, indent=2))


if __name__ == "__main__":
    main()
