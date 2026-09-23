#!/usr/bin/env python3
"""Serial native Influx shard backups, launched locally over SSH.

Requires local boto3/AWS credentials and remote influxd, python3, sudo (for disk
inventory), nice and ionice. Uses the server's own Influx binary. Never modifies
Influx data or retention. Verified staging copies are removed; failures retain
files for inspection. Resume with the same arguments plus --resume RUN/run.json.
"""
from __future__ import annotations

import argparse
import base64
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import shutil
import subprocess
import time
from typing import Any
import uuid
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import boto3
import hjson
from botocore.config import Config


GIB = 1024 ** 3
PART_SIZE = 16 * 1024 ** 2
SSH_OPTIONS = ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=10"]
INVENTORY = """
import json, pathlib, subprocess, sys
root = pathlib.Path(sys.argv[1])
if not root.is_dir(): raise RuntimeError('Database directory does not exist: ' + str(root))
rows = []
for rp in sorted(root.iterdir()):
    if not rp.is_dir() or rp.name.startswith('_'): continue
    for shard in sorted(rp.iterdir()):
        if shard.is_dir() and shard.name.isdigit():
            size = int(subprocess.check_output(['du', '-sb', str(shard)]).split()[0])
            rows.append(dict(rp=rp.name, id=int(shard.name), bytes=size))
print(json.dumps(rows))
"""
# Monitor on the server so disk checks continue if the local machine disconnects.
BACKUP = """
import os, pathlib, shutil, signal, subprocess, sys
path, reserve, database, rp, shard, expected_parent = sys.argv[1:]
reserve = int(reserve)
expected_parent = int(expected_parent) if expected_parent else None
def interrupted(signum, frame): raise RuntimeError('Backup client interrupted')
for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT): signal.signal(sig, interrupted)
pathlib.Path(path).mkdir(mode=0o700)
proc = subprocess.Popen(['nice', '-n', '10', 'ionice', '-c', '3', 'influxd',
    'backup', '-portable', '-host', '127.0.0.1:8088', '-db', database,
    '-rp', rp, '-shard', shard, path], start_new_session=True)
try:
    while proc.poll() is None:
        if expected_parent is not None and os.getppid() != expected_parent:
            raise RuntimeError('Backup controller stopped')
        if shutil.disk_usage(path).free < reserve:
            raise RuntimeError('Free disk space below reserve; stopping backup client')
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: pass
    if proc.returncode: raise RuntimeError('influxd backup failed: ' + str(proc.returncode))
finally:
    if proc.poll() is None:
        os.killpg(proc.pid, signal.SIGTERM)
        try: proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
"""


def ssh(target: str, *command: str) -> str:
    if target == "local":
        return subprocess.check_output(command, text=True).strip()
    return subprocess.check_output(["ssh", *SSH_OPTIONS, target, shlex.join(command)], text=True).strip()


def api_inventory(database: str) -> list[dict[str, Any]]:
    """List shards through the local authenticated API; disk sizes are unknown."""
    root = Path(os.environ.get("JST_ROOT", str(Path.cwd().parent)))
    with (root / ".creds").open() as stream:
        credentials = hjson.load(stream)["INFLUX"]
    request = Request("http://127.0.0.1:8086/query?" + urlencode({"q": "SHOW SHARDS"}))
    token = base64.b64encode(
        (credentials["username"] + ":" + credentials["password"]).encode()
    ).decode()
    request.add_header("Authorization", "Basic " + token)
    with urlopen(request, timeout=30) as response:
        payload = json.load(response)
    if payload.get("error"):
        raise RuntimeError("Influx shard inventory query failed")
    rows = []
    for result in payload.get("results", []):
        if result.get("error"):
            raise RuntimeError("Influx shard inventory query failed")
        for series in result.get("series", []):
            for values in series.get("values", []):
                row = dict(zip(series["columns"], values))
                if row.get("database") == database:
                    rows.append({"id": int(row["id"]), "rp": row["retention_policy"], "bytes": None})
    if not rows:
        raise RuntimeError("No shards found; refusing to publish an empty backup")
    return sorted(rows, key=lambda row: row["id"])


def inventory(args: argparse.Namespace) -> list[dict[str, Any]]:
    if getattr(args, "inventory_api", False):
        return api_inventory(args.database)
    rows = json.loads(ssh(args.ssh, "sudo", "-n", "python3", "-c", INVENTORY,
                          str(PurePosixPath(args.data_dir) / args.database)))
    if not rows:
        raise RuntimeError("No shards found; refusing to publish an empty backup")
    return sorted(rows, key=lambda row: row["bytes"])


def remote_free(args: argparse.Namespace) -> int:
    return int(ssh(args.ssh, "python3", "-c",
                   "import shutil,sys; print(shutil.disk_usage(sys.argv[1]).free)", args.remote_dir))


def check_space(shard_bytes: int | None, remote_bytes: int, local_bytes: int, reserve: int) -> None:
    if shard_bytes is None:
        if min(remote_bytes, local_bytes) <= reserve:
            raise RuntimeError("Free disk space below reserve")
        return
    # Native conversion may temporarily retain uncompressed and compressed files.
    if remote_bytes < int(shard_bytes * 2.2) + reserve:
        raise RuntimeError("Insufficient remote space: require 2.2x shard size plus reserve")
    if local_bytes < int(shard_bytes * 1.2) + reserve:
        raise RuntimeError("Insufficient local space: require 1.2x shard size plus reserve")


def verify(s3: Any, bucket: str, item: dict[str, Any]) -> None:
    head = s3.head_object(Bucket=bucket, Key=item["key"], ChecksumMode="ENABLED")
    if head["ContentLength"] != item["bytes"] or head.get("ChecksumSHA256") != item["s3_checksum"]:
        raise RuntimeError(f'S3 verification failed: {item["key"]}; staging retained')


def upload_verified(s3: Any, bucket: str, key: str, path: Path) -> dict[str, Any]:
    """S3 validates each part's SHA256; verify the final composite checksum too."""
    upload_id = s3.create_multipart_upload(Bucket=bucket, Key=key, ChecksumAlgorithm="SHA256")["UploadId"]
    parts: list[dict[str, Any]] = []
    digests = bytearray()
    whole = hashlib.sha256()
    size = 0
    try:
        total = path.stat().st_size
        started = time.monotonic()
        completed = 0
        print(f"Uploading {path.name} to S3 ({total / GIB:.2f} GiB)", flush=True)
        # Four parts at a time bounds buffering and keeps requests in flight.
        with ThreadPoolExecutor(max_workers=4) as pool, path.open("rb") as stream:
            while True:
                pending: list[tuple[Future[Any], int, str, int]] = []
                for _ in range(4):
                    block = stream.read(PART_SIZE)
                    if not block:
                        break
                    digest = hashlib.sha256(block).digest()
                    encoded = base64.b64encode(digest).decode()
                    number = len(parts) + len(pending) + 1
                    future = pool.submit(s3.upload_part, Bucket=bucket, Key=key,
                                         UploadId=upload_id, PartNumber=number,
                                         Body=block, ChecksumSHA256=encoded)
                    pending.append((future, number, encoded, len(block)))
                    digests.extend(digest)
                    whole.update(block)
                    size += len(block)
                if not pending:
                    break
                for future, number, encoded, length in pending:
                    response = future.result()
                    parts.append({"PartNumber": number, "ETag": response["ETag"], "ChecksumSHA256": encoded})
                    completed += length
                    elapsed = max(time.monotonic() - started, 0.001)
                    print(f"  S3 {completed / total:.0%}: {completed / GIB:.2f}/{total / GIB:.2f} GiB "
                      f"({completed / 1024**2 / elapsed:.1f} MiB/s average)", flush=True)
        if not parts:
            raise RuntimeError(f"Unexpected empty backup file: {path}")
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts})
    except BaseException:
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise
    checksum = base64.b64encode(hashlib.sha256(digests).digest()).decode() + f"-{len(parts)}"
    item = {"key": key, "bytes": size, "sha256": whole.hexdigest(), "s3_checksum": checksum}
    print(f"Verifying S3 checksum: {path.name}", flush=True)
    verify(s3, bucket, item)
    return item


def cleanup_stage(args: argparse.Namespace, remote_root: str, local_root: Path, attempt: str) -> None:
    if not re.fullmatch(r"shard-[0-9]+-[a-f0-9]{8}", attempt):
        raise RuntimeError("Invalid saved staging directory")
    ssh(args.ssh, "rm", "-rf", "--", str(PurePosixPath(remote_root) / attempt))
    local = local_root / attempt
    if local.exists():
        shutil.rmtree(local)


def save(path: Path, state: dict[str, Any]) -> None:
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(state, indent=2) + "\n")
    temp.replace(path)



def host_pressure(args: argparse.Namespace) -> tuple[float, float]:
    """Return load per CPU and CPU I/O-wait percentage over five seconds."""
    program = """
import os, pathlib, time

def cpu(): return [int(v) for v in pathlib.Path('/proc/stat').read_text().splitlines()[0].split()[1:9]]
a = cpu(); time.sleep(5); b = cpu(); d = [y-x for x,y in zip(a,b)]; total = sum(d)
print(os.getloadavg()[0] / (os.cpu_count() or 1), 100*d[4]/total if total else 0)
"""
    load, iowait = ssh(args.ssh, "python3", "-c", program).split()
    return float(load), float(iowait)


def wait_for_capacity(args: argparse.Namespace) -> None:
    if args.max_load_per_cpu is None and args.max_iowait_pct is None:
        return
    while True:
        load, iowait = host_pressure(args)
        load_ok = args.max_load_per_cpu is None or load <= args.max_load_per_cpu
        io_ok = args.max_iowait_pct is None or iowait <= args.max_iowait_pct
        print(f"Host gate: load/cpu={load:.2f}, iowait={iowait:.1f}%", flush=True)
        if load_ok and io_ok:
            return
        print(f"Host busy; waiting {args.health_retry_seconds}s before next shard", flush=True)
        time.sleep(args.health_retry_seconds)

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ssh", required=True, help="SSH destination, or 'local' to run directly on the Influx server")
    parser.add_argument("--recorder", required=True)
    parser.add_argument("--database", default="UMM_MD")
    parser.add_argument("--data-dir", default="/var/lib/influxdb/data")
    parser.add_argument("--remote-dir", default="/tmp")
    parser.add_argument("--local-dir", type=Path, default=Path.home() / "influx_backups")
    parser.add_argument("--bucket", default="jstdata")
    parser.add_argument("--prefix", default="market_data/influx_archive")
    parser.add_argument("--profile")
    parser.add_argument("--inventory-api", action="store_true", help="Direct server mode: query local Influx using .creds INFLUX credentials instead of sudo disk inventory")
    parser.add_argument("--reserve-gib", type=int, default=10)
    parser.add_argument("--pause-between-shards", type=int, default=0)
    parser.add_argument("--max-load-per-cpu", type=float)
    parser.add_argument("--max-iowait-pct", type=float)
    parser.add_argument("--health-retry-seconds", type=int, default=60)
    parser.add_argument("--resume", type=Path, help="Existing run.json; reverify and skip completed shards")
    parser.add_argument("--check", action="store_true", help="Inventory shards and check space only; no backups/uploads")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.@-]*", args.ssh):
        parser.error("Invalid SSH destination")
    if any(not re.fullmatch(r"[A-Za-z0-9_-]+", v) for v in (args.recorder, args.database)):
        parser.error("Invalid recorder/database name")
    if (args.pause_between_shards < 0 or args.health_retry_seconds < 1
            or args.max_load_per_cpu is not None and args.max_load_per_cpu <= 0
            or args.max_iowait_pct is not None and not 0 <= args.max_iowait_pct <= 100):
        parser.error("Invalid pacing or host-pressure threshold")
    if args.reserve_gib < 1 or not args.remote_dir.startswith("/") or not args.data_dir.startswith("/"):
        parser.error("Use absolute remote paths and a positive disk reserve")
    if args.inventory_api and args.ssh != "local":
        parser.error("--inventory-api requires --ssh local")
    version = ssh(args.ssh, "influxd", "version")
    shards = inventory(args)
    args.local_dir.mkdir(parents=True, exist_ok=True)
    free = remote_free(args)
    reserve = args.reserve_gib * GIB
    size = "disk sizes unavailable (API inventory)" if args.inventory_api else f"{sum(s['bytes'] for s in shards)/GIB:.1f} GiB on disk"
    print(f"{version}\n{len(shards)} shards, {size}. "
          f"Remote free: {free/GIB:.1f} GiB; reserve: {args.reserve_gib} GiB", flush=True)
    if args.check:
        for shard in shards:
            check_space(shard["bytes"], free, shutil.disk_usage(args.local_dir).free, reserve)
        print("Free-space reserve checked; shard capacity cannot be assessed without disk sizes." if args.inventory_api else "Space preflight passed. AWS, RPC backup and restore are not tested.")
        return
    settings = {k: getattr(args, k) for k in ("ssh", "recorder", "database", "data_dir", "remote_dir", "bucket", "prefix")}
    if args.inventory_api:
        settings["inventory_api"] = True
        print("Shard sizes unknown; capacity preflight unavailable. Free-space monitoring remains enabled.", flush=True)
    if args.resume:
        state_path = args.resume.resolve()
        state = json.loads(state_path.read_text())
        if state["settings"] != settings:
            raise RuntimeError("Resume settings differ from saved run")
    else:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        local = args.local_dir / f"influx-backup-{args.recorder}-{run_id}"
        local.mkdir(mode=0o700)
        state_path = local / "run.json"
        state = {"run_id": run_id, "settings": settings, "influx_version": version,
                 "shards": shards, "completed": {}, "restore_tested": False,
                 "scope": "Shards present at initial inventory; live writes are not an atomic snapshot"}
        save(state_path, state)
    run_id = state["run_id"]
    if not re.fullmatch(r"\d{8}T\d{6}Z-[a-f0-9]{8}", run_id):
        raise RuntimeError("Invalid saved run ID")
    remote_root = str(PurePosixPath(args.remote_dir) / f"influx-backup-{args.recorder}-{run_id}")
    prefix = f"{args.prefix.strip('/')}/{args.recorder}/{args.database}/{run_id}"
    print(f"Resume state: {state_path}\nDestination: s3://{args.bucket}/{prefix}/", flush=True)
    credentials = {}
    if not args.profile and not os.environ.get("AWS_ACCESS_KEY_ID"):
        root = Path(os.environ.get("JST_ROOT", str(Path.cwd().parent)))
        creds_path = root / ".creds"
        if creds_path.is_file():
            with creds_path.open() as stream:
                aws = hjson.load(stream).get("AWS", {})
            if aws.get("access_key") and aws.get("secret_key"):
                credentials = {"aws_access_key_id": aws["access_key"],
                               "aws_secret_access_key": aws["secret_key"]}
    s3 = boto3.Session(profile_name=args.profile, **credentials).client("s3", config=Config(connect_timeout=10, read_timeout=90, retries={"max_attempts": 3}))
    # Test the exact upload/checksum permissions before starting a large backup.
    upload_verified(s3, args.bucket, f"{prefix}/run-start.json", state_path)
    ssh(args.ssh, "mkdir", "-p", "-m", "700", remote_root)
    lock = str(PurePosixPath(args.remote_dir) / f".influx-backup-{args.database}.lock")
    ssh(args.ssh, "mkdir", "-m", "700", lock)
    try:
        for completed in state["completed"].values():
            for item in completed["files"]:
                verify(s3, args.bucket, item)
            cleanup_stage(args, remote_root, state_path.parent, completed["attempt"])
        pending = [p for p in state["shards"] if str(int(p["id"])) not in state["completed"]]
        for position, planned in enumerate(pending):
            shard_id = str(int(planned["id"]))
            if position and args.pause_between_shards:
                print(f"Pausing {args.pause_between_shards}s before next shard", flush=True)
                time.sleep(args.pause_between_shards)
            wait_for_capacity(args)
            staged = state.get("staged", {}).get(shard_id)
            if staged:
                attempt = staged
                if not re.fullmatch(r"shard-[0-9]+-[a-f0-9]{8}", attempt):
                    raise RuntimeError("Invalid saved staging directory")
                remote = str(PurePosixPath(remote_root) / attempt)
                local = Path(remote) if args.ssh == "local" else state_path.parent / attempt
                print(f"Reusing completed staging copy for shard {shard_id}", flush=True)
            else:
                shard_path = str(PurePosixPath(args.data_dir) / args.database / planned["rp"] / shard_id)
                shard_bytes = None if args.inventory_api else int(ssh(args.ssh, "sudo", "-n", "du", "-sb", shard_path).split()[0])
                check_space(shard_bytes, remote_free(args), shutil.disk_usage(state_path.parent).free, reserve)
                attempt = f"shard-{shard_id}-{uuid.uuid4().hex[:8]}"
                remote = str(PurePosixPath(remote_root) / attempt)
                local = state_path.parent / attempt
                if args.ssh != "local":
                    local.mkdir(mode=0o700)
                print(f"Backing up shard {shard_id}; staging {remote}", flush=True)
                print(ssh(args.ssh, "python3", "-c", BACKUP, remote, str(reserve), args.database, planned["rp"], shard_id, str(os.getpid()) if args.ssh == "local" else ""), flush=True)
                if args.ssh == "local":
                    local = Path(remote)
                else:
                    subprocess.run(["scp", "-r", "-l", "100000", *SSH_OPTIONS, f"{args.ssh}:{remote}/.", str(local)], check=True)
                state.setdefault("staged", {})[shard_id] = attempt
                save(state_path, state)
            files = sorted(local.iterdir())
            if not any(p.suffix == ".manifest" for p in files) or not any(p.name.endswith(f".s{shard_id}.tar.gz") for p in files):
                raise RuntimeError(f"Missing native manifest/shard archive in {local}; staging retained")
            uploaded = [upload_verified(s3, args.bucket, f"{prefix}/{attempt}/{p.name}", p) for p in files if p.is_file()]
            state["completed"][shard_id] = {"attempt": attempt, "files": uploaded}
            state.get("staged", {}).pop(shard_id, None)
            save(state_path, state)
            upload_verified(s3, args.bucket, f"{prefix}/progress.json", state_path)
            # Only this newly created staging directory is deleted, after checksum verification.
            cleanup_stage(args, remote_root, state_path.parent, attempt)
            print(f"Shard {shard_id} verified in S3; staging removed ({len(state['completed'])}/{len(state['shards'])})", flush=True)
        final = inventory(args)
        state["new_shards_during_backup"] = [s for s in final if s["id"] not in {p["id"] for p in state["shards"]}]
        state["finished_at"] = datetime.now(timezone.utc).isoformat()
        save(state_path, state)
        marker = "backup-complete.json" if not state["new_shards_during_backup"] else "backup-needs-followup.json"
        upload_verified(s3, args.bucket, f"{prefix}/{marker}", state_path)
        print(f"All inventoried shards verified. Report: s3://{args.bucket}/{prefix}/{marker}\n"
              "No retention changes made. Restore test still required.", flush=True)
    except BaseException:
        print(f"Stopped; staging retained. Remote lock retained: {lock}. "
              "Before resuming, confirm no backup client is running and remove this empty lock directory.", flush=True)
        raise
    else:
        ssh(args.ssh, "rmdir", lock)


if __name__ == "__main__":
    main()
