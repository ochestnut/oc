#!/usr/bin/env python3
"""Staged Bitstamp MD session reuse test using the unmodified production writer.

No orders, git operations, supervisor operations, or production output names.
Run preflight first; --run opens real connections using the supplied identity.
"""
import argparse
from collections import Counter
import configparser
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import time

PROFILES = {"jdbp": "BITSTAMP_FIX", "bbmm": "BITSTAMP_FIX", "bvbs": "BITSTAMPPERP_FIX"}
MARKER = re.compile(r"MD\|B\|([^|]+)\|([^|]+)\|(\d+)\|(\d+)\|(\d+)\|(\d+)\|(\d+)\|(\d+)")


def metrics(path, output):
    # The structured application log is authoritative; don't double-count stdout.
    text = path.read_text(errors="replace") if path.exists() else ""
    counts = Counter()
    invalid = 0
    last = 0
    for match in MARKER.finditer(text):
        venue, symbol, exch_ts, recv_ts, bid, bid_size, ask, ask_size = match.groups()
        if venue != output:
            continue
        if not (int(exch_ts) > 0 and int(recv_ts) > 0 and
                0 < int(bid) < int(ask) and int(bid_size) > 0 and int(ask_size) > 0):
            invalid += 1
            continue
        counts[symbol] += 1
        last = max(last, int(recv_ts))
    return {"counts": dict(counts), "invalid": invalid, "last_recv_ns": last,
            "logons": text.count("Bitstamp FIX session logged on"),
            "logouts": text.count("Bitstamp FIX session logged out"),
            "rejects": text.count("market-data request rejected")}


def stop(process):
    if process is None:
        return
    # Each child has its own process group, including the writer's listener child.
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=12)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def gateway_metrics(path):
    text = path.read_text(errors="replace") if path.exists() else ""
    text = text.replace("\x01", "|")
    sent, received = Counter(), Counter()
    for line in text.splitlines():
        match = re.search(r"\|35=([^|]+)\|", line)
        if not match:
            continue
        if "Sending FIX message:" in line:
            sent[match[1]] += 1
        elif "Processed message:" in line:
            received[match[1]] += 1
    return {"sent": dict(sent), "received": dict(received),
            "logons": text.count("Successfully logged on to FIX session"),
            "logouts": text.count("Received Logout message")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--umm", type=Path, default=Path.home() / "umm")
    parser.add_argument("--root", type=Path, default=Path.home())
    parser.add_argument("--credentials", type=Path, default=Path.home() / ".creds")
    parser.add_argument("--credentials-key", required=True)
    parser.add_argument("--sender-comp-id", required=True)
    parser.add_argument("--sender-sub-id", required=True)
    parser.add_argument("--port", type=int, default=15101)
    parser.add_argument("--stage-seconds", type=int, default=60)
    parser.add_argument("--overlap-seconds", type=int, default=180)
    parser.add_argument("--stale-seconds", type=int, default=30)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--with-gateway", action="store_true",
                        help="also run the real c++ trading gateway without any bot/client")
    parser.add_argument("--gateway-binary", type=Path)
    args = parser.parse_args()
    assert args.stage_seconds >= 30 and args.overlap_seconds >= 60
    assert args.stale_seconds >= 10
    args.umm = args.umm.resolve()
    args.root = args.root.resolve()
    os.environ["JST_ROOT"] = str(args.root)
    os.umask(0o077)
    templates = Path(__file__).resolve().parent
    sys.path[:0] = [str(args.umm / "src"), str(args.umm / "scripts/md_feed")]
    import json5
    import quickfix  # noqa: F401
    from umm.mktdata.md_configs import EXCHANGE_TO_MODULES, FIX_CFG_EXCHANGES
    from umm.mktdata.read_instrument_cfg import get_exchange_instruments
    from shmem_writer_booktop import load_config

    def interrupted(signum, frame):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)

    creds = json5.loads(args.credentials.read_text())[args.credentials_key]
    assert creds.get("api_key", creds.get("username")), "missing username/api_key"
    assert creds.get("secret_key", creds.get("password")), "missing password/secret_key"
    if args.with_gateway:
        assert creds.get("username") and creds.get("password"), "c++ gateway needs username/password fields"
        assert creds["username"] == creds.get("api_key", creds["username"]), "md and gateway usernames differ"
        assert creds["password"] == creds.get("secret_key", creds["password"]), "md and gateway passwords differ"
        args.gateway_binary = (args.gateway_binary or args.umm / "src/fix_gateway/build/fix_gateway").resolve()
        assert os.access(args.gateway_binary, os.X_OK), "gateway binary missing or not executable"
    del creds
    stunnel = shutil.which("stunnel4") or shutil.which("stunnel")
    assert stunnel, "stunnel is not installed"
    dictionary = args.umm / "venv/share/quickfix/FIX44.xml"
    assert dictionary.is_file(), "missing quickfix dictionary: " + str(dictionary)
    assert Path('/etc/ssl/certs/ca-certificates.crt').is_file(), "missing ubuntu ca bundle"
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", args.port))
    refdata = get_exchange_instruments()
    prepared = {}
    for name, transport in PROFILES.items():
        assert transport in EXCHANGE_TO_MODULES and transport in FIX_CFG_EXCHANGES, transport + " unsupported"
        cfg = json5.loads((templates / (name + ".json5")).read_text())
        venue = "BITSTAMPPERP" if name == "bvbs" else "BITSTAMP"
        for entry in cfg["instruments"]:
            assert refdata[venue]["instruments"][entry["instrument"]]["fix_md_symbol"]
        cfg["ref"].update(exchange=transport, credentials_file=str(args.credentials.resolve()),
                          credentials_key=args.credentials_key, proxy_ws_ip=None)
        cfg.update(main_core=None, ws_core=None, auto_remove=True,
                   recorder_mode=False, structured_recorder_mode=True, enable_measure_stats=False)
        prepared[name] = cfg
    if not args.run:
        print("preflight passed: dependencies, symbols, credentials fields and local port checked; no connections opened")
        print("repeat with --run only after stopping the test-account bots and checking outstanding orders")
        return

    run_dir = args.root / "bitstamp-connection-tests" / (time.strftime("%Y%m%d-%H%M%S") + "-" + str(os.getpid()))
    run_dir.mkdir(parents=True, mode=0o700)
    session = configparser.ConfigParser()
    session.optionxform = str
    session.read(templates / "session.cfg")
    session["DEFAULT"]["DataDictionary"] = str(dictionary)
    session["SESSION"].update(SenderCompID=args.sender_comp_id, SenderSubID=args.sender_sub_id,
                              SocketConnectPort=str(args.port))
    fix_path = run_dir / "session.cfg"
    with fix_path.open("w") as f:
        session.write(f)
    tunnel_path = run_dir / "stunnel.conf"
    tunnel_path.write_text(f"""foreground = yes
debug = info
pid = {run_dir / 'stunnel.pid'}
socket = l:TCP_NODELAY=1
socket = r:TCP_NODELAY=1
[bitstamp_test_md]
client = yes
accept = 127.0.0.1:{args.port}
connect = fixserver.bitstamp.net:5001
verifyChain = yes
checkHost = fixserver.bitstamp.net
CAfile = /etc/ssl/certs/ca-certificates.crt
""")
    for name, cfg in prepared.items():
        cfg["ref"]["fix_cfg_path"] = str(fix_path)
        cfg["output_exchange"] = "BITSTAMP_TEST_" + name.upper() + "_" + str(os.getpid())
        cfg["log_file"] = str(run_dir / (name + ".log"))
        path = run_dir / (name + ".json")
        path.write_text(json.dumps(cfg, indent=2))
        load_config(str(path))  # actual production config validation, no listener setup
    env = dict(os.environ, PYTHONPATH=str(args.umm / "src") + os.pathsep + str(args.root))
    children = {}
    handles = []
    report = {"scope": "three md processes, one host, one account; no order-entry session",
              "stages": [], "result": "incomplete"}
    tunnel = None
    gateway = None
    gateway_log = run_dir / "gateway.log"
    if args.with_gateway:
        report["scope"] = "real c++ gateway plus three md processes; one host/account; no orders"
        (run_dir / "gateway.cfg").write_text(f"""server_id=BITSTAMP_CONN_TEST_{os.getpid()}
exchange_id=BITSTAMP
fix_channel=BITSTAMP_CONN_TEST_{os.getpid()}
redis_url=redis://localhost
fix_host=fixserver.bitstamp.net
fix_port=5001
fix_use_ssl=true
fix_version=FIX.4.4
fix_target_comp_id=BITSTAMP
fix_sender_comp_id={args.sender_comp_id}
fix_sub_id={args.sender_sub_id}
account_id={args.sender_comp_id}
client_id={args.sender_sub_id}
credentials_file={args.credentials.resolve()}
credentials_key={args.credentials_key}
include_trade_info=true
use_async_session=true
sync_cancellation=true
set_cancel_on_disconnect=false
cancel_on_disconnect_tag=0
set_cancel_on_internal_disconnect=false
test_mode=false
tps_limit=1
log_file_path={gateway_log}
""")

    def launch(name):
        handle = (run_dir / (name + ".stdout.log")).open("a")
        handles.append(handle)
        children[name] = subprocess.Popen([sys.executable, "-u", str(args.umm / "scripts/md_feed/shmem_writer_booktop.py"),
                                           "--config", str(run_dir / (name + ".json"))],
                                          cwd=args.umm / "scripts", env=env, stdout=handle,
                                          stderr=subprocess.STDOUT, start_new_session=True)

    def snapshot():
        return {name: metrics(run_dir / (name + ".log"), prepared[name]["output_exchange"]) for name in children}

    def observe(label, seconds):
        before = snapshot()
        gateway_before = gateway_metrics(gateway_log)
        print(label + " (" + str(seconds) + "s)", flush=True)
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            assert tunnel.poll() is None, "stunnel exited"
            if gateway is not None:
                assert gateway.poll() is None, "gateway exited"
                gm = gateway_metrics(gateway_log)
                assert gm["logons"] <= 1 and gm["logouts"] == 0, "gateway disconnected or relogged"
                assert not ({"D", "F", "G", "q"} & gm["sent"].keys()), "unexpected gateway order/cancel traffic"
                assert not ({"3", "5", "j"} & gm["received"].keys()), "gateway reject/logout received"
            for name, process in children.items():
                assert process.poll() is None, name + " writer exited; inspect its private logs"
            current = snapshot()
            for name, data in current.items():
                assert data["logouts"] == 0, name + " disconnected"
                assert data["rejects"] == 0, name + " subscription rejected"
                assert data["invalid"] == 0, name + " emitted invalid quotes"
                assert data["logons"] <= 1, name + " unexpectedly relogged"
                if data["last_recv_ns"]:
                    assert time.time_ns() - data["last_recv_ns"] < args.stale_seconds * 1_000_000_000, name + " quotes stale; inconclusive"
            time.sleep(min(5, max(0, end - time.monotonic())))
        after = snapshot()
        report["stages"].append({"name": label, "before": before, "after": after})
        if gateway is not None:
            gm = gateway_metrics(gateway_log)
            report["stages"][-1]["gateway"] = gm
            assert gm["logons"] == 1, "gateway did not log on"
            assert gm["sent"].get("0", 0) > gateway_before["sent"].get("0", 0), "no gateway heartbeat sent in stage"
            assert gm["received"].get("0", 0) > gateway_before["received"].get("0", 0), "no exchange heartbeat received in stage; inconclusive"
        for name, now in after.items():
            prev = before[name]
            assert now["logouts"] == prev["logouts"], name + " disconnected during " + label
            assert now["rejects"] == prev["rejects"], name + " subscription rejected"
            assert now["invalid"] == prev["invalid"], name + " emitted invalid quotes"
            assert now["logons"] <= max(1, prev["logons"]), name + " unexpectedly relogged"
            assert sum(now["counts"].values()) > sum(prev["counts"].values()), name + " produced no valid quotes; inconclusive"
            assert time.time_ns() - now["last_recv_ns"] < args.stale_seconds * 1_000_000_000, name + " quotes stale; inconclusive"
            print(name + ": " + str(sum(now["counts"].values()) - sum(prev["counts"].values())) + " valid snapshots", flush=True)

    try:
        handle = (run_dir / "stunnel.log").open("w")
        handles.append(handle)
        tunnel = subprocess.Popen([stunnel, str(tunnel_path)], stdout=handle, stderr=subprocess.STDOUT, start_new_session=True)
        time.sleep(2)
        if args.with_gateway:
            handle = (run_dir / "gateway.stdout.log").open("w")
            handles.append(handle)
            gateway = subprocess.Popen([str(args.gateway_binary), str(run_dir / "gateway.cfg"),
                                        "--log-file", str(gateway_log), "--log-level", "INFO", "--no-rotate"],
                                       cwd=args.umm / "src", env=env, stdout=handle,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            observe("trading gateway heartbeat baseline (no orders)", args.stage_seconds)
        launch("jdbp")
        observe("jdbp baseline", args.stage_seconds)
        launch("bbmm")
        observe("two spot connections", args.stage_seconds)
        launch("bvbs")
        observe("two spot plus one perp connection", args.overlap_seconds)
        stop(children.pop("jdbp"))
        observe("remaining connections after stopping jdbp", args.stage_seconds)
        # Separate log generation makes the expected restart distinguishable from an unsolicited reconnect.
        (run_dir / "jdbp.log").rename(run_dir / "jdbp.before-restart.log")
        launch("jdbp")
        observe("all three after restarting jdbp", args.overlap_seconds)
        final = snapshot()
        for name, cfg in prepared.items():
            missing = {x["instrument"] for x in cfg["instruments"]} - final[name]["counts"].keys()
            assert not missing, name + " missing symbols: " + ', '.join(sorted(missing))
        report["result"] = "pass"
        print("pass: all profiles produced valid quotes and survived the staged overlap/restart", flush=True)
    except (Exception, KeyboardInterrupt) as exc:
        report["result"] = "not passed"
        report["reason"] = str(exc) or "interrupted"
        report["last_observation"] = snapshot()
        if gateway is not None:
            report["gateway"] = gateway_metrics(gateway_log)
        print("not passed: " + report["reason"], flush=True)
    finally:
        for process in children.values():
            stop(process)
        stop(gateway)
        stop(tunnel)
        for handle in handles:
            handle.close()
        (run_dir / "summary.json").write_text(json.dumps(report, indent=2))
        print("results: " + str(run_dir))
    return 0 if report["result"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
