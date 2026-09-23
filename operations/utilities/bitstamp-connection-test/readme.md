# bitstamp concurrent md connection test

this harness runs the existing `scripts/md_feed/shmem_writer_booktop.py` from the
server's umm checkout. it does not deploy code, change git state, modify supervisor,
submit orders, or stop existing services.

## prerequisites

- stop the authorized jdbp2 test bots and confirm their outstanding orders are
  handled using the normal trading shutdown procedure. process termination alone
  does not prove orders were cancelled.
- inventory existing fix sessions using the test account and stop them as agreed.
- identify the jdbp2 credential lookup key in the server's credential file. the
  confirmed jdbp2 sender ids are `SenderCompID=jqyr0743` and
  `SenderSubID=50133041` (dhanraj, september 17, 2026). do not disclose credential
  values in logs/chat.
- the server checkout must support `BITSTAMP_FIX` and `BITSTAMPPERP_FIX` and the
  current booktop writer's `structured_recorder_mode` and fix-listener path.
- use the server's umm virtualenv; stunnel and the quickfix dictionary must exist.

## copying from the mac

```sh
scp -r /Users/owenchestnut/Desktop/jst/oc/operations/utilities/bitstamp-connection-test ubuntu@172.33.3.20:~/
```

## on jdbp2

replace `TEST_KEY` with the jdbp2 credential lookup key (not the api key itself).
the sender ids below are confirmed for jdbp2:

```sh
~/umm/venv/bin/python ~/bitstamp-connection-test/test_connections.py \
  --credentials-key TEST_KEY \
  --sender-comp-id jqyr0743 \
  --sender-sub-id 50133041
```

this is preflight only; it opens no exchange connections. repeat the same command
with `--run` to start the test after the test-account shutdown is complete.

to include the actual c++ trading gateway, add `--with-gateway` to both preflight
and run commands. it uses `~/umm/src/fix_gateway/build/fix_gateway` (override with
`--gateway-binary`), direct tls, and the same test-account sender IDs and credentials
as the md processes. the credential entry must contain `username`/`password`;
if api-key fields are also present they must match, so we test one identity.

the gateway has a unique local server/channel name, no attached bot, no order
requests, and cancel-on-disconnect disabled. do not attach a bot or order client to
this test endpoint. `test_mode=false` is necessary: the gateway's test mode skips
the exchange connection. the harness does not inject dummy new/cancel orders.

gateway mode adds a 60-second baseline before starting any md process. each stage
requires real outgoing and incoming fix heartbeats, one successful gateway logon,
and no logout/relogon, rejects, or observed order/cancel messages. those are actual
session messages to/from the venue, not local gateway-to-bot pings. this tests
session coexistence and liveness, not order acceptance or execution latency.

gateway logs can contain raw authentication fields. the results directory is
private; do not share raw logs. share only the generated summary.

the test takes about nine minutes:

1. jdbp spot instrument list alone, 60 seconds.
2. add bbmm spot instrument list, 60 seconds.
3. add bvbs perpetual instrument list, 180 seconds.
4. stop the jdbp-shaped connection; observe survivors for 60 seconds.
5. restart jdbp-shaped connection; observe all three for 180 seconds.

all three use the same supplied test identity and independent quickfix processes.
one separate stunnel listener on localhost:15101 creates independent upstream tls
connections. it uses the same endpoint and certificate checks as jdbp. existing
stunnel processes and port 5101 are untouched.

the harness requires continuing valid positive, uncrossed two-sided quotes,
checks disconnect/relogon/reject events and staleness, and requires every requested
symbol to appear. a quiet market, rejected instrument, missing permission, or
parser problem can yield `not passed`; that alone does not establish a duplicate
session restriction. counts are valid snapshots, not counts of changed booktops.

outputs use unique `BITSTAMP_TEST_*` namespaces. cpu pinning is disabled for this
functional connection test. temporary configs contain credential file paths and
lookup names, not copied secrets. logs/results are private under
`~/bitstamp-connection-tests/<timestamp-pid>/`; share `summary.json` first, not raw
fix logs. ctrl-c cleans up the processes this harness started. production services
are not automatically restarted.

## what this establishes

the actual md implementation can sustain multiple connections with the same test
identity and retain existing sessions as peers start/stop. it also exercises the
production spot/perp symbol sets, parser, and shared-memory publisher.

it does not establish different-source-ip acceptance, production-account access,
latency under production cpu load, or order/cancel processing. without
`--with-gateway`, it also does not test coexistence with an order-entry connection.
gateway mode uses the existing binary's api-version behavior; it does not enable
mandar's separate v2.4 upgrade or prove that upgrade is installed.
