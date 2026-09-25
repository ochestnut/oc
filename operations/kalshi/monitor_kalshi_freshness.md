# kalshi freshness monitor

the monitor lives in oc and reads the symbol schema from an umm checkout, so
contract names stay consistent with the recorder. it needs python 3.10 or newer
with `hjson`, `requests`, and `influxdb`; the existing umm environment provides
these dependencies. no service restart or database writes are needed.

with oc and umm checked out next to each other on the influx recorder:

```bash
cd ~/oc
../umm/venv/bin/python operations/kalshi/monitor_kalshi_freshness.py \
  --json-output /tmp/kalshi_freshness.jsonl
```

defaults: localhost influx, credentials from `~/.creds`, btc/eth/sol/xrp,
15-second pause between observations, 30-minute database lookback, and exchange
market discovery every 60 seconds. discovery uses sequential requests, each
with a 10-second timeout and a 50-page bound per series. failed discovery is
reported explicitly; it prevents a complete coverage conclusion. it can also
lengthen the interval between influx observations. database queries have a
15-second timeout and no automatic retries.

use `--once` for one snapshot, `--underlying BTC` for btc only, `--rows 100`
for a longer console table, or `--host HOST` to read another recorder database.
`--repo PATH` explicitly selects the umm checkout. by default, the monitor uses
the sibling umm checkout when its schema exists, falling back to `~/umm`.
ctrl-c stops the monitor. the optional json-lines file contains every contract
in each snapshot; it grows until stopped and has no automatic rotation.

## interpretation

- `open`: the contract is currently open according to the last successful
  exchange discovery response. recently observed expired contracts also appear,
  with `open=no`; their old timestamps do not indicate a live recording problem.
- `event_age_s`: observation time minus the latest exchange event timestamp.
- `recv_delay_ms`: recorder receipt timestamp minus exchange event timestamp.
  receipt is assigned after websocket decoding/dispatch. this includes local
  scheduling and processing, and requires synchronized clocks.
- `observed_after_recv_s`: observation time minus receipt time, emitted only
  when the latest record changes after the initial baseline. this is an upper
  bound on availability delay that includes polling, discovery, and query time;
  it is not the exact database insertion latency.
- `no_recent_record`: an exchange-listed open contract has no record within the
  lookback. newly opened or quiet markets can legitimately have no observations.
- `old_or_quiet`: latest event is older than `--stale-seconds` (default 120).
  this does not prove dropped data; check source activity and subscriptions.
- `clock_or_timestamp_check`: negative receipt delay or a timestamp in the future.
- `collision`: multiple exchange tickers normalize to the same stored symbol;
  this prevents confirming their individual coverage from that symbol alone.

the monitor samples the latest record per symbol, not every message. it cannot
prove gap-free capture, detect every late backfill behind a newer record, or
infer missing contracts when exchange discovery fails. it monitors influx,
not s3 publication. compare simultaneous observations on different recorder
hosts before drawing conclusions about the best region.

## offline tests

from the oc checkout, using an umm environment with `pytest` installed:

```bash
../umm/venv/bin/python -m pytest operations/kalshi/tests -q
```

tests use mocked database and exchange clients; they do not need credentials
or network access. schema tests use the same local umm checkout as the monitor.
