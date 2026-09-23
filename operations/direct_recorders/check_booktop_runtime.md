# booktop runtime checks

`oc/operations/direct_recorders/check_booktop_runtime.py` collects read-only evidence on bvod or central ondo. it does not connect to hosts, restart programs, change checkpoints, or contact s3. reports go to stdout; shell redirection creates the report file you choose. raw log lines, credentials, and process environments are not included.

build baselines on your workstation using the repository venv (requires hjson):

```sh
PYTHONPATH=umm/src umm/venv/bin/python oc/operations/direct_recorders/check_booktop_runtime.py --build-manifest ../umm_config --profile bvod > /tmp/bvod-runtime.json
PYTHONPATH=umm/src umm/venv/bin/python oc/operations/direct_recorders/check_booktop_runtime.py --build-manifest ../umm_config --profile central-ondo > /tmp/central-ondo-runtime.json
```

copy the script and the corresponding baseline json to the target host using your normal approved access. the probe needs only python 3.9+ and read access to producer configs, logs, uploader buffers, and supervisor status. run as the service account:

```sh
python3 /tmp/check_booktop_runtime.py --manifest /tmp/bvod-runtime.json > /tmp/bvod-runtime-report.json
# on central ondo:
python3 /tmp/check_booktop_runtime.py --manifest /tmp/central-ondo-runtime.json > /tmp/central-ondo-runtime-report.json
```

if supervisor cannot be reached, specify `--supervisorctl /path/to/supervisorctl` and, if needed, `--supervisor-config /path/to/supervisord.conf`. the script only invokes `status`. unavailable status is reported as unverified, not stopped.

the baseline discovers all producers covered by the selected uploaders, including working neighboring groups for comparison. it captures expected config hashes, output exchange/symbols (including overrides), paths, and patterns. probe snapshots are 45 seconds apart by default. each samples the newest two producer logs, the last 256 kib of each log, checkpoint offsets, uploader stdout/stderr, and pending parquet batch counts/bytes/age. use `--interval`, `--max-logs`, `--tail-bytes`, or `--stale-seconds` to adjust; the default stale threshold is 30 minutes.

read the report's `findings` first, then its `before` and `after` evidence:

- stopped/backoff/fatal: supervisor reports a process issue.
- config mismatch: deployed bytes differ from the baseline, or access failed. matching bytes do not establish what a running process loaded.
- no matching or stale producer log: inspect the deployed log path and producer activity.
- no current-format structured records: inspect producer code/config loading. the bounded sample may simply miss an inactive instrument.
- unread bytes without checkpoint movement: possible uploader stall; repeat over a longer interval to exclude normal buffering.
- old pending batches or error markers: investigate uploader errors locally. error markers can predate this sample; buffer age uses file modification time, not market-event time.

`expected_pairs_not_sampled` means absent from the bounded tail, not proven missing coverage. an empty findings list is not a pass for s3 completeness. checkpoints represent durable local buffering, not successful s3 uploads. only the newest configured number of producer files is inspected; older files may have separate backlogs. current runtime evidence cannot establish the cause of the september 12 historical gaps. rerun the archive coverage checker after any eventual repair.
