# direct recorder operations

one-time audits, runtime checks, migration utilities, and historical reports from the direct booktop recorder rollout.

run the tools from `$JST_ROOT` with the umm environment:

```sh
PYTHONPATH=umm/src umm/venv/bin/python oc/operations/direct_recorders/<tool>.py
```

the reusable recorder identities, s3 reader behavior, uploader naming, and backlog discovery remain in `umm`.

- `check_booktop_archives.py`: compare production and direct archives.
- `check_direct_booktop.py`: inspect direct archive health.
- `check_booktop_runtime.py`: collect bounded producer and uploader evidence.
- `migrate_ema_booktop.py`: preserve and migrate the explicitly mapped historical ema objects.
- `reports/`: dated rollout findings and completed migration evidence.
