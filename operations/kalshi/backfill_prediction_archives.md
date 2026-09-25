# kalshi missing archive backfill

reads retained `md_booktop_pred` rows from ty03, limited to kalshi booktop, an
explicit utc data range, and recognized contracts expiring before the range end.
reuses umm's parquet normalization, path naming, and contract expiry parser.
existing archives are never overwritten: conditional `IfNoneMatch=*` creation
also handles another uploader winning the race after the initial daily listing.

one influx query runs at a time for exact contract ids, across every retention
policy. `--batch-size` defaults to 50. `--max-rows` defaults to 500000 per source
slice; oversized reads split by contract, then by aligned time range. a single
15-minute contract chunk above the cap stops the run. partial responses and
unexpected identities also stop it. this is a processing guard, not a hard cap
on response memory or server load. there are no influx writes. transient connection errors, read timeouts, and
influx server errors get three attempts with 5- and 15-second backoffs. each
attempt has a 60-second timeout. validation and authorization failures stop
immediately. retries include the affected contracts, policy, and time range in
the log; exhausted retries leave the last completed checkpoint intact.

uses `umm.tools.progress.Progress`, shared with the s3 catalog builder and
analytics scanners: an updating bar on a terminal, periodic plain text in a
redirected log. `--progress-interval` defaults to 30 seconds; reports occur at
page/file boundaries and while waiting for uploads, with forced reports at phase
transitions, checkpoints, and completion. blocking source or catalog requests
can delay updates until they return.
contract summaries show percentage, rate, elapsed time, eta, cumulative uploads,
existing/invalid archives, and saved contracts in this invocation. eta is an
estimate based on completed contracts. the same overall line shows `phase`,
the current batch range, listing day and object count, and `slice_files=done/total`.
file counts refer to the current source slice (oversized batches can split),
and include existing or invalid files once inspected and uploads once finished.
the contract counter advances after a batch finishes; `saved` advances only
after the catalog merge and checkpoint succeed. listing completion never
reports the whole backfill as 100% complete. retries and failures remain visible. batch timings separate source
reads, archive processing, and catalog updates. archive
processing includes listing, normalization, and uploads. `--s3-workers` defaults
to eight concurrent conditional uploads, with at most eight staged uploads pending;
influx reads and parquet normalization stay sequential. s3 requests use bounded
sdk retries (three total attempts). all uploads finish before catalog/checkpoint
updates; a failed upload stops the batch without advancing its checkpoint. source
rows are normalized only for missing files. existing zero-byte archives are
reported as invalid and preserved. archives can only recover retained influx
history, not already deleted or expired rows.

catalog coverage is merged every five successful batches (`--catalog-every 5`),
and after the final partial group, preserving other
entries and using etag compare-and-swap against concurrent catalog writers.
the separate checkpoint advances only after uploads and catalog merge succeed.
a restart rechecks batches since the last catalog checkpoint and skips files
already uploaded. a crash can repeat up to five batches of inspection, without
overwriting archives. progress summaries distinguish processed contracts from persisted progress
with `saved` and `pending_batches`; the checkpoint file retains the exact cursor. use
the same state and exact range to resume; dry runs never update s3 or state.
`--instruments` restricts a trial to explicit ids. a lock beside the state blocks
another local process using that state. do not run overlapping jobs with separate
states. local staging uses temporary files, removed on normal exit.

run from the umm checkout using its virtualenv:

```bash
cd /Users/owenchestnut/Desktop/jst/umm
JST_ROOT=/Users/owenchestnut/Desktop/jst PYTHONPATH=src venv/bin/python \
  ../oc/operations/kalshi/backfill_prediction_archives.py \
  --start 2026-08-18T00:00:00Z --end 2026-09-16T00:00:00Z \
  --state ../oc/artifacts/kalshi_backfill/state.json
```

add `--apply` to create missing archives and merge catalog coverage. run in a persistent terminal session with stdout/stderr redirected. on macos,
`caffeinate -i` can prevent idle sleep, but does not prevent lid or forced sleep. the local machine must remain awake
and connected to ty03. no production cron or pipeline checkpoint is changed.

cleanup reads its catalog once at startup; a cleanup already in progress may
still skip newly cataloged contracts. a subsequent run loads the new catalog.

archive listings are streamed page by page into a temporary sqlite cache. each
day is listed once per run, even when contracts alternate across many days. the
sqlite page cache is limited to approximately 4 mib; the object index stays on
disk. a day is marked complete only when every page succeeds. uploads add their
keys to the cache, and conditional s3 puts still protect against concurrent
writers. the temporary index is discarded on exit and rebuilt on restart; it is
not the durable progress checkpoint.
