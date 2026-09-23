# prediction series cleanup

checks old kalshi/gemini `md_booktop_pred` data against existing s3 booktop
parquet, then optionally removes verified series from influx without restarting
the database. nothing is uploaded or deleted in s3.

## what counts as preserved

verification preserves quote history: each price **or size** change, its exact
timestamp, and its bid/ask prices and sizes. consecutive unchanged quotes are
compressed independently in each 15-minute window, matching the current exporter.
repeated receive timestamps, throughput observations, and extra source fields are
not preserved by this check. use a native/raw backup instead if those are needed.

the script reads **all retained rows in every retention policy** for an exact
exchange and venue `symbol`, including every `instrument_id` under that identity.
it does not verify just the interval before the cutoff: `DROP SERIES` deletes the
entire matching series across retention policies. one recent row blocks cleanup.

s3 checks use the canonical date-first contract path and the selected recorder's
filename. no local cache, another recorder's file, object count, or row-count-only
comparison is accepted as evidence. missing/legacy-only files, null quotes,
conflicting timestamps and content mismatches block that candidate. extra s3
history is allowed. if retained influx starts mid-chunk and the first retained
unchanged quote was compressed out of s3, the conservative comparison blocks it.

## run a small read-only plan

run with the umm python environment and normal analytics/influx/s3 credentials.
the script expects the usual sibling layout `jst/oc` and `jst/umm`.

```bash
python "$JST_ROOT/oc/operations/influx_archive/cleanup_prediction_series.py" \
  --recorder TY03 plan \
  --exchange KALSHI \
  --before 2026-08-01T00:00:00Z \
  --limit 10 \
  --out /tmp/kalshi-cleanup-plan.json
```

the date is an example: choose the intended history cutoff. it must be at least
seven days old. `--symbol EXACT_VENUE_TICKER` is repeatable and avoids discovery.
otherwise `--limit`/`--offset` page the symbol index, not an age-sorted inventory;
a page containing no old symbols does not prove none exist. scan further pages
before any deletion, because deleting changes offsets. use separate output paths.

planning defaults to one symbol at a time; `plan --workers 4` permits four
independent read-only verification tasks with separate influx connections.
`apply --batch-size 50 --workers 4` verifies up to 50 exact symbols, checks their
combined source snapshot again, and issues one explicitly scoped drop. batches
are deleted sequentially; only the s3 comparisons are parallel. one failed check
blocks the entire current batch. the journal records `batch_dropped` and its
symbol list after verifying absence. the default apply batch size remains one.

`--max-rows` (before `plan`/`apply`) defaults
to 250,000 retained rows per symbol (or per apply batch); exceeding it blocks the operation instead of
verifying a truncated sample. it is a conservative result-size guard, not a server
query-cost guarantee. start with a small batch on the live instance.

the json contains verified candidates, blocked/skipped candidates, source identity,
full-source fingerprints and the s3 objects checked. review `candidates`; remove
any entries you do not want applied. blocked entries are never candidates. a
nonzero plan exit code means at least one candidate was blocked.

## apply a reviewed plan

1. confirm the selected contracts are expired/retired. old last activity alone
   cannot distinguish an expired contract from a feed that stopped working.
2. pause every prediction writer/replay/backfill capable of writing these
   contracts; hold any s3 rewrite/deletion jobs touching their archive objects.
   influx and unrelated market-data writers stay running. the script cannot
   enforce that operational pause; the flags attest that you have done it.
3. run:

```bash
python "$JST_ROOT/oc/operations/influx_archive/cleanup_prediction_series.py" \
  --recorder TY03 apply \
  --plan /tmp/kalshi-cleanup-plan.json \
  --journal /tmp/kalshi-cleanup-apply.jsonl \
  --writers-paused \
  --confirmed-expired
```

for confirmed expired contracts, `--allow-live-writers` may replace
`--writers-paused`. this explicitly accepts the residual race if a replay or
backfill writes between the final source check and deletion. the full source
fingerprint and s3 checks still run; this option does not establish that a contract
is inactive. the selected mode is recorded in the journal.

apply re-reads source and s3, checks the plan fingerprint, then re-reads source
again immediately before each exact exchange/symbol `DROP SERIES`. no automatic
retry of deletion. it journals verified evidence before each drop, verifies the
series is gone from the index and that no points remain, and stops on the first
error. there is no transaction covering verification and deletion; pausing the
relevant writers closes that race, while live-writer mode explicitly accepts it. deletion adds load even though
influx stays online.

4. resume prediction collection and confirm a newly listed contract writes and
   exports successfully. inspect the tag count and errors: deleted candidates
   do not imply a fixed number of reclaimed values if other series share tags.

the journal and plan are created exclusively and never overwritten. after an
interrupted or failed apply, inspect the journal and generate a new plan for the
remaining symbols. do not blindly rerun an old plan. this script has no automatic
restore; the verified parquet retains compressed quote history, not a native
influx backup.

## tests

```bash
python -m unittest discover \
  -s "$JST_ROOT/oc/operations/influx_archive/tests" \
  -p test_cleanup_prediction_series.py -v
```

offline tests only; no production cleanup has been executed by creating this script.
