# log-to-s3 review — 2026-09-12

reviewed local `umm` main at `fcb0ddf15` and local `umm_config` main at `2f9d868b3`. both working trees were clean at the start. this is a code/config review, not verification of deployed processes or live s3 coverage; remote branches were not refreshed.

## where we are

- the direct path is implemented: structured feed logs → shared parser → durable local parquet batches → 15-minute s3 windows → existing analytics loaders.
- booktop, trades, and depth are supported. producers opt into `structured_recorder_mode`; legacy producer formatting remains supported.
- recovery includes sink-specific checkpoints, atomic batch publication, local buffer locking, orphan recovery, failed-upload retries, and late-window merging. trade identity preserves distinct identical fills while deduplicating replays. depth merges keep the latest received snapshot at each exchange timestamp.
- repeated log patterns allow feeds sharing an output namespace to use one uploader. dry runs isolate both checkpoints and buffers from production.
- the reviewed supervisor configs stage direct booktop uploaders for tokyo, frankfurt, and new york, with distinct direct-recorder ids. these entries have `autostart=false`. tokyo's config explicitly leaves direct-to-influx trade/depth recorders on the existing pipeline. merged support does not establish that these processes are running.
- the recorder and s3-client suites pass: **52 passed** using `venv/bin/python -m pytest scripts/md_feed/tests/test_recorder.py src/umm/tools/s3/tests/test_client.py -q`.

## improvements in priority order

### 1. finish analytics integration for direct recorder ids

**confirmed integration gap; first priority.** supervisor writes `ap-northeast-1a_TY03D`, `eu-central-1a_FR01D`, and `us-east-1-nyc-2a_NY01D`, but analytics has no corresponding short aliases. explicit routing for mapped exchanges also rejects the new recorders. the backlog scanner defaults to the old recorder set and uppercases supplied ids, making full case-sensitive ids an awkward workaround.

local reproductions:

- a fixture named with `ap-northeast-1a_TY03D` was discovered with the full id, but `PriceLoader.discover_files(..., recorder="TY03D")` found zero files.
- `MarketData._prep_recorder("NYSE", "NY01D", "s3", "exchange")` raises a routing error; the equivalent derived-feed request using `FR01D` also raises.
- the public routing helper uppercases a supplied full id, changing its case-sensitive filename prefix.

add the aliases, make allowed recorder routing source-aware, preserve full ids, and add an explicit direct-recorder selection to the scanner. avoid routing an s3-only recorder to an influx endpoint. test the public market-data api as well as file discovery. `source="s3", recorder="any"` already offers cross-recorder booktop discovery, but cannot provide recorder-specific receive-time analysis.

references: [registry and defaults](../../../../umm/src/umm/analytics/market_data/_common.py), [api routing](../../../../umm/src/umm/analytics/market_data/api.py), [scanner](../../../../umm/scripts/analytics/md_backlog_scanner.py).

### 2. reconcile late s3 updates with analytics caching

**confirmed policy mismatch; first priority.** `flush_complete_windows()` reads, merges, and replaces an existing object when late records arrive. `is_cached()` considers a local file complete when its modification time is more than 1,800 seconds after the chunk start. subsequent ordinary reads can therefore retain an earlier version indefinitely, including after outage recovery or historical replay.

track object freshness through etag/version/last-modified metadata, or introduce explicit finalization plus a revalidation policy for mutable windows. ensure reconstructed/materialized caches are invalidated when the downloaded source changes. verify by caching a historical window, adding late data, and reading it again through the public api. forced refresh is an interim operational workaround, not automatic coherence.

references: [uploader](../../../../umm/scripts/md_feed/log_to_s3.py), [cache predicate](../../../../umm/src/umm/analytics/market_data/_common.py), [fetchers](../../../../umm/src/umm/analytics/market_data/fetchers.py).

### 3. put the existing safety suites into ci

**confirmed coverage gap; small change.** the main unit-test workflow invokes `pytest tests/`. neither `scripts/md_feed/tests/test_recorder.py` nor `src/umm/tools/s3/tests/test_client.py` is under that directory. the 52 passing cases should run on every relevant pull request, alongside new public-api and cache-coherence regressions.

reference: [unit-test workflow](../.github/workflows/unittest.yml).

### 4. measure archive progress and disk headroom

**operational improvement.** the heartbeat reports stream count; upload failures print errors and retry while the process remains alive. supervisor's restart policy therefore does not establish archive health. retries can accumulate local batches without a disk budget.

expose per-stream last parsed timestamp, unread bytes, rejected structured-record count, oldest pending window, buffer bytes, last successful upload, upload latency, and repeated failures. alert on stalled progress and low free space, using feed schedules to avoid treating a closed market as a failure. coordinate raw-log retention with durable ingestion progress; existing age-based cleanup scripts do not establish that guarantee.

add retry backoff and quarantine/report persistently unreadable batches. decide explicitly how to pause ingestion before filling the producer's disk. checkpoint persistence failures should be surfaced as health failures rather than only printed.

references: [tailer and checkpoint handling](../../../../umm/scripts/md_feed/log_parser.py), [buffer and flush loop](../../../../umm/scripts/md_feed/log_to_s3.py), [age-based archive purge](../../../../umm/scripts/utils/purge_logs_archive.sh).

### 5. harden snapshot and file-lifecycle edge cases

**confirmed depth edge case plus recovery hardening.** a local fixture containing one nonempty depth snapshot followed by a completely empty snapshot round-tripped to only one timestamp. `_bookdepth_df()` emits no rows for the empty observation, so the later book clear is lost. this matters before expanding direct depth recording; the reviewed deployment is primarily booktop.

represent empty books explicitly, including their timestamp and receipt time, and test complete clears and later repopulation. also add tests and handling for same-path file truncation/replacement: checkpoints currently contain only offsets, and a running tailer holds the original file descriptor. bind positions to file identity/generation and detect truncation. complete malformed `MD|` lines currently return `None` and can be checkpointed past; count and retain rejected records for diagnosis.

for stronger power-loss guarantees, fsync containing directories after publishing durable batches/checkpoints; file fsync followed by rename alone does not provide the full directory-entry durability guarantee.

references: [depth conversion](../../../../umm/scripts/md_feed/log_to_s3.py), [parsing and tailing](../../../../umm/scripts/md_feed/log_parser.py).

### 6. enforce output ownership and bound catch-up work

**scaling improvement.** the buffer lock protects one local work directory, while s3 publication is read/merge/write without cross-process coordination. separate buffers or hosts targeting the same recorder/exchange/instrument/window can overwrite one another's additions. current config comments deliberately group overlapping derived feeds into one writer; make this an enforceable invariant.

validate that configured writers have exclusive output ownership. namespace checkpoints by ingestion job/destination, or reject conflicting jobs: every production uploader currently uses the same `.s3.position` suffix even if the recorder or bucket changes.

measure backlog replay before adding concurrency. each historical flush may reread/rewrite entire windows, and synchronous upload work pauses tailing. depth late merges also reconstruct a dense timestamp-by-price matrix with an existing memory guard; an oversized window can repeatedly fail. consider bounded upload workers and streaming depth reconstruction or immutable fragments with controlled compaction, based on measured load.

reference: [uploader](../../../../umm/scripts/md_feed/log_to_s3.py).

### 7. use the new archive for rollout validation and analysis

**available next step after reader fixes.** build a coverage comparison by feed/window/recorder between direct and existing archives: first/last timestamps, missing intervals, row counts interpreted against each path's dedup rules, price/quantity agreement, and receive-time distributions. use the existing backlog scanner for latency comparison once direct-recorder routing works.

save a rollout runbook covering consumer-before-producer deployment, dry-run validation, one-writer ownership, restart recovery, enabled supervisor state, and rollback. after a representative observation period, use measured parity and resource consumption to decide which duplicate recording/export paths can be retired. leave the existing path available until that evidence exists.

## suggested next batch

1. direct-recorder aliases/routing/scanner integration and ci inclusion.
2. late-update cache coherence, with an end-to-end regression.
3. archive-progress metrics, buffer alerts, and a deployment/coverage runbook.
4. depth-empty-state and file-lifecycle hardening before expanding coverage; performance changes after measuring catch-up behavior.

only this review note was added. no production code, deployment settings, or running services were changed.
