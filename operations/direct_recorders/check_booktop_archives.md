# check booktop archives before cutover

`oc/operations/direct_recorders/check_booktop_archives.py` reads production and direct s3 objects and writes local json reports. it does not upload, rename, delete, or migrate objects. it bypasses the analytics data cache.

run from `$JST_ROOT`, choosing an interval after direct recording was enabled:

```sh
PYTHONPATH=umm/src umm/venv/bin/python oc/operations/direct_recorders/check_booktop_archives.py \
  --start '2026-09-12 00:00' --end '2026-09-12 01:00' \
  --output-dir /tmp/booktop-check
```

times are utc, start-inclusive and end-exclusive, and must be 15-minute boundaries. the final window must have closed at least 60 seconds ago. specify a settled historical interval; the check does not assume that late uploads are finished merely because the window is closed.

defaults: bucket `jstdata`, four listing/comparison workers, all production recorders and all booktop exchanges/symbols, including prediction paths. only the requested day partitions are listed. optionally use `--recorder TY03 --recorder FR01 --recorder NY01` to limit production recorder scope, `--bucket` for a different bucket, or `--local-root /path/to/archive` for an offline root containing `market_data/`. `--workers 16` increases read concurrency; memory usage also increases with the number of decoded parquet pairs.

discovery prints progress and saves `inventory.json`. `--inventory /path/to/inventory.json` reuses that listing for the same interval and source, while reading file contents afresh. this intentionally excludes objects added since discovery: omit it when checking fresh coverage for cutover.

## checks

files are paired by exchange, instrument, 15-minute window, and exact recorder id. `ap-northeast-1a_TY03D` is compared only with `ap-northeast-1a_TY03`; it cannot satisfy coverage for a different location. the terminal `D` suffix is reserved for direct copies for this audit.

- **object coverage:** every production booktop object in scope must have a direct counterpart. missing windows are reported even if the same feed has direct data in other windows. missing direct copies for other production recorders also remain visible unless a recorder filter is supplied.
- **exact records:** exchange timestamp, bid/ask prices, bid/ask quantities, and receipt timestamp when present. row order, numeric storage dtype, parquet compression, and metadata columns such as `symbol`/`group` are not equality criteria. duplicate multiplicity is preserved, float comparison is exact, and missing receipt columns are reported as a schema difference.
- **state changes:** a separate diagnostic compares the timestamped price/size changes after removing exact core duplicates and consecutive unchanged states. it ignores receipt times. it does not round timestamps, apply price tolerances, or select a winner among conflicting observations at one timestamp. a match here never turns an exact mismatch into a pass.

the pipeline suppresses consecutive unchanged quotes; the direct path retains them. therefore an exact mismatch with equal state changes may be expected. inspect the reported missing/extra rows and receipt differences before deciding whether the difference is acceptable for cutover.

empty files, missing required core columns, invalid timestamps, out-of-window records, nonfinite prices/sizes, duplicate objects for one window, and read failures are errors. null quantities are compared as nulls; null prices are errors. object version/size metadata is checked around each read to detect concurrent changes, but this is not an atomic snapshot across both archives.

## reports and exit status

`summary.json` records scope, completion, object-coverage result, exact-match result, totals, and per-feed/recorder status counts. `windows.jsonl` contains one record per window with object keys, status, row counts, comparison results, and up to five differing-record samples. timestamps in samples retain nanosecond precision.

statuses are `equal`, `different`, `missing_direct`, `direct_only`, and `error`. direct-only windows are kept visible and prevent an overall exact-match pass; they do not count as missing direct coverage. `object_coverage_complete` means objects exist, not that their contents are valid or equal. use the exact-match result and error counts alongside it.

- exit `0`: nonempty production baseline and every window has matching records.
- exit `1`: comparison or coverage differences, including direct-only windows.
- exit `2`: invalid arguments, discovery/read errors, ambiguous/invalid data, or no production baseline.

each invocation replaces the reports in its output directory. `completed=false` means the run failed during discovery or did not finish; old window reports must not be treated as results of that run.

coverage is relative to production objects observed in the requested interval. feeds missing from both archives cannot be inferred, and this does not establish coverage outside the interval. use multiple representative periods, including relevant market sessions, before retiring the old writer. no migration is performed by this check.
