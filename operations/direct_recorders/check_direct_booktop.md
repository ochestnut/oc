# direct booktop file checks

`oc/operations/direct_recorders/check_direct_booktop.py` performs a read-only s3 audit of direct booktop files, including those without a production counterpart. it discovers all suffix-`D` recorders without a fixed recorder list. intervals are utc, start inclusive and end exclusive, on 15-minute boundaries.

```sh
PYTHONPATH=umm/src umm/venv/bin/python oc/operations/direct_recorders/check_direct_booktop.py \
  --start '2026-09-14 00:45' --end '2026-09-14 01:45' \
  --output-dir /tmp/direct-booktop-health-20260914 \
  --workers 24 --compare-production
```

use a fresh output directory. the checker never uploads, deletes, renames, or changes checkpoints. downloaded data bypasses analytics caches, and object etags and sizes are checked around each read.

hard failures include unreadable/empty parquet, missing required booktop/receive columns, invalid or null timestamps, timestamps outside the filename window, nonnumeric values, infinite prices/quantities, null prices, and duplicate object identities.

it also reports exact duplicate rows, sort order, crossed/locked quotes, nonpositive prices, negative/null quantities, negative exchange-to-receive latency, and latency over five minutes. these diagnostics need interpretation by feed: they do not automatically identify corrupted files or a recorder bug. distinct observations at the same exchange timestamp are retained and are not classified as exact duplicates.

with `--compare-production`, available same-recorder pipeline counterparts receive exact multiset and state-change comparisons. coverage gaps are reported even when a direct object exists at another recorder. missing files in both archives cannot be detected from this inventory alone; inactive feeds and periods outside the selected interval are not certified by a successful check. the audit covers booktop files, not trade/depth files.

outputs: `summary.json`, `inventory.json`, `coverage_gaps.json`, and streamed per-file `files.jsonl`. `valid` indicates structural validity; review diagnostic counts and comparison/coverage results before concluding the data is suitable for a particular use. a completed structural audit does not establish cutover readiness. exit 2 indicates structural/comparison errors or no direct files; value diagnostics and coverage differences remain report findings rather than structural errors.
