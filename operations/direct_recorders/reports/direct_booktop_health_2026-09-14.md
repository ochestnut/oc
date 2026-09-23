# direct booktop health check — september 14

checked every discovered suffix-`D` booktop object for 2026-09-14 00:45–01:45 utc using fresh s3 listings and uncached reads. no remote data, configuration, or processes were changed.

## result

all **9,562 files**, containing **48,476,829 rows**, passed structural validation. **5,549 files had no same-recorder pipeline counterpart** and were still decoded and checked. this extends the earlier pairwise-only validation.

- zero unreadable, empty, schema-invalid, null-timestamp, out-of-window, infinite-value, or null-price failures.
- zero exact duplicate observations, unsorted files, negative quantities, null quantities, or negative exchange-to-receive latencies.
- all filenames matched their exchange, instrument, recorder, date, and 15-minute window.
- recorders: `TY03D` 8,022 files; `TY04D` 1,204; `FR01D` 248; `NY01D` 88.

structural success does not imply every source timestamp or quote is suitable for analytics without interpretation. two source-data conditions and one unresolved comparison issue need attention.

## globex: upstream one-hour timestamp discrepancy

all 126,376 rows in four inspected `FUT-ES26U-USD` files have receive timestamps roughly 3,600 seconds after their exchange timestamps. live ny01 producer-log samples show the same discrepancy, including nq, before s3 serialization. the log-to-s3 path is preserving what it receives.

the deployed atlasfeed adapter declares a fixed `America/New_York` timezone, matching the local implementation in `src/umm/exchanges/options_it/atlasfeed/booktop_md.py`. incorrect interpretation of the globex feed timezone is a likely explanation, but the provider's timezone semantics and host clock should be independently confirmed before applying a correction. no timestamps were modified. latency measurements and cross-feed time alignment for this source require caution until resolved.

live evidence: `/private/tmp/globex-recorder-probe.json`.

## empty book sides explain the apparent quote anomalies

kalshi has 20,956 zero/nonpositive-price rows. inspected examples have a zero bid with zero bid quantity, or the empty-book representation bid 0 / ask 1 with both quantities zero. this is consistent with unavailable book sides, not malformed parquet. only the samples were semantically classified; preserve these observations in the archive and treat empty sides appropriately when deriving analytic prices.

onetrading has 335 zero-sided rows, including 182 apparent crossed quotes and 70 locked quotes. all 335 exactly match observations in the pipeline archive. none of the crossed rows has both prices positive: a zero ask causes the apparent crossing. there are 153 zero-bid and 252 zero-ask rows, overlapping where both sides are empty. these are source observations, not changes introduced by the direct writer.

## comparison with pipeline data

4,013 file pairs were available: 1,768 exactly equal and 2,245 unequal. the direct files contain 24,199,661 additional exact records across paired windows. distinct receive observations and unchanged-quote repetitions are retained in the direct path; exact inequality alone is not loss.

- paradex: 12,763 production records lack a full exact match, but every production core record (exchange timestamp plus all four price/quantity values), including duplicate multiplicity, is present. differences involve receive time.
- kucoin: 425 production records across eight windows lack a corresponding core match. sampled nearest-receive direct observations have matching quote values but different exchange/receive timestamps; tested matching within one microsecond did not explain them. this is not proven data loss or proven benign deduplication. source sampling/emission differences need further investigation before claiming parity.

## coverage is a separate result

631 production windows lack the matching direct recorder; 152 have direct data elsewhere, and 479 have no direct file at any recorder. the latter span 134 distinct exchange/symbol pairs in this interval. this audit does not authorize a blanket booktop pipeline shutdown.

scope is this one settled hour of booktop files. it does not certify the full archive, trade/depth files, feeds absent from both paths, or producer configuration coverage.

## reproducibility

use [the direct-file checker](../check_direct_booktop.md). reports and diagnosis scripts are in `/private/tmp/direct-booktop-health-20260914/`: `summary.json`, `files.jsonl`, `inventory.json`, `filename_check.json`, `coverage_gaps.json`, `kalshi_zero_price_samples.json`, `kucoin_diagnosis.json`, and `remaining_diagnosis.json`.
