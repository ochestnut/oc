# booktop coverage gap investigation

scope: the saved live s3 audit for 2026-09-12 03:45–04:45 utc, compared against local `umm_config` main at `2f9d868b3` and `umm` at `fcb0ddf15` plus the current branch. no remote deployment state was inspected: the ssh status check was not approved. no producer, uploader, or deployment configuration was changed.

the 247 exchange/symbol pairs missing at least one direct window at every recorder divide into three groups:

| group | distinct exchange/symbol pairs |
| --- | ---: |
| mapped to dedicated/direct-to-influx recorder paths without structured market-data log output | 120 |
| mapped to structured producer configs and matching uploader patterns; runtime explanation still unverified | 119 |
| historical naming/source cases still unresolved | 8 |

## 120 pairs use other recording paths

converting the shared-memory booktop writers did not convert every source of the `md_booktop` measurement.

- **gemini predictions: 71 contracts.** `timescaledb/umm2prod-03/supervisord.conf:561` launches `src/umm/exchanges/gemini/prediction_market/md_recorder.py`. its ticker ingester writes `md_booktop` directly to influx. the direct gemini uploader instead matches the shared-memory spot/perp logs on `umm2prod-02`. adding another glob alone will not convert this prediction recorder.
- **depth recorders also produce booktop: 42 pairs.** zerohash (20), bitmex (4), gateio (4), nado (3), bitso/btso (5), bybit (2), bitgetfut (2), and independent reserve (2). `scripts/md_feed/influx_recorder_book.py:136` defaults to `BOOKDEPTH,BOOKTOP`; its booktop path writes directly to influx at line 242 and does not emit `MD|B|` records. zerohash explicitly enables both output types. the smaller bybit and bitget lists match the missing symbols exactly.
- **lmax digital: six pairs.** `mktdata_utils/umm2prod/supervisord.conf:1195` launches `src/umm/exchanges/lmax/influx_recorder.py`, which writes `md_booktop` directly. this is separate from the lmax shared-memory logs covered by the new uploader.
- **bullish: one pair.** `bullish_mktdata/supervisord.conf:35` launches `scripts/md_feed/influx_recorder_booktop.py` for `PAIR-XRP-USDC`; it also writes directly to influx.

next change: provide structured booktop emission for these sources and route those logs through the uploader, or provide an equivalent direct archive sink. preserve depth/trade recording while changing the booktop path. merely disabling the pipeline would remove their current archive source.

## 119 pairs already have producer and uploader configuration

these counts include missing symbols with explicit output-name overrides, such as the three bvar commodity instruments.

| source group | missing pairs | relevant configured producers |
| --- | ---: | --- |
| bvod bnbfut | 26 | `booktop_bnbfut_2`, `_3`, `_4`, and `_bvar` |
| derived arcus/bnbfut | 25 | `derived_arcus_bnbfut_md_1` through `_5` |
| derived ondo/bnbfut | 34 | `derived_ondo_bnbfut_md_1` through `_5`, `_7`, `_8` |
| derived hyperliquid/bnbfut | 12 | `derived_hyperliquid_bnbfut_md_2`, `_3`, `_5` |
| derived onetrading/bnbfut | 1 | `derived_onetrading_bnbfut_md` |
| central ondo | 21 | `ondo_booktop_rec_1` through `_5` |

for these symbols, the checked-in config includes the symbol, enables `structured_recorder_mode`, and names a log matched by its corresponding s3 uploader. the bvod uploader blocks are in `bvod_md_test/supervisord.conf:1797` onward; the derived uploader starts at line 2050. the central ondo uploader is in `timescaledb/umm2prod-03/supervisord.conf:1527`.

the missing derived arcus symbols account for all five configured groups. the missing derived ondo symbols account for entire groups, while other groups have direct data. this is consistent with a process/deployment/logging difference between groups, but does not identify the exact cause.

required live checks, in order:

1. compare deployed code/config revisions and supervisor commands with these producer definitions.
2. inspect producer/uploader status and recent restarts. many checked-in entries have `autostart=false`, including working uploaders, so that flag alone proves nothing about current runtime state.
3. inspect one current log from each missing group: is it being written, and does it contain structured `MD|B|` records with the expected output exchange and symbol?
4. inspect the uploader's matched paths, checkpoints, pending buffer, and recent upload errors for those streams.

structured flags in a file do not prove the running process has loaded them. the known pattern/symbol matches mean there is no evidence yet for fixing these groups by broadening globs.

## eight source/naming cases remain unresolved

five `DERIVED_BITSTAMPPERP_BNBFUT` symbols, one `DERIVED_BITSTAMP_BNBFUT` symbol, and two kalshi `PERP-BTC-USD` / `PERP-ETH-USD` symbols were not matched to the reviewed structured producer definitions. legacy derived attribution can infer exchange names from separate log lines, while the structured format carries explicit output names. compare the original source and actual live output before treating these names as new feeds or migrating them under another name. kalshi prediction-contract coverage does not establish equivalence to these two perp-labelled series.

## recorder-copy gaps are a separate issue

the original 1,355 missing production-recorder windows include 460 windows with a direct copy elsewhere. those can be intentional duplicate-recorder coverage differences, but substituting another location changes receive-time provenance. the 247-pair classification above concerns the remaining 895 exchange/symbol/windows with no direct copy at any location.

detailed evidence is in `/private/tmp/booktop-check-20260912-044900/config_trace.json`, `coverage_gaps.json`, and `windows.jsonl`. this investigation used the saved inventory and did not rerun a current live s3 coverage scan.


## live follow-up: september 12, 05:58–06:02 utc

read-only probes completed on bvod and central ondo. all 39 inspected producer configuration files matched the checked-in baseline. affected arcus, ondo/bnbfut, bnbfut, and central ondo producer logs contain current-format structured booktops. this establishes live emission for sampled groups, not exhaustive per-symbol coverage.

supervisor status was unavailable through the default command. a separate read-only `/proc` inspection confirmed all three uploader processes and their expected pattern arguments, and identified their open input files. sampled uploader logs contained successful upload markers and no error markers; this is not independent s3 verification.

| uploader | unread data in currently open input logs | observed older input files |
| --- | ---: | --- |
| bvod bnbfut | 41.65 gb | all five streams reading files opened september 9 |
| bvod derived | 59.93 gb | all five arcus groups and most affected ondo/hyperliquid groups reading older files |
| central ondo | 1.44 gb | group 3 reading a september 4 file; group 4 september 10; group 1 september 11 |

these are decimal gb and lower bounds on total input backlog: later queued log files are excluded. the log filename indicates file creation/rotation time, not the timestamp of the current record. bvod bnbfut checkpoints advanced between observations, and all three uploader output logs were being modified. central ondo groups 2, 5, 6, and 7 had reached current files by the process inspection.

the shared tailer resumes each stream at its oldest unread file and advances to a newer file after reaching eof. fresh producer logs without checkpoints are therefore consistent with the observed historical replay backlog, rather than missing producer emission. all five arcus groups exhibit this pattern directly. current evidence strongly explains the group-shaped gaps, but does not prove every one of the original 119 historical symbol gaps is recoverable.

next improvement to evaluate: separate historical catch-up from live ingestion, with explicit checkpoint ownership and preservation of unread history. do not delete checkpoints or skip old logs as a shortcut. no production changes or restarts were performed.

local evidence: `/private/tmp/bvod-runtime-report.json`, `/private/tmp/central-ondo-runtime-report.json`, `/private/tmp/bvod-process-report.json`, and `/private/tmp/central-ondo-process-report.json`.


## catch-up and coverage follow-up: september 12, approximately 06:04–06:06 utc

measured all matched producer log files, including queued rotations, twice about 45 seconds apart. no file-read errors were reported. net unread bytes decreased after accounting for incoming writes:

| uploader | total unread logs at second sample | net decrease | observed net rate |
| --- | ---: | ---: | ---: |
| bvod bnbfut | 44.44 gb | 131.88 mb | 2.92 mb/s |
| bvod derived | 86.03 gb | 85.73 mb | 1.90 mb/s |
| central ondo | 4.11 gb | 66.77 mb | 1.48 mb/s |

this is a short sample of input checkpoint progress, not a reliable completion forecast or proof of s3 upload throughput. the totals exceed the previous open-file-only figures because this measurement includes queued files.

fresh s3 object listings completed without errors at 06:05 utc for the original 119 symbols. 14 now have direct objects for every originally missing window: four derived hyperliquid/bnbfut symbols and ten ondo symbols. these same 14 also have a direct object for the latest eligible closed window, 05:45–06:00 utc. 105 symbols still lack at least one originally missing window; 417 of the original 473 missing symbol/windows remain absent, meaning 56 have appeared. direct coverage accepts any suffix-D recorder, consistently with the original 119-symbol classification.

no content or row comparison was performed, per user instruction. object presence does not prove completeness or equality. evidence: `/private/tmp/booktop-119-coverage.json`, `/private/tmp/bvod-backlog-progress.json`, and `/private/tmp/central-ondo-backlog-progress.json`. no production changes were made.


## september 13 recheck

all 119 backlogged pairs now cover all 473 originally missing windows. paired comparisons find no missing production records in this group in either the original interval or the recent hour. other recording paths still have coverage gaps. see [the completed recheck](booktop_validation_2026-09-13.md) for scope, counts, content differences, and limitations.
