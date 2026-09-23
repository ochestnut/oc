# ema archive replacement coverage

**subsequent cleanup completed:** all eight remaining ema prefixes were migrated with every source observation preserved, then deleted after verification. the final s3 listing found zero `market_data/EMA_` objects. the btc/eth mixed-feed prefixes remain. see [the completed migration](../migrate_ema_booktop.md). the audit findings below describe the pre-migration state.


checked: 2026-09-12T06:36:03.928678+00:00

correction: the first audit omitted pipeline exchange canonicalization. applying `canonical_exchange` resolves the 531 apparent gaps for the three coinbase prefixes. the initial 885-missing conclusion is superseded.

read-only inventory of all ten discovered `EMA_*` prefixes. destinations use explicit log-to-influx overrides followed by the analytics canonical exchange map. compared symbol, window and recorder against non-d pipeline objects with nonzero size. contents were not compared and nothing was deleted.

- source objects: 2,107
- same-recorder pipeline counterpart found: 1,753
- no pipeline counterpart at the mapped destination: 354

| prefix | covered | missing |
| --- | ---: | ---: |
| `EMA_10SEC_AAVE_BITSTAMP` | 177 | 0 |
| `EMA_ALTS_BITSTAMP` | 160 | 0 |
| `EMA_ALTS_COINBS` | 177 | 0 |
| `EMA_ASTER_BITSTAMP` | 177 | 0 |
| `EMA_BCH_BITSTAMP` | 0 | 177 |
| `EMA_BTC_COINBS` | 177 | 0 |
| `EMA_ETH_COINBS` | 177 | 0 |
| `EMA_LTC_BITSTAMP` | 0 | 177 |
| `EMA_LUKASZ_BITSTAMP` | 531 | 0 |
| `EMA_UNI_BITSTAMP` | 177 | 0 |

## current recording check

at 06:35 utc, all five investigated feeds (derived coinbase doge/btc/eth and derived bitstamp bch/ltc) had influx rows within a few seconds of the check and direct FR01D objects for the closed 06:15–06:30 utc window. coinbase pipeline objects existed under `DERIVED_BITSTAMP_COINBS_BNBSPOT`, including the open 06:30 window; open-window presence is not final completeness. no pipeline objects were found for bch/ltc in today's expected prefixes.

the remaining 354 historical missing replacements are 177 each for bch and ltc. retain those mislabelled objects until recovery or further verification. source windows span september 10 00:00 through september 11 20:00 utc.

evidence: `/private/tmp/ema-replacement-report.json` and `/private/tmp/ema-current-report.json`.


## fr01 legacy replay check — 06:44 utc

read-only process/checkpoint snapshots 45 seconds apart found the derived uploader running and ten ema streams reading current structured logs. all older matched ema log files were fully checkpointed; no unread closed ema rotations remained among the 22 retained matched ema files. there were zero pending parquet batches under `EMA_*` staging directories. the current ema inputs had only about 28 kb unread at the second snapshot, consistent with live tailing.

a fresh metadata listing found no added or changed objects across the ten ema prefixes compared with the 06:36 utc inventory. their last modifications were september 11 between 20:15 and 21:44 utc.

conclusion: the checked fr01 legacy ema replay has finished and is not currently recreating the incorrect prefixes. a parser mapping fix is not a prerequisite for cleaning up this completed replay under the current checkpoints/configuration; replaying old logs after resetting checkpoints would still expose the legacy naming behavior. preserve the bch/ltc historical objects until their replacement coverage is resolved. the uploader has other staging batches; this finding is specific to ema naming/replay, not every fleet backfill.

evidence: `/private/tmp/fr01-ema-backlog-report.json` and `/private/tmp/ema-prefix-changes.json`. nothing was deleted or restarted.


## completed repair — 2026-09-13T03:38:49.379271+00:00

- migrated 354 historical objects: 177 bch and 177 ltc.
- preserved all 2,482,950 source rows; two existing destinations contributed another 5,685 rows. final verified destinations contain 2,488,635 rows.
- 352 new destinations and two merges; every destination passed readback and source/existing-observation preservation checks.
- removed all 354 mislabelled originals with conditional deletion after re-verification; all source-absence checks passed.
- source backups, two pre-existing destination backups, plan, migration report and deletion report remain in `/private/tmp/ema-bch-ltc-migration/`.
- retained `eu-central-1a_FR01D`; no pipeline cursor changes, recorder-name cutover, or changes to the other eight ema prefixes.

read-only diagnosis: both historical influx datasets are present and discoverable, but the persisted pipeline cursors end at september 8 17:54:05 utc. six-hour stale-cursor skipping in the reviewed code is consistent with the export gap; live pipeline logs remain uninspected.


## subsequent content validation

the september 13 recheck compared all 1,753 remaining ema files with their same-recorder canonical pipeline replacements. object coverage is complete, but every remaining prefix contains unmatched source observations: 2,951 rows total, even excluding receive time. no remaining ema prefix was deleted. see [the validation recheck](booktop_validation_2026-09-13.md) for per-prefix counts.
