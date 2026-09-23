# booktop validation recheck

**subsequent cleanup completed:** all eight remaining ema prefixes were migrated with every source observation preserved, then deleted after verification. the final s3 listing found zero `market_data/EMA_` objects. the btc/eth mixed-feed prefixes remain. see [the completed migration](../migrate_ema_booktop.md). the audit findings below describe the pre-migration state.


checked on 2026-09-13 using fresh s3 listings and uncached parquet reads. both full comparison runs completed without discovery, read, schema, or comparison errors. neither run is a blanket coverage/equality pass. no s3 data was changed or deleted.

## the original backlogged group has caught up

all 119 previously backlogged exchange/symbol pairs now have direct objects for all 473 originally missing symbol/windows. yesterday's follow-up had 105 incomplete symbols and 417 outstanding windows; both counts are now zero.

content comparison strengthens this result: all 473 paired windows in this group preserve every production record, including receive timestamps. 9 windows are exactly equal; 464 contain extra direct records, totalling 702,324 additional observations. direct coverage is now available at the exact production recorder for these pairs in the historical audit.

in the recent hour, the same group has 451 paired windows. none is missing a production record; 15 are exactly equal and 436 contain extra direct records, totalling 584,654. 108 symbols have direct files in all four windows, five in three windows, one in two, two in one, and three in none. the three absent symbols also have no production objects in this interval: `DERIVED_ONDO_BNBFUT/PERP-EWY-USDC`, `DERIVED_ONDO_BNBFUT/PERP-NFLX-USDC`, and `DERIVED_ARCUS_BNBFUT/PERP-GLD-USDG`. absence from both archives does not establish producer health or failure.

ssh probes to bvod and ty03 timed out. this establishes archived coverage and paired-record preservation, not zero remaining local backlog on every uploader.

## full archive comparison

all intervals are utc, start inclusive and end exclusive. counts refer to feed/recorder/15-minute windows.

| measure | original interval, sep 12 03:45–04:45 | recent interval, sep 13 02:30–03:30 |
| --- | ---: | ---: |
| production windows | 4,415 | 4,177 |
| exact matches | 1,817 | 1,835 |
| unequal paired files | 2,024 | 1,967 |
| missing same-recorder direct objects | 574 | 375 |
| of those, direct exists elsewhere | 152 | 152 |
| no direct object at any recorder | 422 | 223 |
| distinct pairs with a missing-anywhere window | 128 | 57 |
| direct-only windows | 4,247 | 5,689 |
| errors | 0 | 0 |

same-recorder gaps in the original interval fell from 1,355 to 574. most remaining missing-anywhere feeds are the previously identified direct-to-influx/depth-recorder paths and unresolved legacy names. these are not the 119-symbol backlog group. the pipeline still supplies their archives.

recent missing-anywhere windows by exchange: bitgetfut 8, bitmex 16, btso 20, bullish 4, bybit 8, derived bitstampperp/bnbfut 20, derived bitstamp/bnbfut 4, gateio 16, indrsrv 6, kalshi 8, lmax 20, nado 12, uniswap v3 arbitrum 1, zerohash 80. the uniswap window is an additional current-period gap, not established as the same historical recording-path exception.

of 1,967 unequal recent paired files, 1,951 preserve all production records and contain extra direct records. only 16 have production records without exact direct matches:

- kucoin: 159 records across seven windows. timestamps are absent from their paired direct files; the tested match on receive time plus all four price/size values also finds no counterparts for these rows. root cause remains unresolved.
- paradex: 9,856 exact record differences across nine windows. all production core records (exchange timestamp and four price/size values), including multiplicity, are present. the differences involve receive time rather than missing core observations.

across all recent paired files, direct archives contain 2,854,189 extra exact records. exact equality is stricter than preservation of production observations; extra rows do not by themselves establish a problem. direct-only and missing-direct statuses are inventory findings: the checker does not decode unpaired files. feeds missing from both archives and periods outside the selected intervals are not validated.

## legacy folder deletion remains blocked by unmatched observations

all 354 btc/eth legacy files were compared with both candidate canonical pipeline archives. all windows have replacements, but 749,086 of 24,211,283 source rows still lack an exact six-column match, unchanged from the previous check. ignoring receive time leaves 747,730 unmatched rows. the mixed-feed attribution problem remains; these cannot be blindly renamed or deleted.

all 1,753 files in the eight remaining ema prefixes were compared with their same-recorder canonical pipeline counterparts. all windows have replacements, but 2,951 of 24,894,612 source rows lack a match, even ignoring receive time:

| legacy prefix | objects | unmatched source rows |
| --- | ---: | ---: |
| `EMA_10SEC_AAVE_BITSTAMP` | 177 | 66 |
| `EMA_ALTS_BITSTAMP` | 160 | 28 |
| `EMA_ALTS_COINBS` | 177 | 403 |
| `EMA_ASTER_BITSTAMP` | 177 | 134 |
| `EMA_BTC_COINBS` | 177 | 1,060 |
| `EMA_ETH_COINBS` | 177 | 508 |
| `EMA_LUKASZ_BITSTAMP` | 531 | 614 |
| `EMA_UNI_BITSTAMP` | 177 | 138 |

every remaining ema prefix contains unmatched observations. these exact-match counts are not automatically counts of distinct lost market events: numeric representation, timestamp differences, and pipeline filtering can matter. they are sufficient to reject unverified deletion. preserve or explicitly reconcile the unmatched observations before removing their only verified source.

the previously completed bch/ltc migration is separate: those two legacy prefixes remain absent. the eight ema and two ambiguous btc/eth prefixes were retained. this recheck does not authorize deleting other valid derived-feed prefixes.

## evidence

- `/private/tmp/booktop-recheck-20260913-historical/`: full summary, fresh inventory, window comparisons, and coverage analysis.
- `/private/tmp/booktop-recheck-20260913-current/`: equivalent reports plus `difference_diagnosis.json`.
- `/private/tmp/legacy-recheck-20260913/`: fresh legacy inventories and complete btc/eth and ema content comparisons, with the read-only scripts used for this recheck.

## unmatched ema row diagnosis

sampled 24 files across all eight remaining ema prefixes, selecting the first affected file, the middle affected file, and the most affected file per prefix. these contain 102 unmatched core observations:

- 85 have an exchange timestamp already present in the pipeline file, but different price or quantity values.
- none is an unchanged repetition of the immediately preceding source quote when sorted by exchange timestamp and receive time.
- eight match the most recent pipeline quote at or before their timestamp; this subset can be consistent with filtering repeated states, but does not explain the whole sample.

for example, an aave observation at `2026-09-10T00:23:07.826381000` has ask quantity `3.22` in the legacy file and `4.305` in the pipeline, with different receive times. another sampled aave observation has a different ask price at the same exchange timestamp. these are not harmless exact duplicate rows.

`src/umm/analytics/market_data/fetchers.py` explicitly drops duplicate exchange timestamps before filtering consecutive unchanged prices and quantities. same-timestamp intermediate updates being collapsed is therefore a plausible explanation for much of the sample, not an established classification of all 2,951 unmatched rows. evidence: `/private/tmp/legacy-recheck-20260913/ema-row-diagnosis.json` and `diagnose_ema_rows.py`.


## subsequent user-authorized deletion

on 2026-09-13T05:10:24.050732+00:00, the user explicitly requested deletion after being informed of the unresolved historical observations. deleted all 354 inventoried objects (177 per prefix) with etag-conditional requests. fresh listings confirmed zero remaining objects under `DERIVED_BTC_BITSTAMP` and `DERIVED_ETH_BITSTAMP`; no deletion errors occurred. this was intentional deletion of unresolved history, not a successful attribution or migration. no new source-content backup was created by this deletion operation. inventory and execution evidence: `/private/tmp/legacy-btc-eth-deletion/plan.json` and `deletion.json`.
