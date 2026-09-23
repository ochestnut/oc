# ema archive repair

`oc/operations/direct_recorders/migrate_ema_booktop.py` repairs explicitly verified legacy ema booktop prefixes for `eu-central-1a_FR01D`. the default scope is `bch-ltc`; `--scope remaining` selects the eight other verified prefixes. symbols, direct recorder ids, exchange timestamps, receive times, and distinct price/quantity observations are preserved. the ambiguous `DERIVED_BTC_BITSTAMP` and `DERIVED_ETH_BITSTAMP` prefixes are excluded.

target identities come from producer output configuration: bitstamp ema sources map to `DERIVED_BITSTAMP_BNBSPOT`; coinbase ema sources map to `DERIVED_COINBS_BNBSPOT`. the pipeline's broader canonical alias for the latter is not used to rename direct producer output. the multi-symbol lukasz source permits only bnb, etc, and pnut. source/symbol/recorder combinations outside the explicit allowlist fail validation.

run from `$JST_ROOT` with the umm venv and configured aws access:

```sh
PYTHONPATH=umm/src umm/venv/bin/python oc/operations/direct_recorders/migrate_ema_booktop.py --report-dir /tmp/ema-bch-ltc-migration
PYTHONPATH=umm/src umm/venv/bin/python oc/operations/direct_recorders/migrate_ema_booktop.py --report-dir /tmp/ema-bch-ltc-migration --apply --workers 16
PYTHONPATH=umm/src umm/venv/bin/python oc/operations/direct_recorders/migrate_ema_booktop.py --report-dir /tmp/ema-bch-ltc-migration --delete-verified --workers 16
```

for the remaining eight prefixes, use a fresh report directory and pass `--scope remaining` in every phase:

```sh
PYTHONPATH=umm/src umm/venv/bin/python oc/operations/direct_recorders/migrate_ema_booktop.py --scope remaining --report-dir /tmp/ema-remaining-direct-migration-20260913 --workers 48
PYTHONPATH=umm/src umm/venv/bin/python oc/operations/direct_recorders/migrate_ema_booktop.py --scope remaining --report-dir /tmp/ema-remaining-direct-migration-20260913 --apply --workers 48
PYTHONPATH=umm/src umm/venv/bin/python oc/operations/direct_recorders/migrate_ema_booktop.py --scope remaining --report-dir /tmp/ema-remaining-direct-migration-20260913 --delete-verified --workers 48
```

the first command previews and backs up source/destination bytes locally. it validates the six-column booktop schema, nonempty records, non-null values, filename identity and timestamp window. the manifest stores source checksums and object etags. an existing plan cannot be overwritten by another preview. preserve this directory through cleanup; it supports reruns and recovery.

apply rechecks source identity, reads current destinations and merges exact observations. only exact duplicate rows are removed. timestamp and receive-time differences are retained. destination writes use `IfMatch` or `IfNoneMatch` so a concurrent update fails instead of being overwritten. every written destination is downloaded again and checked for preservation of both source and pre-existing destination observations.

delete mode repeats the preservation check against source observations, current destinations, and the backed-up pre-migration destinations and verifies the destination etag before conditionally deleting the source with its original etag. it checks that the source is absent afterward. use this only after successful migration verification. no transaction spans source and destination: do not run competing historical repairs against these objects. local backups remain available; on versioned buckets normal deletion creates a delete marker rather than permanently removing older versions.

## export investigation

read-only investigation found all 177 affected historical windows for each instrument present in fr01 influx: 1,526,622 bch rows and 968,632 ltc rows. the current pipeline symbol-discovery implementation finds both symbols. the saved s3 booktop cursor for each ends at `2026-09-08T17:54:05`, despite fresh influx data.

the reviewed sweep skips cursors more than six hours stale unless explicit recovery, force or a start boundary overrides that behavior. stale cursor handling is therefore a supported explanation for the missing pipeline output; live pipeline logs were not inspected to establish the exact skip execution. this migration repairs direct archive names, not the pipeline cursor gap. operational recovery should run on the owning pipeline host with its own state and synchronization controls, or be addressed as part of the separately planned booktop pipeline cutover.


## completed repair — 2026-09-13T03:38:49.379271+00:00

- migrated 354 historical objects: 177 bch and 177 ltc.
- preserved all 2,482,950 source rows; two existing destinations contributed another 5,685 rows. final verified destinations contain 2,488,635 rows.
- 352 new destinations and two merges; every destination passed readback and source/existing-observation preservation checks.
- removed all 354 mislabelled originals with conditional deletion after re-verification; all source-absence checks passed.
- source backups, two pre-existing destination backups, plan, migration report and deletion report remain in `/private/tmp/ema-bch-ltc-migration/`.
- retained `eu-central-1a_FR01D`; no pipeline cursor changes, recorder-name cutover, or changes to the other eight ema prefixes.

read-only diagnosis: both historical influx datasets are present and discoverable, but the persisted pipeline cursors end at september 8 17:54:05 utc. six-hour stale-cursor skipping in the reviewed code is consistent with the export gap; live pipeline logs remain uninspected.


## remaining eight ema prefixes: completed migration

completed on 2026-09-13; final s3 listing at 04:29:09 utc found zero objects under `market_data/EMA_`.

- migrated and verified all 1,753 source files, preserving 24,894,612 source rows, including the 2,951 observations previously unmatched by pipeline copies.
- retained 19,165 pre-existing destination rows across seven destinations; the verified destination total is 24,913,777 rows. 1,746 destinations were created and seven were merged.
- 1,222 files went to `DERIVED_BITSTAMP_BNBSPOT`; 531 went to the producers' explicit `DERIVED_COINBS_BNBSPOT` identity. all retain `eu-central-1a_FR01D`. the broader pipeline alias was deliberately not used as the direct producer destination.
- all 1,753 originals were conditionally deleted after source, destination, and backed-up pre-existing data preservation checks. migration and deletion each reported zero errors.
- the separate ambiguous `DERIVED_BTC_BITSTAMP` and `DERIVED_ETH_BITSTAMP` folders remain at 177 objects each.
- backups, exact source/destination plan, migration/deletion reports, and final prefix counts are in `/private/tmp/ema-remaining-direct-migration-20260913/`.
- six migration tests passed, including same-timestamp quantity preservation, explicit symbol/recorder routing, and rejection of ambiguous btc/eth sources. no pipeline cursor, recorder-name cutover, or production-process changes were made.

an earlier preview in `/private/tmp/ema-remaining-migration-20260913/` used pipeline canonical names and was never applied; only the `ema-remaining-direct-migration-20260913` plan was executed.


## subsequent user-authorized deletion

on 2026-09-13T05:10:24.050732+00:00, the user explicitly requested deletion after being informed of the unresolved historical observations. deleted all 354 inventoried objects (177 per prefix) with etag-conditional requests. fresh listings confirmed zero remaining objects under `DERIVED_BTC_BITSTAMP` and `DERIVED_ETH_BITSTAMP`; no deletion errors occurred. this was intentional deletion of unresolved history, not a successful attribution or migration. no new source-content backup was created by this deletion operation. inventory and execution evidence: `/private/tmp/legacy-btc-eth-deletion/plan.json` and `deletion.json`.
