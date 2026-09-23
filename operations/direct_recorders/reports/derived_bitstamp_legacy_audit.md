# legacy btc/eth bitstamp archive audit

**current status:** both legacy prefixes were deleted at the user’s explicit request. the investigation below documents the unresolved history before deletion.


`DERIVED_BTC_BITSTAMP` and `DERIVED_ETH_BITSTAMP` are ambiguous legacy names. each has 177 `eu-central-1a_FR01D` booktop files, covering the 15-minute windows from 2026-09-10 00:00 through 2026-09-11 20:00 utc. these are data coverage bounds, not rollout dates.

## attribution

in `umm_config/bbmm_md_test/config`, both `derived_btc_bitstamp_bnbspot_md.json5` and `derived_btc_bitstamp_coinbs_md.json5` output `PAIR-BTC-USD`, under `DERIVED_BITSTAMP_BNBSPOT` and `DERIVED_BITSTAMP_COINBS` respectively. the eth configurations have the equivalent relationship.

the legacy filename parser in `scripts/md_feed/log_parser.py` drops the last underscore-separated component. consequently both btc input filenames can yield `DERIVED_BTC_BITSTAMP`, and both eth filenames can yield `DERIVED_ETH_BITSTAMP`. current structured booktop records carry their exchange in-band.

all 354 source windows have a nonempty same-location pipeline object under **both** candidate destinations. this is object coverage, not proof that every source observation is preserved.

the first btc file contains exact six-column matches from both destinations: 11,209 rows under the binance-reference feed and 4,073 under the coinbase-reference feed. of 16,055 source rows, 773 have no exact counterpart in their union; 769 remain unmatched even after ignoring receive time. 252 of those 769 observations have timestamps absent from both candidate pipeline files. exact-value mismatches may also include numeric representation or receive-time differences; they are not all established missing events. this is a real mixed-feed archive, not a one-to-one alias suitable for the bch/ltc migration script.

## complete comparison

all 354 legacy files were compared against both candidate same-window, same-location pipeline archives across all six columns. the source files contain 24,211,283 rows; 749,086 have no exact six-column match in the union of the two candidate pipeline files. this count includes receive time and uses exact numeric equality; it must not be represented as 749,086 definitively missing market events. the first-window follow-up above separately establishes observations absent by timestamp.

## recovery constraint

retain the ambiguous originals. recover provenance from the original per-producer logs, or another independently attributed historical source, before assigning unmatched rows. exact matches can identify already-preserved observations, but unmatched rows cannot safely be assigned to either feed solely by folder name, timestamp, or price proximity.

live ssh access to fr01 timed out on two attempts during this audit. an earlier local runtime snapshot only listed retained btc/eth logs starting 2026-09-11 20:03 and 2026-09-12 00:10; it does not establish availability of logs for the entire legacy interval.

no s3 objects were changed by this audit. the bch/ltc migration script remains restricted to its two unambiguous source prefixes.

## other screenshot names

`DERIVED_BYBIT_HYPERLIQUID` and `DERIVED_BITSTAMP_HYPERLIQUID_BYBIT` are explicit configured output identities. other derived-feed prefixes must be checked individually; do not delete the entire derived namespace as legacy naming.

## evidence

local read-only inventory: `/private/tmp/derived-alias-inventory.json`.

same-window pipeline coverage: `/private/tmp/derived-alias-coverage.json`.

full content comparison: `/private/tmp/derived-alias-content-comparison.json`.


## subsequent recheck

fresh comparison on september 13 found the same 749,086 unmatched full rows; 747,730 remain unmatched without receive time. the 354 ambiguous source files remain in place. see [the validation recheck](booktop_validation_2026-09-13.md).

## attempted producer-log recovery

following authorization to separate and migrate these prefixes, ssh to `FR01` (`172.33.10.230`) again timed out. no remote producer logs were read.

checked the repository's raw-log backup destination, `jstdata/log_archives/`, which is populated by `scripts/utils/move_to_s3.sh`. the `JDBP` prefix contains only its folder marker. the shared `md1`, `md2`, `Others`, and empty-name prefixes have historical archives but no objects whose filename begins with `202609`. scanning that month prefix across every discovered archive host found archives for other books, but no matching btc/eth producer source archive was identified. this is not proof that no backup exists under another naming scheme or storage location.

recovery still needs access to the logs corresponding to `*.bbmm.derived_{btc,eth}_bitstamp_{bnbspot,coinbs}.md_feed.log` for the legacy data interval, or an independently attributed historical equivalent. no btc/eth source or destination objects were changed. local archive-inventory evidence: `/private/tmp/september-log-archives.json`.

## deeper live search and cleanup evidence

with fr01 ssh restored, a recursive read-only search covered `/home/ubuntu`, `/mnt`, `/data`, `/opt`, and `/srv`, including individually compressed files, archive/backup directory links, and accessible process file descriptors. no original btc/eth bitstamp/bnbspot or bitstamp/coinbs producer history for the required interval was found. active matching producer logs start september 12; relevant compressed archive files are from january/february. the only related deleted-but-open descriptor belongs to a different `new_derived_eth_bitstamp_coinbs` producer, not one of the four required producers. permission errors were restricted to endpoint-security directories under `/opt/Cynet`.

found deployed `/home/ubuntu/cleanup_log.sh`, invoked by cron at `20 0 * * *` (server-local schedule). it sets `DRY_RUN=false` and deletes `*.log` and `*.log.*` files whose modification time is older than one hour. it performs no archiving and no uploader-checkpoint check; its filename patterns also include position files.

`/home/ubuntu/logs/20260913.cleanup.log` contains a deletion marker and explicitly lists all four `20260911_200316.bbmm.derived_{btc,eth}_bitstamp_{bnbspot,coinbs}.md_feed.log` files plus their checkpoint files. this directly supports deletion of those retained september 11 logs. it does not independently establish the deletion time of every earlier file covering september 10–11.

no cleanup settings, source logs, or s3 files were changed. the ambiguous s3 archives remain preserved. local evidence: `/private/tmp/fr01-history-search.json`, `/private/tmp/fr01-cleanup-evidence.json`, and their corresponding read-only probe scripts.


## subsequent user-authorized deletion

on 2026-09-13T05:10:24.050732+00:00, the user explicitly requested deletion after being informed of the unresolved historical observations. deleted all 354 inventoried objects (177 per prefix) with etag-conditional requests. fresh listings confirmed zero remaining objects under `DERIVED_BTC_BITSTAMP` and `DERIVED_ETH_BITSTAMP`; no deletion errors occurred. this was intentional deletion of unresolved history, not a successful attribution or migration. no new source-content backup was created by this deletion operation. inventory and execution evidence: `/private/tmp/legacy-btc-eth-deletion/plan.json` and `deletion.json`.
