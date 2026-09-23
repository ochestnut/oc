# md recording and analytics alignment audit

2026-09-21. read-only inspection of checked-in supervisor configuration, analytics code, live influx inventory, and sampled s3 object names. production recorder configuration was not changed. reader fixes were implemented and saved by the user on `sync_analytics_0921` at `26ea6fd10`; the working checkout subsequently moved to main.

## result

analytics and recording are not fully aligned. the main confirmed fault is influx routing and catalog discovery, not missing s3 exchange registrations.

- 316 recorder/consumer stanzas inspected across 10 relevant supervisor files; all scanned supervisor files parsed successfully.
- four influx hosts queried: ty03, ty04, fr01, ny01. 2,269 host/exchange/output/symbol observations over the preceding three days; no query failures.
- after canonicalizing aliases: 47 exchanges, 1,967 exchange/output/symbol identities.
- 113 identities have data only outside the current default host. all observed misses are booktop identities; sampled live trades/depth defaults have corresponding data.
- s3 root discovery returned 64 prefixes, including administrative and historical prefixes. one recent symbol/day partition for each of 90 exchange/output combinations was listed; all 90 had parquet objects. prediction-specific s3 coverage was not separately sampled.
- this is a routing/inventory audit, not proof of continuous capture, complete venue instrument coverage, parquet content correctness, or all historical windows. a three-day absence does not prove a feed never existed.

## behavior before the fixes

| api/source | behavior | implication |
| --- | --- | --- |
| booktop/mid/trades, s3 | discovers matching day partitions across recorder filenames; merges records | new exchange/recorder names do not require exchange routing entries for this path |
| booktop/mid/trades, influx | chooses the first configured exchange recorder, otherwise ty03 | missing exchange mappings and partial symbol coverage cause misses |
| depth, either source | resolves to one recorder | does not have the booktop/trade s3 cross-recorder behavior |
| influx available_exchanges/available_symbols, any | directly queries central ty03 | regional-only feeds are hidden even if a load route exists |
| influx activity_summary, any | exchange-level host routing | inherits the same per-symbol coverage gaps |

## confirmed misses

| exchange | missed symbols | observed hosts for missed symbols | default host |
| --- | ---: | --- | --- |
| `BLUEOCEAN` | 2 | NY01 | TY04 |
| `BNBFUT` | 26 | TY04 | TY03 |
| `DERIVED_ARCUS_BNBFUT` | 25 | TY04 | TY03 |
| `DERIVED_BITSTAMPPERP_BNBFUT` | 5 | FR01 | TY03 |
| `DERIVED_BITSTAMP_BNBFUT` | 1 | FR01 | TY03 |
| `DERIVED_BNBFUT_NASDAQ` | 2 | TY04 | TY03 |
| `DERIVED_ONDO_NASDAQ` | 3 | TY04 | TY03 |
| `DERIVED_ONETRADING_BNBFUT` | 1 | TY04 | TY03 |
| `HYPERLIQUID` | 29 | TY04 | TY03 |
| `NASDAQ_UTP` | 2 | FR01, NY01 | TY04 |
| `NYSE` | 5 | FR01, NY01 | TY04 |
| `ONDO-SPOT` | 12 | TY04 | TY03 |

the full symbol-level evidence is in the local archive at `archive/cleanup_20260922/operations/direct_recorders/reports/md_analytics_alignment_20260921.json` (relative to the oc root), under `routing_misses`. the original file is also retained in the initial git commit. counts describe observed routing misses, not lost data.

## implemented reader fixes

1. add the seven missing regional exchange defaults: `ONDO-SPOT`, `DERIVED_ARCUS_BNBFUT`, `DERIVED_BNBFUT_NASDAQ`, `DERIVED_ONDO_NASDAQ`, `DERIVED_ONETRADING_BNBFUT` → `TY04`; `DERIVED_BITSTAMPPERP_BNBFUT`, `DERIVED_BITSTAMP_BNBFUT` → `FR01`.
2. make influx any routing sensitive to symbol, output type, and requested time window. retain preferred hosts when they have data and check other recorder hosts when absent. adding static exchange entries alone leaves 64 misses across bnbfut, hyperliquid, blueocean, nasdaq, and nyse. do not redirect all bnbfut/hyperliquid outputs to ty04: their trades/depth remain on ty03.
3. make influx any catalog discovery inspect all known recording databases, canonicalize aliases, and union symbols. activity queries should follow the same routing rules. distinguish unavailable hosts from genuinely absent data.
4. allow explicit recorder selection independently of preferred any ordering. currently exchanges with a map reject hosts omitted from that map, even where this audit observed data (e.g. nasdaq/nyse on fr01). keep explicit reads scoped to the requested host.
5. register direct archive aliases separately (`TY03D`, `TY04D`, `FR01D`, `NY01D`) if explicit direct reads are supported; resolve their influx host deliberately or reject direct-only aliases for influx. do not replace existing aliases or rename production output files. s3 any already discovers these long ids.
6. document/test that depth any selects a single recorder. no depth routing miss was observed in this inventory; do not introduce an unreviewed cross-recorder depth merge.
7. add regression coverage for regional-only exchanges, symbols absent on the preferred host, output types on different hosts, explicit regional reads, multi-host catalogs, direct s3 filenames, and partial host failures. the saved json contains the inventory evidence for future comparisons; a scheduled drift check was not added.

## s3 naming findings

four direct recorder ids were absent from `RECORDER_REGISTRY` and now have aliases: `ap-northeast-1a_TY03D`, `ap-northeast-1c_TY04D`, `eu-central-1a_FR01D`, `us-east-1-nyc-2a_NY01D`. these are dynamically discoverable by s3 any; they are not themselves a reason for any reads to fail.
for the sampled ondo spot dram partition, both ty04 and ty04d objects exist for windows 14:45 and 15:00 utc. the earlier concern that only the direct archive name might exist is not supported by this sample. s3 parquet readback remains separate from this filename inventory.
legacy and direct producers coexist for several feeds. do not rename direct writers or stop legacy exporters as part of a reader routing fix; existing cutover readiness work documents coverage and overwrite risks.

## complete observed exchange inventory (pre-fix defaults)

hosts below have at least one observed symbol, not necessarily every symbol. `prediction_booktop` is kept separate from standard booktop.

| exchange | observed output → hosts | preferred any host |
| --- | --- | --- |
| `ARCUS` | bookdepth: TY03; booktop: TY03, TY04; trades: TY03 | TY03 |
| `BITGETFUT` | bookdepth: TY03; booktop: TY03; trades: TY03 | TY03 |
| `BITSTAMP` | bookdepth: TY03; booktop: FR01, TY03; trades: TY03 | TY03 |
| `BITSTAMPPERP` | bookdepth: TY03; booktop: FR01, TY03; trades: TY03 | TY03 |
| `BLUEOCEAN` | booktop: NY01, TY04 | TY04 |
| `BNBFUT` | bookdepth: TY03; booktop: FR01, TY03, TY04; trades: TY03 | TY03 |
| `BNBSPOT` | booktop: FR01, TY03, TY04; trades: FR01, TY03 | TY03 |
| `BNB_US` | booktop: TY03; trades: TY03 | TY03 |
| `BTSO` | bookdepth: TY03; booktop: TY03; trades: TY03 | TY03 |
| `BULLISH` | booktop: TY03 | TY03 |
| `BYBIT` | bookdepth: TY03; booktop: FR01, TY03; trades: TY03 | TY03 |
| `BYBITFUT` | booktop: TY03; trades: TY03 | TY03 |
| `COINBS` | booktop: FR01, TY03; trades: FR01, TY03 | TY03 |
| `DERIVED_ARCUS_BNBFUT` | booktop: TY04 | TY03 |
| `DERIVED_BITSTAMPPERP_BNBFUT` | booktop: FR01 | TY03 |
| `DERIVED_BITSTAMP_BNBFUT` | booktop: FR01 | TY03 |
| `DERIVED_BITSTAMP_BNBSPOT` | booktop: FR01 | FR01 |
| `DERIVED_BITSTAMP_COINBS` | booktop: FR01 | FR01 |
| `DERIVED_BITSTAMP_COINBS_BNBSPOT` | booktop: FR01 | FR01 |
| `DERIVED_BITSTAMP_HYPERLIQUID_BYBIT` | booktop: FR01 | FR01 |
| `DERIVED_BNBFUT_NASDAQ` | booktop: TY04 | TY03 |
| `DERIVED_HYPERLIQUID_BNBFUT` | booktop: TY04 | TY04 |
| `DERIVED_ONDO_BNBFUT` | booktop: TY04 | TY04 |
| `DERIVED_ONDO_NASDAQ` | booktop: TY04 | TY03 |
| `DERIVED_ONETRADING_BNBFUT` | booktop: TY04 | TY03 |
| `DEXALOT` | booktop: TY03; trades: TY03 | TY03 |
| `GATEIO` | bookdepth: TY03; booktop: TY03; trades: TY03 | TY03 |
| `GEMINI` | booktop: TY03; prediction_booktop: TY03; trades: TY03 | TY03 |
| `HTX` | booktop: TY03; trades: TY03 | TY03 |
| `HYPERLIQUID` | booktop: FR01, TY03, TY04; trades: TY03 | TY03 |
| `INDRSRV` | bookdepth: TY03; booktop: TY03; trades: TY03 | TY03 |
| `KALSHI` | booktop: TY03 | TY03 |
| `KRAKEN` | booktop: TY03; trades: TY03 | TY03 |
| `KRAKENFUT` | booktop: TY03 | TY03 |
| `KUCOIN` | bookdepth: TY03; booktop: TY03; trades: TY03 | TY03 |
| `LMAX` | bookdepth: TY03; booktop: TY03; trades: TY03 | TY03 |
| `NADO` | bookdepth: TY03; booktop: TY03; trades: TY03 | TY03 |
| `NASDAQ_UTP` | booktop: FR01, NY01, TY04 | TY04 |
| `NYSE` | booktop: FR01, NY01, TY04 | TY04 |
| `OKX` | bookdepth: TY03; booktop: TY03; trades: TY03 | TY03 |
| `ONDO` | bookdepth: TY03; booktop: TY03, TY04; trades: TY03 | TY03 |
| `ONDO-SPOT` | booktop: TY04 | TY03 |
| `ONETRADING` | bookdepth: TY03; booktop: TY03; trades: TY03 | TY03 |
| `PARADEX` | bookdepth: TY03; booktop: TY03 | TY03 |
| `UNISWAP_V3_ARBITRUM` | booktop: TY03; trades: TY03 | TY03 |
| `UNISWAP_V3_BASE` | trades: TY03 | TY03 |
| `ZEROHASH` | bookdepth: TY03; booktop: TY03; trades: TY03 | TY03 |

## configured feeds needing deployment/health follow-up

these exchange labels appear in checked-in influx consumer/recorder stanzas but had no matching exchange in the three-day inventory on the four audited hosts. this is not proof of failure: configs can be staged, disabled, moved, or use a different label.

| exchange | checked-in context | follow-up |
| --- | --- | --- |
| `BITMEX` | ty03 trades and depth, autostart true | confirm deployed process status and recent output |
| `BNBSPOT_SBE_FX` | fr01 adjusted consumer, autostart true | confirm output label and process status |
| `GLOBEX` | cme consumer, autostart true | confirm host and live session coverage |
| `DERIVED_ONETRADING_BNBSPOT` | botm consumers, autostart true | confirm storage destination is one of the audited hosts |
| `DERIVED_ONDO_NYSE` | ty04 consumer, autostart false | staged/disabled in checked-in config; do not add an assumed live route |
| `LMAX_WEEKEND` | ty03 consumer, autostart false | verify intended weekend schedule before classifying as missing |

## code evidence

- `umm/src/umm/analytics/market_data/_common.py`: recorder registry, exchange preferences, exchange aliases.
- `umm/src/umm/analytics/market_data/api.py`: `_resolve_recorder`, `_prep_recorder`, `available_exchanges`, `available_symbols`, `activity_summary`, `load_book`.
- `umm/src/umm/analytics/market_data/fetchers.py`: `_dump_all` dynamically discovers s3 booktop/trade sources.
- `umm/src/umm/analytics/market_data/loaders.py`: any merge and recorder filename matching.
- `oc/operations/direct_recorders/reports/booktop_cutover_readiness.md`: existing direct/legacy cutover constraints.

## implementation state

umm was on `kalshi_md_onboard` with unrelated unfinished kalshi edits during the audit. those files were left untouched. prepare fixes on a separate branch after the user checkpoints the current work; the assistant performs no git mutations. monitoring and analytics archive migration are separate from correcting reader routing.

## validation and remaining limits

- all 676 analytics unit tests passed on the saved implementation, including 10 new routing tests and existing s3 merge coverage.
- influx any now tries preferred then other known hosts for an empty requested window; it does not fill gaps in a nonempty preferred result. `df.attrs["recorder"]` identifies the selected host.
- catalogs union the four physical recording databases. activity summaries keep the preferred nonempty summary per symbol and expose its recorder; duplicate host counts are not added.
- unavailable hosts produce warnings; if no data can be found and hosts were unavailable, reads raise instead of claiming confirmed absence. parsing/programming failures are not suppressed.
- the configuration health follow-ups above remain unresolved by a reader change.
- a full live api smoke check remains incomplete. the first attempt reached the database but the temporary verification script lacked the multiprocessing entry-point guard. permission for the corrected retry was declined. the live inventory audit succeeded separately, but this is not a successful end-to-end validation of the new reader path.
