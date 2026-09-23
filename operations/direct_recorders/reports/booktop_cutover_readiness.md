# booktop recorder cutover readiness

fresh s3 inventory for 2026-09-13 04:30–04:45 utc found 1,043 production feed/recorder/windows. 93 lack the exact corresponding direct recorder; 38 have a direct object at another location, while 55 distinct exchange/symbol pairs have no direct counterpart anywhere. these feeds prevent a blanket booktop pipeline shutdown. retirement of legacy ema and btc/eth folders does not establish future recording coverage.

| feed | pairs without direct coverage |
| --- | ---: |
| zerohash | 20 |
| btso | 5 |
| lmax | 5 |
| derived bitstampperp/bnbfut | 5 |
| bitmex | 4 |
| gateio | 4 |
| nado | 3 |
| bitgetfut | 2 |
| bybit | 2 |
| kalshi | 2 |
| bullish | 1 |
| indrsrv | 1 |
| derived bitstamp/bnbfut | 1 |

## required cutover controls

1. define producer ownership per exchange, instrument, recorder, and data type. use an explicit verified coverage manifest to select direct-owned booktops, keeping exceptions on the pipeline; alternatively convert all exceptions before blanket retirement. a single observed window is not an exhaustive manifest of configured or intermittently active feeds.
2. remove pipeline booktop writes for each selected target before its direct writer takes the normal recorder name. include pending local pipeline chunks and sync/recovery work, not just future influx queries: the pipeline and direct uploader otherwise write the same s3 object keys without a shared transaction.
3. separate uploader buffer identity from its output recorder name or explicitly transfer all pending buffers under stopped-writer locks. `log_to_s3.py` currently derives its staging directory and lock from the recorder id; simply removing `D` switches directories and can leave durable buffered data behind. keep committed log checkpoints and use one reader per stream.
4. scope the new output name to booktops unless direct ownership of trades and depth is also established. the uploader handles multiple record types; a global recorder rename can affect every emitted type.
5. choose an explicit utc 15-minute cutover boundary, restart the selected writers with preserved state, and verify fresh normal-name outputs across representative periods. keep rollback configuration and original buffered/history data until verification passes.
6. perform the separately planned one-time historical `D` migration afterward, merging rather than overwriting existing normal-name files and preserving distinct observations. removal of historical `D` objects is a separate verified phase.

`scripts/influx_to_s3/sweep/run.sh` launches standard booktop export as part of `core`, alongside fills, pnl, trades, funding, and positions. do not stop the entire `core` supervisor program. prediction booktops are a separate pipeline group and require their own coverage decision.

fr01's deployed daily log cleanup deletes inactive logs without archiving or checking upload state. preserve logs through cutover and recovery; changing recorder names does not remedy this existing recovery risk.

no production recorder names, supervisor processes, or pipeline recording settings were changed by this readiness check. evidence and full comparison outputs are in `/private/tmp/booktop-cutover-check-20260913/`.
