# Bitstamp exchange-time comparison

From `oc/analytics/scripts`:

```bash
../../../umm/venv/bin/python bvbs_markouts.py \
  --mode overlay --assets BTC ETH SOL XRP DOGE \
  --start 2026-09-09T00:00:00Z --end 2026-09-09T23:00:00Z \
  --trade-recorder TY03 --recorder FR01 \
  --ref DERIVED_BITSTAMPPERP_BNBFUT --source influx \
  --output ../../bvbs_exchange_comparison.pdf
```

The daily requested interval matches Kushaal: 00:00-23:00 UTC. A run before
23:00 uses one common run-time cutoff for all assets and both samples; the PDF
shows both requested bounds and that cutoff.

Each asset has JST fills on the left and the public tape on the right, with
maker/taker curves and count bars. The paired charts share x/y scales. Exchange
time only; no receive-time panels or receive-time eligibility requirement.

## Common settings

Both samples use the shared `compute_markouts` Basic Fills calculation:

- Exact same requested UTC window, perp instrument and derived USD reference.
- Default signed horizon ladder from -60 to +60 seconds; symlog axis, 2 ms linear threshold.
- Equal execution weighting; buy/sell pooled, split by maker/taker.
- Execution-price baseline, before fees, backward as-of reference alignment.
- No extrapolation beyond observed reference coverage. Counts may vary by horizon.
- No added stale-reference exclusion by default. `--max-ref-age-ms` enables an
  optional fixed-cohort sensitivity check and is not part of the basic comparison.

Public trade side is the aggressor side; it is reversed once for the maker curve.
Public maker/taker are two perspectives on the same tape, not independent samples.
Missing JST roles are shown as zero counts; no alternative book or window is substituted.

Kushaal's [original BVBS chart set](https://jstcapital.slack.com/archives/C09GGNK5EBB/p1788368100117419)
shows these five assets, the derived reference, +/-60 s symlog curves and trade-count
bars. This report matches those visible conventions and the existing shared Basic
Fills implementation. It uses September 9's newly recorded tape, not Kushaal's
September 2 sample. Hidden historical settings cannot be established from a screenshot.

## Outputs

- PDF: five exchange-time pages, JST versus public tape.
- `.csv`: pooled and buy/sell curves and counts at each sampled horizon.
- `.comparison.csv`: JST-minus-public at available -100 ms and +10/50/100 ms, +1 s
  checkpoints. Positive means a higher markout for JST. The basic ladder has no T0 point.
- `.json`: parameters, tape timing diagnostics and input cohort counts.

`--mode market` runs without JST fills. `--mode fills` retains the fill-only report,
now exchange-time only. `--horizon` controls the comparison's full range. The default
output filenames differ by mode so an overlay does not replace the fill-only PDF.

These are descriptive comparisons, not size/time-matched samples or causal latency
estimates. Thin JST samples cannot establish relative performance. Inspect trade
counts and recorded tape gaps before interpreting a market-wide benchmark. Raw
receive-minus-exchange timing diagnostics do not establish exchange internal latency.

Tom's [request](https://jstcapital.slack.com/archives/D09CZ9Z0SRK/p1788985907015819)
asks for this same-setup public-tape comparison. Expected maker/taker curve shapes
are diagnostic hypotheses, never criteria used to alter the data.

## Tests

```bash
../../../umm/venv/bin/python -m unittest test_bvbs_comparison test_bvbs_markouts test_bitstamp_market_markouts
```
