"""Exchange-time public tape versus BVBS fill comparison.

Uses the shared Basic Fills Markout calculation and horizons by default.
A fixed cohort with a staleness threshold is available only when requested.
Public maker/taker are two perspectives on the same executions, not two samples.
"""
from io import BytesIO
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from bvbs_markouts import MarketData, BotData, reference_frames, signed_horizons_ns, compute_markouts, sci_fmt, _symlog_ticks
from matplotlib.ticker import FixedLocator, FuncFormatter
from bitstamp_market_markouts import normalize_frame


def prepare(frame, label):
    frame = normalize_frame(frame, label)
    for col in ('price', 'qty'):
        frame[col] = pd.to_numeric(frame[col], errors='raise')
        if not (np.isfinite(frame[col]) & frame[col].gt(0)).all():
            raise ValueError(f'{label}: invalid {col}')
    if not frame.side.isin(['buy', 'sell']).all():
        raise ValueError(f'{label}: unknown trade side')
    return frame


def lookup(ref, targets, max_age_ns):
    times = ref.timestamp.to_numpy(dtype='datetime64[ns]').astype('int64')
    mid = ref.mid_price.to_numpy(dtype=float)
    idx = np.searchsorted(times, targets, side='right') - 1
    age = targets - times[np.maximum(idx, 0)]
    valid = (idx >= 0) & (targets <= times[-1]) & (age <= max_age_ns)
    prices = mid[np.maximum(idx, 0)]
    return prices, valid & np.isfinite(prices) & (prices > 0), age / 1e6


def eligible(frame, refs, horizons, max_age_ms):
    ts = frame.timestamp.to_numpy(dtype='datetime64[ns]').astype('int64')
    keep = np.ones(len(frame), dtype=bool)
    for ref in refs.values():
        for horizon in horizons:
            keep &= lookup(ref, ts + horizon, max_age_ms * 1e6)[1]
    return keep


def curve(frame, ref, horizons, max_age_ms):
    ts = frame.timestamp.to_numpy(dtype='datetime64[ns]').astype('int64')
    price = frame.price.to_numpy(dtype=float)
    sign = np.where(frame.side.eq('buy'), 1., -1.)
    rows = []
    for h in horizons:
        mid, valid, age = lookup(ref, ts + h, max_age_ms * 1e6)
        if not valid.all():
            raise ValueError('Curve received an ineligible cohort')
        values = sign * (mid - price) / price * 10000
        for side in ('all', 'buy', 'sell'):
            selected = np.ones(len(frame), dtype=bool) if side == 'all' else frame.side.eq(side).to_numpy()
            v = values[selected]
            rows.append(dict(horizon_sec=h / 1e9, side=side, n_trades=len(v),
                             mean_markout_bps=float(v.mean()) if len(v) else np.nan,
                             median_markout_bps=float(np.median(v)) if len(v) else np.nan,
                             ref_age_p50_ms=float(np.median(age[selected])) if len(v) else np.nan,
                             ref_age_p99_ms=float(np.quantile(age[selected], .99)) if len(v) else np.nan))
    return pd.DataFrame(rows)


def aligned_curve(frame, ref, horizons, max_age_ms=None):
    if max_age_ms is not None:
        return curve(frame, ref, horizons, max_age_ms)
    # Use the same engine/settings as the Basic Fills Markout, for both samples.
    pooled = compute_markouts(frame, ref, horizons_ns=horizons, by_side=False,
                              by_liquidity=False, volume_weighted=False,
                              mark_to='fill_price', alignment='backward').df.assign(side='all')
    sides = compute_markouts(frame, ref, horizons_ns=horizons, by_side=True,
                             by_liquidity=False, volume_weighted=False,
                             mark_to='fill_price', alignment='backward').df
    return pd.concat([pooled, sides], ignore_index=True)


def cohorts(tape, fills, refs, horizons, max_age_ms):
    tape = tape.copy()
    tape['liquidity'] = 'taker'
    maker = tape.copy()
    maker['side'] = maker.side.map({'buy': 'sell', 'sell': 'buy'})
    maker['liquidity'] = 'maker'
    groups = {'Public maker': maker, 'Public taker': tape}
    if fills is not None:
        if not fills.liquidity.isin(['maker', 'taker']).all():
            raise ValueError('Unknown fill liquidity; cannot compare roles')
        groups.update({f'JST {role}': f for role, f in fills.groupby('liquidity')})
    kept, diagnostics = {}, []
    for label, frame in groups.items():
        mask = np.ones(len(frame), dtype=bool) if max_age_ms is None else eligible(frame, refs, horizons, max_age_ms)
        kept[label] = frame.loc[mask].copy()
        diagnostics.append(dict(series=label, input_count=len(frame), retained_count=int(mask.sum()),
                                excluded_count=int((~mask).sum())))
        if not mask.any():
            raise RuntimeError(f'No fully covered trades for {label}; inspect recorder/window/reference age')
    return kept, diagnostics


def plot(asset, clock, results, a):
    fig, axes = plt.subplots(2, 2, figsize=(16, 9), sharey='row',
                             gridspec_kw={'height_ratios': [3, 1]})
    colors = {'maker': '#168477', 'taker': '#b27a08'}
    for col, prefix in enumerate(('JST', 'Public')):
        subset = results[(results.side == 'all') & results.series.str.startswith(prefix)]
        counts = {}
        ax = axes[0, col]
        for role in ('maker', 'taker'):
            g = subset[subset.series.eq(f'{prefix} {role}')].sort_values('horizon_sec')
            if g.empty:
                continue
            counts[role] = int(g.n_trades.max())
            ax.plot(g.horizon_sec, g.mean_markout_bps, marker='.', markersize=4,
                    color=colors[role], linestyle='-' if role == 'maker' else '--',
                    label=f'{role.capitalize()} (n={counts[role]:,})')
        ax.set_xscale('symlog', linthresh=.002)
        ax.set_xlim(-a.horizon, a.horizon)
        ax.xaxis.set_major_locator(FixedLocator(_symlog_ticks(a.horizon)))
        ax.xaxis.set_major_formatter(FuncFormatter(sci_fmt))
        ax.set(title=f'{a.book} fills' if prefix == 'JST' else 'Market tape',
               xlabel='Time from execution (s / ms labeled)', ylabel='Mean markout (bps)')
        ax.axvline(0, c='gray', lw=.7)
        ax.axhline(0, c='gray', lw=.7)
        ax.grid(alpha=.2)
        if counts:
            ax.legend(loc='best')
        else:
            ax.text(.5, .5, 'No fills in this window', transform=ax.transAxes, ha='center')
        bars = axes[1, col]
        roles = ['maker', 'taker']
        values = [counts.get(role, 0) for role in roles]
        bars.bar(roles, values, color=[colors[role] for role in roles])
        for i, value in enumerate(values):
            bars.text(i, value, f'{value:,}', ha='center', va='bottom')
        bars.set(ylabel='Executions', title='Trade counts')
    top = max(ax.get_ylim()[1] for ax in axes[1])
    for ax in axes[1]:
        ax.set_ylim(0, max(top * 1.15, 1))
    fig.suptitle(f'{a.book} | {a.exchange} | PERP-{asset}-USD\n{a.ref} | Exchange time', fontsize=15)
    fig.text(.055, .885, f'Requested UTC: {a.start.isoformat()} to {a.requested_end.isoformat()} | Data cutoff: {a.end.isoformat()}\n'
             f'{a.source} | tape {a.trade_recorder} | reference {a.recorder} | identical x/y scales', fontsize=9)
    fig.text(.055, .025, 'Equal execution weighting; buy/sell pooled by liquidity; execution-price baseline; before fees; backward as-of.\n'
             'Public maker/taker are opposite sides of the same tape. No extrapolation beyond reference coverage.\n'
             'Counts can vary by horizon. Small JST samples are descriptive only; no size or intrawindow timing matching.', fontsize=9)
    fig.tight_layout(rect=(.02, .10, .99, .87))
    return fig


def run(a):
    horizons = signed_horizons_ns(a.horizon)
    exports, quality, pages = [], [], []
    try:
        for asset in a.assets:
            symbol = f'PERP-{asset}-USD'
            print(f'Loading {symbol}: tape {a.trade_recorder}, reference {a.recorder}', flush=True)
            tape = prepare(MarketData.load_trades(exchange=a.exchange, symbol=symbol, start=a.start,
                end=a.end, recorder=a.trade_recorder, source=a.source, timestamp='exchange',
                mm_perspective=False, force=True), 'Public tape')
            # Both sides use exactly the requested UTC interval.
            start = a.start.tz_localize(None)
            end = a.end.tz_localize(None)
            tape = tape[tape.timestamp.between(start, end, inclusive='both')]
            fills = None
            if a.mode == 'overlay':
                raw_fills = BotData.load_fills(a.book, start, end, exchange=a.exchange,
                    symbol=symbol, source=a.source, force=True)
                if not raw_fills.empty:
                    fills = prepare(raw_fills, 'JST fills')
                    fills = fills[fills.timestamp.between(start, end, inclusive='both')]
            padding = pd.Timedelta(seconds=a.horizon + (a.max_ref_age_ms or 1000)/1000)
            book = MarketData.load_best_levels(exchange=a.ref, symbol=symbol, start=start-padding,
                end=end+padding, recorder=a.recorder, source=a.source, force=True, timestamp='exchange')
            if book.empty:
                raise RuntimeError(f'No reference for {symbol}')
            refs = reference_frames(book)
            for clock, ref in refs.items():
                if ref.timestamp.isna().any():
                    raise ValueError(f'Missing reference timestamp: {clock}')
            groups, diagnostic = cohorts(tape, fills, refs, horizons, a.max_ref_age_ms)
            gaps = tape.timestamp.diff().dt.total_seconds().dropna()
            tape_quality = dict(unique_timestamps=int(tape.timestamp.nunique()),
                                same_timestamp_rows=int(tape.timestamp.duplicated().sum()),
                                gap_p50_seconds=float(gaps.median()) if len(gaps) else None,
                                gap_max_seconds=float(gaps.max()) if len(gaps) else None)
            if 'recv' in tape:
                arrival = (pd.to_datetime(tape.recv, utc=True) - pd.to_datetime(tape.timestamp, utc=True)).dt.total_seconds()*1000
                tape_quality['recv_minus_exchange_p50_ms'] = float(arrival.median())
                tape_quality['recv_minus_exchange_p99_ms'] = float(arrival.quantile(.99))
            quality.append(dict(asset=asset, actual_start=str(start), actual_end=str(end),
                                tape_diagnostics=tape_quality, cohorts=diagnostic))
            for clock, ref in refs.items():
                result = pd.concat([aligned_curve(f, ref, horizons, a.max_ref_age_ms).assign(series=label)
                                    for label, f in groups.items()], ignore_index=True)
                exports.append(result.assign(asset=asset, clock=clock))
                pages.append(plot(asset, clock, result, a))
        full = pd.concat(exports, ignore_index=True)
        checkpoints = full[full.horizon_sec.isin([-.1, 0, .01, .05, .1, 1.])].copy()
        public = checkpoints[checkpoints.series.str.startswith('Public')].copy()
        public['role'] = public.series.str.split().str[-1]
        ours = checkpoints[checkpoints.series.str.startswith('JST')].copy()
        ours['role'] = ours.series.str.split().str[-1]
        gap = ours.merge(public, on=['asset', 'clock', 'role', 'side', 'horizon_sec'], suffixes=('_jst', '_public'))
        gap['jst_minus_public_bps'] = gap.mean_markout_bps_jst - gap.mean_markout_bps_public
        buffer = BytesIO()
        with PdfPages(buffer) as pdf:
            for fig in pages:
                pdf.savefig(fig)
        a.output.parent.mkdir(parents=True, exist_ok=True)
        a.output.write_bytes(buffer.getvalue())
        full.to_csv(a.output.with_suffix('.csv'), index=False)
        gap.to_csv(a.output.with_suffix('.comparison.csv'), index=False)
        metadata = dict(settings={k: str(v) for k, v in vars(a).items()}, quality=quality,
                        methodology='sign*(reference_mid(h)-execution_price)/execution_price*10000; '
                        'equal trades; backward as-of; exchange time only; shared basic markout horizon coverage; '
                        'descriptive comparison, not size/time matched or causal latency attribution')
        a.output.with_suffix('.json').write_text(json.dumps(metadata, indent=2))
        print(f'Saved {a.output} and CSV/JSON diagnostics', flush=True)
        print(json.dumps(quality, indent=2), flush=True)
    finally:
        for fig in pages:
            plt.close(fig)
