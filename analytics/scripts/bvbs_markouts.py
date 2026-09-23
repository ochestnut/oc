"""BVBS perp markouts: derived reference and exchange-time comparisons.

Executions and reference prices use exchange time. An exact historical
reproduction requires the original data cutoff and recorder.
"""
from analytics_paths import output_path
import argparse
from io import BytesIO
from pathlib import Path
import sys
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, FuncFormatter
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'umm/src'))
from umm.analytics.market_data import MarketData
from umm.analytics.trading_data import BotData
from umm.analytics.compute.markout import compute_markouts, signed_horizons_ns
from umm.analytics.display.markout import sci_fmt, _symlog_ticks
from matplotlib.backends.backend_pdf import PdfPages


def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--start', default='2026-09-09T00:00:00Z')
    p.add_argument('--end', default='2026-09-09T23:00:00Z')
    p.add_argument('--assets', nargs='+', default=['BTC','ETH','SOL','XRP','DOGE'])
    p.add_argument('--book', default='BVBS')
    p.add_argument('--exchange', default='BITSTAMPPERP')
    p.add_argument('--ref', default='DERIVED_BITSTAMPPERP_BNBFUT')
    p.add_argument('--recorder', default='FR01')
    p.add_argument('--source', choices=['influx','s3'], default='influx')
    p.add_argument('--mode', choices=['fills', 'market', 'overlay'], default='fills')
    p.add_argument('--trade-recorder', default='TY03', help='Public tape recorder; independent of reference recorder')
    p.add_argument('--horizon', type=float, default=60, help='Comparison half-window in seconds')
    p.add_argument('--max-ref-age-ms', type=float, default=None, help='Optional stale-reference filter; omitted matches basic markout settings')
    p.add_argument('--output', type=Path)
    a=p.parse_args(argv)
    if a.output is None:
        filename = 'bvbs_markouts.pdf' if a.mode == 'fills' else f'bvbs_{a.mode}_comparison.pdf'
        a.output = output_path(filename, "reports")
    a.start=pd.to_datetime(a.start,utc=True)
    a.requested_end=pd.to_datetime(a.end,utc=True)
    a.end=min(a.requested_end, pd.Timestamp.now(tz='UTC'))
    if a.end<=a.start: p.error('end must follow start')
    if not np.isfinite(a.horizon) or a.horizon < 1: p.error('horizon must be finite and at least 1 second')
    if a.max_ref_age_ms is not None and (not np.isfinite(a.max_ref_age_ms) or a.max_ref_age_ms <= 0): p.error('max-ref-age-ms must be finite and positive')
    if a.output.suffix.lower()!='.pdf': p.error('output must end in .pdf')
    return a


def reference_frames(book, include_receive=False):
    b=book.reset_index() if 'timestamp' not in book.columns else book.copy()
    if include_receive and ('recv' not in b or b.recv.isna().any()):
        raise ValueError('Reference receive timestamps missing; cannot compare clocks')
    b['mid_price']=(b.bid_price.astype('float32')+b.ask_price.astype('float32'))*np.float32(.5)
    refs={}
    clocks = [('Exchange time', 'timestamp')]
    if include_receive:
        clocks.append(('Receive time', 'recv'))
    for name,col in clocks:
        refs[name]=pd.DataFrame({'timestamp':pd.to_datetime(b[col],utc=True).dt.tz_localize(None), 'mid_price':b.mid_price}).sort_values('timestamp',kind='stable')
    return refs


def draw(a, asset, fills, results, refs, page):
    fig,axes=plt.subplots(len(results),3,figsize=(16,5),squeeze=False,gridspec_kw={'width_ratios':[1.4,1.4,.65]})
    colors={'maker':'#168477','taker':'#b27a08'}
    counts=fills.groupby('liquidity').size()
    for row,(clock,res) in enumerate(results.items()):
        for liq,g in res.groupby('liquidity'):
            g=g.sort_values('horizon_sec')
            label=f'{liq.capitalize()} (n={counts[liq]})'
            for col in [0,1]:
                s=g if col==0 else g[g.horizon_sec.abs()<=1]
                axes[row,col].plot(s.horizon_sec*(1000 if col==1 else 1),s.mean_markout_bps,
                                  color=colors.get(liq,'grey'),ls='-' if liq=='maker' else '--',label=label,lw=1.7)
        for col in [0,1]:
            ax=axes[row,col]
            ax.axvline(0,color='grey',lw=.6);ax.axhline(0,color='grey',lw=.6)
            ax.grid(alpha=.18);ax.legend(fontsize=8)
            ax.set_ylabel('Mean markout (bps)')
            ax.set_title(f'{clock} reference | '+('Full range' if col==0 else 'Detail'))
        ax=axes[row,0]
        ax.set_xscale('symlog',linthresh=.002)
        ax.xaxis.set_major_locator(FixedLocator(_symlog_ticks(60)))
        ax.xaxis.set_major_formatter(FuncFormatter(sci_fmt))
        ax.tick_params(axis='x',labelsize=8)
        ax.set_xlabel('Time relative to fill (s / ms labeled)')
        axes[row,1].set(xlim=(-1000,1000),xlabel='Milliseconds relative to fill')
        ax=axes[row,2]
        ax.bar(counts.index,counts.values,color=[colors.get(x,'grey') for x in counts.index])
        for i,v in enumerate(counts.values):ax.text(i,v,str(v),ha='center',va='bottom')
        ax.set(title='Fill counts',ylabel='Number of fills',ylim=(0,max(counts.values)*1.2))
    fig.suptitle(f'{a.book} | {a.exchange} | PERP-{asset}-USD\nReference: {a.ref}',fontsize=15,y=.98)
    fig.text(.055,.885,f'UTC: {a.start.isoformat()} to {a.end.isoformat()} | {a.source} / {a.recorder}\n'
             f'Actual fills: {fills.timestamp.min()} to {fills.timestamp.max()} UTC | Reference ticks: {len(refs["Exchange time"]):,}',fontsize=9)
    fig.text(.055,.025,'Exchange timestamps for executions and reference prices.\n'
             'Equal fill weighting; buy/sell pooled by maker/taker; execution-price baseline; before fees; backward as-of.\n'
             'Positive = favorable to the trader. No extrapolation outside reference coverage; horizon counts are exported separately.',fontsize=8)
    fig.text(.96,.025,f'{page}/{len(a.assets)}',ha='right')
    fig.tight_layout(rect=(.02,.085,.99,.865))
    return fig


def main():
    a=parse_args()
    if a.mode != 'fills':
        from bvbs_comparison import run
        return run(a)
    print(f'Effective UTC cutoff: {a.end}',flush=True)
    allfills=BotData.load_fills(a.book,a.start,a.end,exchange=a.exchange,source=a.source,force=True)
    if allfills.empty:raise RuntimeError('No requested fills; report not written')
    allfills['timestamp']=pd.to_datetime(allfills.timestamp,utc=True).dt.tz_localize(None)
    horizons=np.unique(np.r_[signed_horizons_ns(60),np.arange(-1000,1001,10,dtype=np.int64)*1000000])
    pages=[];exports=[]
    try:
        for asset in a.assets:
            symbol=f'PERP-{asset}-USD'
            f=allfills[allfills.symbol.eq(symbol)].copy()
            if f.empty:raise RuntimeError(f'No fills for {symbol}; report not written')
            print(f'{symbol}: {f.groupby("liquidity").size().to_dict()}',flush=True)
            b=MarketData.load_best_levels(exchange=a.ref,symbol=symbol,start=a.start-pd.Timedelta('61s'),
                end=a.end+pd.Timedelta('61s'),recorder=a.recorder,source=a.source,force=True,timestamp='exchange')
            if b.empty:raise RuntimeError(f'No reference for {symbol}')
            refs=reference_frames(b);results={}
            for clock,ref in refs.items():
                r=compute_markouts(f,ref,horizons_ns=horizons,by_side=False,volume_weighted=False,mark_to='fill_price',alignment='backward').df
                if not r.n_trades.gt(0).any():raise RuntimeError(f'No reference overlap: {symbol}/{clock}')
                results[clock]=r
                exports.append(r.assign(asset=asset,clock=clock))
            pages.append(draw(a,asset,f,results,refs,len(pages)+1))
        buf=BytesIO()
        with PdfPages(buf) as pdf:
            for fig in pages:pdf.savefig(fig)
        a.output.parent.mkdir(parents=True,exist_ok=True)
        a.output.write_bytes(buf.getvalue())
        pd.concat(exports).to_csv(a.output.with_suffix('.csv'),index=False)
        print(f'Saved {a.output}',flush=True)
    finally:
        for fig in pages:plt.close(fig)

if __name__=='__main__':main()
