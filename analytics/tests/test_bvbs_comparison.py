import unittest
import numpy as np
import pandas as pd
from bvbs_comparison import cohorts, curve, eligible, aligned_curve, plot
from bvbs_markouts import compute_markouts, parse_args, reference_frames
import matplotlib.pyplot as plt


class ComparisonTests(unittest.TestCase):
    def setUp(self):
        self.ref = pd.DataFrame({'timestamp': pd.date_range('2026-09-09', periods=41, freq='100ms'),
                                 'mid_price': np.linspace(99, 103, 41)})
        self.trades = pd.DataFrame({'timestamp': pd.to_datetime(['2026-09-09 00:00:01', '2026-09-09 00:00:02']),
                                    'side': ['buy', 'sell'], 'price': [100., 101.], 'qty': [1., 3.],
                                    'liquidity': ['taker', 'maker']})
        self.h = np.array([-100_000_000, 0, 100_000_000, 1_000_000_000])

    def test_matches_shared_analytics_formula(self):
        expected = compute_markouts(self.trades, self.ref, horizons_ns=self.h, by_liquidity=False,
                                    by_side=False, volume_weighted=False, mark_to='fill_price').df
        actual = curve(self.trades, self.ref, self.h, 500)
        np.testing.assert_allclose(actual[actual.side.eq('all')].mean_markout_bps, expected.mean_markout_bps)

    def test_public_maker_taker_mirror_and_fills_keep_role(self):
        groups, _ = cohorts(self.trades, self.trades, {'exchange': self.ref}, self.h, 500)
        maker = curve(groups['Public maker'], self.ref, self.h, 500)
        taker = curve(groups['Public taker'], self.ref, self.h, 500)
        np.testing.assert_allclose(maker[maker.side.eq('all')].mean_markout_bps,
                                   -taker[taker.side.eq('all')].mean_markout_bps)
        self.assertEqual(groups['JST maker'].side.tolist(), ['sell'])

    def test_fixed_cohort_excludes_gap_in_either_clock(self):
        sparse = self.ref.iloc[[0, 10, 40]]
        keep = eligible(self.trades, {'exchange': self.ref, 'receive': sparse}, self.h, 100)
        self.assertFalse(keep.any())

    def test_no_forward_look_or_extrapolation(self):
        future = self.trades.copy()
        future['timestamp'] = pd.Timestamp('2026-09-09 00:00:05')
        self.assertFalse(eligible(future, {'exchange': self.ref}, self.h, 10000).any())
        shifted = self.ref.assign(timestamp=self.ref.timestamp + pd.Timedelta('50ms'))
        out = curve(self.trades.iloc[:1], shifted, np.array([0]), 100)
        self.assertAlmostEqual(out[out.side.eq('all')].mean_markout_bps.iloc[0], -10)

    def test_comparison_cli_validation(self):
        args = parse_args(['--mode', 'overlay', '--start', '2026-09-09T16:00Z', '--end', '2026-09-09T18:00Z'])
        self.assertEqual(args.trade_recorder, 'TY03')
        self.assertEqual(args.ref, 'DERIVED_BITSTAMPPERP_BNBFUT')
        self.assertEqual(args.horizon, 60)
        self.assertIsNone(args.max_ref_age_ms)

    def test_exchange_only_does_not_require_receive(self):
        book = self.ref.rename(columns={'mid_price': 'bid_price'})
        book['ask_price'] = book.bid_price + 2
        refs = reference_frames(book)
        self.assertEqual(list(refs), ['Exchange time'])
        np.testing.assert_allclose(refs['Exchange time'].mid_price, book.bid_price + 1)

    def test_default_retains_same_samples_as_basic_markout(self):
        sparse = self.ref.iloc[[0, 10, 40]]
        groups, _ = cohorts(self.trades, self.trades, {'Exchange time': sparse}, self.h, None)
        self.assertEqual(len(groups['Public taker']), 2)
        result = aligned_curve(groups['Public taker'], sparse, self.h)
        expected = compute_markouts(self.trades, sparse, horizons_ns=self.h,
                                    by_liquidity=False).df
        np.testing.assert_allclose(result[result.side.eq('all')].mean_markout_bps, expected.mean_markout_bps)

    def test_side_by_side_and_shared_axes(self):
        a = parse_args(['--mode', 'overlay'])
        results = pd.concat([aligned_curve(self.trades, self.ref, self.h).assign(series=label)
                             for label in ['JST maker', 'Public maker', 'Public taker']])
        fig = plot('BTC', 'Exchange time', results, a)
        try:
            self.assertEqual(fig.axes[0].get_title(), 'BVBS fills')
            self.assertEqual(fig.axes[1].get_title(), 'Market tape')
            self.assertEqual(fig.axes[0].get_xlim(), fig.axes[1].get_xlim())
            self.assertEqual(fig.axes[0].get_ylim(), fig.axes[1].get_ylim())
            self.assertEqual(fig.axes[0].get_xscale(), 'symlog')
        finally:
            plt.close(fig)


if __name__ == '__main__':
    unittest.main()
