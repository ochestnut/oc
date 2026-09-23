import unittest
import pandas as pd
import numpy as np
from bvbs_markouts import reference_frames, compute_markouts

class ReferenceClockTests(unittest.TestCase):
    def test_only_reference_clock_changes(self):
        b=pd.DataFrame({'timestamp':pd.to_datetime(['2026-09-09 00:00:00','2026-09-09 00:00:01','2026-09-09 00:00:02']),
                        'recv':pd.to_datetime(['2026-09-09 00:00:00.100','2026-09-09 00:00:01.100','2026-09-09 00:00:02.100']),
                        'bid_price':[99.,100.,101.],'ask_price':[101.,102.,103.]})
        f=pd.DataFrame({'timestamp':pd.to_datetime(['2026-09-09 00:00:01.050']), 'side':['buy'], 'liquidity':['taker'],'price':[100.],'qty':[1.]})
        refs=reference_frames(b, include_receive=True)
        out={k:compute_markouts(f,v,horizons_ns=np.array([0])).df for k,v in refs.items()}
        self.assertAlmostEqual(out['Exchange time'].mean_markout_bps.iloc[0],100.)
        self.assertAlmostEqual(out['Receive time'].mean_markout_bps.iloc[0],0.)
        self.assertEqual(out['Receive time'].n_trades.iloc[0],1)
        self.assertEqual(f.timestamp.iloc[0],pd.Timestamp('2026-09-09 00:00:01.050'))

    def test_missing_receive_time_fails(self):
        with self.assertRaisesRegex(ValueError,'receive timestamps missing'):
            reference_frames(pd.DataFrame({'timestamp':[pd.Timestamp('2026-09-09')]}), include_receive=True)

if __name__=='__main__':unittest.main()
