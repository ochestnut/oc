"""Offline regression checks; no database access required."""
import unittest
from unittest.mock import patch
import numpy as np
import pandas as pd
import bitstamp_market_markouts as report


class MarkoutReportTests(unittest.TestCase):
    def args(self, *extra):
        return report.parse_args(["--start", "2026-09-08", "--end", "2026-09-09", "--assets", "SOL", *extra])

    def trade(self):
        return pd.DataFrame({"timestamp": [pd.Timestamp("2026-09-08 12:00:00")],
                             "price": [100.], "qty": [1.], "side": ["buy"], "liquidity": ["maker"]})

    def mid(self, price):
        return pd.DataFrame({"timestamp": pd.to_datetime(["2026-09-08 11:58", "2026-09-08 12:02"]),
                             "mid_price": [price, price]})

    def test_two_refs_use_distinct_prices(self):
        args = self.args("--mode", "overlay", "--books", "BBMM", "--refs", "BITSTAMP", "COINBASE")
        trades = {"JST BBMM": self.trade()}
        refs = {"BITSTAMP": self.mid(101), "COINBS": self.mid(102)}
        result = report.calculate(args, trades, refs)
        self.assertTrue(np.allclose(result["BITSTAMP"]["mean_markout_bps"], 100))
        self.assertTrue(np.allclose(result["COINBS"]["mean_markout_bps"], 200))

    def test_overlay_loads_bitstamp_fills_and_preserves_sides(self):
        args = self.args("--mode", "overlay", "--books", "BBMM", "--refs", "BITSTAMP", "COINBASE")
        public = self.trade().assign(liquidity="market")
        with patch.object(report.MarketData, "load_trades", return_value=public) as tape, \
             patch.object(report.BotData, "load_fills", return_value=self.trade()) as fills, \
             patch.object(report.MarketData, "load_mid_price", return_value=self.mid(101)) as mids:
            trades, refs, missing = report.load_asset(args, "SOL")
        self.assertEqual(trades["Public maker"]["side"].iloc[0], "sell")
        self.assertEqual(trades["Public taker"]["side"].iloc[0], "buy")
        self.assertEqual(trades["JST BBMM"]["side"].iloc[0], "buy")
        self.assertEqual(fills.call_args.kwargs["exchange"], "BITSTAMP")
        self.assertEqual(fills.call_args.kwargs["symbol"], "PAIR-SOL-USD")
        self.assertEqual(tape.call_args.kwargs["timestamp"], "exchange")
        self.assertEqual([c.kwargs["exchange"] for c in mids.call_args_list], ["BITSTAMP", "COINBS"])
        for call in mids.call_args_list:
            self.assertLess(call.kwargs["start"], args.start)
            self.assertGreater(call.kwargs["end"], args.end)
        self.assertEqual(missing, [])

    def test_timezone_normalization(self):
        trade = self.trade()
        trade["timestamp"] = pd.to_datetime(["2026-09-08 08:00:00-04:00"])
        result = report.normalize_frame(trade, "test")
        self.assertEqual(result["timestamp"].iloc[0], pd.Timestamp("2026-09-08 12:00:00"))

    def test_missing_requested_fills_fails(self):
        args = self.args("--mode", "fills", "--books", "BBMM")
        with patch.object(report.BotData, "load_fills", return_value=pd.DataFrame()):
            with self.assertRaisesRegex(RuntimeError, "No Bitstamp fills"):
                report.load_asset(args, "SOL")

    def test_backward_alignment(self):
        args = self.args("--horizon", "1")
        mid = pd.DataFrame({"timestamp": pd.to_datetime(["2026-09-08 11:59:58", "2026-09-08 12:00:00.500", "2026-09-08 12:00:02"], format="mixed"),
                            "mid_price": [100., 101., 102.]})
        result = report.calculate(args, {"JST BBMM": self.trade()}, {"BITSTAMP": mid})["BITSTAMP"]
        self.assertTrue((result.loc[result.horizon_sec == 0, "mean_markout_bps"] == 0).all())
        self.assertTrue(np.allclose(result.loc[result.horizon_sec == 1, "mean_markout_bps"], 100))


if __name__ == "__main__":
    unittest.main()
