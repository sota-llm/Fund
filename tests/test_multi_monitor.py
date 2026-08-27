import datetime as dt
import unittest

import multi_monitor


class MultiMonitorUnitTests(unittest.TestCase):
    def test_prior_trading_day_skips_weekend(self):
        self.assertEqual(multi_monitor.prior_trading_day("2026-08-24"), "2026-08-21")
        self.assertEqual(multi_monitor.prior_trading_day("2026-08-27"), "2026-08-26")

    def test_effective_quote_time_is_capped_at_market_close(self):
        self.assertEqual(
            multi_monitor.effective_quote_time(["2026-08-27T16:12:00+08:00"]),
            "2026-08-27T15:00:00+08:00",
        )
        self.assertEqual(
            multi_monitor.effective_quote_time(["2026-08-27T14:30:00+08:00"]),
            "2026-08-27T14:30:00+08:00",
        )

    def test_holdings_are_deduplicated_across_funds(self):
        metadata = {
            "a": {"holdings": {"holdings": [{"code": "1"}, {"code": "2"}]}},
            "b": {"holdings": {"holdings": [{"code": "2"}, {"code": "3"}]}},
        }
        codes = {item["code"] for item in multi_monitor.deduplicate_holdings(metadata)}
        self.assertEqual(codes, {"1", "2", "3"})


if __name__ == "__main__":
    unittest.main()
