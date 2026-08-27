import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path

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

    def test_export_static_writes_overview_details_and_manifest(self):
        class FakeEngine:
            funds = {"001": {}, "002": {}}

            def refresh_market(self):
                return {
                    "as_of": "2026-08-27T15:00:00+08:00",
                    "pipeline": {"detail_mode": "按需生成"},
                }

            def get_detail(self, code):
                return {"fund": {"code": code}}

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            result = multi_monitor.export_static(FakeEngine(), output)
            overview = json.loads((output / "overview.json").read_text(encoding="utf-8"))
            detail = json.loads((output / "funds" / "001.json").read_text(encoding="utf-8"))
            manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))

        self.assertEqual(overview["publication"]["mode"], "static_snapshot")
        self.assertEqual(detail["fund"]["code"], "001")
        self.assertEqual(manifest["fund_codes"], ["001", "002"])
        self.assertEqual(len(result["details"]), 2)


if __name__ == "__main__":
    unittest.main()
