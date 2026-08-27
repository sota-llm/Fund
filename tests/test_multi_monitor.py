import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import multi_monitor


class MultiMonitorUnitTests(unittest.TestCase):
    def test_prior_trading_day_skips_weekend(self):
        self.assertEqual(multi_monitor.prior_trading_day("2026-08-24"), "2026-08-21")
        self.assertEqual(multi_monitor.prior_trading_day("2026-08-27"), "2026-08-26")

    def test_natural_day_window_includes_end_date_and_thirty_days(self):
        start = multi_monitor.natural_day_window_start("2026-08-27", 30)
        self.assertEqual(start, "2026-07-29")
        self.assertEqual(
            (dt.date.fromisoformat("2026-08-27") - dt.date.fromisoformat(start)).days,
            29,
        )

    def test_natural_day_window_rejects_non_positive_length(self):
        with self.assertRaises(ValueError):
            multi_monitor.natural_day_window_start("2026-08-27", 0)

    def test_comparison_uses_market_trading_dates_inside_calendar_window(self):
        engine = object.__new__(multi_monitor.MultiFundEngine)
        engine.latest = {"as_of": "2026-08-27T15:00:00+08:00"}
        holdings = [
            {"code": f"00000{index}", "weight_pct": 10.0}
            for index in range(1, 6)
        ]
        holding_history = {
            "2026-07-28": 1.0,  # 30自然日窗口之外
            "2026-07-29": 1.0,
            "2026-08-01": 1.0,  # 模拟异常的非交易日持仓记录
            "2026-08-03": 1.0,
        }
        market_history = {
            "2026-07-29": 0.5,
            "2026-08-03": 0.5,
        }
        fund = {
            "holdings": holdings,
            "top10_weight_pct": 50.0,
            "previous_nav": {"date": "2026-08-26"},
            "estimated_nav": None,
        }
        nav_rows = [
            {"date": "2026-07-29", "unit_nav": 1.0},
            {"date": "2026-08-03", "unit_nav": 1.1},
        ]
        with (
            patch.object(
                multi_monitor.core,
                "fetch_tencent_history",
                return_value=holding_history,
            ),
            patch.object(
                multi_monitor.core,
                "fetch_tencent_symbol_history",
                return_value=market_history,
            ),
        ):
            result = engine._build_30d(fund, nav_rows)

        self.assertEqual(result["window"]["start"], "2026-07-29")
        self.assertEqual(result["window"]["end"], "2026-08-27")
        self.assertEqual(
            [point["date"] for point in result["points"]],
            ["2026-07-29", "2026-08-03"],
        )

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
