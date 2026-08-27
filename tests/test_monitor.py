import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).resolve().parents[1] / "monitor.py"
SPEC = importlib.util.spec_from_file_location("fund_monitor", MODULE_PATH)
monitor = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
sys.modules[SPEC.name] = monitor
SPEC.loader.exec_module(monitor)


class MonitorUnitTests(unittest.TestCase):
    def test_market_symbol(self):
        self.assertEqual(monitor.tencent_symbol("300502"), "sz300502")
        self.assertEqual(monitor.tencent_symbol("688498"), "sh688498")

    def test_number_parser(self):
        self.assertEqual(monitor.parse_number("8.71%"), 8.71)
        self.assertEqual(monitor.parse_number("173,454.20"), 173454.2)
        self.assertIsNone(monitor.parse_number("--"))

    def test_market_status(self):
        trading = monitor.dt.datetime(2026, 8, 27, 10, 0, tzinfo=monitor.TZ)
        closed = monitor.dt.datetime(2026, 8, 27, 15, 30, tzinfo=monitor.TZ)
        self.assertEqual(monitor.market_status(trading), "交易中")
        self.assertEqual(monitor.market_status(closed), "已收盘")

    def test_a_share_session_boundaries(self):
        for hhmm in ("09:30", "11:30", "13:00", "15:00"):
            self.assertTrue(monitor.in_a_share_session(hhmm))
        for hhmm in ("09:29", "11:31", "12:30", "15:01"):
            self.assertFalse(monitor.in_a_share_session(hhmm))

    def test_comparison_rows_exclude_compounded_fund_estimates(self):
        rows = [
            {
                "date": "2026-08-25",
                "market_return_pct": 9,
                "cpo_return_pct": 9,
                "fund_estimate_return_pct": 9,
                "top10_return_pct": 9,
            },
            {
                "date": "2026-08-26",
                "market_return_pct": 10,
                "cpo_return_pct": 10,
                "fund_estimate_return_pct": 10,
                "top10_return_pct": 10,
            },
            {
                "date": "2026-08-27",
                "market_return_pct": -10,
                "cpo_return_pct": -10,
                "fund_estimate_return_pct": -10,
                "top10_return_pct": -10,
            },
        ]
        points = monitor.normalize_comparison_rows(rows)
        self.assertEqual(points[0]["market_pct"], 0)
        self.assertAlmostEqual(points[1]["market_pct"], 10)
        self.assertAlmostEqual(points[2]["market_pct"], -1)
        self.assertNotIn("fund_estimate_pct", points[0])

    def test_today_estimate_extends_only_previous_trading_day_formal_nav(self):
        cached = {
            "report_date": "2026-06-30",
            "member_signature": "BKTEST",
            "member_count": 1,
            "history_member_count": 1,
            "daily_rows": [
                {
                    "date": "2026-08-25",
                    "market_return_pct": 0,
                    "cpo_return_pct": 0,
                    "top10_return_pct": 0,
                },
                {
                    "date": "2026-08-26",
                    "market_return_pct": 1,
                    "cpo_return_pct": 2,
                    "top10_return_pct": 3,
                },
            ],
        }
        common_args = (
            [],
            [{"code": "BKTEST"}],
            47.21,
            "2026-06-30",
            "2026-08-27",
        )
        current_args = (4.0, 5.0, 10.0, 6.0)
        with patch.object(
            monitor, "load_cache", return_value=(cached, monitor.iso_now())
        ):
            warnings: list[str] = []
            result = monitor.build_comparison_series(
                *common_args,
                [
                    {"date": "2026-08-26", "unit_nav": 110.0},
                    {"date": "2026-08-25", "unit_nav": 100.0},
                ],
                *current_args,
                warnings,
            )
        self.assertEqual(result["estimate_base_date"], "2026-08-26")
        self.assertEqual(result["estimate_extension_date"], "2026-08-27")
        self.assertEqual(result["estimate_daily_pct"], 10.0)
        self.assertAlmostEqual(result["points"][-1]["estimated_unit_nav"], 121.0)
        self.assertAlmostEqual(result["points"][-1]["today_estimate_pct"], 21.0)

        with patch.object(
            monitor, "load_cache", return_value=(cached, monitor.iso_now())
        ):
            warnings = []
            result = monitor.build_comparison_series(
                *common_args,
                [{"date": "2026-08-25", "unit_nav": 100.0}],
                *current_args,
                warnings,
            )
        self.assertIsNone(result["estimate_extension_date"])
        self.assertIsNone(result["points"][-1]["today_estimate_pct"])
        self.assertTrue(any("缺少上一交易日 2026-08-26" in item for item in warnings))

    def test_nav_history_paginates_past_twenty_rows(self):
        def fake_fetch(url, **_kwargs):
            page = 1 if "pageIndex=1" in url else 2
            start = 0 if page == 1 else 20
            count = 20 if page == 1 else 5
            rows = [
                {
                    "FSRQ": f"2026-08-{25 - i:02d}",
                    "DWJZ": "2.0000",
                    "LJJZ": "3.0000",
                    "JZZZL": "1.00",
                }
                for i in range(start, start + count)
            ]
            return {"ErrCode": 0, "Data": {"LSJZList": rows}}

        with patch.object(monitor, "fetch_json", side_effect=fake_fetch) as mocked, patch.object(
            monitor, "save_cache"
        ):
            result = monitor.fetch_nav_history("025348", 25)
        self.assertEqual(mocked.call_count, 2)
        self.assertEqual(len(result["rows"]), 25)


if __name__ == "__main__":
    unittest.main()
