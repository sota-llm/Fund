#!/usr/bin/env python3
"""十只硬科技基金的共享行情监控器。

资料层（基金持仓、资产配置、正式净值）低频刷新；行情层把所有披露持仓
去重后批量读取；分钟线和近30个自然日组合回算仅在用户打开详情时按需生成。
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import math
import statistics
import threading
import time
import urllib.parse
import webbrowser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import monitor as core


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
FUND_DATA_DIR = DATA_DIR / "funds"
MULTI_CACHE_DIR = DATA_DIR / "cache_multi"
OVERVIEW_PATH = DATA_DIR / "overview.json"
STATIC_DATA_DIR = ROOT / "static-data"
FUNDS_PATH = ROOT / "funds.json"
THEMES = ("全部", "CPO", "半导体", "PCB", "MLCC")

# 仅用于说明“披露前十中的直接主题暴露”。它不是行业指数成分表，也不把未
# 披露持仓推断为某个主题。
THEME_CODES: dict[str, set[str]] = {
    "CPO": {
        "300308", "300502", "688498", "300394", "300548", "300570",
        "688313", "688048", "600105", "600487", "688800", "688807",
    },
    "半导体": {
        "300223", "301611", "300604", "688521", "688361", "688037",
        "688120", "300567", "002371", "688012", "688072", "688409",
        "603061", "688200", "688082", "603986", "688403", "688041",
        "000725", "688347", "688172", "688702", "688548", "688256",
        "300054", "002156", "688385",
    },
    "PCB": {
        "002463", "002916", "688183", "002384", "300476", "002938",
        "603228", "603920", "600183", "688519", "301511", "301377",
        "301200", "603186",
    },
    "MLCC": {"300408", "000636", "605376"},
}


def read_pool() -> dict[str, Any]:
    return json.loads(FUNDS_PATH.read_text(encoding="utf-8"))


def parse_iso(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def cache_file(code: str) -> Path:
    return MULTI_CACHE_DIR / "funds" / f"{code}.json"


def read_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def batched(items: list[dict[str, Any]], size: int = 80) -> list[list[dict[str, Any]]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def deduplicate_holdings(metadata: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    by_code: dict[str, dict[str, Any]] = {}
    for item in metadata.values():
        for holding in item["holdings"]["holdings"]:
            by_code.setdefault(str(holding["code"]), holding)
    return list(by_code.values())


def prior_trading_day(date_value: str) -> str:
    value = dt.date.fromisoformat(date_value) - dt.timedelta(days=1)
    while value.weekday() >= 5:
        value -= dt.timedelta(days=1)
    return value.isoformat()


def natural_day_window_start(date_value: str, days: int = 30) -> str:
    """返回包含结束日期在内的自然日窗口起点。"""
    if days < 1:
        raise ValueError("days 必须大于等于 1")
    end = dt.date.fromisoformat(date_value)
    return (end - dt.timedelta(days=days - 1)).isoformat()


def effective_quote_time(quote_times: list[str]) -> str:
    """行情页面收盘后仍可能更新服务器时间；展示时间最多记到15:00。"""
    parsed = [parse_iso(value) for value in quote_times]
    values = [value for value in parsed if value is not None]
    if not values:
        return core.iso_now()
    latest = max(values)
    close = latest.replace(hour=15, minute=0, second=0, microsecond=0)
    return min(latest, close).isoformat(timespec="seconds")


class MultiFundEngine:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        pool = read_pool()
        self.pool_note = pool["universe_note"]
        self.selection_as_of = pool["selection_as_of"]
        self.funds = {item["code"]: item for item in pool["funds"]}
        self.lock = threading.RLock()
        self.metadata: dict[str, dict[str, Any]] = {}
        self.latest: dict[str, Any] | None = read_json(OVERVIEW_PATH)
        self.detail_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self.last_metadata_refresh: str | None = None

    def _metadata_fresh(self, payload: dict[str, Any] | None) -> bool:
        if not payload:
            return False
        saved = parse_iso(payload.get("saved_at"))
        if not saved:
            return False
        return (core.now_cn() - saved).total_seconds() < int(
            self.config.get("metadata_refresh_seconds", 21600)
        )

    def _fetch_fund_metadata(self, spec: dict[str, Any], force: bool) -> dict[str, Any]:
        code = spec["code"]
        cached = read_json(cache_file(code))
        if not force and self._metadata_fresh(cached):
            return cached
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            profile_future = pool.submit(core.fetch_profile, code)
            holdings_future = pool.submit(core.fetch_holdings, code)
            nav_future = pool.submit(core.fetch_nav_history, code, 50)
            profile = profile_future.result()
            holdings = holdings_future.result()
            nav = nav_future.result()
        payload = {
            "saved_at": core.iso_now(),
            "spec": spec,
            "profile": profile,
            "holdings": holdings,
            "nav": nav,
        }
        core.atomic_json(cache_file(code), payload)
        return payload

    def refresh_metadata(self, force: bool = False) -> dict[str, Any]:
        started = time.perf_counter()
        results: dict[str, dict[str, Any]] = {}
        errors: list[str] = []
        workers = min(5, len(self.funds))
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(self._fetch_fund_metadata, spec, force): code
                for code, spec in self.funds.items()
            }
            for future in concurrent.futures.as_completed(futures):
                code = futures[future]
                try:
                    results[code] = future.result()
                except Exception as exc:
                    cached = read_json(cache_file(code))
                    if cached:
                        results[code] = cached
                        errors.append(f"{code} 资料刷新失败，使用缓存：{exc}")
                    else:
                        errors.append(f"{code} 无资料可用：{exc}")
        if len(results) < len(self.funds):
            raise core.SourceError("基金资料不完整：" + "；".join(errors))
        with self.lock:
            self.metadata = results
            self.last_metadata_refresh = core.iso_now()
        return {
            "seconds": time.perf_counter() - started,
            "errors": errors,
            "fund_count": len(results),
        }

    def ensure_metadata(self) -> None:
        if not self.metadata:
            self.refresh_metadata(False)
            return
        oldest = min(
            (parse_iso(item.get("saved_at")) or dt.datetime.min.replace(tzinfo=core.TZ))
            for item in self.metadata.values()
        )
        if (core.now_cn() - oldest).total_seconds() >= int(
            self.config.get("metadata_refresh_seconds", 21600)
        ):
            self.refresh_metadata(False)

    def _fetch_all_quotes(
        self, unique: list[dict[str, Any]]
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], list[str], int]:
        primary: dict[str, dict[str, Any]] = {}
        backup: dict[str, dict[str, Any]] = {}
        warnings: list[str] = []
        groups = batched(unique, 80)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(2, len(groups) * 2)) as pool:
            tasks: dict[concurrent.futures.Future[Any], tuple[str, int]] = {}
            for index, group in enumerate(groups):
                tasks[pool.submit(core.fetch_quotes_eastmoney, group)] = ("eastmoney", index)
                tasks[pool.submit(core.fetch_quotes_tencent, group)] = ("tencent", index)
            for future in concurrent.futures.as_completed(tasks):
                source, index = tasks[future]
                try:
                    values = future.result()
                    (primary if source == "eastmoney" else backup).update(values)
                except Exception as exc:
                    warnings.append(f"{source} 第{index + 1}批行情失败：{exc}")
        if len(primary) < max(1, len(unique) // 2) and len(backup) < max(1, len(unique) // 2):
            cached = read_json(MULTI_CACHE_DIR / "quotes.json") or {}
            values = cached.get("quotes") or {}
            if not values:
                raise core.SourceError("两路批量行情均不可用，且没有共享行情缓存")
            warnings.append("两路批量行情均不可用，本轮使用上次共享行情缓存。")
            return values, {}, warnings, len(groups) * 2
        chosen = primary or backup
        core.atomic_json(
            MULTI_CACHE_DIR / "quotes.json",
            {"saved_at": core.iso_now(), "quotes": chosen},
        )
        return chosen, backup if primary else primary, warnings, len(groups) * 2

    def _fund_snapshot(
        self,
        code: str,
        meta: dict[str, Any],
        quotes: dict[str, dict[str, Any]],
        crosscheck: dict[str, dict[str, Any]],
        quote_date: str,
    ) -> dict[str, Any]:
        profile = meta["profile"]
        holdings_payload = meta["holdings"]
        holdings = holdings_payload["holdings"]
        enriched: list[dict[str, Any]] = []
        discrepancies: list[float] = []
        for holding in holdings:
            quote = quotes.get(holding["code"])
            if not quote:
                continue
            other = crosscheck.get(holding["code"])
            difference = (
                abs(float(quote["return_pct"]) - float(other["return_pct"]))
                if other
                else None
            )
            if difference is not None:
                discrepancies.append(difference)
            enriched.append(
                {
                    **holding,
                    "price": quote["price"],
                    "return_pct": quote["return_pct"],
                    "quote_time": quote.get("quote_time"),
                    "contribution_pct_point": float(holding["weight_pct"])
                    * float(quote["return_pct"])
                    / 100,
                    "crosscheck_difference_pct_point": difference,
                    "theme_tags": [
                        theme
                        for theme, codes in THEME_CODES.items()
                        if holding["code"] in codes
                    ],
                }
            )
        top_weight = sum(float(item["weight_pct"]) for item in enriched)
        direct = sum(float(item["contribution_pct_point"]) for item in enriched)
        normalized = direct / top_weight * 100 if top_weight else None
        stock_pct = float(profile["stock_allocation_pct"])
        estimate = normalized * stock_pct / 100 if normalized is not None else None
        nav = meta["nav"]["rows"][0]
        estimated_nav = (
            float(nav["unit_nav"]) * (1 + estimate / 100)
            if estimate is not None and nav["date"] == prior_trading_day(quote_date)
            else None
        )
        report_date = holdings_payload["report_date"]
        disclosure_age = (core.now_cn().date() - dt.date.fromisoformat(report_date)).days
        missing = len(holdings) - len(enriched)
        posture = "研究级估算"
        if missing or not crosscheck or disclosure_age > 100:
            posture = "初步估算"
        theme_weights = {
            theme: round(
                sum(
                    float(item["weight_pct"])
                    for item in holdings
                    if item["code"] in codes
                ),
                2,
            )
            for theme, codes in THEME_CODES.items()
        }
        return {
            "code": code,
            "name": profile.get("name") or meta["spec"]["name_hint"],
            "themes": meta["spec"]["themes"],
            "focus": meta["spec"]["focus"],
            "selection_source": meta["spec"]["selection_source"],
            "estimate_pct": estimate,
            "estimated_nav": estimated_nav,
            "previous_nav": nav,
            "stock_allocation_pct": stock_pct,
            "allocation_date": profile["allocation_date"],
            "holding_report_date": report_date,
            "holding_disclosure_age_days": disclosure_age,
            "top10_weight_pct": top_weight,
            "top10_direct_contribution_pct_point": direct,
            "top10_normalized_pct": normalized,
            "theme_disclosed_weights_pct": theme_weights,
            "quote_count": len(enriched),
            "holding_count": len(holdings),
            "max_crosscheck_difference_pct_point": max(discrepancies) if discrepancies else None,
            "posture": posture,
            "holdings": enriched,
        }

    def refresh_market(self) -> dict[str, Any]:
        started = time.perf_counter()
        self.ensure_metadata()
        with self.lock:
            metadata = dict(self.metadata)
        unique = deduplicate_holdings(metadata)
        quotes, crosscheck, warnings, request_count = self._fetch_all_quotes(unique)
        quote_times = [item.get("quote_time") for item in quotes.values() if item.get("quote_time")]
        as_of = effective_quote_time(quote_times)
        quote_date = as_of[:10]
        funds = [
            self._fund_snapshot(code, metadata[code], quotes, crosscheck, quote_date)
            for code in self.funds
        ]
        estimates = [float(item["estimate_pct"]) for item in funds if item["estimate_pct"] is not None]
        theme_summary = []
        for theme in THEMES[1:]:
            members = [item for item in funds if theme in item["themes"]]
            values = [float(item["estimate_pct"]) for item in members if item["estimate_pct"] is not None]
            theme_summary.append(
                {
                    "theme": theme,
                    "fund_count": len(members),
                    "average_estimate_pct": statistics.mean(values) if values else None,
                }
            )
        raw_rows = sum(len(item["holdings"]["holdings"]) for item in metadata.values())
        elapsed = time.perf_counter() - started
        overview = {
            "kind": "hard_tech_fund_overview.v1",
            "generated_at": core.iso_now(),
            "as_of": as_of,
            "market_status": core.market_status(core.now_cn()),
            "selection_as_of": self.selection_as_of,
            "universe_note": self.pool_note,
            "summary": {
                "fund_count": len(funds),
                "up_count": sum(1 for value in estimates if value > 0),
                "down_count": sum(1 for value in estimates if value < 0),
                "flat_count": sum(1 for value in estimates if value == 0),
                "average_estimate_pct": statistics.mean(estimates) if estimates else None,
                "theme_summary": theme_summary,
            },
            "pipeline": {
                "refresh_seconds": int(self.config.get("refresh_seconds", 30)),
                "metadata_refresh_seconds": int(self.config.get("metadata_refresh_seconds", 21600)),
                "ui_refresh_seconds": 10,
                "cycle_seconds": elapsed,
                "raw_holding_rows": raw_rows,
                "unique_stocks": len(unique),
                "dedup_saved_rows": raw_rows - len(unique),
                "quote_batch_requests": request_count,
                "detail_mode": "分钟线和近30个自然日重仓组合仅在点击具体基金时按需生成并缓存",
                "metadata_last_refreshed_at": self.last_metadata_refresh,
            },
            "model": {
                "name": "披露前十归一化 × 披露股票仓位",
                "formula": "Σ(持仓占净值×个股涨幅) ÷ 前十合计权重 × 股票仓位",
                "nav_formula": "仅当T-1正式净值存在时：T日预估净值=T-1正式净值×(1+T日模型涨幅)",
                "note": "不连续叠加预测；下一交易日以新公布的正式净值重新起算。",
            },
            "funds": funds,
            "quality": {
                "warnings": warnings,
                "quote_primary": (
                    "东方财富"
                    if any(item.get("source") == "eastmoney" for item in quotes.values())
                    else "腾讯或缓存"
                ),
                "quote_crosscheck": "腾讯行情",
                "disclaimer": "所有盘中涨幅均由最近披露持仓估算，不是基金公司正式净值；观察池不是投资推荐。",
            },
            "sources": [
                {
                    "name": "天天基金/东方财富基金档案",
                    "use": "正式净值、股票仓位、最新披露前十大持仓",
                    "url": "https://fund.eastmoney.com/",
                },
                {
                    "name": "东方财富行情",
                    "use": "去重后持仓的批量实时行情主源",
                    "url": "https://quote.eastmoney.com/",
                },
                {
                    "name": "腾讯行情",
                    "use": "实时行情交叉核验、按需分钟线与历史日线",
                    "url": "https://gu.qq.com/",
                },
            ],
        }
        core.atomic_json(OVERVIEW_PATH, overview)
        for fund in funds:
            core.atomic_json(FUND_DATA_DIR / f"{fund['code']}.json", fund)
        with self.lock:
            self.latest = overview
        return overview

    def _build_intraday(self, fund: dict[str, Any]) -> dict[str, Any]:
        holdings = fund["holdings"]
        series: dict[str, dict[str, Any]] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            futures = {
                pool.submit(core.fetch_tencent_intraday, item["code"]): item["code"]
                for item in holdings
            }
            for future in concurrent.futures.as_completed(futures):
                code = futures[future]
                try:
                    series[code] = future.result()
                except Exception:
                    continue
        if len(series) < max(5, len(holdings) - 2):
            return {"trade_date": None, "points": [], "source": "腾讯个股分钟线（按需）"}
        dates = [item["trade_date"] for item in series.values()]
        trade_date = max(set(dates), key=dates.count)
        series = {code: item for code, item in series.items() if item["trade_date"] == trade_date}
        by_code = {item["code"]: item for item in holdings}
        all_times = sorted(
            {
                hhmm
                for item in series.values()
                for hhmm in item["points"]
                if core.in_a_share_session(hhmm)
            }
        )
        top_weight = sum(float(item["weight_pct"]) for item in holdings)
        stock_pct = float(fund["stock_allocation_pct"])
        last_prices: dict[str, float] = {}
        points: list[dict[str, Any]] = []
        for hhmm in all_times:
            for code, item in series.items():
                if hhmm in item["points"]:
                    last_prices[code] = float(item["points"][hhmm])
            available = [code for code in last_prices if code in by_code and code in series]
            if len(available) < max(5, len(series) - 2):
                continue
            direct = sum(
                float(by_code[code]["weight_pct"])
                / 100
                * (last_prices[code] / float(series[code]["previous_close"]) - 1)
                * 100
                for code in available
            )
            estimate = direct / top_weight * stock_pct if top_weight else None
            points.append({"time": hhmm, "estimate_pct": estimate})
        return {
            "trade_date": trade_date,
            "point_count": len(points),
            "points": points,
            "source": "腾讯个股分钟线（打开详情时按需重建）",
        }

    def _build_30d(self, fund: dict[str, Any], nav_rows: list[dict[str, Any]]) -> dict[str, Any]:
        data_date = (self.latest or {}).get("as_of", core.iso_now())[:10]
        begin = natural_day_window_start(data_date, 30)
        histories: dict[str, dict[str, float]] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = {
                pool.submit(core.fetch_tencent_history, item["code"], begin, data_date, 70): item["code"]
                for item in fund["holdings"]
            }
            market_future = pool.submit(
                core.fetch_tencent_symbol_history, "sh000300", begin, data_date, 70
            )
            for future in concurrent.futures.as_completed(futures):
                code = futures[future]
                try:
                    histories[code] = future.result()
                except Exception:
                    histories[code] = {}
            try:
                market = market_future.result()
            except Exception:
                market = {}
        holding_by_code = {item["code"]: item for item in fund["holdings"]}
        holding_dates = {
            date
            for values in histories.values()
            for date in values
            if begin <= date < data_date
        }
        # 沪深300的实际行情日期优先作为交易日历；指数源不可用时才退回
        # 持仓行情日期。两者都只包含真实返回的交易记录，不创建自然日空点。
        trading_dates = {
            date for date in market if begin <= date < data_date
        } or holding_dates
        dates = sorted(trading_dates)
        nav_by_date = {row["date"]: float(row["unit_nav"]) for row in nav_rows}
        baseline_nav_date = next((date for date in dates if date in nav_by_date), None)
        if baseline_nav_date:
            dates = [date for date in dates if date >= baseline_nav_date]
        baseline_nav = nav_by_date.get(baseline_nav_date) if baseline_nav_date else None
        market_level = 1.0
        top10_level = 1.0
        points: list[dict[str, Any]] = []
        top_weight = float(fund["top10_weight_pct"])
        for index, date in enumerate(dates):
            available = [code for code in holding_by_code if date in histories.get(code, {})]
            top_return = None
            if len(available) >= max(5, len(holding_by_code) - 2) and top_weight:
                direct = sum(
                    float(holding_by_code[code]["weight_pct"])
                    / 100
                    * float(histories[code][date])
                    for code in available
                )
                top_return = direct / top_weight * 100
            if index:
                if date in market:
                    market_level *= 1 + float(market[date]) / 100
                if top_return is not None:
                    top10_level *= 1 + top_return / 100
            nav = nav_by_date.get(date)
            points.append(
                {
                    "date": date,
                    "market_pct": (market_level - 1) * 100 if market else None,
                    "top10_pct": (top10_level - 1) * 100 if top_return is not None else None,
                    "formal_fund_pct": (nav / baseline_nav - 1) * 100 if nav and baseline_nav else None,
                    "formal_unit_nav": nav,
                    "estimated_fund_pct": None,
                    "estimated_unit_nav": None,
                }
            )
        previous = fund["previous_nav"]
        if (
            points
            and fund.get("estimated_nav") is not None
            and previous.get("date") == prior_trading_day(data_date)
        ):
            points.append(
                {
                    "date": data_date,
                    "market_pct": None,
                    "top10_pct": None,
                    "formal_fund_pct": None,
                    "formal_unit_nav": None,
                    "estimated_fund_pct": (
                        float(fund["estimated_nav"]) / baseline_nav - 1
                    )
                    * 100
                    if baseline_nav
                    else None,
                    "estimated_unit_nav": fund["estimated_nav"],
                }
            )
        return {
            "points": points,
            "formal_nav_through": previous.get("date"),
            "window": {
                "type": "calendar_days",
                "days": 30,
                "start": begin,
                "end": data_date,
            },
            "method": "仅保留最近30个自然日内有交易数据的日期；正式净值只画已公布值；当日预测只从T-1正式净值虚线延伸一次。",
        }

    def get_detail(self, code: str) -> dict[str, Any]:
        if code not in self.funds:
            raise KeyError(code)
        now = time.time()
        cached = self.detail_cache.get(code)
        status = core.market_status(core.now_cn())
        ttl = 55 if status == "交易中" else 6 * 3600
        if cached and now - cached[0] < ttl:
            return cached[1]
        if not self.latest:
            self.refresh_market()
        with self.lock:
            overview = self.latest or {}
            meta = self.metadata[code]
        fund = next(item for item in overview["funds"] if item["code"] == code)
        started = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            intraday_future = pool.submit(self._build_intraday, fund)
            comparison_future = pool.submit(self._build_30d, fund, meta["nav"]["rows"])
            intraday = intraday_future.result()
            comparison = comparison_future.result()
        detail = {
            "kind": "hard_tech_fund_detail.v1",
            "generated_at": core.iso_now(),
            "as_of": overview["as_of"],
            "market_status": overview["market_status"],
            "fund": fund,
            "model": overview["model"],
            "intraday": intraday,
            "comparison_30d": comparison,
            "detail_build_seconds": time.perf_counter() - started,
            "quality": overview["quality"],
            "sources": overview["sources"],
        }
        self.detail_cache[code] = (now, detail)
        return detail


def export_static(
    engine: MultiFundEngine, output_dir: Path = STATIC_DATA_DIR
) -> dict[str, Any]:
    """生成可由 GitHub Pages 直接读取的一致性静态快照。

    所有详情先在内存中成功生成，再写入公开目录，避免网络中断时留下
    “新总览 + 旧详情”的半套数据。
    """
    overview = engine.refresh_market()
    exported_at = core.iso_now()
    publication = {
        "mode": "static_snapshot",
        "exported_at": exported_at,
        "automatic_refresh": False,
        "detail_count": len(engine.funds),
    }
    details: dict[str, dict[str, Any]] = {}
    for code in engine.funds:
        detail = dict(engine.get_detail(code))
        detail["publication"] = publication
        details[code] = detail

    published_overview = {
        **overview,
        "pipeline": {
            **overview["pipeline"],
            "detail_mode": "静态版已预生成全部基金的分钟线、近30个自然日走势和持仓详情",
        },
        "publication": publication,
    }
    core.atomic_json(output_dir / "overview.json", published_overview)
    for code, detail in details.items():
        core.atomic_json(output_dir / "funds" / f"{code}.json", detail)
    manifest = {
        "kind": "hard_tech_fund_static_manifest.v1",
        "exported_at": exported_at,
        "as_of": overview["as_of"],
        "fund_codes": list(details),
    }
    core.atomic_json(output_dir / "manifest.json", manifest)
    return {
        "overview": published_overview,
        "details": details,
        "manifest": manifest,
    }


class AppHandler(SimpleHTTPRequestHandler):
    engine: MultiFundEngine

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def _json(self, status: int, value: Any) -> None:
        raw = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/overview":
            value = self.engine.latest or read_json(OVERVIEW_PATH)
            if value is None:
                self._json(503, {"error": "后台正在生成第一份总览数据"})
            else:
                self._json(200, value)
            return
        if parsed.path == "/api/fund":
            code = urllib.parse.parse_qs(parsed.query).get("code", [""])[0]
            try:
                self._json(200, self.engine.get_detail(code))
            except KeyError:
                self._json(404, {"error": f"基金代码不在观察池：{code}"})
            except Exception as exc:
                self._json(502, {"error": f"详情生成失败：{exc}"})
            return
        super().do_GET()


def refresh_loop(engine: MultiFundEngine, stop: threading.Event) -> None:
    interval = max(15, int(engine.config.get("refresh_seconds", 30)))
    while not stop.is_set():
        started = time.time()
        try:
            overview = engine.refresh_market()
            summary = overview["summary"]
            pipeline = overview["pipeline"]
            print(
                f"[{overview['generated_at']}] {summary['fund_count']}只基金 "
                f"平均估算 {core.fmt(summary['average_estimate_pct'])}；"
                f"{pipeline['unique_stocks']}只股票 / {pipeline['quote_batch_requests']}次批量行情 / "
                f"{pipeline['cycle_seconds']:.2f}秒",
                flush=True,
            )
        except Exception as exc:
            print(f"[{core.iso_now()}] 多基金刷新失败：{exc}", flush=True)
        stop.wait(max(1, interval - (time.time() - started)))


def serve(engine: MultiFundEngine, port: int, open_browser: bool) -> None:
    stop = threading.Event()
    worker = threading.Thread(target=refresh_loop, args=(engine, stop), daemon=True)
    worker.start()
    AppHandler.engine = engine
    server = ThreadingHTTPServer(("127.0.0.1", port), AppHandler)
    url = f"http://127.0.0.1:{port}/dashboard.html"
    print(f"硬科技基金总览：{url}")
    print("按 Ctrl+C 停止。")
    if open_browser:
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(description="十只硬科技基金共享行情监控")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("refresh", help="刷新一次总览")
    export_parser = sub.add_parser(
        "export-static", help="刷新并导出一份供 GitHub Pages 使用的完整静态快照"
    )
    export_parser.add_argument(
        "--output",
        type=Path,
        default=STATIC_DATA_DIR,
        help="静态 JSON 输出目录（默认 static-data）",
    )
    serve_parser = sub.add_parser("serve", help="持续刷新并启动总览页")
    serve_parser.add_argument("--port", type=int)
    serve_parser.add_argument("--open", action="store_true")
    args = parser.parse_args()
    config = core.read_config()
    engine = MultiFundEngine(config)
    if args.command == "refresh":
        overview = engine.refresh_market()
        print(
            json.dumps(
                {
                    "as_of": overview["as_of"],
                    "fund_count": overview["summary"]["fund_count"],
                    "average_estimate_pct": overview["summary"]["average_estimate_pct"],
                    "pipeline": overview["pipeline"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if args.command == "export-static":
        result = export_static(engine, args.output)
        print(
            json.dumps(
                {
                    "exported_at": result["manifest"]["exported_at"],
                    "as_of": result["manifest"]["as_of"],
                    "fund_count": len(result["details"]),
                    "output": str(args.output),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    serve(engine, args.port or int(config.get("server_port", 8765)), args.open)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
