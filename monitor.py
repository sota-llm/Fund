#!/usr/bin/env python3
"""Fund 的单基金兼容监控器。

仅使用 Python 标准库。数据来自公开网页/行情接口，盘中结果是基于最近披露
持仓的模型估值，不是基金管理人公布的正式净值。
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import html
import json
import math
import os
import re
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import dataclass, asdict
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
CACHE_DIR = DATA_DIR / "cache"
LATEST_PATH = DATA_DIR / "latest.json"
HISTORY_PATH = DATA_DIR / "history.jsonl"
CONFIG_PATH = ROOT / "config.json"
TZ = dt.timezone(dt.timedelta(hours=8), "Asia/Shanghai")

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
)

EASTMONEY_UT = "267f9ad526dbe6b0262ab19316f5a25b"


class SourceError(RuntimeError):
    pass


@dataclass
class Holding:
    code: str
    market: int
    name: str
    weight_pct: float
    shares_10k: float | None = None
    market_value_10k: float | None = None


def now_cn() -> dt.datetime:
    return dt.datetime.now(TZ)


def iso_now() -> str:
    return now_cn().isoformat(timespec="seconds")


def read_config() -> dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # 多基金资料会并行写缓存；临时文件必须按线程隔离，避免一个线程先
    # replace 后另一个线程找不到同名 .tmp。
    tmp = path.with_suffix(
        path.suffix + f".tmp.{os.getpid()}.{threading.get_ident()}"
    )
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(value, fh, ensure_ascii=False, indent=2, allow_nan=False)
    os.replace(tmp, path)


def cache_path(name: str) -> Path:
    return CACHE_DIR / f"{name}.json"


def save_cache(name: str, value: Any) -> None:
    atomic_json(cache_path(name), {"saved_at": iso_now(), "value": value})


def load_cache(name: str) -> tuple[Any, str] | tuple[None, None]:
    path = cache_path(name)
    if not path.exists():
        return None, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload["value"], payload.get("saved_at")
    except (OSError, ValueError, KeyError):
        return None, None


def fetch_bytes(
    url: str,
    *,
    referer: str | None = None,
    timeout: float = 15,
    retries: int = 3,
) -> bytes:
    headers = {"User-Agent": UA, "Accept": "*/*"}
    if referer:
        headers["Referer"] = referer
    last: Exception | None = None
    for attempt in range(retries):
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=timeout) as response:
                data = response.read()
            if not data:
                raise SourceError(f"空响应: {url}")
            return data
        except (urllib.error.URLError, TimeoutError, OSError, SourceError) as exc:
            last = exc
            if attempt + 1 < retries:
                time.sleep(0.4 * (2**attempt))
    raise SourceError(f"请求失败: {url}: {last}")


def fetch_text(url: str, **kwargs: Any) -> str:
    raw = fetch_bytes(url, **kwargs)
    for encoding in ("utf-8-sig", "gb18030", "utf-8"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def fetch_json(url: str, **kwargs: Any) -> Any:
    text = fetch_text(url, **kwargs).strip()
    if text.startswith("jQuery") or text.startswith("jsonp"):
        text = text[text.find("(") + 1 : text.rfind(")")]
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SourceError(f"JSON 解析失败: {url}: {exc}") from exc


def strip_tags(value: str) -> str:
    value = re.sub(r"<br\s*/?>", " ", value, flags=re.I)
    value = re.sub(r"<[^>]+>", "", value)
    return html.unescape(value).strip()


def parse_number(value: str) -> float | None:
    cleaned = value.replace(",", "").replace("%", "").strip()
    if not cleaned or cleaned in {"--", "-"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _extract_js_var(text: str, name: str) -> str:
    match = re.search(rf"var\s+{re.escape(name)}\s*=\s*(.*?);", text, re.S)
    if not match:
        raise SourceError(f"缺少字段 {name}")
    return match.group(1).strip()


def fetch_profile(fund_code: str) -> dict[str, Any]:
    url = f"https://fund.eastmoney.com/pingzhongdata/{fund_code}.js?v={int(time.time())}"
    text = fetch_text(url, referer=f"https://fund.eastmoney.com/{fund_code}.html")
    name_match = re.search(r'var\s+fS_name\s*=\s*"([^"]+)"', text)
    if not name_match:
        raise SourceError("基金名称解析失败")
    allocation = json.loads(_extract_js_var(text, "Data_assetAllocation"))
    categories = allocation.get("categories") or []
    series = allocation.get("series") or []
    stock_series = next((x for x in series if x.get("name") == "股票占净比"), None)
    if not categories or not stock_series or not stock_series.get("data"):
        raise SourceError("资产配置解析失败")
    stock_codes = json.loads(_extract_js_var(text, "stockCodesNew"))
    result = {
        "name": name_match.group(1),
        "allocation_date": categories[-1],
        "stock_allocation_pct": float(stock_series["data"][-1]),
        "holding_codes": [x.split(".")[-1] for x in stock_codes],
        "source_url": url,
    }
    save_cache("profile", result)
    return result


def fetch_holdings(fund_code: str) -> dict[str, Any]:
    nonce = f"{time.time():.6f}"
    url = (
        "https://fundf10.eastmoney.com/FundArchivesDatas.aspx"
        f"?type=jjcc&code={fund_code}&topline=10&year=&month=&rt={nonce}"
    )
    text = fetch_text(
        url,
        referer=f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html",
    )
    content_match = re.search(r'content:"(.*)",arryear:', text, re.S)
    if not content_match:
        raise SourceError("持仓内容解析失败")
    content = content_match.group(1).replace('\\"', '"').replace("\\'", "'")
    date_match = re.search(r"截止至：.*?(\d{4}-\d{2}-\d{2})", content, re.S)
    table_match = re.search(r"<table.*?</table>", content, re.S | re.I)
    if not date_match or not table_match:
        raise SourceError("持仓日期或表格缺失")

    holdings: list[Holding] = []
    for row in re.findall(r"<tr>(.*?)</tr>", table_match.group(0), re.S | re.I):
        code_match = re.search(r"unify/r/([01])\.(\d{6})", row)
        if not code_match:
            continue
        cells = [strip_tags(x) for x in re.findall(r"<td[^>]*>(.*?)</td>", row, re.S | re.I)]
        if len(cells) < 7:
            continue
        code = code_match.group(2)
        market = int(code_match.group(1))
        name = cells[2]
        # 服务器端表格有两种形态：盘中含最新价/涨跌幅，盘后可能省略。
        weight_cell = next((x for x in cells[3:] if x.endswith("%")), None)
        if weight_cell is None:
            continue
        weight_idx = cells.index(weight_cell)
        weight = parse_number(weight_cell)
        shares = parse_number(cells[weight_idx + 1]) if len(cells) > weight_idx + 1 else None
        market_value = parse_number(cells[weight_idx + 2]) if len(cells) > weight_idx + 2 else None
        if weight is None:
            continue
        holdings.append(Holding(code, market, name, weight, shares, market_value))
    if len(holdings) < 5:
        raise SourceError(f"持仓数量异常: {len(holdings)}")
    result = {
        "report_date": date_match.group(1),
        "holdings": [asdict(x) for x in holdings],
        "source_url": f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html",
    }
    save_cache("holdings", result)
    return result


def fetch_nav_history(fund_code: str, page_size: int = 90) -> dict[str, Any]:
    # 该接口即使传入更大的 pageSize，实际单页仍最多返回20条，必须翻页。
    per_page = 20
    page_count = max(1, math.ceil(page_size / per_page))
    raw_rows: list[dict[str, Any]] = []
    source_url = f"https://fund.eastmoney.com/{fund_code}.html"
    for page_index in range(1, page_count + 1):
        url = (
            "https://api.fund.eastmoney.com/f10/lsjz"
            f"?fundCode={fund_code}&pageIndex={page_index}&pageSize={per_page}"
        )
        payload = fetch_json(url, referer="https://fundf10.eastmoney.com/")
        if payload.get("ErrCode") != 0:
            raise SourceError(f"净值接口错误: {payload.get('ErrMsg')}")
        page_rows = payload.get("Data", {}).get("LSJZList") or []
        if not page_rows:
            break
        raw_rows.extend(page_rows)
        if len(page_rows) < per_page:
            break
    deduplicated = {row["FSRQ"]: row for row in raw_rows if row.get("FSRQ")}
    rows = [deduplicated[date] for date in sorted(deduplicated, reverse=True)][:page_size]
    if not rows:
        raise SourceError("净值历史为空")
    result = {
        "rows": [
            {
                "date": row["FSRQ"],
                "unit_nav": float(row["DWJZ"]),
                "cumulative_nav": float(row["LJJZ"]),
                "return_pct": parse_number(row.get("JZZZL", "")),
            }
            for row in rows
        ],
        "source_url": source_url,
    }
    save_cache("nav_history", result)
    return result


def fetch_hsfund_nav(fund_code: str) -> dict[str, Any]:
    url = f"http://www.hsfund.com/osoa/views/fund/detail/{fund_code}.html?fund_code={fund_code}"
    text = fetch_text(url)
    date_match = re.search(r"净值（(\d{4}-\d{2}-\d{2})）", text)
    nav_match = re.search(r'id="base_unit_nv"[^>]*>([0-9.]+)<', text)
    if not date_match or not nav_match:
        raise SourceError("华商基金官网净值解析失败")
    result = {
        "date": date_match.group(1),
        "unit_nav": float(nav_match.group(1)),
        "source_url": url,
    }
    save_cache("hsfund_nav", result)
    return result


def secid(holding: dict[str, Any]) -> str:
    return f"{holding['market']}.{holding['code']}"


def tencent_symbol(code: str) -> str:
    return ("sh" if code.startswith(("5", "6", "9")) else "sz") + code


def fetch_quotes_eastmoney(holdings: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    ids = ",".join(secid(x) for x in holdings)
    fields = "f2,f3,f12,f13,f14,f15,f16,f17,f18,f124"
    url = (
        "https://push2.eastmoney.com/api/qt/ulist.np/get"
        f"?fltt=2&invt=2&fields={fields}&ut={EASTMONEY_UT}&secids={ids}"
    )
    payload = fetch_json(url, referer="https://quote.eastmoney.com/")
    rows = (payload.get("data") or {}).get("diff") or []
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = row.get("f12")
        if not code or row.get("f3") in (None, "-"):
            continue
        timestamp = row.get("f124")
        quote_time = None
        if isinstance(timestamp, (int, float)) and timestamp > 0:
            quote_time = dt.datetime.fromtimestamp(timestamp, TZ).isoformat(timespec="seconds")
        result[code] = {
            "code": code,
            "name": row.get("f14"),
            "price": float(row["f2"]),
            "return_pct": float(row["f3"]),
            "prev_close": float(row["f18"]),
            "high": float(row["f15"]),
            "low": float(row["f16"]),
            "open": float(row["f17"]),
            "quote_time": quote_time,
            "source": "eastmoney",
        }
    if len(result) < max(1, len(holdings) // 2):
        raise SourceError(f"东方财富行情数量异常: {len(result)}/{len(holdings)}")
    return result


def fetch_quotes_tencent(holdings: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    symbols = ",".join(tencent_symbol(x["code"]) for x in holdings)
    url = f"https://qt.gtimg.cn/q={symbols}"
    raw = fetch_bytes(url, referer="https://gu.qq.com/")
    text = raw.decode("gb18030", errors="replace")
    result: dict[str, dict[str, Any]] = {}
    for line in text.splitlines():
        if '="' not in line:
            continue
        body = line.split('="', 1)[1].rsplit('"', 1)[0]
        parts = body.split("~")
        if len(parts) < 35 or not parts[2] or not parts[32]:
            continue
        quote_time = None
        try:
            quote_time = dt.datetime.strptime(parts[30], "%Y%m%d%H%M%S").replace(tzinfo=TZ).isoformat(timespec="seconds")
        except ValueError:
            pass
        result[parts[2]] = {
            "code": parts[2],
            "name": parts[1],
            "price": float(parts[3]),
            "return_pct": float(parts[32]),
            "prev_close": float(parts[4]),
            "high": float(parts[33]),
            "low": float(parts[34]),
            "open": float(parts[5]),
            "quote_time": quote_time,
            "source": "tencent",
        }
    if len(result) < max(1, len(holdings) // 2):
        raise SourceError(f"腾讯行情数量异常: {len(result)}/{len(holdings)}")
    return result


def fetch_market_quote() -> dict[str, Any]:
    """沪深300实时快照；与腾讯历史日线组成大盘对比序列。"""
    quote = fetch_quotes_eastmoney([{"market": 1, "code": "000300"}]).get("000300")
    if not quote:
        raise SourceError("沪深300行情缺失")
    save_cache("market_quote", quote)
    return quote


def fetch_cpo_board(board_code: str) -> dict[str, Any]:
    fields = "f57,f58,f43,f44,f45,f46,f60,f169,f170"
    url = (
        "https://push2.eastmoney.com/api/qt/stock/get"
        f"?secid=90.{board_code}&fields={fields}"
    )
    payload = fetch_json(url, referer="https://quote.eastmoney.com/")
    row = payload.get("data") or {}
    if row.get("f170") is None:
        raise SourceError("CPO板块行情缺失")
    result = {
        "code": row.get("f57", board_code),
        "name": row.get("f58", "CPO概念"),
        "index": row.get("f43") / 100 if row.get("f43") is not None else None,
        "prev_close": row.get("f60") / 100 if row.get("f60") is not None else None,
        "return_pct": row.get("f170") / 100,
        "source_url": f"https://quote.eastmoney.com/bk/90.{board_code}.html",
    }
    save_cache("cpo_board", result)
    return result


def fetch_cpo_members(board_code: str) -> dict[str, Any]:
    fields = "f12,f13,f14,f2,f3,f18,f20,f21"
    query = urllib.parse.urlencode(
        {
            "pn": 1,
            "pz": 500,
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": f"b:{board_code}",
            "fields": fields,
        }
    )
    url = f"https://push2delay.eastmoney.com/api/qt/clist/get?{query}"
    payload = fetch_json(url, referer="https://quote.eastmoney.com/")
    data = payload.get("data") or {}
    rows = data.get("diff") or []
    if not rows:
        raise SourceError("CPO成分股为空")
    members = [
        {
            "code": row["f12"],
            "name": row.get("f14"),
            "return_pct": row.get("f3"),
            "market_cap": row.get("f20"),
            "float_market_cap": row.get("f21"),
        }
        for row in rows
        if row.get("f12")
    ]
    result = {"count": int(data.get("total") or len(members)), "members": members}
    save_cache("cpo_members", result)
    return result


def fetch_tencent_symbol_history(
    symbol: str, begin: str, end: str, limit: int = 200
) -> dict[str, float]:
    url = (
        "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        f"?param={symbol},day,{begin},{end},{limit},qfq"
    )
    payload = fetch_json(url, referer="https://gu.qq.com/")
    data = (payload.get("data") or {}).get(symbol) or {}
    rows = data.get("qfqday") or data.get("day") or []
    closes: list[tuple[str, float]] = []
    for row in rows:
        if len(row) >= 3:
            closes.append((row[0], float(row[2])))
    returns: dict[str, float] = {}
    for idx in range(1, len(closes)):
        date, close = closes[idx]
        previous = closes[idx - 1][1]
        if previous:
            returns[date] = (close / previous - 1) * 100
    return returns


def fetch_tencent_history(code: str, begin: str, end: str, limit: int = 200) -> dict[str, float]:
    return fetch_tencent_symbol_history(tencent_symbol(code), begin, end, limit)


def fetch_tencent_intraday(code: str) -> dict[str, Any]:
    """取单只A股最近交易日的完整分钟线。"""
    symbol = tencent_symbol(code)
    url = f"https://web.ifzq.gtimg.cn/appstock/app/minute/query?code={symbol}"
    payload = fetch_json(url, referer="https://gu.qq.com/")
    data = (payload.get("data") or {}).get(symbol) or {}
    quote = (data.get("qt") or {}).get(symbol) or []
    raw_points = (data.get("data") or {}).get("data") or []
    if len(quote) < 31 or not raw_points:
        raise SourceError(f"{code} 分钟行情为空")
    previous_close = float(quote[4])
    trade_date = dt.datetime.strptime(quote[30][:8], "%Y%m%d").date().isoformat()
    points: dict[str, float] = {}
    for raw in raw_points:
        parts = raw.split()
        if len(parts) < 2 or len(parts[0]) != 4:
            continue
        hhmm = f"{parts[0][:2]}:{parts[0][2:]}"
        points[hhmm] = float(parts[1])
    if not points:
        raise SourceError(f"{code} 分钟行情解析为空")
    return {
        "code": code,
        "trade_date": trade_date,
        "previous_close": previous_close,
        "points": points,
        "source_url": url,
    }


def fetch_board_intraday(board_code: str) -> dict[str, Any]:
    """取东财概念板块最近交易日的完整分钟线。"""
    suffix = (
        f"/api/qt/stock/trends2/get?secid=90.{board_code}&ndays=1&iscr=0"
        "&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58"
    )
    payload = None
    last_error: Exception | None = None
    for host in ("push2.eastmoney.com", "push2delay.eastmoney.com", "push2his.eastmoney.com"):
        try:
            payload = fetch_json(f"https://{host}{suffix}", referer="https://quote.eastmoney.com/")
            if payload.get("data"):
                break
        except Exception as exc:
            last_error = exc
    data = (payload or {}).get("data") or {}
    raw_points = data.get("trends") or []
    previous_close = data.get("preClose")
    if not raw_points or not previous_close:
        raise SourceError(f"{board_code} 分钟行情为空: {last_error}")
    points: dict[str, float] = {}
    trade_date = None
    for raw in raw_points:
        parts = raw.split(",")
        if len(parts) < 3:
            continue
        stamp = parts[0]
        trade_date = stamp[:10]
        points[stamp[11:16]] = float(parts[2])
    if not points or not trade_date:
        raise SourceError(f"{board_code} 分钟行情解析为空")
    return {
        "code": board_code,
        "trade_date": trade_date,
        "previous_close": float(previous_close),
        "points": points,
        "source_url": f"https://quote.eastmoney.com/bk/90.{board_code}.html",
    }


def in_a_share_session(hhmm: str) -> bool:
    return "09:30" <= hhmm <= "11:30" or "13:00" <= hhmm <= "15:00"


def build_intraday_series(
    holdings: list[dict[str, Any]],
    beta: float,
    use_calibrated: bool,
    stock_allocation_pct: float,
    board_code: str,
    warnings: list[str],
) -> dict[str, Any]:
    """用完整分钟线重建09:30—15:00曲线，而不是使用程序启动后的采样点。"""
    cached, saved_at = load_cache("intraday_series")
    if cached and saved_at:
        try:
            age_seconds = (now_cn() - dt.datetime.fromisoformat(saved_at)).total_seconds()
            cache_complete = bool(cached.get("complete"))
            if cache_complete or age_seconds < 55:
                return cached
        except (ValueError, TypeError):
            pass

    stock_series: dict[str, dict[str, Any]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        future_map = {
            pool.submit(fetch_tencent_intraday, item["code"]): item["code"]
            for item in holdings
        }
        for future in concurrent.futures.as_completed(future_map):
            code = future_map[future]
            try:
                stock_series[code] = future.result()
            except Exception as exc:
                warnings.append(f"{code} 分钟线缺失：{exc}")

    if len(stock_series) < max(5, len(holdings) - 2):
        if cached:
            warnings.append("当日分钟线不完整，继续使用上一份完整分钟曲线。")
            return cached
        return {
            "trade_date": None,
            "points": [],
            "point_count": 0,
            "complete": False,
            "source": "minute_reconstruction",
        }

    trade_dates = [item["trade_date"] for item in stock_series.values()]
    trade_date = max(set(trade_dates), key=trade_dates.count)
    stock_series = {
        code: item for code, item in stock_series.items() if item["trade_date"] == trade_date
    }
    try:
        board_series = fetch_board_intraday(board_code)
        if board_series["trade_date"] != trade_date:
            warnings.append(
                f"CPO分钟线日期 {board_series['trade_date']} 与持仓分钟线 {trade_date} 不一致。"
            )
            board_series = None
    except Exception as exc:
        board_series = None
        warnings.append(f"CPO分钟曲线暂不可用：{exc}")

    all_times = sorted(
        {
            hhmm
            for item in stock_series.values()
            for hhmm in item["points"]
            if in_a_share_session(hhmm)
        }
    )
    top_weight = sum(float(item["weight_pct"]) for item in holdings)
    holding_by_code = {item["code"]: item for item in holdings}
    last_prices: dict[str, float] = {}
    last_board_price: float | None = None
    points: list[dict[str, Any]] = []
    for hhmm in all_times:
        for code, item in stock_series.items():
            if hhmm in item["points"]:
                last_prices[code] = float(item["points"][hhmm])
        contributions: list[float] = []
        for code, price in last_prices.items():
            holding = holding_by_code.get(code)
            series = stock_series.get(code)
            if not holding or not series or not series["previous_close"]:
                continue
            stock_return = (price / float(series["previous_close"]) - 1) * 100
            contributions.append(float(holding["weight_pct"]) / 100 * stock_return)
        if len(contributions) < max(5, len(stock_series) - 2):
            continue
        top_contribution = sum(contributions)
        top_normalized = top_contribution / top_weight * 100 if top_weight else None
        simple_estimate = (
            top_normalized * stock_allocation_pct / 100 if top_normalized is not None else None
        )
        estimate = beta * top_contribution if use_calibrated else simple_estimate
        cpo_return = None
        if board_series:
            if hhmm in board_series["points"]:
                last_board_price = float(board_series["points"][hhmm])
            if last_board_price is not None:
                cpo_return = (last_board_price / board_series["previous_close"] - 1) * 100
        points.append(
            {
                "as_of": f"{trade_date}T{hhmm}:00+08:00",
                "time": hhmm,
                "estimate_pct": estimate,
                "simple_estimate_pct": simple_estimate,
                "cpo_board_pct": cpo_return,
                "top10_contribution_pct_point": top_contribution,
            }
        )

    complete = bool(points and points[-1]["time"] >= "15:00")
    result = {
        "trade_date": trade_date,
        "points": points,
        "point_count": len(points),
        "first_time": points[0]["time"] if points else None,
        "last_time": points[-1]["time"] if points else None,
        "complete": complete,
        "source": "腾讯个股分钟线 + 东方财富CPO分钟线",
    }
    save_cache("intraday_series", result)
    return result


def normalize_comparison_rows(
    rows: list[dict[str, Any]], limit: int = 30
) -> list[dict[str, Any]]:
    """把市场与披露组合日收益复合累计，并以窗口首日收盘统一归零。"""
    mapping = {
        "market_return_pct": "market_pct",
        "cpo_return_pct": "cpo_pct",
        "top10_return_pct": "top10_pct",
    }
    clean = [
        row
        for row in rows
        if row.get("date")
        and all(isinstance(row.get(key), (int, float)) for key in mapping)
    ][-limit:]
    levels = {key: 1.0 for key in mapping}
    points: list[dict[str, Any]] = []
    for idx, row in enumerate(clean):
        if idx:
            for key in mapping:
                levels[key] *= 1 + float(row[key]) / 100
        point: dict[str, Any] = {"date": row["date"]}
        for source_key, output_key in mapping.items():
            point[output_key] = (levels[source_key] - 1) * 100
        points.append(point)
    return points


def build_comparison_series(
    holdings: list[dict[str, Any]],
    cpo_members: list[dict[str, Any]],
    top_weight: float,
    report_date: str,
    data_date: str,
    nav_rows: list[dict[str, Any]],
    current_market_pct: float | None,
    current_cpo_pct: float | None,
    current_fund_pct: float | None,
    current_top10_pct: float | None,
    warnings: list[str],
) -> dict[str, Any]:
    """生成近30个自然日的正式净值、披露组合及市场对比曲线。"""
    member_codes = sorted({str(item["code"]) for item in cpo_members if item.get("code")})
    signature = ",".join(member_codes)
    cached, saved_at = load_cache("comparison_history")
    use_cache = False
    if cached and saved_at:
        try:
            age = now_cn() - dt.datetime.fromisoformat(saved_at)
            use_cache = (
                age.total_seconds() < 6 * 3600
                and cached.get("report_date") == report_date
                and cached.get("member_signature") == signature
            )
        except (ValueError, TypeError):
            use_cache = False

    if use_cache:
        history_payload = cached
    else:
        end = data_date
        begin = (dt.date.fromisoformat(data_date) - dt.timedelta(days=95)).isoformat()
        requested_codes = sorted({str(item["code"]) for item in holdings} | set(member_codes))
        histories: dict[str, dict[str, float]] = {}
        index_history: dict[str, float] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            future_map = {
                pool.submit(fetch_tencent_history, code, begin, end, 120): code
                for code in requested_codes
            }
            index_future = pool.submit(
                fetch_tencent_symbol_history, "sh000300", begin, end, 120
            )
            for future in concurrent.futures.as_completed(future_map):
                code = future_map[future]
                try:
                    values = future.result()
                    if values:
                        histories[code] = values
                except Exception:
                    continue
            try:
                index_history = index_future.result()
            except Exception as exc:
                warnings.append(f"沪深300历史日线暂不可用：{exc}")

        holding_codes = [str(item["code"]) for item in holdings]
        holding_by_code = {str(item["code"]): item for item in holdings}
        minimum_holdings = max(5, len(holding_codes) - 2)
        minimum_members = max(15, math.ceil(len(member_codes) * 0.6))
        daily_rows: list[dict[str, Any]] = []
        for date in sorted(index_history):
            # 当日实时涨幅由快照补入，历史接口只负责已完成交易日。
            if date >= data_date:
                continue
            available_holdings = [
                code for code in holding_codes if date in histories.get(code, {})
            ]
            cpo_returns = [
                float(histories[code][date])
                for code in member_codes
                if date in histories.get(code, {})
            ]
            if len(available_holdings) < minimum_holdings or len(cpo_returns) < minimum_members:
                continue
            top_contribution = sum(
                float(holding_by_code[code]["weight_pct"])
                / 100
                * float(histories[code][date])
                for code in available_holdings
            )
            daily_rows.append(
                {
                    "date": date,
                    "market_return_pct": float(index_history[date]),
                    "cpo_return_pct": statistics.mean(cpo_returns),
                    "top10_return_pct": (
                        top_contribution / top_weight * 100 if top_weight else None
                    ),
                }
            )
        history_payload = {
            "report_date": report_date,
            "member_signature": signature,
            "member_count": len(member_codes),
            "history_member_count": len(
                [code for code in member_codes if code in histories]
            ),
            "daily_rows": daily_rows,
        }
        save_cache("comparison_history", history_payload)

    daily_rows = list(history_payload.get("daily_rows") or [])
    current_values = (
        current_market_pct,
        current_cpo_pct,
        current_top10_pct,
    )
    if all(isinstance(value, (int, float)) for value in current_values):
        daily_rows = [row for row in daily_rows if row.get("date") != data_date]
        daily_rows.append(
            {
                "date": data_date,
                "market_return_pct": float(current_market_pct),
                "cpo_return_pct": float(current_cpo_pct),
                "top10_return_pct": float(current_top10_pct),
            }
        )
    window_start = (dt.date.fromisoformat(data_date) - dt.timedelta(days=30)).isoformat()
    window_rows = [row for row in daily_rows if window_start <= row.get("date", "") <= data_date]
    nav_by_date = {
        str(row["date"]): float(row["unit_nav"])
        for row in nav_rows
        if row.get("date") and isinstance(row.get("unit_nav"), (int, float))
    }
    baseline_date = next(
        (row["date"] for row in window_rows if row.get("date") in nav_by_date),
        window_rows[0]["date"] if window_rows else None,
    )
    if baseline_date:
        window_rows = [row for row in window_rows if row.get("date") >= baseline_date]
    points = normalize_comparison_rows(window_rows, max(1, len(window_rows)))
    baseline_nav = nav_by_date.get(baseline_date) if baseline_date else None
    for point in points:
        unit_nav = nav_by_date.get(point["date"])
        point["formal_unit_nav"] = unit_nav
        point["formal_fund_pct"] = (
            (unit_nav / baseline_nav - 1) * 100
            if unit_nav is not None and baseline_nav
            else None
        )
        point["today_estimate_pct"] = None
        point["estimated_unit_nav"] = None

    formal_dates = [point["date"] for point in points if point["formal_unit_nav"] is not None]
    formal_nav_through = formal_dates[-1] if formal_dates else None
    estimate_extension_date = None
    estimate_base_date = None
    estimate_daily_pct = None
    previous_trade_date = (
        window_rows[-2]["date"]
        if len(window_rows) >= 2 and window_rows[-1].get("date") == data_date
        else None
    )
    if (
        points
        and formal_nav_through
        and formal_nav_through == previous_trade_date
        and isinstance(current_fund_pct, (int, float))
        and points[-1]["date"] == data_date
        and baseline_nav
    ):
        last_nav = nav_by_date[formal_nav_through]
        estimated_nav = last_nav * (1 + float(current_fund_pct) / 100)
        points[-1]["today_estimate_pct"] = (estimated_nav / baseline_nav - 1) * 100
        points[-1]["estimated_unit_nav"] = estimated_nav
        estimate_extension_date = data_date
        estimate_base_date = formal_nav_through
        estimate_daily_pct = float(current_fund_pct)
    elif (
        points
        and formal_nav_through
        and data_date > formal_nav_through
        and isinstance(current_fund_pct, (int, float))
    ):
        warnings.append(
            f"{data_date} 估算未延伸到净值曲线：缺少上一交易日 {previous_trade_date or '--'} 的正式净值。"
        )

    if len(points) < 15:
        warnings.append(f"近30个自然日对比曲线有效交易日不足：{len(points)}个。")
    return {
        "window_days": 30,
        "window_start": window_start,
        "start_date": points[0]["date"] if points else None,
        "end_date": points[-1]["date"] if points else None,
        "point_count": len(points),
        "baseline": "最近30个自然日，首个交易日收盘=0%",
        "formal_nav_through": formal_nav_through,
        "estimate_extension_date": estimate_extension_date,
        "estimate_base_date": estimate_base_date,
        "estimate_daily_pct": estimate_daily_pct,
        "market": {"code": "000300", "name": "沪深300"},
        "cpo": {
            "code": "BK1128",
            "name": "CPO概念",
            "method": "当前成分股等权日收益回算；当日使用板块快照",
            "member_count": history_payload.get("member_count", len(member_codes)),
            "history_member_count": history_payload.get("history_member_count"),
        },
        "fund_method": "历史仅用正式单位净值；T日未公布时，以T-1日正式净值乘以T日重仓模型涨幅，只延伸一天",
        "top10_method": "当前披露前十持仓按内部权重归一化的模拟组合（非基金真实净值）",
        "points": points,
    }


def build_calibration(
    holdings: list[dict[str, Any]],
    nav_rows: list[dict[str, Any]],
    report_date: str,
    stock_allocation_pct: float,
    history_days: int,
    calibration_days: int,
) -> dict[str, Any]:
    cached, saved_at = load_cache("calibration")
    if cached and saved_at:
        try:
            age = now_cn() - dt.datetime.fromisoformat(saved_at)
            if (
                age.total_seconds() < 6 * 3600
                and cached.get("report_date") == report_date
                and cached.get("calibration_days") == calibration_days
            ):
                return cached
        except (ValueError, TypeError):
            pass

    end_date = max(row["date"] for row in nav_rows)
    begin_dt = dt.date.fromisoformat(end_date) - dt.timedelta(days=history_days + 10)
    begin = begin_dt.isoformat()
    histories: dict[str, dict[str, float]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        future_map = {
            pool.submit(fetch_tencent_history, item["code"], begin, end_date): item["code"]
            for item in holdings
        }
        for future in concurrent.futures.as_completed(future_map):
            code = future_map[future]
            try:
                histories[code] = future.result()
            except Exception:
                histories[code] = {}

    actual = {
        row["date"]: row["return_pct"]
        for row in nav_rows
        if row.get("return_pct") is not None and row["date"] > report_date
    }
    candidate_dates = sorted(actual)
    points: list[dict[str, float | str]] = []
    for date in candidate_dates:
        available = [item for item in holdings if date in histories.get(item["code"], {})]
        if len(available) < max(5, len(holdings) - 2):
            continue
        x = sum(item["weight_pct"] / 100 * histories[item["code"]][date] for item in available)
        points.append({"date": date, "x": x, "y": float(actual[date])})
    points = points[-calibration_days:]
    top_weight = sum(item["weight_pct"] for item in holdings)
    prior_beta = stock_allocation_pct / top_weight if top_weight else 1.0
    denom = sum(float(p["x"]) ** 2 for p in points)
    raw_beta = sum(float(p["x"]) * float(p["y"]) for p in points) / denom if denom else prior_beta
    raw_beta = min(3.5, max(0.3, raw_beta))
    shrink = len(points) / (len(points) + 15)
    beta = prior_beta * (1 - shrink) + raw_beta * shrink
    residuals = [float(p["y"]) - beta * float(p["x"]) for p in points]
    mae = statistics.mean(abs(x) for x in residuals) if residuals else None
    rmse = math.sqrt(statistics.mean(x * x for x in residuals)) if residuals else None
    mean_y = statistics.mean(float(p["y"]) for p in points) if points else 0.0
    ss_tot = sum((float(p["y"]) - mean_y) ** 2 for p in points)
    ss_res = sum(x * x for x in residuals)
    r2 = 1 - ss_res / ss_tot if ss_tot else None
    result = {
        "report_date": report_date,
        "calibration_days": calibration_days,
        "sample_count": len(points),
        "prior_beta": prior_beta,
        "raw_beta": raw_beta,
        "beta": beta,
        "mae_pct_point": mae,
        "rmse_pct_point": rmse,
        "r2": r2,
        "window_start": points[0]["date"] if points else None,
        "window_end": points[-1]["date"] if points else None,
    }
    save_cache("calibration", result)
    return result


def market_status(moment: dt.datetime) -> str:
    if moment.weekday() >= 5:
        return "休市"
    hhmm = moment.hour * 60 + moment.minute
    if 9 * 60 + 30 <= hhmm <= 11 * 60 + 30 or 13 * 60 <= hhmm <= 15 * 60:
        return "交易中"
    if hhmm < 9 * 60 + 30:
        return "盘前"
    return "已收盘"


def fallback(name: str, loader: Callable[[], Any], warnings: list[str]) -> Any:
    try:
        return loader()
    except Exception as exc:
        cached, saved_at = load_cache(name)
        if cached is None:
            raise
        warnings.append(f"{name} 实时抓取失败，使用 {saved_at} 的缓存：{exc}")
        return cached


def safe_cpo_board(board_code: str, warnings: list[str]) -> dict[str, Any]:
    """CPO指数是旁路指标，失败时不能阻断基金本身的估值刷新。"""
    try:
        return fetch_cpo_board(board_code)
    except Exception as exc:
        cached, saved_at = load_cache("cpo_board")
        if cached is not None:
            warnings.append(f"CPO指数快照接口暂时断连，使用 {saved_at} 的缓存。")
            return cached
        warnings.append("CPO指数快照暂不可用，但基金估值继续更新。")
        return {
            "code": board_code,
            "name": "CPO概念",
            "index": None,
            "prev_close": None,
            "return_pct": None,
            "source_url": f"https://quote.eastmoney.com/bk/90.{board_code}.html",
        }


def read_recent_history(max_rows: int = 240) -> list[dict[str, Any]]:
    if not HISTORY_PATH.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with HISTORY_PATH.open("r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except OSError:
        return []
    return rows[-max_rows:]


def append_history(snapshot: dict[str, Any]) -> None:
    row = {
        "as_of": snapshot["as_of"],
        "estimate_pct": snapshot["estimate"]["central_pct"],
        "simple_estimate_pct": snapshot["estimate"]["simple_scaled_pct"],
        "cpo_board_pct": snapshot["cpo"]["board"]["return_pct"],
        "top10_contribution_pct_point": snapshot["estimate"]["top10_contribution_pct_point"],
    }
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with HISTORY_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def refresh(config: dict[str, Any]) -> dict[str, Any]:
    warnings: list[str] = []
    fund_code = config["fund_code"]
    board_code = config["cpo_board_code"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futures = {
            "profile": pool.submit(lambda: fallback("profile", lambda: fetch_profile(fund_code), warnings)),
            "holdings": pool.submit(lambda: fallback("holdings", lambda: fetch_holdings(fund_code), warnings)),
            "nav": pool.submit(lambda: fallback("nav_history", lambda: fetch_nav_history(fund_code), warnings)),
            "official": pool.submit(lambda: fallback("hsfund_nav", lambda: fetch_hsfund_nav(fund_code), warnings)),
            "cpo_board": pool.submit(safe_cpo_board, board_code, warnings),
        }
        results = {name: future.result() for name, future in futures.items()}

    profile = results["profile"]
    holdings_payload = results["holdings"]
    nav_payload = results["nav"]
    official_nav = results["official"]
    holdings = holdings_payload["holdings"]

    if set(profile.get("holding_codes", [])) != {x["code"] for x in holdings}:
        warnings.append("持仓页面与基金行情页的前十代码不完全一致，请留意平台更新时差。")

    primary_quotes: dict[str, dict[str, Any]] = {}
    backup_quotes: dict[str, dict[str, Any]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        quote_future = pool.submit(fetch_quotes_eastmoney, holdings)
        backup_future = pool.submit(fetch_quotes_tencent, holdings)
        cpo_future = pool.submit(lambda: fallback("cpo_members", lambda: fetch_cpo_members(board_code), warnings))
        market_future = pool.submit(
            lambda: fallback("market_quote", fetch_market_quote, warnings)
        )
        try:
            primary_quotes = quote_future.result()
        except Exception as exc:
            warnings.append(f"东方财富行情失败，改用腾讯行情：{exc}")
        try:
            backup_quotes = backup_future.result()
        except Exception as exc:
            warnings.append(f"腾讯校验行情失败：{exc}")
        cpo_members = cpo_future.result()
        market_quote = market_future.result()

    member_returns = [
        float(x["return_pct"])
        for x in cpo_members["members"]
        if x.get("return_pct") is not None
    ]
    if member_returns:
        member_average = statistics.mean(member_returns)
        results["cpo_board"]["member_average_pct"] = member_average
        if results["cpo_board"].get("return_pct") is None or results["cpo_board"].get("derived_from_components"):
            results["cpo_board"]["return_pct"] = statistics.mean(member_returns)
            results["cpo_board"]["derived_from_components"] = True
            warnings.append("CPO指数改用BK1128成分股涨跌幅等权平均复算。")
            save_cache("cpo_board", results["cpo_board"])
        elif abs(float(results["cpo_board"]["return_pct"]) - member_average) > 0.05:
            warnings.append(
                f"CPO指数与成分等权复算相差 {abs(float(results['cpo_board']['return_pct']) - member_average):.2f} 个百分点。"
            )

    quotes = primary_quotes or backup_quotes
    if not quotes:
        cached_quotes, saved_at = load_cache("quotes")
        if not cached_quotes:
            raise SourceError("两路实时行情均不可用，且没有缓存")
        quotes = cached_quotes
        warnings.append(f"两路行情均失败，使用 {saved_at} 的缓存")
    else:
        save_cache("quotes", quotes)

    discrepancies: list[float] = []
    for code, quote in quotes.items():
        other = backup_quotes.get(code) if quote.get("source") == "eastmoney" else primary_quotes.get(code)
        if other:
            discrepancies.append(abs(quote["return_pct"] - other["return_pct"]))
    max_discrepancy = max(discrepancies) if discrepancies else None
    if max_discrepancy is not None and max_discrepancy > 0.15:
        warnings.append(f"两路行情最大差异 {max_discrepancy:.2f} 个百分点，可能存在更新时间差。")

    cpo_codes = {x["code"] for x in cpo_members["members"]}
    enriched: list[dict[str, Any]] = []
    for item in holdings:
        quote = quotes.get(item["code"])
        if not quote:
            warnings.append(f"缺少 {item['name']}({item['code']}) 行情")
            continue
        contribution = item["weight_pct"] * quote["return_pct"] / 100
        enriched.append(
            {
                **item,
                "price": quote["price"],
                "return_pct": quote["return_pct"],
                "contribution_pct_point": contribution,
                "quote_time": quote.get("quote_time"),
                "quote_source": quote.get("source"),
                "is_cpo": item["code"] in cpo_codes,
            }
        )

    top_weight = sum(x["weight_pct"] for x in enriched)
    top_contribution = sum(x["contribution_pct_point"] for x in enriched)
    top_normalized = top_contribution / top_weight * 100 if top_weight else None
    stock_allocation = float(profile["stock_allocation_pct"])
    simple_scaled = top_normalized * stock_allocation / 100 if top_normalized is not None else None

    calibration = build_calibration(
        holdings,
        nav_payload["rows"],
        holdings_payload["report_date"],
        stock_allocation,
        int(config.get("history_days", 75)),
        int(config.get("calibration_days", 30)),
    )
    calibrated = calibration["beta"] * top_contribution
    use_calibrated = calibration["sample_count"] >= 10
    central = calibrated if use_calibrated else simple_scaled
    mae = calibration.get("mae_pct_point")
    half_range = max(0.35, (mae or 0.35) * 1.5)

    cpo_holdings = [x for x in enriched if x["is_cpo"]]
    cpo_weight = sum(x["weight_pct"] for x in cpo_holdings)
    cpo_contribution = sum(x["contribution_pct_point"] for x in cpo_holdings)
    cpo_normalized = cpo_contribution / cpo_weight * 100 if cpo_weight else None

    em_nav = nav_payload["rows"][0]
    nav_diff = None
    if official_nav["date"] == em_nav["date"]:
        nav_diff = abs(official_nav["unit_nav"] - em_nav["unit_nav"])
        if nav_diff > 0.0005:
            warnings.append(
                f"昨日净值两源不一致：华商 {official_nav['unit_nav']:.4f} / 东财 {em_nav['unit_nav']:.4f}"
            )
    else:
        warnings.append(
            f"净值更新日期不同：华商 {official_nav['date']} / 东财 {em_nav['date']}"
        )

    disclosure_age = (now_cn().date() - dt.date.fromisoformat(holdings_payload["report_date"])).days
    if disclosure_age > 75:
        warnings.append(f"持仓披露距今 {disclosure_age} 天，换仓可能导致估值偏差。")

    quote_times = [x["quote_time"] for x in enriched if x.get("quote_time")]
    data_time = max(quote_times) if quote_times else iso_now()
    intraday = build_intraday_series(
        holdings,
        float(calibration["beta"]),
        use_calibrated,
        stock_allocation,
        board_code,
        warnings,
    )
    if intraday.get("points"):
        last_intraday = intraday["points"][-1]
        intraday["estimate_close_difference_pct_point"] = abs(
            float(last_intraday["estimate_pct"]) - float(central)
        )
        if intraday["estimate_close_difference_pct_point"] > 0.05:
            warnings.append(
                f"分钟线末值与实时估值相差 {intraday['estimate_close_difference_pct_point']:.2f} 个百分点。"
            )
        board_close = last_intraday.get("cpo_board_pct")
        board_snapshot = results["cpo_board"].get("return_pct")
        if board_close is not None and board_snapshot is not None:
            intraday["cpo_close_difference_pct_point"] = abs(
                float(board_close) - float(board_snapshot)
            )
    comparison = build_comparison_series(
        holdings,
        cpo_members["members"],
        top_weight,
        holdings_payload["report_date"],
        data_time[:10],
        nav_payload["rows"],
        market_quote.get("return_pct"),
        results["cpo_board"].get("return_pct"),
        central,
        top_normalized,
        warnings,
    )
    source_posture = "研究级估算"
    if disclosure_age > 100 or len(enriched) < len(holdings) or not backup_quotes:
        source_posture = "初步估算"

    snapshot: dict[str, Any] = {
        "kind": "fund_monitor.v1",
        "as_of": data_time,
        "generated_at": iso_now(),
        "market_status": market_status(now_cn()),
        "fund": {
            "code": fund_code,
            "name": profile.get("name") or config.get("fund_name"),
            "previous_nav": em_nav,
            "official_nav_crosscheck": official_nav,
            "nav_crosscheck_difference": nav_diff,
            "stock_allocation_pct": stock_allocation,
            "allocation_date": profile["allocation_date"],
            "holding_report_date": holdings_payload["report_date"],
            "holding_disclosure_age_days": disclosure_age,
            "top10_weight_pct": top_weight,
        },
        "estimate": {
            "central_pct": central,
            "low_pct": central - half_range,
            "high_pct": central + half_range,
            "method": "滚动校准" if use_calibrated else "持仓覆盖率外推",
            "top10_contribution_pct_point": top_contribution,
            "top10_normalized_pct": top_normalized,
            "simple_scaled_pct": simple_scaled,
            "calibrated_pct": calibrated,
            "range_half_width_pct_point": half_range,
            "calibration": calibration,
        },
        "cpo": {
            "board": results["cpo_board"],
            "member_count": cpo_members["count"],
            "fund_holding_count": len(cpo_holdings),
            "fund_weight_pct": cpo_weight,
            "fund_contribution_pct_point": cpo_contribution,
            "fund_normalized_pct": cpo_normalized,
            "holding_codes": [x["code"] for x in cpo_holdings],
        },
        "holdings": enriched,
        "intraday": {key: value for key, value in intraday.items() if key != "points"},
        "comparison_30d": {
            key: value for key, value in comparison.items() if key != "points"
        },
        "quality": {
            "posture": source_posture,
            "quote_primary": "eastmoney" if primary_quotes else "tencent",
            "quote_crosscheck_count": len(discrepancies),
            "max_quote_difference_pct_point": max_discrepancy,
            "warnings": warnings,
            "disclaimer": "盘中结果根据最近披露持仓估算，不是基金公司正式净值，也不构成投资建议。",
        },
        "sources": [
            {
                "id": "S1",
                "name": "华商基金官网",
                "use": "正式单位净值交叉核验",
                "url": official_nav["source_url"],
                "as_of": official_nav["date"],
            },
            {
                "id": "S2",
                "name": "天天基金/东方财富基金档案",
                "use": "基金净值、资产配置、最新披露前十持仓",
                "url": holdings_payload["source_url"],
                "as_of": holdings_payload["report_date"],
            },
            {
                "id": "S3",
                "name": "东方财富行情",
                "use": "前十持仓实时行情、沪深300、CPO指数及成分",
                "url": results["cpo_board"]["source_url"],
                "as_of": data_time,
            },
            {
                "id": "S4",
                "name": "腾讯行情",
                "use": "实时行情交叉核验、历史校准及近30日回算",
                "url": "https://gu.qq.com/",
                "as_of": data_time,
            },
        ],
    }
    append_history(snapshot)
    snapshot["intraday_history"] = intraday.get("points", [])
    snapshot["comparison_30d_points"] = comparison.get("points", [])
    atomic_json(LATEST_PATH, snapshot)
    return snapshot


def fmt(value: float | None, digits: int = 2) -> str:
    return "--" if value is None else f"{value:+.{digits}f}%"


def refresh_loop(config: dict[str, Any], stop: threading.Event) -> None:
    interval = max(10, int(config.get("refresh_seconds", 30)))
    while not stop.is_set():
        started = time.time()
        try:
            snapshot = refresh(config)
            print(
                f"[{snapshot['generated_at']}] 估值 {fmt(snapshot['estimate']['central_pct'])} "
                f"CPO {fmt(snapshot['cpo']['board']['return_pct'])}",
                flush=True,
            )
        except Exception as exc:
            print(f"[{iso_now()}] 刷新失败: {exc}", file=sys.stderr, flush=True)
        elapsed = time.time() - started
        stop.wait(max(1, interval - elapsed))


def serve(config: dict[str, Any], port: int, open_browser: bool) -> None:
    stop = threading.Event()
    worker = threading.Thread(target=refresh_loop, args=(config, stop), daemon=True)
    worker.start()
    handler = lambda *args, **kwargs: SimpleHTTPRequestHandler(*args, directory=str(ROOT), **kwargs)
    server = ThreadingHTTPServer(("127.0.0.1", port), handler)
    url = f"http://127.0.0.1:{port}/dashboard.html"
    print(f"监控页: {url}")
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


def doctor(config: dict[str, Any]) -> int:
    tests: list[tuple[str, Callable[[], Any]]] = [
        ("基金持仓", lambda: fetch_holdings(config["fund_code"])),
        ("基金净值", lambda: fetch_nav_history(config["fund_code"], 3)),
        ("华商官网", lambda: fetch_hsfund_nav(config["fund_code"])),
        ("基金配置", lambda: fetch_profile(config["fund_code"])),
        ("CPO指数（含缓存回退）", lambda: safe_cpo_board(config["cpo_board_code"], [])),
        ("CPO成分", lambda: fetch_cpo_members(config["cpo_board_code"])),
    ]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"✅ {name}")
        except Exception as exc:
            failed += 1
            print(f"❌ {name}: {exc}")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Fund 单基金兼容监控器")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("refresh", help="刷新一次并生成 data/latest.json")
    serve_parser = sub.add_parser("serve", help="持续刷新并启动本地监控页")
    serve_parser.add_argument("--port", type=int)
    serve_parser.add_argument("--open", action="store_true", help="自动打开浏览器")
    sub.add_parser("doctor", help="检查所有数据源")
    args = parser.parse_args()
    config = read_config()
    if args.command == "refresh":
        snapshot = refresh(config)
        print(
            json.dumps(
                {
                    "as_of": snapshot["as_of"],
                    "estimate_pct": snapshot["estimate"]["central_pct"],
                    "range": [snapshot["estimate"]["low_pct"], snapshot["estimate"]["high_pct"]],
                    "cpo_board_pct": snapshot["cpo"]["board"]["return_pct"],
                    "cpo_fund_weight_pct": snapshot["cpo"]["fund_weight_pct"],
                    "cpo_fund_contribution_pct_point": snapshot["cpo"]["fund_contribution_pct_point"],
                    "warnings": snapshot["quality"]["warnings"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if args.command == "serve":
        serve(config, args.port or int(config.get("server_port", 8765)), args.open)
        return 0
    return doctor(config)


if __name__ == "__main__":
    raise SystemExit(main())
