"""个股爬虫模块 / Stock Crawler Module
个股相关的 HTTP 请求和数据解析
"""

import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import aiohttp

from src.crawlers.base import get_http_session


async def get_stock_list_api() -> List[Dict]:
    """从API获取股票列表 - 异步版本
    Returns:
        股票列表，每个元素包含 code 和 name
    """
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": 500,
        "po": 1,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "wbp2u": "|0|0|0|web",
        "fid": "f20",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81 s:2048",
        "fields": "f12,f14",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://quote.eastmoney.com/",
    }

    try:
        session = await get_http_session()
        async with session.get(
            url,
            params=params,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:
            data = await response.json()

            if data.get("data") and data["data"].get("diff"):
                stocks = []
                for item in data["data"]["diff"]:
                    stocks.append(
                        {
                            "code": item.get("f12", ""),
                            "name": item.get("f14", ""),
                        }
                    )
                return stocks
    except Exception as e:
        print(f"获取股票列表失败: {e}")

    return []


def generate_secid(stock_code: str) -> str:
    """生成股票 secid
    Args:
        stock_code: 股票代码
    Returns:
        secid 字符串
    """
    if stock_code[:3] in ("000", "399"):
        return f"0.{stock_code}"
    elif stock_code[0] == "6":
        return f"1.{stock_code}"
    else:
        return f"0.{stock_code}"


async def get_stock_kline(
    stock_code: str, stock_name: str, start_date: str
) -> List[str]:
    """获取股票K线数据 - 异步版本
    Args:
        stock_code: 股票代码
        stock_name: 股票名称
        start_date: 开始日期 (YYYY-MM-DD 格式)
    Returns:
        K线数据字符串列表
    """
    secid = generate_secid(stock_code)
    start_date_str = start_date.replace("-", "")

    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "beg": start_date_str,
        "end": "20500101",
        "lmt": "1000000",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": f"https://quote.eastmoney.com/{stock_code}.html",
    }

    try:
        session = await get_http_session()
        async with session.get(
            url,
            params=params,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:
            content = await response.text()

            # 处理JSONP回调
            if "jQuery" in content or "(" in content:
                match = re.search(r"jQuery.*?\((.*?)\);", content)
                if match:
                    content = match.group(1)

            data = json.loads(content)

            if not data.get("data") or not data.get("data", {}).get("klines"):
                return []

            return data["data"]["klines"]

    except Exception as e:
        print(f"获取股票 {stock_name} ({stock_code}) K线数据失败: {e}")
        return []


def parse_stock_kline(
    kline_str: str,
) -> Optional[Dict]:
    """解析股票K线数据字符串
    Args:
        kline_str: K线数据字符串，格式：日期,开盘,收盘,最高,最低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率
    Returns:
        解析后的数据字典，包含 timestamp, financial_data, raw_data
    """
    parts = kline_str.split(",")
    if len(parts) < 11:
        return None

    try:
        date = datetime.strptime(parts[0], "%Y-%m-%d")
    except:
        return None

    return {
        "date": date,
        "open": float(parts[1]) if parts[1] else 0,
        "close": float(parts[2]) if parts[2] else 0,
        "high": float(parts[3]) if parts[3] else 0,
        "low": float(parts[4]) if parts[4] else 0,
        "volume": float(parts[5]) if parts[5] else 0,
        "amount": float(parts[6]) if parts[6] else 0,
        "change": float(parts[9]) if parts[9] else None,
        "change_pct": float(parts[8]) if parts[8] else None,
        "raw_data": {
            "amplitude": float(parts[7]) if parts[7] else 0,
            "turnover_rate": float(parts[10]) if parts[10] else 0,
        },
    }
