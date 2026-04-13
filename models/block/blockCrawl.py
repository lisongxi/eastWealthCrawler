"""股票板块相关 / Stock Sector Related
使用异步 aiohttp 进行 HTTP 请求
"""

import asyncio
import json
import re
from typing import List

import aiohttp

from settings.settings import load_settings

# 共享 aiohttp Session（应在应用生命周期内复用）
_session: aiohttp.ClientSession | None = None


async def get_http_session() -> aiohttp.ClientSession:
    """获取或创建全局 aiohttp Session"""
    global _session
    if _session is None or _session.closed:
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
        _session = aiohttp.ClientSession(connector=connector)
    return _session


async def close_http_session():
    """关闭全局 aiohttp Session"""
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None


async def get_block_list_db(limit=None):
    """获取板块列表（用于数据库存储）- 异步版本
    Args:
        limit: 获取板块数量限制，None表示不限制
    Returns:
        板块列表，每个元素包含 code 和 name
    """
    url = "https://push2.eastmoney.com/api/qt/clist/get"

    params = {
        "np": 1,
        "fltt": 1,
        "invt": 2,
        "fs": "m:90+t:2+f:!50",
        "fields": "f12,f13,f14,f1,f2,f4,f3,f152,f20,f8,f104,f105,f128,f140,f141,f207,f208,f209,f136,f222",
        "fid": "f3",
        "pn": 1,
        "pz": limit if limit else 500,
        "po": 1,
        "dect": 1,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "wbp2u": "|0|0|0|web",
    }

    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://quote.eastmoney.com/center/gridlist.html",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    }

    try:
        session = await get_http_session()
        async with session.get(
            url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            data = await response.json()

            if data.get("data") and data["data"].get("diff"):
                blocks = []
                for item in data["data"]["diff"]:
                    block_info = {
                        "code": item.get("f12", ""),
                        "name": item.get("f14", ""),
                    }
                    blocks.append(block_info)

                print(f"成功获取 {len(blocks)} 个板块信息")
                return blocks
            else:
                print("未获取到板块数据")
                return []

    except Exception as e:
        print(f"获取板块列表失败: {e}")
        return []


async def get_block_kline_db(
    block_code: str, block_name: str, start_date: str = ""
) -> List:
    """获取板块价格K线数据（用于数据库存储）- 异步版本
    使用JSONP格式请求
    Args:
        block_code: 板块代码
        block_name: 板块名称
        start_date: 开始日期（可选，默认从配置读取）
    Returns:
        价格K线数据列表
    """
    import time

    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

    if not start_date:
        settings = load_settings()
        start_date = settings.sync.start_date

    start_date_str = start_date.replace("-", "")

    cb = f"jQuery{int(time.time() * 1000)}"

    params = {
        "cb": cb,
        "secid": f"90.{block_code}",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "beg": start_date_str,
        "end": "20500101",
        "lmt": "1000000",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
    }

    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Referer": f"https://quote.eastmoney.com/bk/90.{block_code}.html",
        "Sec-Fetch-Dest": "script",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
    }

    cookies = {
        "qgqp_b_id": "05ae85b295694ce66917993d6f23e369",
        "st_nvi": "qd1JRLiNe_E2OF2IDNadqdda0",
        "nid18": "091896bda237c70325347f0441ca596c",
        "nid18_create_time": "1768113730542",
        "gviem": "hRqVDs4tspISE7xEoGjQFb1a9",
        "gviem_create_time": "1768113730542",
        "st_si": "70276712164160",
        "websitepoptg_api_time": "1775372773146",
        "fullscreengg": "1",
        "fullscreengg2": "1",
        "st_pvi": "89090147348376",
        "st_sp": "2026-01-11 14:42:10",
        "st_inirUrl": "https://cn.bing.com/",
        "st_sn": "10",
        "st_psi": "20260405152406815-113200301353-1796905289",
        "st_asi": "20260405152406815-113200301353-1796905289-hqzx.hsjAghqdy.dtt.lcKx-1",
    }

    try:
        session = await get_http_session()
        async with session.get(
            url,
            params=params,
            headers=headers,
            cookies=cookies,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:
            text = await response.text()

            match = re.search(r"\((.*)\)", text, re.S)
            if match:
                data = json.loads(match.group(1))
            else:
                data = await response.json()

            if data.get("data") and data["data"].get("klines"):
                klines = data["data"]["klines"]
                print(
                    f"  成功获取板块 {block_name} ({block_code}) 的价格K线数据: {len(klines)} 条"
                )
                return klines
            else:
                print(f"  板块 {block_name} ({block_code}) 没有价格K线数据")
                return []

    except Exception as e:
        print(f"  获取板块 {block_name} ({block_code}) 的价格K线数据失败: {e}")
        return []


async def get_block_capital_flow_db(block_code, block_name):
    """获取板块资金流数据（用于数据库存储）- 异步版本
    Args:
        block_code: 板块代码
        block_name: 板块名称
    Returns:
        资金流数据列表
    """
    url = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"

    params = {
        "secid": f"90.{block_code}",
        "fields1": "f1,f2,f3,f4,f5,f6,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63",
        "klt": "101",
        "lmt": "1000000",
        "beg": "19900101",
        "end": "20500101",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
    }

    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Referer": f"https://quote.eastmoney.com/bk/90.{block_code}.html",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            session = await get_http_session()
            async with session.get(
                url,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:
                data = await response.json()

                if data.get("data") and data["data"].get("klines"):
                    klines = data["data"]["klines"]
                    print(
                        f"  成功获取板块 {block_name} ({block_code}) 的资金流数据: {len(klines)} 条"
                    )
                    return klines
                else:
                    print(f"  板块 {block_name} ({block_code}) 没有资金流数据")
                    return []

        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
                continue
            print(f"  获取板块 {block_name} ({block_code}) 的资金流数据失败: {e}")
            return []


def parse_block_price_kline(kline_str, block_code, block_name):
    """解析板块价格K线数据 / Parse sector price K-line data
    Args:
        kline_str: K线数据字符串
        block_code: 板块代码
        block_name: 板块名称
    Returns:
        解析后的数据字典
    """
    try:
        # K线数据格式：日期,开盘,收盘,最高,最低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率
        parts = kline_str.split(",")

        if len(parts) >= 11:
            return {
                "b_date": parts[0],
                "open_price": float(parts[1]) if parts[1] else 0,
                "close_price": float(parts[2]) if parts[2] else 0,
                "top_price": float(parts[3]) if parts[3] else 0,
                "low_price": float(parts[4]) if parts[4] else 0,
                "turnover": float(parts[5]) if parts[5] else 0,
                "transaction": float(parts[6]) if parts[6] else 0,
                "amplitude": float(parts[7]) if parts[7] else 0,
                "quote_change": float(parts[8]) if parts[8] else 0,
                "change_amount": float(parts[9]) if parts[9] else 0,
                "turnover_rate": float(parts[10]) if parts[10] else 0,
                "block_code": block_code,
                "block_name": block_name,
            }
    except Exception as e:
        print(f"解析K线数据失败: {e}, 数据: {kline_str}")

    return None


def parse_block_capital_flow(kline_str, block_code, block_name):
    """解析板块资金流数据 / Parse sector capital flow data
    Args:
        kline_str: 资金流数据字符串
        block_code: 板块代码
        block_name: 板块名称
    Returns:
        解析后的数据字典
    """
    try:
        # 资金流数据格式（根据实际API返回）：
        # 日期,超大单净流入,大单净流入,中单净流入,小单净流入,?,涨跌幅,?,?,?,?,收盘价,?
        parts = kline_str.split(",")

        if len(parts) >= 12:
            return {
                "b_date": parts[0],
                "d_closing_price": (
                    float(parts[11]) if len(parts) > 11 and parts[11] else 0
                ),
                "d_quote_change": float(parts[6]) if len(parts) > 6 and parts[6] else 0,
                "main_net_inflow": (
                    float(parts[1]) if len(parts) > 1 and parts[1] else 0
                ),
                "super_large_net_inflow": 0,  # API不单独提供
                "large_net_inflow": (
                    float(parts[2]) if len(parts) > 2 and parts[2] else 0
                ),
                "mid_net_inflow": float(parts[3]) if len(parts) > 3 and parts[3] else 0,
                "small_net_inflow": (
                    float(parts[4]) if len(parts) > 4 and parts[4] else 0
                ),
                "main_net_proportion": 0,
                "large_net_proportion": 0,
                "mid_net_proportion": 0,
                "small_net_proportion": 0,
                "super_large_net_proportion": 0,
                "block_code": block_code,
                "block_name": block_name,
            }
    except Exception as e:
        print(f"解析资金流数据失败: {e}, 数据: {kline_str}")

    return None
