"""基础信息爬虫模块 / Info Crawler Module
用于更新板块和个股的基本信息
"""

import time
from datetime import datetime
from typing import List

import requests

from database import mysql1
from models.block.blockInfo import BlockInfo
from models.stock.stockInfo import StockInfo
from settings.settings import load_settings


def update_block_info() -> bool:
    """更新板块基本信息表"""
    settings = load_settings()
    request_config = settings.sync.request

    url = "https://push2.eastmoney.com/api/qt/clist/get"
    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://quote.eastmoney.com/center/gridlist.html",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    }

    try:
        if mysql1.is_closed():
            mysql1.connect()

        all_blocks = []
        page = 1

        # 翻页获取所有板块
        while True:
            params = {
                "np": 1,
                "fltt": 1,
                "invt": 2,
                "fs": "m:90+t:2+f:!50",  # 概念板块
                "fields": "f12,f13,f14,f1,f2,f4,f3,f152,f20,f8,f104,f105,f128,f140,f141,f207,f208,f209,f136,f222",
                "fid": "f3",
                "pn": page,
                "pz": 100,  # 每页100条
                "po": 1,
                "dect": 1,
                "ut": "fa5fd1943c7b386f172d6893dbfba10b",
                "wbp2u": "|0|0|0|web",
            }

            # 使用配置的重试机制
            for attempt in range(request_config.max_retries):
                try:
                    response = requests.get(
                        url,
                        params=params,
                        headers=headers,
                        timeout=request_config.timeout,
                    )
                    data = response.json()
                    break
                except Exception as e:
                    if attempt < request_config.max_retries - 1:
                        time.sleep(request_config.retry_delay)
                        continue
                    raise

            if not data.get("data") or not data["data"].get("diff"):
                break

            all_blocks.extend(data["data"]["diff"])

            # 检查是否还有更多页（每页100条）
            if len(data["data"]["diff"]) < 100:
                break
            page += 1
            time.sleep(request_config.page_delay)

        if not all_blocks:
            print("获取板块列表失败")
            return False

        today = datetime.now().date()
        existing_codes = set()

        # 获取现有板块代码
        with mysql1:
            for block in BlockInfo.select(BlockInfo.block_code).where(
                BlockInfo.status == 1
            ):
                existing_codes.add(block.block_code)

        new_codes = set()
        blocks_to_insert = []

        with mysql1:
            for item in all_blocks:
                code = item.get("f12", "")
                name = item.get("f14", "")

                if not code:
                    continue

                # 板块分类
                block_type = "概念"
                if item.get("f13") == 2:
                    block_type = "行业"
                elif item.get("f13") == 3:
                    block_type = "地域"

                # 热度排名
                hot_rank = item.get("f152") or item.get("f3")

                # 当前价格和涨跌幅
                current_price = item.get("f2", "")
                change_pct = item.get("f4", "")

                new_codes.add(code)

                # 新增或更新
                if code not in existing_codes:
                    blocks_to_insert.append(
                        BlockInfo(
                            block_code=code,
                            block_name=name,
                            block_type=block_type,
                            hot_rank=hot_rank,
                            current_price=current_price,
                            change_pct=change_pct,
                            status=1,
                            created_at=today,
                            updated_at=today,
                        )
                    )
                else:
                    # 更新
                    BlockInfo.update(
                        block_name=name,
                        block_type=block_type,
                        hot_rank=hot_rank,
                        current_price=current_price,
                        change_pct=change_pct,
                        updated_at=today,
                    ).where(BlockInfo.block_code == code).execute()

            # 批量插入新板块
            if blocks_to_insert:
                BlockInfo.bulk_create(blocks_to_insert)

            # 将不存在的状态设为0
            if new_codes:
                BlockInfo.update(status=0, updated_at=today).where(
                    BlockInfo.status == 1, BlockInfo.block_code.not_in(new_codes)
                ).execute()

        print(
            f"板块信息更新完成: 新增 {len(blocks_to_insert)} 个, 共 {len(new_codes)} 个"
        )
        return True

    except Exception as e:
        print(f"更新板块信息失败: {e}")
        import traceback

        traceback.print_exc()
        return False


def update_stock_info() -> bool:
    """更新个股基本信息表 - 支持翻页获取完整数据"""
    settings = load_settings()
    request_config = settings.sync.request

    url = "https://push2.eastmoney.com/api/qt/clist/get"
    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://quote.eastmoney.com/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    }

    try:
        if mysql1.is_closed():
            mysql1.connect()

        all_stocks = []
        page = 1
        max_pages = 20  # 最多20页，每页500条 = 10000条

        # 翻页获取所有股票
        while page <= max_pages:
            params = {
                "pn": page,
                "pz": 100,  # 每页100条
                "po": 1,
                "np": 1,
                "fltt": 2,
                "invt": 2,
                "ut": "fa5fd1943c7b386f172d6893dbfba10b",
                "wbp2u": "|0|0|0|web",
                "fid": "f20",
                "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81,m:1+t:80 s:2048",
                "fields": "f12,f14,f13,f4",
            }

            # 使用配置的重试机制
            for attempt in range(request_config.max_retries):
                try:
                    response = requests.get(
                        url,
                        params=params,
                        headers=headers,
                        timeout=request_config.timeout,
                    )
                    data = response.json()
                    break
                except Exception as e:
                    if attempt < request_config.max_retries - 1:
                        time.sleep(request_config.retry_delay)
                        continue
                    raise

            if not data.get("data") or not data["data"].get("diff"):
                break

            page_stocks = data["data"]["diff"]
            all_stocks.extend(page_stocks)

            # 检查是否还有更多页
            if len(page_stocks) < 100:
                break
            page += 1
            time.sleep(request_config.page_delay * 3)
            print(f"已获取 {len(all_stocks)} 条股票数据, 正在处理第 {page} 页...")

        if not all_stocks:
            print("获取股票列表为空")
            return False

        today = datetime.now().date()
        existing_codes = set()

        # 获取现有股票代码
        with mysql1:
            for stock in StockInfo.select(StockInfo.stock_code).where(
                StockInfo.status == 1
            ):
                existing_codes.add(stock.stock_code)

        new_codes = set()
        stocks_to_insert = []

        with mysql1:
            for item in all_stocks:
                code = item.get("f12", "")
                name = item.get("f14", "")

                if not code:
                    continue

                # 确定市场
                if code.startswith("6"):
                    market = "沪市"
                else:
                    market = "深市"

                # 股票类型
                stock_type = "主板"
                if item.get("f13") == "b":
                    stock_type = "创业板"
                elif item.get("f13") == "ge":
                    stock_type = "科创板"

                # 当前涨跌幅
                change_pct = item.get("f4", "")

                new_codes.add(code)

                # 新增或更新
                if code not in existing_codes:
                    stocks_to_insert.append(
                        StockInfo(
                            stock_code=code,
                            stock_name=name,
                            market=market,
                            stock_type=stock_type,
                            change_pct=change_pct,
                            status=1,
                            created_at=today,
                            updated_at=today,
                        )
                    )
                else:
                    # 更新
                    StockInfo.update(
                        stock_name=name,
                        market=market,
                        stock_type=stock_type,
                        change_pct=change_pct,
                        updated_at=today,
                    ).where(StockInfo.stock_code == code).execute()

            # 批量插入新股票
            if stocks_to_insert:
                StockInfo.bulk_create(stocks_to_insert)

            # 将不存在的状态设为0
            if new_codes:
                StockInfo.update(status=0, updated_at=today).where(
                    StockInfo.status == 1, StockInfo.stock_code.not_in(new_codes)
                ).execute()

        print(
            f"股票信息更新完成: 新增 {len(stocks_to_insert)} 个, 共 {len(new_codes)} 个"
        )
        return True

    except Exception as e:
        print(f"更新股票信息失败: {e}")
        import traceback

        traceback.print_exc()
        return False


def get_block_list_from_db() -> List[dict]:
    """从数据库获取板块列表（用于爬取）"""
    try:
        if mysql1.is_closed():
            mysql1.connect()

        blocks = []
        with mysql1:
            for block in BlockInfo.select().where(BlockInfo.status == 1):
                blocks.append(
                    {
                        "code": block.block_code,
                        "name": block.block_name,
                    }
                )
        return blocks
    except Exception as e:
        print(f"获取板块列表失败: {e}")
        return []


def get_stock_list_from_db() -> List[dict]:
    """从数据库获取股票列表（用于爬取）"""
    try:
        if mysql1.is_closed():
            mysql1.connect()

        stocks = []
        with mysql1:
            for stock in StockInfo.select().where(StockInfo.status == 1):
                stocks.append(
                    {
                        "code": stock.stock_code,
                        "name": stock.stock_name,
                    }
                )
        return stocks
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return []
