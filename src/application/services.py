"""
应用程序服务和用例
支持并发爬取 + 令牌桶限流
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from settings.settings import Settings
from src.container.container import get_container
from src.domain.entities import (
    CrawlerConfiguration,
    CrawlerEvent,
    CrawlerMetrics,
    CrawlerStatus,
    CrawlResult,
    DataSourceType,
    SyncType,
)
from src.events.event_bus import EventBus
from src.pipeline.data_pipeline import DataPipeline

# 导入令牌桶限流器
try:
    from src.common.rate_limiter import init_rate_limiter, rate_limiter

    RATE_LIMITER_AVAILABLE = True
except ImportError:
    RATE_LIMITER_AVAILABLE = False


class CrawlerService(ABC):
    """爬虫服务抽象基类"""

    def __init__(self, event_bus: EventBus, data_pipeline: DataPipeline):
        self.event_bus = event_bus  # 事件总线
        self.data_pipeline = data_pipeline  # 数据管道
        self.logger = logging.getLogger(__name__)  # 日志记录器
        self.metrics = CrawlerMetrics(start_time=datetime.now())  # 性能指标

    @abstractmethod
    async def crawl(self, config: CrawlerConfiguration) -> List[CrawlResult]:
        """执行爬虫过程"""
        pass

    async def _rate_limit_wait(self):
        """令牌桶限流等待"""
        if not RATE_LIMITER_AVAILABLE:
            return

        from settings.settings import load_settings

        settings: Settings = load_settings()

        # 检查是否启用令牌桶限流
        if hasattr(settings.sync, "rate_limit") and settings.sync.rate_limit.enabled:
            await rate_limiter.acquire()

    def _publish_event(
        self,
        event_type: str,
        status: CrawlerStatus,
        message: Optional[str] = None,
        data: Optional[Dict] = None,
    ):
        """发布爬虫事件"""
        event = CrawlerEvent(
            event_type=event_type,
            crawler_id=self.__class__.__name__,
            status=status,
            message=message,
            data=data,
        )
        self.event_bus.publish(event)

    def _update_metrics(self, **kwargs):
        """更新爬虫指标"""
        for key, value in kwargs.items():
            if hasattr(self.metrics, key):
                setattr(self.metrics, key, value)

    def _finalize_metrics(self):
        """完成指标并发布"""
        self.metrics.end_time = datetime.now()
        self.metrics.processing_time = self.metrics.duration()
        self._publish_event(
            "metrics_completed", CrawlerStatus.COMPLETED, data=self.metrics.model_dump()
        )

    def _delete_stock_data(self, stock_code: str, source_type: DataSourceType):
        """删除股票数据（全量模式重新爬取前调用）"""
        from database import mysql1
        from s_stock.stockDM import StockKline

        try:
            if mysql1.is_closed():
                mysql1.connect()

            with mysql1:
                StockKline.delete().where(StockKline.stock_code == stock_code).execute()
                self.logger.info(f"删除股票 {stock_code} K线数据")
        except Exception as e:
            self.logger.warning(f"删除股票数据失败: {e}")


class BlockCrawlerService(CrawlerService):
    """板块数据爬虫服务"""

    async def crawl(self, config: CrawlerConfiguration) -> List[CrawlResult]:
        """爬取板块数据 - 并发版本"""
        self._publish_event("crawl_started", CrawlerStatus.RUNNING)

        try:
            # 获取板块列表
            block_list = await self._get_block_list()
            self.logger.info(f"开始并发爬取 {len(block_list)} 个板块")

            # 为每个板块创建并发任务
            async def crawl_with_limit(block):
                # 检查是否需要爬取（仅全量模式需要检查任务状态）
                need_crawl = True
                if config.sync_type == SyncType.FULL:
                    from database import is_task_completed

                    task_code = block["code"]
                    if config.source_type == DataSourceType.BLOCK_CAPITAL_FLOW:
                        task_type = "block_capital_flow"
                    elif config.source_type == DataSourceType.BLOCK_KLINE:
                        task_type = "block_kline"
                    else:
                        task_type = "unknown"

                    if is_task_completed(task_type, task_code, "full"):
                        self.logger.info(f"跳过已完成的任务: {task_type} - {task_code}")
                        need_crawl = False

                # 只有需要爬取才执行限流和爬取
                if need_crawl:
                    # 令牌桶限流
                    await self._rate_limit_wait()
                    result = await self._crawl_block_data(block, config)
                    if result:
                        processed_result = await self.data_pipeline.process(result)
                        self._update_metrics(
                            total_data_points=len(processed_result.data_points)
                        )
                        return processed_result
                return None

            # 使用 asyncio.gather 并发执行所有任务
            tasks = [crawl_with_limit(block) for block in block_list]
            results: list = await asyncio.gather(*tasks, return_exceptions=True)

            # 过滤掉None和异常
            processed_results = [
                r for r in results if r is not None and not isinstance(r, Exception)
            ]

            for r in processed_results:
                if isinstance(r, Exception):
                    self.logger.error(f"爬取失败: {r}")

            self.logger.info(f"板块爬取完成，成功 {len(processed_results)} 个")
            self._publish_event("crawl_completed", CrawlerStatus.COMPLETED)
            self._finalize_metrics()
            return processed_results

        except Exception as e:
            self._publish_event("crawl_failed", CrawlerStatus.FAILED, str(e))
            self.metrics.end_time = datetime.now()
            raise

    async def _get_block_list(self) -> List[Dict]:
        """获取要爬取的板块列表（先从数据库，没有则从API获取）"""
        from database import get_block_list_from_db

        blocks = get_block_list_from_db()

        # 如果数据库有数据就直接使用
        if blocks:
            self.logger.info(f"从数据库获取 {len(blocks)} 个板块")
            return blocks

        # 否则从API获取（备用方案）
        self.logger.info("从API获取板块列表...")
        import requests

        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "np": 1,
            "fltt": 1,
            "invt": 2,
            "fs": "m:90+t:2+f:!50",
            "fields": "f12,f14",
            "fid": "f3",
            "pn": 1,
            "pz": 500,
            "po": 1,
            "dect": 1,
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        }

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://quote.eastmoney.com/",
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            data = response.json()

            if data.get("data") and data["data"].get("diff"):
                blocks = []
                for item in data["data"]["diff"]:
                    blocks.append(
                        {"code": item.get("f12", ""), "name": item.get("f14", "")}
                    )
                self.logger.info(f"成功获取 {len(blocks)} 个板块")
                return blocks
        except Exception as e:
            self.logger.error(f"获取板块列表失败: {e}")

        return []

    def _get_latest_block_date(self, block_code: str) -> Optional[str]:
        """从数据库获取某板块的最新日期"""
        from database import mysql1
        from s_block.blockDM import BlockKline

        try:
            if mysql1.is_closed():
                mysql1.connect()

            query = (
                BlockKline.select(BlockKline.b_date)
                .where(BlockKline.block_code == block_code)
                .order_by(BlockKline.b_date.desc())
                .limit(1)
            )
            result = list(query)
            if result:
                return result[0].b_date.strftime("%Y-%m-%d")
        except Exception as e:
            self.logger.warning(f"获取板块 {block_code} 最新日期失败: {e}")
        return None

    def _delete_block_data(self, block_code: str, source_type: DataSourceType):
        """删除板块数据（全量模式重新爬取前调用）"""
        from database import mysql1
        from s_block.blockDM import BlockCapitalFlow, BlockKline

        try:
            if mysql1.is_closed():
                mysql1.connect()

            with mysql1:
                if source_type == DataSourceType.BLOCK_CAPITAL_FLOW:
                    BlockCapitalFlow.delete().where(
                        BlockCapitalFlow.block_code == block_code
                    ).execute()
                    self.logger.info(f"删除板块 {block_code} 资金流数据")
                elif source_type == DataSourceType.BLOCK_KLINE:
                    BlockKline.delete().where(
                        BlockKline.block_code == block_code
                    ).execute()
                    self.logger.info(f"删除板块 {block_code} K线数据")
        except Exception as e:
            self.logger.warning(f"删除板块数据失败: {e}")

    def _delete_stock_data(self, stock_code: str, source_type: DataSourceType):
        """删除股票数据（全量模式重新爬取前调用）"""
        from database import mysql1
        from s_stock.stockDM import StockKline

        try:
            if mysql1.is_closed():
                mysql1.connect()

            with mysql1:
                StockKline.delete().where(StockKline.stock_code == stock_code).execute()
                self.logger.info(f"删除股票 {stock_code} K线数据")
        except Exception as e:
            self.logger.warning(f"删除股票数据失败: {e}")

    async def _crawl_block_data(
        self, block: Dict, config: CrawlerConfiguration
    ) -> Optional[CrawlResult]:
        """为单个板块爬取数据"""
        from datetime import datetime, timedelta

        from src.domain.entities import (
            BlockIdentifier,
            CapitalFlowData,
            CrawlDataPoint,
            FinancialData,
        )

        identifier = BlockIdentifier(code=block["code"], name=block["name"])

        # 根据同步类型决定开始日期
        from database import is_task_completed, mark_task_completed
        from settings.settings import load_settings

        settings = load_settings()
        task_code = block["code"]

        # 全量模式：检查任务是否已完成
        if config.sync_type == SyncType.FULL:
            # 确定任务类型
            if config.source_type == DataSourceType.BLOCK_CAPITAL_FLOW:
                task_type = "block_capital_flow"
            elif config.source_type == DataSourceType.BLOCK_KLINE:
                task_type = "block_kline"
            else:
                task_type = "unknown"

            # 检查是否已完成（full模式）
            if is_task_completed(task_type, task_code, "full"):
                self.logger.info(f"跳过已完成的任务: {task_type} - {task_code}")
                return None

            # 删除该板块的已有数据（重新爬取）
            self._delete_block_data(task_code, config.source_type)

        if config.sync_type == SyncType.FULL:
            # 全量模式：从配置文件的 start_date 开始
            start_date = settings.sync.start_date
        else:
            # 增量模式：从数据库中获取最新日期 + 1 天
            start_date = self._get_latest_block_date(block["code"])
            if start_date:
                latest = datetime.strptime(start_date, "%Y-%m-%d")
                start_date = (latest + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = settings.sync.start_date

        # 根据配置类型获取不同的数据
        data_points = []

        if config.source_type == DataSourceType.BLOCK_KLINE:
            # 获取板块K线数据
            from s_block.blockCrawl import get_block_kline_db, parse_block_price_kline

            klines = get_block_kline_db(block["code"], block["name"], start_date)

            for kline_str in klines:
                parsed = parse_block_price_kline(
                    kline_str, block["code"], block["name"]
                )
                if parsed:
                    date_str = parsed["b_date"]
                    try:
                        date = datetime.strptime(date_str, "%Y-%m-%d")
                    except:
                        date = datetime.now()

                    financial_data = FinancialData(
                        open=parsed["open_price"],
                        close=parsed["close_price"],
                        high=parsed["top_price"],
                        low=parsed["low_price"],
                        volume=parsed["turnover"],
                        amount=parsed["transaction"],
                        change=parsed.get("change_amount"),
                        change_pct=parsed.get("quote_change"),
                    )
                    data_points.append(
                        CrawlDataPoint(
                            timestamp=date,
                            financial_data=financial_data,
                            raw_data=parsed,
                        )
                    )

        elif config.source_type == DataSourceType.BLOCK_CAPITAL_FLOW:
            # 获取板块资金流数据
            from s_block.blockCrawl import (
                get_block_capital_flow_db,
                parse_block_capital_flow,
            )

            # 获取资金流数据
            capital_klines = get_block_capital_flow_db(block["code"], block["name"])
            if not capital_klines:
                self.logger.info(
                    f"No capital flow data found for block: {block['name']}"
                )
                return None

            for kline_str in capital_klines:
                parsed = parse_block_capital_flow(
                    kline_str, block["code"], block["name"]
                )
                if parsed:
                    date_str = parsed["b_date"]
                    try:
                        date = datetime.strptime(date_str, "%Y-%m-%d")
                    except:
                        date = datetime.now()

                    # 使用API返回的收盘价和涨跌幅
                    closing_price = parsed.get("d_closing_price", 0)
                    quote_change = parsed.get("d_quote_change", 0)

                    # 资金流数据需要转换为FinancialData格式
                    financial_data = FinancialData(
                        open=closing_price,
                        close=closing_price,
                        high=closing_price,
                        low=closing_price,
                        volume=0,
                        amount=0,
                        change=None,
                        change_pct=quote_change,
                    )
                    data_points.append(
                        CrawlDataPoint(
                            timestamp=date,
                            financial_data=financial_data,
                            capital_flow=CapitalFlowData(
                                main_inflow=parsed.get("main_net_inflow", 0),
                                main_outflow=0,
                                main_net=parsed.get("main_net_inflow", 0),
                                large_inflow=(
                                    parsed.get("large_net_inflow", 0)
                                    if parsed.get("large_net_inflow", 0) > 0
                                    else 0
                                ),
                                large_outflow=(
                                    -parsed.get("large_net_inflow", 0)
                                    if parsed.get("large_net_inflow", 0) < 0
                                    else 0
                                ),
                                large_net=parsed.get("large_net_inflow", 0),
                                medium_inflow=(
                                    parsed.get("mid_net_inflow", 0)
                                    if parsed.get("mid_net_inflow", 0) > 0
                                    else 0
                                ),
                                medium_outflow=(
                                    -parsed.get("mid_net_inflow", 0)
                                    if parsed.get("mid_net_inflow", 0) < 0
                                    else 0
                                ),
                                medium_net=parsed.get("mid_net_inflow", 0),
                                small_inflow=(
                                    parsed.get("small_net_inflow", 0)
                                    if parsed.get("small_net_inflow", 0) > 0
                                    else 0
                                ),
                                small_outflow=(
                                    -parsed.get("small_net_inflow", 0)
                                    if parsed.get("small_net_inflow", 0) < 0
                                    else 0
                                ),
                                small_net=parsed.get("small_net_inflow", 0),
                            ),
                            raw_data=parsed,
                        )
                    )

        if not data_points:
            return None

        # 全量模式：标记任务完成
        if config.sync_type == SyncType.FULL:
            if config.source_type == DataSourceType.BLOCK_CAPITAL_FLOW:
                task_type = "block_capital_flow"
            else:
                task_type = "block_kline"
            mark_task_completed(task_type, block["code"], "full")

        return CrawlResult(
            source_type=config.source_type,
            identifier=identifier,
            data_points=data_points,
            sync_type=config.sync_type,
        )


class StockCrawlerService(CrawlerService):
    """股票数据爬虫服务"""

    async def crawl(self, config: CrawlerConfiguration) -> List[CrawlResult]:
        """爬取股票数据 - 并发版本"""
        self._publish_event("crawl_started", CrawlerStatus.RUNNING)

        try:
            # 获取股票列表
            stock_list = await self._get_stock_list()
            self.logger.info(f"开始并发爬取 {len(stock_list)} 只股票")

            # 为每只股票创建并发任务
            async def crawl_with_limit(stock):
                # 检查是否需要爬取（仅全量模式需要检查任务状态）
                need_crawl = True
                if config.sync_type == SyncType.FULL:
                    from database import is_task_completed

                    task_code = stock["code"]
                    task_type = "stock_kline"

                    if is_task_completed(task_type, task_code, "full"):
                        self.logger.info(f"跳过已完成的任务: {task_type} - {task_code}")
                        need_crawl = False

                # 只有需要爬取才执行限流和爬取
                if need_crawl:
                    # 令牌桶限流
                    await self._rate_limit_wait()
                    result = await self._crawl_stock_data(stock, config)
                    if result:
                        processed_result = await self.data_pipeline.process(result)
                        self._update_metrics(
                            total_data_points=len(processed_result.data_points)
                        )
                        return processed_result
                return None

            # 使用 asyncio.gather 并发执行所有任务
            tasks = [crawl_with_limit(stock) for stock in stock_list]
            results: list = await asyncio.gather(*tasks, return_exceptions=True)

            # 过滤掉None和异常
            processed_results = [
                r for r in results if r is not None and not isinstance(r, Exception)
            ]

            for r in processed_results:
                if isinstance(r, Exception):
                    self.logger.error(f"爬取失败: {r}")

            self.logger.info(f"股票爬取完成，成功 {len(processed_results)} 只")
            self._publish_event("crawl_completed", CrawlerStatus.COMPLETED)
            self._finalize_metrics()
            return processed_results

        except Exception as e:
            self._publish_event("crawl_failed", CrawlerStatus.FAILED, str(e))
            self.metrics.end_time = datetime.now()
            raise

    async def _get_stock_list(self) -> List[Dict]:
        """获取要爬取的股票列表（先从数据库，没有则从API获取）"""
        from database import get_stock_list_from_db

        stocks = get_stock_list_from_db()

        # 如果数据库有数据就直接使用
        if stocks:
            self.logger.info(f"从数据库获取 {len(stocks)} 只股票")
            return stocks

        # 否则从API获取（备用方案）
        self.logger.info("从API获取股票列表...")
        import requests

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
            response = requests.get(url, params=params, headers=headers, timeout=10)
            data = response.json()

            if data.get("data") and data["data"].get("diff"):
                stocks = []
                for item in data["data"]["diff"]:
                    stocks.append(
                        {"code": item.get("f12", ""), "name": item.get("f14", "")}
                    )
                self.logger.info(f"成功获取 {len(stocks)} 只股票")
                return stocks
        except Exception as e:
            self.logger.error(f"获取股票列表失败: {e}")

        return []

    async def _crawl_stock_data(
        self, stock: Dict, config: CrawlerConfiguration
    ) -> Optional[CrawlResult]:
        """为单只股票爬取真实K线数据"""
        import json
        import re

        import requests

        from src.domain.entities import CrawlDataPoint, FinancialData, StockIdentifier

        stock_code = stock["code"]
        stock_name = stock["name"]

        # 生成secid
        if stock_code[:3] in ("000", "399"):
            secid = f"0.{stock_code}"
        elif stock_code[0] == "6":
            secid = f"1.{stock_code}"
        else:
            secid = f"0.{stock_code}"

        identifier = StockIdentifier(code=stock_code, name=stock_name, secid=secid)

        # 根据同步类型决定开始日期
        from datetime import datetime, timedelta

        from database import is_task_completed, mark_task_completed
        from settings.settings import load_settings

        settings = load_settings()
        task_code = stock_code

        # 全量模式：检查任务是否已完成
        if config.sync_type == SyncType.FULL:
            # 股票K线任务类型
            task_type = "stock_kline"

            # 检查是否已完成（full模式）
            if is_task_completed(task_type, task_code, "full"):
                self.logger.info(f"跳过已完成的任务: {task_type} - {task_code}")
                return None

            # 删除该股票的已有数据（重新爬取）
            self._delete_stock_data(task_code, config.source_type)

        if config.sync_type == SyncType.FULL:
            # 全量模式：从配置文件的 start_date 开始
            start_date = settings.sync.start_date
        else:
            # 增量模式：从数据库中获取最新日期 + 1 天
            start_date = self._get_latest_date(stock_code)
            if start_date:
                # 从最新日期的下一开始
                latest = datetime.strptime(start_date, "%Y-%m-%d")
                start_date = (latest + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                # 如果数据库没有数据，则从配置的开始日期
                start_date = settings.sync.start_date

        # 将日期格式转换为YYYYMMDD格式
        start_date_str = start_date.replace("-", "")

        # 获取K线数据
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",  # 日K
            "fqt": "1",  # 前复权
            "beg": start_date_str,  # 从配置文件中读取的开始日期
            "end": "20500101",
            "lmt": "1000000",  # 不限制数量
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": f"https://quote.eastmoney.com/{stock['code']}.html",
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            content = response.text

            # 处理JSONP回调
            if "jQuery" in content or "(" in content:
                match = re.search(r"jQuery.*?\((.*?)\);", content)
                if match:
                    content = match.group(1)

            data = json.loads(content)

            if not data.get("data") or not data["data"].get("klines"):
                self.logger.warning(f"股票 {stock['name']} 没有K线数据")
                return None

            klines = data["data"]["klines"]
            data_points = []

            for kline_str in klines:  # 获取所有历史数据，从开始日期到现在
                parts = kline_str.split(",")
                if len(parts) >= 11:
                    try:
                        date = datetime.strptime(parts[0], "%Y-%m-%d")
                    except:
                        continue

                    financial_data = FinancialData(
                        open=float(parts[1]) if parts[1] else 0,
                        close=float(parts[2]) if parts[2] else 0,
                        high=float(parts[3]) if parts[3] else 0,
                        low=float(parts[4]) if parts[4] else 0,
                        volume=float(parts[5]) if parts[5] else 0,
                        amount=float(parts[6]) if parts[6] else 0,
                        change=float(parts[9]) if len(parts) > 9 and parts[9] else None,
                        change_pct=(
                            float(parts[8]) if len(parts) > 8 and parts[8] else None
                        ),
                    )
                    data_points.append(
                        CrawlDataPoint(
                            timestamp=date,
                            financial_data=financial_data,
                            raw_data={
                                "amplitude": (
                                    float(parts[7])
                                    if len(parts) > 7 and parts[7]
                                    else 0
                                ),
                                "turnover_rate": (
                                    float(parts[10])
                                    if len(parts) > 10 and parts[10]
                                    else 0
                                ),
                            },
                        )
                    )

            self.logger.info(
                f"股票 {stock['name']} 获取到 {len(data_points)} 条K线数据"
            )

            if not data_points:
                return None

            # 全量模式：标记任务完成
            if config.sync_type == SyncType.FULL:
                task_type = "stock_kline"
                mark_task_completed(task_type, stock_code, "full")

            return CrawlResult(
                source_type=config.source_type,
                identifier=identifier,
                data_points=data_points,
                sync_type=config.sync_type,
            )

        except Exception as e:
            self.logger.error(f"爬取股票 {stock['name']} 数据失败: {e}")
            return None

    def _get_latest_date(self, stock_code: str) -> Optional[str]:
        """从数据库获取某只股票的最新日期"""
        from database import mysql1
        from s_stock.stockDM import StockKline

        try:
            if mysql1.is_closed():
                mysql1.connect()

            # 查询该股票的最新日期
            query = (
                StockKline.select(StockKline.s_date)
                .where(StockKline.stock_code == stock_code)
                .order_by(StockKline.s_date.desc())
                .limit(1)
            )
            result = list(query)
            if result:
                return result[0].s_date.strftime("%Y-%m-%d")
        except Exception as e:
            self.logger.warning(f"获取股票 {stock_code} 最新日期失败: {e}")
        return None


class CrawlerOrchestrator:
    """协调多个爬虫服务"""

    def __init__(self, event_bus: EventBus, data_pipeline: DataPipeline):
        self.event_bus = event_bus  # 事件总线
        self.data_pipeline = data_pipeline  # 数据管道
        self.container = get_container()  # 依赖注入容器
        self.logger = logging.getLogger(__name__)  # 日志记录器

    async def run_all_crawlers(self):
        """运行所有配置的爬虫"""
        self.logger.info("开始爬虫编排")

        # 获取所有爬虫配置
        configs = self._get_crawler_configurations()

        # 运行每个爬虫
        all_results = []
        for config in configs:
            try:
                crawler_service = self._get_crawler_service(config.source_type)
                results = await crawler_service.crawl(config)
                all_results.extend(results)
            except Exception as e:
                self.logger.error(f"爬虫 {config.source_type} 失败: {e}")

        self.logger.info("爬虫编排完成")
        return all_results

    def _get_crawler_configurations(self) -> List[CrawlerConfiguration]:
        """获取所有爬虫的配置"""
        # 从配置文件加载同步模式
        from settings.settings import load_settings

        settings = load_settings()
        sync_mode = settings.sync.mode.lower()

        # 根据配置确定同步类型
        if sync_mode == "full":
            sync_type = SyncType.FULL
        else:
            sync_type = SyncType.INCREMENTAL

        return [
            CrawlerConfiguration(
                source_type=DataSourceType.BLOCK_CAPITAL_FLOW, sync_type=sync_type
            ),
            CrawlerConfiguration(
                source_type=DataSourceType.BLOCK_KLINE, sync_type=sync_type
            ),
            CrawlerConfiguration(
                source_type=DataSourceType.STOCK_KLINE, sync_type=sync_type
            ),
        ]

    def _get_crawler_service(self, source_type: DataSourceType) -> CrawlerService:
        """根据源类型获取相应的爬虫服务"""
        if source_type in (
            DataSourceType.BLOCK_CAPITAL_FLOW,
            DataSourceType.BLOCK_KLINE,
        ):
            return self.container.resolve(BlockCrawlerService)
        elif source_type == DataSourceType.STOCK_KLINE:
            return self.container.resolve(StockCrawlerService)
        else:
            raise ValueError(f"未知源类型: {source_type}")


class CrawlerFactory:
    """创建爬虫服务的工厂类"""

    @staticmethod
    def create_crawler_service(
        service_type: str, event_bus: EventBus, data_pipeline: DataPipeline
    ) -> CrawlerService:
        """根据类型创建爬虫服务"""
        if service_type == "block":
            return BlockCrawlerService(event_bus, data_pipeline)
        elif service_type == "stock":
            return StockCrawlerService(event_bus, data_pipeline)
        else:
            raise ValueError(f"未知爬虫服务类型: {service_type}")
