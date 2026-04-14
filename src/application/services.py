"""
应用程序服务和用例
支持并发爬取 + 令牌桶限流 + 统一错误处理
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from functools import wraps
from typing import Dict, List, Optional

from settings.settings import Settings
from src.container.container import get_container
from src.domain.entities import (CrawlerConfiguration, CrawlerEvent,
                                 CrawlerMetrics, CrawlerStatus, CrawlResult,
                                 DataSourceType, SyncType)
from src.events.event_bus import EventBus, EventType
from src.pipeline.data_pipeline import DataPipeline

# 导入令牌桶限流器
try:
    from src.infrastructure.rate_limiter import get_rate_limiter

    RATE_LIMITER_AVAILABLE = True
except ImportError:
    RATE_LIMITER_AVAILABLE = False

# 导入统一错误处理
try:
    from src.infrastructure.error_handling import (ErrorCategory, ErrorFactory,
                                           ErrorSeverity)

    ERROR_HANDLING_AVAILABLE = True
except ImportError:
    ERROR_HANDLING_AVAILABLE = False


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

    def _handle_error(self, error: Exception, context: str):
        """统一错误处理"""
        if ERROR_HANDLING_AVAILABLE:
            app_error = ErrorFactory.create_from_exception(
                error,
                context={"service": self.__class__.__name__, "context": context},
                severity=ErrorSeverity.ERROR,
            )
            self.logger.error(f"[{context}] {error}")
            return app_error
        else:
            self.logger.error(f"[{context}] {error}")
            return None

    async def _rate_limit_wait(self):
        """令牌桶限流等待"""
        if not RATE_LIMITER_AVAILABLE:
            return

        from settings.settings import load_settings

        settings: Settings = load_settings()

        # 检查是否启用令牌桶限流
        if hasattr(settings.sync, "rate_limit") and settings.sync.rate_limit.enabled:
            rate_limiter = get_rate_limiter()
            await rate_limiter.acquire()

    # 事件类型字符串到枚举的映射
    _EVENT_TYPE_MAP = {
        "crawl_started": EventType.CRAWLER_STARTED,
        "crawl_completed": EventType.CRAWLER_COMPLETED,
        "crawl_failed": EventType.CRAWLER_FAILED,
        "metrics_completed": EventType.CRAWLER_COMPLETED,
    }

    def _publish_event(
        self,
        event_type: str,
        status: CrawlerStatus,
        message: Optional[str] = None,
        data: Optional[Dict] = None,
    ):
        """发布爬虫事件"""
        # 映射字符串到枚举值，保持向后兼容
        event_type_enum = self._EVENT_TYPE_MAP.get(
            event_type, EventType.CRAWLER_COMPLETED
        )
        event = CrawlerEvent(
            event_type=event_type_enum.value,
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
        from models.stock.stockDM import StockKline

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
        from src.crawlers.info_crawler import get_block_list_from_db

        blocks = get_block_list_from_db()

        # 如果数据库有数据就直接使用
        if blocks:
            self.logger.info(f"从数据库获取 {len(blocks)} 个板块")
            return blocks

        # 否则从API获取（备用方案）
        self.logger.info("从API获取板块列表...")

        try:
            from src.crawlers.block_crawler import get_block_list_db

            blocks = await get_block_list_db(limit=500)
            if blocks:
                self.logger.info(f"成功获取 {len(blocks)} 个板块")
                return blocks
        except Exception as e:
            self.logger.error(f"获取板块列表失败: {e}")

        return []

    def _get_latest_block_date(self, block_code: str) -> Optional[str]:
        """从数据库获取某板块的最新日期"""
        from database import mysql1
        from models.block.blockDM import BlockKline

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
        from models.block.blockDM import BlockCapitalFlow, BlockKline

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
        from models.stock.stockDM import StockKline

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

        from src.domain.entities import (BlockIdentifier, CapitalFlowData,
                                         CrawlDataPoint, FinancialData)

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
            from src.crawlers.block_crawler import (get_block_kline_db,
                                                    parse_block_price_kline)

            klines = await get_block_kline_db(block["code"], block["name"], start_date)

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
            from src.crawlers.block_crawler import (get_block_capital_flow_db,
                                                   parse_block_capital_flow)

            # 获取资金流数据
            capital_klines = await get_block_capital_flow_db(
                block["code"], block["name"]
            )
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
        from src.crawlers.info_crawler import get_stock_list_from_db
        from src.crawlers.stock_crawler import get_stock_list_api

        stocks = get_stock_list_from_db()

        # 如果数据库有数据就直接使用
        if stocks:
            self.logger.info(f"从数据库获取 {len(stocks)} 只股票")
            return stocks

        # 否则从API获取（备用方案）
        self.logger.info("从API获取股票列表...")
        stocks = await get_stock_list_api()
        if stocks:
            self.logger.info(f"成功获取 {len(stocks)} 只股票")
        return stocks

    async def _crawl_stock_data(
        self, stock: Dict, config: CrawlerConfiguration
    ) -> Optional[CrawlResult]:
        """为单只股票爬取真实K线数据 - 异步版本"""
        from datetime import datetime, timedelta

        from src.crawlers.stock_crawler import (generate_secid, get_stock_kline,
                                               parse_stock_kline)
        from src.domain.entities import CrawlDataPoint, FinancialData, StockIdentifier

        from database import is_task_completed, mark_task_completed
        from settings.settings import load_settings

        stock_code = stock["code"]
        stock_name = stock["name"]
        secid = generate_secid(stock_code)
        identifier = StockIdentifier(code=stock_code, name=stock_name, secid=secid)

        settings = load_settings()
        task_code = stock_code

        # 全量模式：检查任务是否已完成
        if config.sync_type == SyncType.FULL:
            task_type = "stock_kline"

            if is_task_completed(task_type, task_code, "full"):
                self.logger.info(f"跳过已完成的任务: {task_type} - {task_code}")
                return None

            # 删除该股票的已有数据（重新爬取）
            self._delete_stock_data(task_code, config.source_type)

        if config.sync_type == SyncType.FULL:
            start_date = settings.sync.start_date
        else:
            # 增量模式：从数据库中获取最新日期 + 1 天
            start_date = self._get_latest_date(stock_code)
            if start_date:
                latest = datetime.strptime(start_date, "%Y-%m-%d")
                start_date = (latest + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = settings.sync.start_date

        # 获取K线数据
        klines = await get_stock_kline(stock_code, stock_name, start_date)

        if not klines:
            self.logger.warning(f"股票 {stock_name} 没有K线数据")
            return None

        data_points = []
        for kline_str in klines:
            parsed = parse_stock_kline(kline_str)
            if not parsed:
                continue

            financial_data = FinancialData(
                open=parsed["open"],
                close=parsed["close"],
                high=parsed["high"],
                low=parsed["low"],
                volume=parsed["volume"],
                amount=parsed["amount"],
                change=parsed["change"],
                change_pct=parsed["change_pct"],
            )
            data_points.append(
                CrawlDataPoint(
                    timestamp=parsed["date"],
                    financial_data=financial_data,
                    raw_data=parsed["raw_data"],
                )
            )

        self.logger.info(f"股票 {stock_name} 获取到 {len(data_points)} 条K线数据")

        if not data_points:
            return None

        # 全量模式：标记任务完成
        if config.sync_type == SyncType.FULL:
            mark_task_completed("stock_kline", stock_code, "full")

        return CrawlResult(
            source_type=config.source_type,
            identifier=identifier,
            data_points=data_points,
            sync_type=config.sync_type,
        )

    def _get_latest_date(self, stock_code: str) -> Optional[str]:
        """从数据库获取某只股票的最新日期"""
        from database import mysql1
        from models.stock.stockDM import StockKline

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
