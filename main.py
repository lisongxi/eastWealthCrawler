"""
东方财富股票数据爬虫 - 主入口点
使用事件驱动架构爬取股票数据
"""

import asyncio
import logging
from datetime import datetime, time, timedelta

from src.infrastructure.error_handling import ErrorMiddleware, setup_error_handling

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/application.log")],
)

logger = logging.getLogger(__name__)


async def wait_until(target_time):
    """等待到指定时间"""
    now = datetime.now()
    target_datetime = datetime.combine(now.date(), target_time)
    if target_datetime <= now:
        # 如果目标时间已过，等待到第二天
        target_datetime += timedelta(days=1)
    wait_seconds = (target_datetime - now).total_seconds()
    logger.info(f"等待到 {target_time}，需要等待 {wait_seconds:.2f} 秒")
    await asyncio.sleep(wait_seconds)


async def execute_crawler(crawler_orchestrator, error_handler):
    """执行爬虫"""
    try:
        start_time = datetime.now()

        # 用错误中间件包装爬虫执行
        error_middleware = ErrorMiddleware(error_handler)
        crawl_with_error_handling = error_middleware(
            crawler_orchestrator.run_all_crawlers
        )

        results = await crawl_with_error_handling()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info(f"Crawling completed in {duration:.2f} seconds")
        logger.info(f"Total results: {len(results)}")

        # 显示部分结果信息
        for i, result in enumerate(results[:3]):  # 显示前3条结果
            logger.info(
                f"Result {i + 1}: {result.source_type.value} - "
                f"{result.identifier.name} ({result.identifier.code}) - "
                f"{len(result.data_points)} data points"
            )

    except asyncio.CancelledError:
        # 用户中断，不记录为错误
        logger.info("爬虫执行被中断")
        raise
    except Exception as e:
        logger.error(f"Crawler execution error: {e}")
        # 使用 ErrorFactory 创建 ApplicationError
        from src.infrastructure.error_handling import ErrorFactory

        application_error = ErrorFactory.create_from_exception(
            e, context={"function": "execute_crawler"}
        )
        error_handler.handle_error(application_error)


async def main():
    """主应用程序入口点"""
    from settings.settings import load_settings
    from src.application.services import CrawlerOrchestrator
    from src.container.container import get_container
    from src.events.event_bus import EventBus, LoggingEventHandler, get_event_bus
    from src.infrastructure.error_handling import ErrorMiddleware, setup_error_handling
    from src.infrastructure.health_monitoring import HealthCheckScheduler, HealthMonitor
    from src.pipeline.data_pipeline import DataPipeline, PipelineFactory

    logger.info("Starting East Money Stock Data Crawler")

    # 加载配置
    settings = load_settings()
    sync_mode = settings.sync.mode.lower()
    incremental_time_str = settings.sync.incremental_time

    # 根据模式初始化数据库
    from database import close_db, ensure_info_tables, ensure_tables_exist, init_db
    from src.crawlers.info_crawler import update_block_info, update_stock_info

    init_db()
    if sync_mode == "full":
        logger.info("全量模式：检查并创建数据表...")
        ensure_tables_exist()
    else:
        logger.info("增量模式：检查并创建数据表...")
        ensure_tables_exist()

    # 确保基本信息表存在
    logger.info("确保基本信息表存在...")
    ensure_info_tables()

    # 更新板块和个股基本信息（失败不影响继续运行）
    logger.info("更新板块和个股基本信息...")
    try:
        update_block_info()
    except Exception as e:
        logger.warning(f"更新板块信息失败: {e}")

    try:
        update_stock_info()
    except Exception as e:
        logger.warning(f"更新股票信息失败: {e}")

    close_db()

    # 初始化依赖注入容器（必须先于限流器初始化）
    container = get_container()

    # 初始化令牌桶限流器
    from src.infrastructure.rate_limiter import RateLimiter, init_rate_limiter

    if hasattr(settings.sync, "rate_limit") and settings.sync.rate_limit.enabled:
        bucket_size = settings.sync.rate_limit.bucket_size
        refill_rate = settings.sync.rate_limit.refill_rate

        # 创建并注册限流器到容器
        rate_limiter = RateLimiter()
        rate_limiter.init(bucket_size, refill_rate)
        container.register_instance(RateLimiter, rate_limiter)

        # 同时初始化全局实例（保持向后兼容）
        await init_rate_limiter(bucket_size, refill_rate)
        logger.info(
            f"令牌桶限流已启用: 桶大小={bucket_size}, 令牌速率={refill_rate}/秒"
        )

    # 解析增量同步时间
    try:
        hour, minute = map(int, incremental_time_str.split(":"))
        incremental_time = time(hour, minute)
    except ValueError:
        logger.warning(
            f"无效的增量同步时间格式: {incremental_time_str}，使用默认值 20:00"
        )
        incremental_time = time(20, 0)

    logger.info(f"同步模式: {sync_mode}")
    if sync_mode == "incremental":
        logger.info(f"增量同步时间: {incremental_time}")

    # 设置错误处理
    error_handler = setup_error_handling()

    # 初始化事件总线
    event_bus = get_event_bus()

    # 注册事件处理器
    logging_handler = LoggingEventHandler(event_bus)
    logging_handler.register_handlers()

    # 启动事件总线并等待就绪
    event_bus_task = asyncio.create_task(event_bus.start())
    await asyncio.sleep(0.1)  # 等待事件总线启动

    # 初始化数据管道
    data_pipeline = PipelineFactory.create_standard_pipeline()

    # 初始化健康监控
    health_monitor = HealthMonitor(event_bus)
    health_scheduler = HealthCheckScheduler(health_monitor)

    # 设置 shutdown_event 用于优雅停止
    shutdown_event = asyncio.Event()

    def signal_handler(signum, frame):
        """处理中断信号"""
        logger.info("接收到终止信号，准备停止...")
        shutdown_event.set()

    # 注册信号处理器 (使用标准 signal 模块)
    import signal

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 将 shutdown_event 传递给限流器，以便能够响应中断
    if hasattr(settings.sync, "rate_limit") and settings.sync.rate_limit.enabled:
        from src.infrastructure.rate_limiter import get_rate_limiter

        rate_limiter = get_rate_limiter()
        rate_limiter.set_interrupt_event(shutdown_event)

    # 启动健康监控
    health_task = asyncio.create_task(health_scheduler.start(interval=30))
    await asyncio.sleep(0.1)  # 等待健康监控启动

    try:
        # 在创建编排器之前在DI容器中注册组件
        container.register_instance(EventBus, event_bus)
        container.register_instance(DataPipeline, data_pipeline)
        container.register_instance(HealthMonitor, health_monitor)

        # 注册爬虫服务以便后续解析
        from src.application.services import BlockCrawlerService, StockCrawlerService

        container.register_singleton(BlockCrawlerService)
        container.register_singleton(StockCrawlerService)

        # 在依赖注册完成后初始化爬虫编排器
        crawler_orchestrator = CrawlerOrchestrator(event_bus, data_pipeline)
        container.register_instance(CrawlerOrchestrator, crawler_orchestrator)

        logger.info("All components initialized and registered")

        # 执行爬虫
        if sync_mode == "full":
            # 全量模式：只运行一次，执行完就退出，但可以中途 Ctrl+C 中断
            logger.info("全量模式：开始执行爬虫（按 Ctrl+C 中断）...")
            try:
                await execute_crawler(crawler_orchestrator, error_handler)
                logger.info("全量模式：爬虫执行完成！")
            except asyncio.CancelledError:
                logger.info("全量模式：被用户中断")
            return
        else:
            # 增量模式：每天指定时间执行
            logger.info("增量模式：启动定时任务...")
            while not shutdown_event.is_set():
                # 等待到指定时间
                await wait_until(incremental_time)
                if shutdown_event.is_set():
                    break
                # 执行爬虫
                logger.info("增量模式：开始执行爬虫...")
                await execute_crawler(crawler_orchestrator, error_handler)
                logger.info("增量模式：爬虫执行完成，等待下一次执行...")

        # 等待中断信号
        logger.info("按 Ctrl+C 停止程序...")
        await shutdown_event.wait()

    except Exception as e:
        logger.error(f"Application error: {e}")
        # 使用 ErrorFactory 创建 ApplicationError
        from src.infrastructure.error_handling import ErrorFactory

        application_error = ErrorFactory.create_from_exception(
            e, context={"function": "main"}
        )
        error_handler.handle_error(application_error)
    finally:
        logger.info("Shutting down application...")

        # 取消健康任务
        health_task.cancel()
        try:
            if not health_task.done():
                await health_task
        except asyncio.CancelledError:
            pass

        # 关闭 aiohttp Session
        try:
            from src.crawlers import close_http_session

            await close_http_session()
        except Exception as e:
            logger.warning(f"关闭 HTTP Session 失败: {e}")

        # 首先停止事件总线以允许待处理事件完成
        await event_bus.stop()
        if not event_bus_task.done():
            event_bus_task.cancel()
            try:
                await event_bus_task
            except asyncio.CancelledError:
                pass

        logger.info("Application shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Application error: {e}")
        import traceback

        traceback.print_exc()
