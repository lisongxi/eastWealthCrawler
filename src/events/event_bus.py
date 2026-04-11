"""
Event Bus Implementation
Event-driven architecture for decoupling components
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional, Type


class EventType(Enum):
    """标准事件类型"""

    CRAWLER_STARTED = "crawler_started"  # 爬虫开始
    CRAWLER_COMPLETED = "crawler_completed"  # 爬虫完成
    CRAWLER_FAILED = "crawler_failed"  # 爬虫失败
    DATA_PROCESSED = "data_processed"  # 数据处理
    SYSTEM_HEALTH = "system_health"  # 系统健康
    CONFIGURATION_CHANGED = "configuration_changed"  # 配置变更


@dataclass
class EventSubscription:
    """事件订阅信息"""

    callback: Callable  # 回调函数
    event_type: Optional[str] = None  # 事件类型
    priority: int = 0  # 优先级


class EventBus:
    """事件驱动架构的事件总线"""

    def __init__(self):
        self._subscriptions: Dict[str, List[EventSubscription]] = {}  # 订阅列表
        self._logger = logging.getLogger(__name__)  # 日志记录器
        self._running = False  # 运行状态
        self._event_queue = asyncio.Queue()  # 事件队列

    def subscribe(self, event_type: str, callback: Callable, priority: int = 0):
        """订阅特定事件类型"""
        if event_type not in self._subscriptions:
            self._subscriptions[event_type] = []

        subscription = EventSubscription(callback, event_type, priority)
        self._subscriptions[event_type].append(subscription)
        self._subscriptions[event_type].sort(key=lambda x: x.priority, reverse=True)

        self._logger.debug(f"订阅事件类型: {event_type}")

    def subscribe_all(self, callback: Callable, priority: int = 0):
        """订阅所有事件"""
        self.subscribe("*", callback, priority)

    def unsubscribe(self, event_type: str, callback: Callable):
        """取消订阅事件类型"""
        if event_type in self._subscriptions:
            self._subscriptions[event_type] = [
                sub
                for sub in self._subscriptions[event_type]
                if sub.callback != callback
            ]

    def publish(self, event: Any):
        """发布事件"""
        if not self._running:
            self._logger.warning("事件总线未运行，事件未处理")
            return

        # 将事件放入队列进行异步处理
        self._event_queue.put_nowait(event)
        self._logger.debug(f"事件已发布: {type(event).__name__}")

    async def start(self):
        """启动事件总线处理"""
        if self._running:
            return

        self._running = True
        self._logger.info("事件总线已启动")

        while self._running:
            try:
                event = await self._event_queue.get()
                await self._process_event(event)
            except Exception as e:
                self._logger.error(f"处理事件时出错: {e}")

    async def stop(self):
        """停止事件总线"""
        self._running = False
        self._logger.info("事件总线已停止")

    async def _process_event(self, event: Any):
        """通过通知所有订阅者来处理事件"""
        event_type = self._get_event_type(event)

        # 通知特定订阅者
        if event_type in self._subscriptions:
            for subscription in self._subscriptions[event_type]:
                try:
                    if asyncio.iscoroutinefunction(subscription.callback):
                        await subscription.callback(event)
                    else:
                        subscription.callback(event)
                except Exception as e:
                    self._logger.error(f"订阅者 {subscription.callback} 出错: {e}")

        # 通知通配符订阅者
        if "*" in self._subscriptions:
            for subscription in self._subscriptions["*"]:
                try:
                    if asyncio.iscoroutinefunction(subscription.callback):
                        await subscription.callback(event)
                    else:
                        subscription.callback(event)
                except Exception as e:
                    self._logger.error(
                        f"通配符订阅者 {subscription.callback} 出错: {e}"
                    )

    def _get_event_type(self, event: Any) -> str:
        """从事件对象获取事件类型"""
        if hasattr(event, "event_type"):
            return getattr(event, "event_type")
        elif hasattr(event, "__class__"):
            return event.__class__.__name__.lower()
        else:
            return "unknown"

    def get_subscription_count(self, event_type: Optional[str] = None) -> int:
        """获取订阅数量"""
        if event_type:
            return len(self._subscriptions.get(event_type, []))
        else:
            return sum(len(subs) for subs in self._subscriptions.values())


# 全局事件总线实例
_event_bus_instance = None


def get_event_bus() -> EventBus:
    """获取全局事件总线实例"""
    global _event_bus_instance
    if _event_bus_instance is None:
        _event_bus_instance = EventBus()
    return _event_bus_instance


class EventHandler:
    """事件处理器基类"""

    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus  # 事件总线
        self.logger = logging.getLogger(__name__)  # 日志记录器

    def register_handlers(self):
        """注册事件处理器"""
        pass

    def unregister_handlers(self):
        """取消注册事件处理器"""
        pass


class LoggingEventHandler(EventHandler):
    """事件日志处理器"""

    def register_handlers(self):
        """注册日志处理器"""
        # 只订阅关键事件类型，避免记录dict类型事件
        self.event_bus.subscribe("crawl_started", self._log_event)
        self.event_bus.subscribe("crawl_completed", self._log_event)
        self.event_bus.subscribe("crawl_failed", self._log_event)
        self.event_bus.subscribe("metrics_completed", self._log_event)
        self.event_bus.subscribe("data_saved", self._log_event)
        self.event_bus.subscribe("error_occurred", self._log_event)

    def _log_event(self, event: Any):
        """记录事件信息"""
        event_type = getattr(event, "event_type", type(event).__name__)
        self.logger.info(f"[Event] 事件已接收: {event_type}")

        try:
            if hasattr(event, "crawler_id"):
                self.logger.info(f"   爬虫ID: {event.crawler_id}")
            if hasattr(event, "status"):
                self.logger.info(f"   状态: {event.status.value}")
            if hasattr(event, "message"):
                self.logger.info(f"   消息: {event.message}")
            if hasattr(event, "data") and event.data:
                self.logger.debug(f"   数据: {event.data}")
        except Exception as e:
            self.logger.debug(f"事件详情解析失败: {e}")


class MetricsEventHandler(EventHandler):
    """指标收集事件处理器"""

    def __init__(self, event_bus: EventBus):
        super().__init__(event_bus)
        self.metrics = {}  # 指标数据

    def register_handlers(self):
        """注册指标处理器"""
        self.event_bus.subscribe(
            EventType.CRAWLER_STARTED.value, self._on_crawler_started
        )
        self.event_bus.subscribe(
            EventType.CRAWLER_COMPLETED.value, self._on_crawler_completed
        )
        self.event_bus.subscribe(
            EventType.CRAWLER_FAILED.value, self._on_crawler_failed
        )

    def _on_crawler_started(self, event: Any):
        """处理爬虫开始事件"""
        crawler_id = getattr(event, "crawler_id", "unknown")
        self.metrics[crawler_id] = {"start_time": datetime.now(), "status": "running"}

    def _on_crawler_completed(self, event: Any):
        """处理爬虫完成事件"""
        crawler_id = getattr(event, "crawler_id", "unknown")
        if crawler_id in self.metrics:
            self.metrics[crawler_id]["end_time"] = datetime.now()
            self.metrics[crawler_id]["status"] = "completed"

    def _on_crawler_failed(self, event: Any):
        """处理爬虫失败事件"""
        crawler_id = getattr(event, "crawler_id", "unknown")
        if crawler_id in self.metrics:
            self.metrics[crawler_id]["end_time"] = datetime.now()
            self.metrics[crawler_id]["status"] = "failed"
            self.metrics[crawler_id]["error"] = getattr(event, "message", "未知错误")
