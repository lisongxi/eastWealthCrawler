"""
健康检查和监控系统
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import time
import psutil
import platform

from src.domain.entities import CrawlerStatus, CrawlerMetrics
from src.events.event_bus import EventBus, EventType


class HealthStatus(Enum):
    """系统健康状态"""

    HEALTHY = "healthy"  # 健康
    DEGRADED = "degraded"  # 降级
    UNHEALTHY = "unhealthy"  # 不健康
    UNKNOWN = "unknown"  # 未知


class ComponentStatus(Enum):
    """组件健康状态"""

    OPERATIONAL = "operational"  # 运行正常
    DEGRADED = "degraded"  # 降级
    FAILED = "failed"  # 失败
    UNKNOWN = "unknown"  # 未知


@dataclass
class SystemMetrics:
    """系统性能指标"""

    cpu_usage: float = 0.0  # CPU使用率
    memory_usage: float = 0.0  # 内存使用率
    disk_usage: float = 0.0  # 磁盘使用率
    uptime: float = 0.0  # 运行时间
    timestamp: str = ""  # 时间戳

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()


@dataclass
class ComponentHealth:
    """组件健康信息"""

    name: str  # 组件名称
    status: ComponentStatus  # 状态
    last_check: str = ""  # 最后检查时间
    details: Optional[Dict[str, Any]] = None  # 详细信息

    def __post_init__(self):
        if not self.last_check:
            self.last_check = datetime.now().isoformat()

    def dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "name": self.name,
            "status": self.status.value,
            "last_check": self.last_check,
            "details": self.details,
        }


@dataclass
class SystemHealth:
    """整体系统健康状态"""

    status: HealthStatus  # 健康状态
    components: List[ComponentHealth]  # 组件列表
    metrics: SystemMetrics  # 系统指标
    timestamp: str = ""  # 时间戳
    issues: List[str] = field(default_factory=list)  # 问题列表

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()
        if self.issues is None:
            self.issues = []


class HealthMonitor:
    """系统健康监控器"""

    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus  # 事件总线
        self.logger = logging.getLogger(__name__)  # 日志记录器
        self.components: Dict[str, ComponentHealth] = {}  # 组件字典
        self.metrics_history: List[SystemMetrics] = []  # 指标历史
        self.health_history: List[SystemHealth] = []  # 健康历史
        self._running = False  # 运行状态
        self._check_interval = 60  # 检查间隔（秒）

    async def start(self):
        """启动健康监控"""
        if self._running:
            return

        self._running = True
        self.logger.info("健康监控器已启动")

        while self._running:
            try:
                await self._perform_health_check()
                await asyncio.sleep(self._check_interval)
            except Exception as e:
                self.logger.error(f"健康检查失败: {e}")
                await asyncio.sleep(10)  # 等待后重试

    async def stop(self):
        """停止健康监控"""
        self._running = False
        self.logger.info("健康监控器已停止")

    async def _perform_health_check(self):
        """执行全面健康检查"""
        start_time = time.time()

        # 收集系统指标
        system_metrics = self._collect_system_metrics()

        # 检查组件健康状态
        component_health = await self._check_component_health()

        # 确定整体健康状态
        health_status = self._determine_health_status(component_health)

        # 创建健康报告
        health_report = SystemHealth(
            status=health_status,
            components=list(component_health.values()),
            metrics=system_metrics,
        )

        # 存储历史记录
        self.health_history.append(health_report)
        self.metrics_history.append(system_metrics)

        # 清理旧数据
        self._cleanup_history()

        # 发布健康事件
        self.event_bus.publish(
            {
                "event_type": EventType.SYSTEM_HEALTH.value,
                "health_status": health_status.value,
                "timestamp": datetime.now().isoformat(),
                "metrics": system_metrics,
                "components": [comp.dict() for comp in component_health.values()],
            }
        )

        self.logger.debug(f"健康检查完成，耗时 {time.time() - start_time:.2f}s")

    def _collect_system_metrics(self) -> SystemMetrics:
        """收集系统性能指标"""
        try:
            # CPU使用率
            cpu_usage = psutil.cpu_percent(interval=1)

            # 内存使用率
            memory = psutil.virtual_memory()
            memory_usage = memory.percent

            # 磁盘使用率
            disk = psutil.disk_usage("/")
            disk_usage = disk.percent

            # 运行时间
            uptime = time.time() - psutil.boot_time()

            return SystemMetrics(
                cpu_usage=cpu_usage,
                memory_usage=memory_usage,
                disk_usage=disk_usage,
                uptime=uptime,
            )
        except Exception as e:
            self.logger.error(f"收集系统指标失败: {e}")
            return SystemMetrics()

    async def _check_component_health(self) -> Dict[str, ComponentHealth]:
        """检查所有注册组件的健康状态"""
        component_health = {}

        # 检查数据库组件
        component_health["database"] = await self._check_database_health()

        # 检查事件总线组件
        component_health["event_bus"] = self._check_event_bus_health()

        # 检查爬虫组件
        component_health["crawlers"] = await self._check_crawler_health()

        # 检查管道组件
        component_health["pipeline"] = self._check_pipeline_health()

        return component_health

    async def _check_database_health(self) -> ComponentHealth:
        """检查数据库健康状态"""
        try:
            # 在实际实现中，这里会测试数据库连接性
            # 目前，我们模拟一个成功的检查
            return ComponentHealth(
                name="database",
                status=ComponentStatus.OPERATIONAL,
                details={"connection": "ok", "response_time": "0.1s"},
            )
        except Exception as e:
            return ComponentHealth(
                name="database",
                status=ComponentStatus.FAILED,
                details={"error": str(e)},
            )

    def _check_event_bus_health(self) -> ComponentHealth:
        """检查事件总线健康状态"""
        try:
            # 通过发布测试事件来测试事件总线
            test_event = {
                "event_type": "health_check",
                "timestamp": datetime.now().isoformat(),
            }
            self.event_bus.publish(test_event)

            return ComponentHealth(
                name="event_bus",
                status=ComponentStatus.OPERATIONAL,
                details={
                    "subscriptions": self.event_bus.get_subscription_count(),
                    "test_event": "published",
                },
            )
        except Exception as e:
            return ComponentHealth(
                name="event_bus",
                status=ComponentStatus.FAILED,
                details={"error": str(e)},
            )

    async def _check_crawler_health(self) -> ComponentHealth:
        """检查爬虫健康状态"""
        try:
            # 在实际实现中，这里会检查爬虫状态
            # 目前，我们模拟一个健康状态
            return ComponentHealth(
                name="crawlers",
                status=ComponentStatus.OPERATIONAL,
                details={"active_crawlers": 0, "recent_success": True},
            )
        except Exception as e:
            return ComponentHealth(
                name="crawlers",
                status=ComponentStatus.DEGRADED,
                details={"error": str(e)},
            )

    def _check_pipeline_health(self) -> ComponentHealth:
        """检查数据管道健康状态"""
        try:
            # 在实际实现中，这里会检查管道状态
            return ComponentHealth(
                name="pipeline",
                status=ComponentStatus.OPERATIONAL,
                details={"processed_items": 0, "errors": 0},
            )
        except Exception as e:
            return ComponentHealth(
                name="pipeline",
                status=ComponentStatus.DEGRADED,
                details={"error": str(e)},
            )

    def _determine_health_status(
        self, components: Dict[str, ComponentHealth]
    ) -> HealthStatus:
        """根据组件确定整体健康状态"""
        failed_components = [
            c for c in components.values() if c.status == ComponentStatus.FAILED
        ]
        degraded_components = [
            c for c in components.values() if c.status == ComponentStatus.DEGRADED
        ]

        if failed_components:
            return HealthStatus.UNHEALTHY
        elif degraded_components:
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY

    def _cleanup_history(self):
        """清理旧的健康历史"""
        # 只保留最近100个健康报告
        if len(self.health_history) > 100:
            self.health_history = self.health_history[-100:]

        # 只保留最近100个指标
        if len(self.metrics_history) > 100:
            self.metrics_history = self.metrics_history[-100:]

    def get_current_health(self) -> Optional[SystemHealth]:
        """获取当前系统健康状态"""
        return self.health_history[-1] if self.health_history else None

    def get_health_history(self, limit: int = 10) -> List[SystemHealth]:
        """获取健康历史"""
        return self.health_history[-limit:]

    def get_metrics_history(self, limit: int = 10) -> List[SystemMetrics]:
        """获取指标历史"""
        return self.metrics_history[-limit:]


class HealthCheckEndpoint:
    """外部监控的健康检查端点"""

    def __init__(self, health_monitor: HealthMonitor):
        self.health_monitor = health_monitor  # 健康监控器

    def get_health_status(self) -> Dict[str, Any]:
        """获取当前健康状态"""
        current_health = self.health_monitor.get_current_health()

        if current_health:
            return {
                "status": current_health.status.value,
                "timestamp": current_health.timestamp,
                "components": {
                    comp.name: {
                        "status": comp.status.value,
                        "last_check": comp.last_check,
                    }
                    for comp in current_health.components
                },
                "metrics": {
                    "cpu_usage": current_health.metrics.cpu_usage,
                    "memory_usage": current_health.metrics.memory_usage,
                    "disk_usage": current_health.metrics.disk_usage,
                    "uptime": current_health.metrics.uptime,
                },
            }
        else:
            return {
                "status": HealthStatus.UNKNOWN.value,
                "message": "没有可用的健康数据",
            }

    def get_detailed_health(self) -> Dict[str, Any]:
        """获取详细的健康信息"""
        current_health = self.health_monitor.get_current_health()

        if current_health:
            return {
                **self.get_health_status(),
                "components": [
                    {
                        "name": comp.name,
                        "status": comp.status.value,
                        "last_check": comp.last_check,
                        "details": comp.details,
                    }
                    for comp in current_health.components
                ],
                "issues": current_health.issues,
                "history": [
                    {"timestamp": h.timestamp, "status": h.status.value}
                    for h in self.health_monitor.get_health_history(5)
                ],
            }
        else:
            return self.get_health_status()


class PerformanceMonitor:
    """爬虫性能监控器"""

    def __init__(self):
        self.crawler_metrics: Dict[str, List[CrawlerMetrics]] = {}  # 爬虫指标
        self.performance_history: List[Dict] = []  # 性能历史

    def record_crawler_metrics(self, crawler_id: str, metrics: CrawlerMetrics):
        """记录爬虫性能指标"""
        if crawler_id not in self.crawler_metrics:
            self.crawler_metrics[crawler_id] = []

        self.crawler_metrics[crawler_id].append(metrics)

        # 存储到性能历史
        self.performance_history.append(
            {
                "crawler_id": crawler_id,
                "timestamp": metrics.start_time.isoformat(),
                "duration": metrics.duration(),
                "data_points": metrics.total_data_points,
                "success_rate": (
                    metrics.successful_requests / metrics.total_requests
                    if metrics.total_requests > 0
                    else 0
                ),
            }
        )

        # 清理旧数据
        if len(self.performance_history) > 100:
            self.performance_history = self.performance_history[-100:]

    def get_crawler_performance(self, crawler_id: str, limit: int = 10) -> List[Dict]:
        """获取特定爬虫的性能指标"""
        if crawler_id in self.crawler_metrics:
            metrics = self.crawler_metrics[crawler_id][-limit:]
            return [
                {
                    "timestamp": m.start_time.isoformat(),
                    "duration": m.duration(),
                    "data_points": m.total_data_points,
                    "success_rate": (
                        m.successful_requests / m.total_requests
                        if m.total_requests > 0
                        else 0
                    ),
                }
                for m in metrics
            ]
        return []

    def get_overall_performance(self, limit: int = 10) -> Dict[str, Any]:
        """获取整体系统性能"""
        recent = (
            self.performance_history[-limit:] if limit > 0 else self.performance_history
        )

        if not recent:
            return {
                "average_duration": 0,
                "total_data_points": 0,
                "average_success_rate": 0,
                "crawlers_active": 0,
            }

        total_duration = sum(p["duration"] or 0 for p in recent)
        total_data_points = sum(p["data_points"] for p in recent)
        total_success_rate = sum(p["success_rate"] for p in recent)
        active_crawlers = len(set(p["crawler_id"] for p in recent))

        return {
            "average_duration": total_duration / len(recent) if recent else 0,
            "total_data_points": total_data_points,
            "average_success_rate": total_success_rate / len(recent) if recent else 0,
            "crawlers_active": active_crawlers,
            "recent_performance": recent[-5:],  # 最近5次运行
        }


class AlertManager:
    """健康监控的警报管理器"""

    def __init__(self, health_monitor: HealthMonitor):
        self.health_monitor = health_monitor  # 健康监控器
        self.alert_rules = []  # 警报规则
        self.active_alerts = []  # 活跃警报

    def add_alert_rule(
        self, condition: Callable, message: str, severity: str = "warning"
    ):
        """添加警报规则"""
        self.alert_rules.append(
            {"condition": condition, "message": message, "severity": severity}
        )

    def check_alerts(self):
        """检查所有警报规则"""
        current_health = self.health_monitor.get_current_health()
        if not current_health:
            return

        for rule in self.alert_rules:
            if rule["condition"](current_health):
                alert = {
                    "message": rule["message"],
                    "severity": rule["severity"],
                    "timestamp": datetime.now().isoformat(),
                    "health_status": current_health.status.value,
                }

                if alert not in self.active_alerts:
                    self.active_alerts.append(alert)
                    self._trigger_alert(alert)

    def _trigger_alert(self, alert: Dict):
        """触发警报"""
        self.health_monitor.logger.warning(
            f"警报: {alert['message']} (严重程度: {alert['severity']})"
        )

        # 在实际实现中，这里可以发送邮件、通知等
        # 目前，我们只是记录日志

    def clear_alerts(self):
        """清除所有活跃警报"""
        self.active_alerts.clear()

    def get_active_alerts(self) -> List[Dict]:
        """获取活跃警报"""
        return self.active_alerts.copy()


class HealthCheckScheduler:
    """健康检查调度器"""

    def __init__(self, health_monitor: HealthMonitor):
        self.health_monitor = health_monitor  # 健康监控器
        self._running = False  # 运行状态

    async def start(self, interval: int = 60):
        """启动定时健康检查"""
        if self._running:
            return

        self._running = True
        self.health_monitor.logger.info(f"健康检查调度器已启动，间隔 {interval}s")

        while self._running:
            try:
                await self.health_monitor._perform_health_check()
                await asyncio.sleep(interval)
            except Exception as e:
                self.health_monitor.logger.error(f"定时健康检查失败: {e}")
                await asyncio.sleep(10)  # 等待后重试

    async def stop(self):
        """停止定时健康检查"""
        self._running = False
        self.health_monitor.logger.info("健康检查调度器已停止")


# 工具函数
def get_system_info() -> Dict[str, Any]:
    """获取系统信息"""
    return {
        "platform": platform.system(),
        "platform_version": platform.version(),
        "python_version": platform.python_version(),
        "processor": platform.processor(),
        "machine": platform.machine(),
        "node": platform.node(),
    }


def format_health_status(health: SystemHealth) -> str:
    """格式化健康状态为字符串"""
    status_indicator = {
        HealthStatus.HEALTHY: "[OK]",
        HealthStatus.DEGRADED: "[WARN]",
        HealthStatus.UNHEALTHY: "[ERROR]",
        HealthStatus.UNKNOWN: "[?]",
    }

    components_status = ", ".join(
        f"{comp.name}:{comp.status.value[0]}" for comp in health.components
    )

    return (
        f"{status_indicator[health.status]} 系统 {health.status.value.upper()} - "
        f"CPU:{health.metrics.cpu_usage:.1f}% "
        f"内存:{health.metrics.memory_usage:.1f}% "
        f"磁盘:{health.metrics.disk_usage:.1f}% - "
        f"组件: [{components_status}]"
    )
