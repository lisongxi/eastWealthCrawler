"""
Unified Error Handling System
"""

import asyncio
import logging
import traceback
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Type


class ErrorSeverity(Enum):
    """错误严重程度级别"""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """错误分类"""

    CONFIGURATION = "configuration"
    NETWORK = "network"
    DATABASE = "database"
    VALIDATION = "validation"
    PROCESSING = "processing"
    SYSTEM = "system"
    UNKNOWN = "unknown"


@dataclass
class ApplicationError:
    """标准化应用程序错误"""

    message: str  # 错误消息
    severity: ErrorSeverity = ErrorSeverity.ERROR  # 严重程度
    category: ErrorCategory = ErrorCategory.UNKNOWN  # 错误分类
    original_exception: Optional[Exception] = None  # 原始异常
    context: Optional[Dict[str, Any]] = None  # 上下文信息
    timestamp: Optional[str] = None  # 时间戳

    def __post_init__(self):
        from datetime import datetime

        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """转换错误为字典"""
        return {
            "message": self.message,
            "severity": self.severity.value,
            "category": self.category.value,
            "exception_type": (
                type(self.original_exception).__name__
                if self.original_exception
                else None
            ),
            "exception_message": (
                str(self.original_exception) if self.original_exception else None
            ),
            "context": self.context,
            "timestamp": self.timestamp,
        }


class ErrorHandler:
    """集中式错误处理器"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)  # 日志记录器
        self.error_handlers = {}  # 错误处理器字典
        self.fallback_handler = self._default_error_handler  # 回退处理器

    def register_handler(self, error_type: Type[BaseException], handler: Callable):
        """为特定异常类型注册自定义错误处理器"""
        self.error_handlers[error_type] = handler

    def handle_error(self, error: ApplicationError):
        """处理应用程序错误"""
        try:
            # 记录错误
            self._log_error(error)

            # 查找适当的处理器
            handler = self._find_handler(error)
            if handler:
                handler(error)
            else:
                self.fallback_handler(error)

        except Exception as e:
            self.logger.error(f"错误处理失败: {e}")

    def _find_handler(self, error: ApplicationError) -> Optional[Callable]:
        """查找适当的错误处理器"""
        if error.original_exception:
            for exception_type, handler in self.error_handlers.items():
                if isinstance(error.original_exception, exception_type):
                    return handler
        return None

    def _log_error(self, error: ApplicationError):
        """根据严重程度记录错误"""
        log_method = getattr(self.logger, error.severity.value)
        log_method(f"{error.category.value.upper()} - {error.message}")

        if error.original_exception:
            self.logger.debug(f"异常详情: {traceback.format_exc()}")

    def _default_error_handler(self, error: ApplicationError):
        """默认错误处理器"""
        self.logger.error(f"未处理的 {error.category.value} 错误: {error.message}")

        if error.severity == ErrorSeverity.CRITICAL:
            # 对于严重错误，我们可能需要触发额外操作
            self.logger.critical("发生严重错误，系统可能不稳定")


class ErrorMiddleware:
    """错误处理中间件"""

    def __init__(self, error_handler: ErrorHandler):
        self.error_handler = error_handler  # 错误处理器

    def __call__(self, func):
        """错误处理装饰器"""

        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    self._handle_exception(e, func.__name__)
                    raise

            return async_wrapper
        else:

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    self._handle_exception(e, func.__name__)
                    raise

            return sync_wrapper

    def _handle_exception(self, exception: Exception, context: str):
        """处理异常并创建应用程序错误"""
        error = self._create_application_error(exception, context)
        self.error_handler.handle_error(error)

    def _create_application_error(
        self, exception: Exception, context: str
    ) -> ApplicationError:
        """从异常创建应用程序错误"""
        # 将异常类型映射到分类
        category_mapping = {
            ConnectionError: ErrorCategory.NETWORK,
            TimeoutError: ErrorCategory.NETWORK,
            ValueError: ErrorCategory.VALIDATION,
            TypeError: ErrorCategory.VALIDATION,
            KeyError: ErrorCategory.CONFIGURATION,
            FileNotFoundError: ErrorCategory.CONFIGURATION,
        }

        category = category_mapping.get(type(exception), ErrorCategory.UNKNOWN)

        # 确定严重程度
        severity = ErrorSeverity.ERROR
        if isinstance(exception, (KeyboardInterrupt, SystemExit)):
            severity = ErrorSeverity.CRITICAL

        return ApplicationError(
            message=str(exception),
            severity=severity,
            category=category,
            original_exception=exception,
            context={"function": context},
        )


class ErrorContext:
    """错误上下文管理器"""

    def __init__(self, error_handler: ErrorHandler, context: Dict[str, Any]):
        self.error_handler = error_handler  # 错误处理器
        self.context = context  # 上下文信息

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            error = ApplicationError(
                message=str(exc_val),
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.UNKNOWN,
                original_exception=exc_val,
                context=self.context,
            )
            self.error_handler.handle_error(error)
            return True  # 抑制异常
        return False


class ErrorRegistry:
    """错误跟踪和分析注册表"""

    def __init__(self):
        self.errors: List[ApplicationError] = []  # 错误列表
        self.error_stats: Dict[str, int] = {}  # 错误统计

    def record_error(self, error: ApplicationError):
        """记录错误"""
        self.errors.append(error)

        # 更新统计信息
        category = error.category.value
        self.error_stats[category] = self.error_stats.get(category, 0) + 1

        severity = error.severity.value
        stat_key = f"{category}_{severity}"
        self.error_stats[stat_key] = self.error_stats.get(stat_key, 0) + 1

    def get_error_stats(self) -> Dict[str, int]:
        """获取错误统计"""
        return self.error_stats.copy()

    def get_recent_errors(self, limit: int = 10) -> List[ApplicationError]:
        """获取最近的错误"""
        return self.errors[-limit:]

    def clear_errors(self):
        """清除记录的错误"""
        self.errors.clear()
        self.error_stats.clear()


class ErrorResponse:
    """标准化错误响应"""

    def __init__(self, error: ApplicationError):
        self.error = error  # 错误对象

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典用于API响应"""
        return {
            "error": {
                "message": self.error.message,
                "code": self.error.category.value,
                "severity": self.error.severity.value,
                "timestamp": self.error.timestamp,
            },
            "details": self.error.context,
        }

    def to_json(self) -> str:
        """转换为JSON字符串"""
        import json

        return json.dumps(self.to_dict())


class ErrorFactory:
    """创建标准化错误的工厂类"""

    @staticmethod
    def create_error(
        message: str,
        severity: ErrorSeverity = ErrorSeverity.ERROR,
        category: ErrorCategory = ErrorCategory.UNKNOWN,
        original_exception: Optional[Exception] = None,
        context: Optional[Dict] = None,
    ) -> ApplicationError:
        """创建应用程序错误"""
        return ApplicationError(
            message=message,
            severity=severity,
            category=category,
            original_exception=original_exception,
            context=context,
        )

    @staticmethod
    def create_from_exception(
        exception: Exception,
        context: Optional[Dict] = None,
        severity: Optional[ErrorSeverity] = None,
    ) -> ApplicationError:
        """从异常创建错误"""
        # 映射异常类型
        exception_mapping = {
            ValueError: (ErrorCategory.VALIDATION, ErrorSeverity.WARNING),
            TypeError: (ErrorCategory.VALIDATION, ErrorSeverity.WARNING),
            KeyError: (ErrorCategory.CONFIGURATION, ErrorSeverity.ERROR),
            FileNotFoundError: (ErrorCategory.CONFIGURATION, ErrorSeverity.ERROR),
            ConnectionError: (ErrorCategory.NETWORK, ErrorSeverity.ERROR),
            TimeoutError: (ErrorCategory.NETWORK, ErrorSeverity.ERROR),
            asyncio.CancelledError: (ErrorCategory.SYSTEM, ErrorSeverity.INFO),
        }

        category, default_severity = exception_mapping.get(
            type(exception), (ErrorCategory.UNKNOWN, ErrorSeverity.ERROR)
        )

        return ApplicationError(
            message=str(exception),
            severity=severity or default_severity,
            category=category,
            original_exception=exception,
            context=context,
        )


# 全局错误处理器实例
_global_error_handler = None


def get_error_handler() -> ErrorHandler:
    """获取全局错误处理器"""
    global _global_error_handler
    if _global_error_handler is None:
        _global_error_handler = ErrorHandler()
    return _global_error_handler


def setup_error_handling():
    """设置全局错误处理"""
    handler = get_error_handler()

    # 注册常见错误处理器
    handler.register_handler(
        ValueError,
        lambda e: logging.getLogger(__name__).warning(f"验证错误: {e.message}"),
    )
    handler.register_handler(
        KeyError, lambda e: logging.getLogger(__name__).error(f"配置错误: {e.message}")
    )
    handler.register_handler(
        asyncio.CancelledError,
        lambda e: logging.getLogger(__name__).error(f"用户取消: {e.message}"),
    )

    return handler
