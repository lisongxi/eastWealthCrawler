"""令牌桶限流器
用于控制并发请求速率
支持通过中断事件优雅停止
"""

import asyncio
import time


class TokenBucket:
    """令牌桶限流器"""

    def __init__(self, bucket_size: int = None, refill_rate: float = None):
        """
        初始化令牌桶
        Args:
            bucket_size: 桶的大小（最大令牌数）
            refill_rate: 令牌发放速率（每秒发放的令牌数）
        """
        # 从配置获取默认值
        if bucket_size is None or refill_rate is None:
            try:
                from settings.settings import load_settings

                settings = load_settings()
                self.bucket_size = bucket_size or settings.sync.rate_limit.bucket_size
                self.refill_rate = refill_rate or settings.sync.rate_limit.refill_rate
            except Exception:
                # 配置不可用时使用硬编码默认值
                self.bucket_size = bucket_size or 2
                self.refill_rate = refill_rate or 0.2
        else:
            self.bucket_size = bucket_size
            self.refill_rate = refill_rate
        self.tokens = float(self.bucket_size)  # 当前令牌数
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(
        self, tokens: int = 1, interrupt_event: asyncio.Event = asyncio.Event()
    ):
        """
        获取令牌
        Args:
            tokens: 需要获取的令牌数
            interrupt_event: 可选的中断事件，用于优雅停止
        """
        async with self._lock:
            while True:
                # 检查中断事件
                if interrupt_event and interrupt_event.is_set():
                    raise asyncio.CancelledError("限流器被中断")

                await self._refill()

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True

                # 等待足够的时间让令牌补充
                wait_time = (tokens - self.tokens) / self.refill_rate

                # 释放锁，让其他协程运行
                await asyncio.sleep(min(wait_time, 0.1))

    async def _refill(self):
        """补充令牌"""
        now = time.time()
        elapsed = now - self.last_refill

        # 计算应该补充的令牌数
        new_tokens = elapsed * self.refill_rate
        self.tokens = min(self.bucket_size, self.tokens + new_tokens)
        self.last_refill = now

    def get_available_tokens(self) -> int:
        """获取当前可用令牌数"""
        return int(self.tokens)


class RateLimiter:
    """全局限流器管理器"""

    def __init__(self):
        self._bucket: TokenBucket = TokenBucket()
        self._interrupt_event: asyncio.Event = asyncio.Event()

    def init(self, bucket_size: int, refill_rate: float):
        """初始化限流器"""
        self._bucket = TokenBucket(bucket_size=bucket_size, refill_rate=refill_rate)

    def set_interrupt_event(self, event: asyncio.Event):
        """设置中断事件"""
        self._interrupt_event = event

    async def acquire(self, tokens: int = 1):
        """获取令牌"""
        if self._bucket:
            await self._bucket.acquire(tokens, self._interrupt_event)

    def get_available_tokens(self) -> int:
        """获取当前可用令牌数"""
        if self._bucket:
            return self._bucket.get_available_tokens()
        return 0


# 全局限流器实例
_rate_limiter_instance = None


def get_rate_limiter() -> RateLimiter:
    """获取全局限流器实例（委托给容器管理）"""
    global _rate_limiter_instance
    if _rate_limiter_instance is None:
        from src.container.container import get_container

        container = get_container()
        try:
            _rate_limiter_instance = container.resolve(RateLimiter)
        except ValueError:
            # 如果容器中没有，则使用全局实例
            _rate_limiter_instance = RateLimiter()
    return _rate_limiter_instance


async def init_rate_limiter(bucket_size: int, refill_rate: float):
    """初始化限流器"""
    rate_limiter = get_rate_limiter()
    rate_limiter.init(bucket_size, refill_rate)


async def acquire():
    """获取令牌"""
    rate_limiter = get_rate_limiter()
    await rate_limiter.acquire()


# 保持向后兼容的全局实例引用
rate_limiter = RateLimiter()
