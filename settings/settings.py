"""
配置文件读取 / Configuration File Reading
"""

from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field

# 配置文件路径 / Configuration file path
SETTINGS_FILE = Path(__file__).parent / "settings.yaml"


class SysAdmin(BaseModel):
    """系统管理员配置 / System administrator configuration"""

    username: str
    password: str


class Log(BaseModel):
    """日志配置 / Log configuration"""

    freq: str
    useUTC: bool


class User(BaseModel):
    """用户配置 / User configuration"""

    username: str
    password: str


class MySQL(BaseModel):
    """MySQL配置 / MySQL configuration"""

    database: str
    host: str
    port: int
    user: str
    password: str


class CrawlerConfig(BaseModel):
    """爬虫配置 / Crawler configuration"""

    full_history_start_date: str = Field(
        default="2019-01-01", description="全量模式起始日期 / Full mode start date"
    )
    block_limit: int = Field(
        default=0,
        description="板块数量限制，0表示不限制 / Block limit, 0 means no limit",
    )
    stock_limit: int = Field(
        default=0,
        description="个股数量限制，0表示不限制 / Stock limit, 0 means no limit",
    )
    request_delay: int = Field(
        default=3, description="请求延迟(秒) / Request delay in seconds"
    )
    batch_size: int = Field(default=5, description="批量处理数量 / Batch size")
    max_retries: int = Field(default=3, description="最大重试次数 / Max retry times")


class RateLimit(BaseModel):
    """令牌桶限流配置 / Token bucket rate limit configuration"""

    enabled: bool = Field(
        default=False, description="是否启用令牌桶限流 / Enable token bucket rate limit"
    )
    bucket_size: int = Field(
        default=2,
        description="桶的大小（同时最大并发请求数）/ Bucket size (max concurrent requests)",
    )
    refill_rate: float = Field(
        default=0.2, description="令牌发放速率（每秒）/ Token refill rate (per second)"
    )


class Sync(BaseModel):
    """同步配置 / Sync configuration"""

    mode: str = Field(
        default="incremental",
        description="同步模式: incremental(增量) 或 full(全量) / Sync mode: incremental or full",
    )
    incremental_time: str = Field(
        default="20:00",
        description="增量同步的时间，格式：HH:MM / Incremental sync time, format: HH:MM",
    )
    start_date: str = Field(
        default="2019-01-01",
        description="数据开始日期，格式：YYYY-MM-DD / Data start date, format: YYYY-MM-DD",
    )
    rate_limit: RateLimit = Field(
        default_factory=RateLimit,
        description="令牌桶限流配置 / Token bucket rate limit configuration",
    )


class EastWealth(BaseModel):
    """东方财富配置 / East Money configuration"""

    user: User
    mysql: MySQL
    crawler: CrawlerConfig = Field(default_factory=CrawlerConfig)


class Settings(BaseModel):
    """配置根对象 / Configuration root object"""

    sysAdmin: str
    log: Log
    sync: Sync = Field(default_factory=Sync)
    eastWealth: EastWealth


def get_settings() -> Settings:
    """读取配置文件 / Read configuration file"""
    if not SETTINGS_FILE.exists():
        raise FileNotFoundError(f"配置文件不存在: {SETTINGS_FILE}")

    with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return Settings(**data)


# 单例模式 / Singleton pattern
_settings: Optional[Settings] = None


def load_settings() -> Settings:
    """加载配置（单例）/ Load configuration (singleton)"""
    global _settings
    if _settings is None:
        _settings = get_settings()
    return _settings
