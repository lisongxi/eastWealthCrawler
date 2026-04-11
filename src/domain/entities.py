"""
领域实体和值对象
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


class DataSourceType(Enum):
    """数据源类型"""

    BLOCK_CAPITAL_FLOW = "block_capital_flow"  # 板块资金流
    BLOCK_KLINE = "block_kline"  # 板块K线
    STOCK_KLINE = "stock_kline"  # 股票K线


class SyncType(Enum):
    """数据同步类型"""

    FULL = "full"  # 全量同步
    INCREMENTAL = "incremental"  # 增量同步


class CrawlerStatus(Enum):
    """爬虫状态"""

    INITIALIZED = "initialized"  # 已初始化
    RUNNING = "running"  # 运行中
    COMPLETED = "completed"  # 已完成
    FAILED = "failed"  # 失败
    PAUSED = "paused"  # 已暂停


@dataclass
class StockIdentifier:
    """股票标识值对象"""

    code: str  # 股票代码
    name: str  # 股票名称
    secid: str  # 东方财富特定标识符

    def __post_init__(self):
        if not self.secid and self.code:
            self.secid = self._generate_secid(self.code)

    def _generate_secid(self, stock_code: str) -> str:
        """从股票代码生成东方财富secid"""
        if stock_code[:3] in ("000", "399"):  # 深证指数
            return f"0.{stock_code}"
        elif stock_code[0] == "6":  # 上海股票
            return f"1.{stock_code}"
        else:  # 深圳股票
            return f"0.{stock_code}"


@dataclass
class BlockIdentifier:
    """板块标识值对象"""

    code: str  # 板块代码
    name: str  # 板块名称

    def get_secid(self) -> str:
        """获取东方财富板块secid"""
        return f"90.{self.code}"


class FinancialData(BaseModel):
    """金融数据值对象"""

    open: float = Field(..., description="开盘价")
    close: float = Field(..., description="收盘价")
    high: float = Field(..., description="最高价")
    low: float = Field(..., description="最低价")
    volume: float = Field(..., description="成交量")
    amount: float = Field(..., description="成交金额")
    change: Optional[float] = Field(None, description="价格变动")
    change_pct: Optional[float] = Field(None, description="价格变动百分比")


class CapitalFlowData(BaseModel):
    """资金流数据值对象"""

    main_inflow: float = Field(..., description="主力资金流入")
    main_outflow: float = Field(..., description="主力资金流出")
    main_net: float = Field(..., description="主力资金净流入")
    large_inflow: float = Field(..., description="大单资金流入")
    large_outflow: float = Field(..., description="大单资金流出")
    large_net: float = Field(..., description="大单资金净流入")
    medium_inflow: float = Field(..., description="中单资金流入")
    medium_outflow: float = Field(..., description="中单资金流出")
    medium_net: float = Field(..., description="中单资金净流入")
    small_inflow: float = Field(..., description="小单资金流入")
    small_outflow: float = Field(..., description="小单资金流出")
    small_net: float = Field(..., description="小单资金净流入")


class CrawlDataPoint(BaseModel):
    """爬取的单个数据点"""

    timestamp: datetime  # 时间戳
    financial_data: FinancialData  # 金融数据
    capital_flow: Optional[CapitalFlowData] = None  # 资金流数据
    raw_data: Dict[str, Any] = Field(default_factory=dict)  # 原始数据


class CrawlResult(BaseModel):
    """爬取结果实体"""

    source_type: DataSourceType  # 数据源类型
    identifier: Union[StockIdentifier, BlockIdentifier]  # 标识符
    data_points: List[CrawlDataPoint]  # 数据点列表
    sync_type: SyncType  # 同步类型
    metadata: Dict[str, Any] = Field(default_factory=dict)  # 元数据


class CrawlerConfiguration(BaseModel):
    """爬虫配置实体"""

    source_type: DataSourceType  # 数据源类型
    sync_type: SyncType = SyncType.INCREMENTAL  # 同步类型
    max_retries: int = 3  # 最大重试次数
    timeout: int = 30  # 超时时间（秒）
    concurrent_requests: int = 10  # 并发请求数
    batch_size: int = 1000  # 批处理大小
    headers: Dict[str, str] = Field(default_factory=dict)  # 请求头
    base_url: str = "https://push2.eastmoney.com"  # 基础URL


class DataPipelineConfiguration(BaseModel):
    """数据管道配置"""

    processing_steps: List[str] = Field(
        default_factory=lambda: [
            "validation",  # 验证
            "transformation",  # 转换
            "enrichment",  # 富化
            "storage",  # 存储
        ]
    )
    validation_rules: Dict[str, Any] = Field(default_factory=dict)  # 验证规则
    transformation_rules: Dict[str, Any] = Field(default_factory=dict)  # 转换规则
    storage_strategy: str = "database"  # 存储策略


class CrawlerEvent(BaseModel):
    """爬虫事件实体"""

    event_type: str  # 事件类型
    timestamp: datetime = Field(default_factory=lambda: datetime.now())  # 时间戳
    crawler_id: str  # 爬虫ID
    status: CrawlerStatus  # 状态
    message: Optional[str] = None  # 消息
    data: Optional[Dict[str, Any]] = None  # 数据


class CrawlerMetrics(BaseModel):
    """爬虫性能指标"""

    start_time: datetime  # 开始时间
    end_time: Optional[datetime] = None  # 结束时间
    total_requests: int = 0  # 总请求数
    successful_requests: int = 0  # 成功请求数
    failed_requests: int = 0  # 失败请求数
    total_data_points: int = 0  # 总数据点数
    processing_time: Optional[float] = None  # 处理时间（秒）

    def duration(self) -> Optional[float]:
        """计算持续时间（秒）"""
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
