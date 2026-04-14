"""
数据管道实现
统一的数据处理管道，确保一致的数据流
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Union

from src.domain.entities import CrawlDataPoint, CrawlResult


class PipelineStepType(Enum):
    """管道步骤类型"""

    VALIDATION = "validation"  # 验证
    TRANSFORMATION = "transformation"  # 转换
    ENRICHMENT = "enrichment"  # 富化
    STORAGE = "storage"  # 存储
    MONITORING = "monitoring"  # 监控


@dataclass
class PipelineStep:
    """管道步骤配置"""

    step_type: PipelineStepType  # 步骤类型
    handler: Callable  # 处理函数
    priority: int = 0  # 优先级
    enabled: bool = True  # 是否启用


class PipelineStepHandler(ABC):
    """管道步骤处理器抽象基类"""

    @abstractmethod
    async def process(
        self, data: Union[CrawlResult, CrawlDataPoint, Any]
    ) -> Union[CrawlResult, CrawlDataPoint, Any]:
        """在此管道步骤中处理数据"""
        pass


class ValidationHandler(PipelineStepHandler):
    """验证管道步骤"""

    async def process(self, data: CrawlResult) -> CrawlResult:
        """验证爬取结果数据"""
        if not data or not data.data_points:
            raise ValueError("空的爬取结果")

        for data_point in data.data_points:
            if not data_point.timestamp:
                raise ValueError("数据点缺少时间戳")
            if not data_point.financial_data:
                raise ValueError("数据点缺少金融数据")

        return data


class TransformationHandler(PipelineStepHandler):
    """转换管道步骤"""

    async def process(self, data: CrawlResult) -> CrawlResult:
        """转换数据格式"""
        # 添加必要的转换
        for data_point in data.data_points:
            # 示例：计算附加字段
            if data_point.financial_data.open and data_point.financial_data.close:
                change = (
                    data_point.financial_data.close - data_point.financial_data.open
                )
                change_pct = (
                    (change / data_point.financial_data.open) * 100
                    if data_point.financial_data.open != 0
                    else 0
                )

                data_point.financial_data.change = round(change, 2)
                data_point.financial_data.change_pct = round(change_pct, 2)

        return data


class EnrichmentHandler(PipelineStepHandler):
    """富化管道步骤"""

    async def process(self, data: CrawlResult) -> CrawlResult:
        """用附加信息丰富数据"""
        # 添加元数据或附加上下文
        data.metadata["processed_timestamp"] = datetime.now().isoformat()
        data.metadata["data_points_count"] = len(data.data_points)

        return data


class StorageHandler(PipelineStepHandler):
    """存储管道步骤 - 保存数据到数据库"""

    def __init__(self):
        from database import init_db, mysql1

        self.db = mysql1
        self.logger = logging.getLogger(__name__)
        # 确保数据库已连接
        try:
            init_db()
        except:
            pass

    async def process(self, data: CrawlResult) -> CrawlResult:
        """保存爬取结果到数据库"""
        from src.domain.entities import DataSourceType

        try:
            # 根据数据源类型保存到不同的表
            if data.source_type == DataSourceType.BLOCK_KLINE:
                await self._save_block_kline(data)
            elif data.source_type == DataSourceType.BLOCK_CAPITAL_FLOW:
                await self._save_block_capital_flow(data)
            elif data.source_type == DataSourceType.STOCK_KLINE:
                await self._save_stock_kline(data)

            self.logger.info(
                f"成功保存 {len(data.data_points)} 条数据到数据库: {data.source_type.value}"
            )
            return data

        except Exception as e:
            self.logger.error(f"保存数据到数据库失败: {e}")
            raise

    async def _save_block_kline(self, data: CrawlResult):
        """保存板块K线数据"""
        from models.block.blockDM import BlockKline

        block_code = data.identifier.code
        block_name = data.identifier.name

        count = 0
        with self.db.atomic():
            for data_point in data.data_points:
                try:
                    BlockKline.create(
                        block_code=block_code,
                        block_name=block_name,
                        b_date=data_point.timestamp.date(),
                        open_price=data_point.financial_data.open,
                        close_price=data_point.financial_data.close,
                        top_price=data_point.financial_data.high,
                        low_price=data_point.financial_data.low,
                        turnover=data_point.financial_data.volume,
                        transaction=data_point.financial_data.amount,
                        amplitude=data_point.raw_data.get("amplitude", 0),
                        quote_change=data_point.financial_data.change_pct or 0,
                        change_amount=data_point.financial_data.change or 0,
                        turnover_rate=data_point.raw_data.get("turnover_rate", 0),
                    )
                    count += 1
                except Exception as e:
                    self.logger.warning(f"保存价格数据失败: {e}")

        data.metadata["saved_count"] = count

    async def _save_block_capital_flow(self, data: CrawlResult):
        """保存板块资金流数据"""
        from models.block.blockDM import BlockCapitalFlow

        block_code = data.identifier.code
        block_name = data.identifier.name

        count = 0
        with self.db.atomic():
            for data_point in data.data_points:
                try:
                    if data_point.capital_flow:
                        BlockCapitalFlow.create(
                            block_code=block_code,
                            block_name=block_name,
                            b_date=data_point.timestamp.date(),
                            main_net_inflow=data_point.capital_flow.main_net,
                            small_net_inflow=data_point.capital_flow.small_net,
                            mid_net_inflow=data_point.capital_flow.medium_net,
                            large_net_inflow=data_point.capital_flow.large_net,
                            super_large_net_inflow=0,  # 资金流API不提供单独的超级大单数据
                            main_net_proportion=0,
                            small_net_proportion=0,
                            mid_net_proportion=0,
                            large_net_proportion=0,
                            super_large_net_proportion=0,
                            d_closing_price=data_point.financial_data.close,
                            d_quote_change=data_point.raw_data.get("d_quote_change", 0),
                        )
                        count += 1
                except Exception as e:
                    self.logger.warning(f"保存资金流数据失败: {e}")

        data.metadata["saved_count"] = count

    async def _save_stock_kline(self, data: CrawlResult):
        """保存股票K线数据"""
        from models.stock.stockDM import StockKline

        stock_code = data.identifier.code
        stock_name = data.identifier.name

        count = 0
        with self.db.atomic():
            for data_point in data.data_points:
                try:
                    StockKline.create(
                        stock_code=stock_code,
                        stock_name=stock_name,
                        s_date=data_point.timestamp.date(),
                        open_price=data_point.financial_data.open,
                        close_price=data_point.financial_data.close,
                        top_price=data_point.financial_data.high,
                        low_price=data_point.financial_data.low,
                        turnover=data_point.financial_data.volume,
                        transaction=data_point.financial_data.amount,
                        amplitude=data_point.raw_data.get("amplitude", 0),
                        quote_change=data_point.financial_data.change_pct or 0,
                        change_amount=data_point.financial_data.change or 0,
                        turnover_rate=data_point.raw_data.get("turnover_rate", 0),
                    )
                    count += 1
                except Exception as e:
                    self.logger.warning(f"保存股票数据失败: {e}")

        data.metadata["saved_count"] = count


class DataPipeline:
    """数据处理管道"""

    def __init__(self):
        self._steps: List[PipelineStep] = []  # 管道步骤列表
        self._logger = logging.getLogger(__name__)  # 日志记录器

    def add_step(
        self,
        step_type: PipelineStepType,
        handler: PipelineStepHandler,
        priority: int = 0,
        enabled: bool = True,
    ):
        """添加管道步骤"""
        step = PipelineStep(
            step_type=step_type,
            handler=handler.process,
            priority=priority,
            enabled=enabled,
        )
        self._steps.append(step)
        self._steps.sort(key=lambda x: x.priority)

    def remove_step(self, step_type: PipelineStepType):
        """按类型移除管道步骤"""
        self._steps = [step for step in self._steps if step.step_type != step_type]

    async def process(self, data: CrawlResult) -> CrawlResult:
        """通过管道处理数据"""
        self._logger.info(f"开始管道处理: {data.source_type.value}")

        try:
            result = data
            for step in self._steps:
                if step.enabled:
                    self._logger.debug(f"处理步骤: {step.step_type.value}")
                    result = await step.handler(result)

            self._logger.info(f"管道处理完成: {data.source_type.value}")
            return result

        except Exception as e:
            self._logger.error(f"管道处理失败: {e}")
            raise

    def get_steps(self) -> List[PipelineStep]:
        """获取所有管道步骤"""
        return self._steps.copy()

    def get_metrics(self) -> Dict:
        """获取管道指标"""
        return {"total_processed": 0, "average_processing_time": 0}


class PipelineFactory:
    """创建数据管道的工厂类"""

    @staticmethod
    def create_standard_pipeline() -> DataPipeline:
        """创建标准数据处理管道"""
        pipeline = DataPipeline()

        # 按顺序添加标准步骤
        pipeline.add_step(PipelineStepType.VALIDATION, ValidationHandler(), priority=10)
        pipeline.add_step(
            PipelineStepType.TRANSFORMATION, TransformationHandler(), priority=20
        )
        pipeline.add_step(PipelineStepType.ENRICHMENT, EnrichmentHandler(), priority=30)
        pipeline.add_step(PipelineStepType.STORAGE, StorageHandler(), priority=40)

        return pipeline

    @staticmethod
    def create_custom_pipeline(steps_config: List[Dict]) -> DataPipeline:
        """从配置创建自定义管道"""
        pipeline = DataPipeline()

        for config in steps_config:
            step_type = PipelineStepType(config["type"])
            priority = config.get("priority", 0)
            enabled = config.get("enabled", True)

            if step_type == PipelineStepType.VALIDATION:
                handler = ValidationHandler()
            elif step_type == PipelineStepType.TRANSFORMATION:
                handler = TransformationHandler()
            elif step_type == PipelineStepType.ENRICHMENT:
                handler = EnrichmentHandler()
            else:
                continue

            pipeline.add_step(step_type, handler, priority, enabled)

        return pipeline


class PipelineStepDecorator:
    """管道步骤装饰器"""

    def __init__(self, step_type: PipelineStepType, priority: int = 0):
        self.step_type = step_type  # 步骤类型
        self.priority = priority  # 优先级

    def __call__(self, handler_class: type):
        """装饰器实现"""
        if not issubclass(handler_class, PipelineStepHandler):
            raise ValueError(f"{handler_class.__name__} 必须继承自 PipelineStepHandler")

        # 存储元数据 - 使用setattr避免类型检查警告
        setattr(handler_class, "_pipeline_step_type", self.step_type)
        setattr(handler_class, "_pipeline_priority", self.priority)

        return handler_class


# 装饰器使用示例
@PipelineStepDecorator(PipelineStepType.VALIDATION, priority=10)
class CustomValidationHandler(ValidationHandler):
    """带装饰器的自定义验证处理器"""

    def __init__(self):
        super().__init__()
        self._logger = logging.getLogger(__name__)

    async def process(self, data: CrawlResult) -> CrawlResult:
        """自定义验证逻辑"""
        # 首先调用父类验证
        result = await super().process(data)

        # 添加自定义验证规则
        if len(data.data_points) > 1000:
            self._logger.warning(f"大数据集: {len(data.data_points)} 个数据点")

        return result


class PipelineMonitor:
    """监控管道执行"""

    def __init__(self, pipeline: DataPipeline):
        self.pipeline = pipeline  # 数据管道
        self.metrics = {
            "total_processed": 0,  # 总处理数
            "processing_times": [],  # 处理时间列表
            "step_times": {},  # 步骤时间
        }

    async def monitor_process(self, data: CrawlResult) -> CrawlResult:
        """监控管道处理"""
        start_time = datetime.now()

        # 通过管道处理
        result = await self.pipeline.process(data)

        # 记录指标
        processing_time = (datetime.now() - start_time).total_seconds()
        self.metrics["total_processed"] += 1
        self.metrics["processing_times"].append(processing_time)

        return result

    def get_metrics(self) -> Dict:
        """获取管道指标"""
        avg_time = (
            sum(self.metrics["processing_times"])
            / len(self.metrics["processing_times"])
            if self.metrics["processing_times"]
            else 0
        )
        return {**self.metrics, "average_processing_time": avg_time}
