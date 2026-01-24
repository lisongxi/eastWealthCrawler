"""
爬虫策略模式实现
允许使用不同的爬虫策略进行互换
"""

from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
import logging
from datetime import datetime

from src.domain.entities import (
    CrawlerConfiguration,
    CrawlResult,
    DataSourceType,
    StockIdentifier,
    BlockIdentifier,
    FinancialData,
    CapitalFlowData,
    CrawlDataPoint,
)


class CrawlerStrategy(ABC):
    """爬虫策略抽象基类"""

    def __init__(self, config: CrawlerConfiguration):
        self.config = config  # 配置
        self.logger = logging.getLogger(__name__)  # 日志记录器

    @abstractmethod
    async def crawl(self) -> List[CrawlResult]:
        """执行爬虫策略"""
        pass

    @abstractmethod
    async def get_target_list(self) -> List[Dict[str, Any]]:
        """获取要爬取的目标列表"""
        pass

    @abstractmethod
    async def crawl_single_target(
        self, target: Dict[str, Any]
    ) -> Optional[CrawlResult]:
        """爬取单个目标的数据"""
        pass


class BlockCrawlerStrategy(CrawlerStrategy):
    """板块数据爬虫策略"""

    async def crawl(self) -> List[CrawlResult]:
        """使用此策略爬取板块数据"""
        self.logger.info(
            f"开始 {self.__class__.__name__} 爬取 {self.config.source_type.value}"
        )

        targets = await self.get_target_list()
        results = []

        for target in targets:
            try:
                result = await self.crawl_single_target(target)
                if result:
                    results.append(result)
            except Exception as e:
                self.logger.error(f"爬取目标 {target} 失败: {e}")

        self.logger.info(f"完成 {self.__class__.__name__}，获得 {len(results)} 个结果")
        return results

    async def get_target_list(self) -> List[Dict[str, Any]]:
        """获取要爬取的板块列表（从东方财富API获取真实数据）"""
        # 导入真实的板块获取函数
        from s_block.blockCrawl import get_block_list_db

        blocks = get_block_list_db(limit=10)

        # 根据配置类型添加类型标记
        data_type = (
            "capital_flow"
            if self.config.source_type == DataSourceType.BLOCK_CAPITAL_FLOW
            else "price"
        )
        for block in blocks:
            block["type"] = data_type

        self.logger.info(f"从东方财富获取到 {len(blocks)} 个板块")
        return blocks

    async def crawl_single_target(
        self, target: Dict[str, Any]
    ) -> Optional[CrawlResult]:
        """爬取单个板块的真实数据"""
        from s_block.blockCrawl import (
            get_block_kline_db,
            get_block_capital_flow_db,
            parse_block_price_kline,
            parse_block_capital_flow,
        )

        block_code = target["code"]
        block_name = target["name"]
        is_capital_flow = target.get("type") == "capital_flow"

        identifier = BlockIdentifier(code=block_code, name=block_name)
        data_points = []

        try:
            if (
                is_capital_flow
                or self.config.source_type == DataSourceType.BLOCK_CAPITAL_FLOW
            ):
                # 获取板块资金流数据
                klines = get_block_capital_flow_db(block_code, block_name)
                if not klines:
                    self.logger.warning(f"板块 {block_name} ({block_code}) 没有资金流数据")
                    return None

                for kline_str in klines:
                    parsed = parse_block_capital_flow(kline_str, block_code, block_name)
                    if parsed:
                        try:
                            date = datetime.strptime(parsed["b_date"], "%Y-%m-%d")
                        except:
                            continue

                        # 资金流API只提供收盘价，open/high/low/volume/amount都设为0或收盘价
                        closing_price = float(parsed.get("d_closing_price", 0) or 0)
                        financial_data = FinancialData(
                            open=closing_price,
                            close=closing_price,
                            high=closing_price,
                            low=closing_price,
                            volume=0.0,
                            amount=0.0,
                            change=None,
                            change_pct=None,
                        )

                        # 填充资金流数据
                        main_net = parsed.get("main_net_inflow", 0)
                        large_net = parsed.get("large_net_inflow", 0)
                        mid_net = parsed.get("mid_net_inflow", 0)
                        small_net = parsed.get("small_net_inflow", 0)
                        super_large_net = parsed.get("super_large_net_inflow", 0)

                        # 根据净流入计算流入和流出
                        capital_flow = CapitalFlowData(
                            main_inflow=main_net if main_net > 0 else 0,
                            main_outflow=-main_net if main_net < 0 else 0,
                            main_net=main_net,
                            large_inflow=large_net if large_net > 0 else 0,
                            large_outflow=-large_net if large_net < 0 else 0,
                            large_net=large_net,
                            medium_inflow=mid_net if mid_net > 0 else 0,
                            medium_outflow=-mid_net if mid_net < 0 else 0,
                            medium_net=mid_net,
                            small_inflow=small_net if small_net > 0 else 0,
                            small_outflow=-small_net if small_net < 0 else 0,
                            small_net=small_net,
                        )

                        data_points.append(
                            CrawlDataPoint(
                                timestamp=date,
                                financial_data=financial_data,
                                capital_flow=capital_flow,
                                raw_data=parsed,
                            )
                        )
            else:
                # 获取板块K线数据
                klines = get_block_kline_db(block_code, block_name)

                for kline_str in klines:
                    parsed = parse_block_price_kline(kline_str, block_code, block_name)
                    if parsed:
                        try:
                            date = datetime.strptime(parsed["b_date"], "%Y-%m-%d")
                        except:
                            continue

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

            self.logger.info(f"板块 {block_name} 获取到 {len(data_points)} 条数据")

        except Exception as e:
            self.logger.error(f"爬取板块 {block_name} 数据失败: {e}")

        if not data_points:
            return None

        return CrawlResult(
            source_type=self.config.source_type,
            identifier=identifier,
            data_points=data_points,
            sync_type=self.config.sync_type,
        )


class StockCrawlerStrategy(CrawlerStrategy):
    """股票数据爬虫策略"""

    async def crawl(self) -> List[CrawlResult]:
        """使用此策略爬取股票数据"""
        self.logger.info(
            f"开始 {self.__class__.__name__} 爬取 {self.config.source_type.value}"
        )

        targets = await self.get_target_list()
        results = []

        for target in targets:
            try:
                result = await self.crawl_single_target(target)
                if result:
                    results.append(result)
            except Exception as e:
                self.logger.error(f"爬取目标 {target} 失败: {e}")

        self.logger.info(f"完成 {self.__class__.__name__}，获得 {len(results)} 个结果")
        return results

    async def get_target_list(self) -> List[Dict[str, Any]]:
        """获取要爬取的股票列表（从东方财富API获取真实数据）"""
        import requests

        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1,
            "pz": 20,  # 获取20只股票作为示例
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
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
                for item in data["data"]["diff"][:20]:  # 限制为20只股票
                    stocks.append(
                        {"code": item.get("f12", ""), "name": item.get("f14", "")}
                    )
                self.logger.info(f"从东方财富获取到 {len(stocks)} 只股票")
                return stocks
        except Exception as e:
            self.logger.error(f"获取股票列表失败: {e}")

        return []

    async def crawl_single_target(
        self, target: Dict[str, Any]
    ) -> Optional[CrawlResult]:
        """爬取单只股票的真实K线数据"""
        import requests
        import re
        import json

        stock_code = target["code"]
        stock_name = target["name"]

        # 生成secid
        if stock_code[:3] in ("000", "399"):
            secid = f"0.{stock_code}"
        elif stock_code[0] == "6":
            secid = f"1.{stock_code}"
        else:
            secid = f"0.{stock_code}"

        identifier = StockIdentifier(code=stock_code, name=stock_name, secid=secid)

        # 获取K线数据
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",  # 日K
            "fqt": "1",  # 前复权
            "beg": "0",
            "end": "20500101",
            "lmt": "1000000",
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": f"https://quote.eastmoney.com/{stock_code}.html",
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
                self.logger.warning(f"股票 {stock_name} 没有K线数据")
                return None

            klines = data["data"]["klines"]
            data_points = []

            for kline_str in klines[-100:]:  # 获取最近100天数据
                parts = kline_str.split(",")
                if len(parts) >= 11:
                    try:
                        date = datetime.strptime(parts[0], "%Y-%m-%d")
                    except:
                        continue

                    financial_data = FinancialData(
                        open=float(parts[1]) if len(parts) > 1 and parts[1] else 0.0,
                        close=float(parts[2]) if len(parts) > 2 and parts[2] else 0.0,
                        high=float(parts[3]) if len(parts) > 3 and parts[3] else 0.0,
                        low=float(parts[4]) if len(parts) > 4 and parts[4] else 0.0,
                        volume=float(parts[5]) if len(parts) > 5 and parts[5] else 0.0,
                        amount=float(parts[6]) if len(parts) > 6 and parts[6] else 0.0,
                        change=None,
                        change_pct=None,
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
                                "quote_change": (
                                    float(parts[8])
                                    if len(parts) > 8 and parts[8]
                                    else 0
                                ),
                                "change_amount": (
                                    float(parts[9])
                                    if len(parts) > 9 and parts[9]
                                    else 0
                                ),
                            },
                        )
                    )

            self.logger.info(f"股票 {stock_name} 获取到 {len(data_points)} 条K线数据")

            if not data_points:
                return None

            return CrawlResult(
                source_type=self.config.source_type,
                identifier=identifier,
                data_points=data_points,
                sync_type=self.config.sync_type,
            )

        except Exception as e:
            self.logger.error(f"爬取股票 {stock_name} 数据失败: {e}")
            return None


class CrawlerStrategyFactory:
    """创建爬虫策略的工厂类"""

    @staticmethod
    def create_strategy(config: CrawlerConfiguration) -> CrawlerStrategy:
        """根据配置创建适当的爬虫策略"""
        if config.source_type in (
            DataSourceType.BLOCK_CAPITAL_FLOW,
            DataSourceType.BLOCK_KLINE,
        ):
            return BlockCrawlerStrategy(config)
        elif config.source_type == DataSourceType.STOCK_KLINE:
            return StockCrawlerStrategy(config)
        else:
            raise ValueError(f"未知源类型: {config.source_type}")


class StrategyContext:
    """使用爬虫策略的上下文"""

    def __init__(self, strategy: CrawlerStrategy):
        self._strategy = strategy  # 策略

    @property
    def strategy(self) -> CrawlerStrategy:
        """获取当前策略"""
        return self._strategy

    @strategy.setter
    def strategy(self, strategy: CrawlerStrategy):
        """设置当前策略"""
        self._strategy = strategy

    async def execute(self) -> List[CrawlResult]:
        """执行当前策略"""
        return await self._strategy.crawl()


class CompositeCrawlerStrategy(CrawlerStrategy):
    """组合多个策略的复合策略"""

    def __init__(self, config: CrawlerConfiguration):
        super().__init__(config)
        self._strategies: List[CrawlerStrategy] = []  # 策略列表

    def add_strategy(self, strategy: CrawlerStrategy):
        """向复合策略添加策略"""
        self._strategies.append(strategy)

    async def crawl(self) -> List[CrawlResult]:
        """执行复合中的所有策略"""
        all_results = []
        for strategy in self._strategies:
            results = await strategy.crawl()
            all_results.extend(results)
        return all_results

    async def get_target_list(self) -> List[Dict[str, Any]]:
        """从所有策略获取组合目标列表"""
        all_targets = []
        for strategy in self._strategies:
            targets = await strategy.get_target_list()
            all_targets.extend(targets)
        return all_targets

    async def crawl_single_target(
        self, target: Dict[str, Any]
    ) -> Optional[CrawlResult]:
        """复合策略不实现单目标爬取"""
        raise NotImplementedError("复合策略不实现单目标爬取")


class AdaptiveCrawlerStrategy(CrawlerStrategy):
    """可以在不同方法之间切换的自适应策略"""

    def __init__(self, config: CrawlerConfiguration):
        super().__init__(config)
        is_block_type = config.source_type in (
            DataSourceType.BLOCK_CAPITAL_FLOW,
            DataSourceType.BLOCK_KLINE,
        )
        self._primary_strategy = (
            BlockCrawlerStrategy(config)
            if is_block_type
            else StockCrawlerStrategy(config)
        )
        self._fallback_strategy = (
            StockCrawlerStrategy(config)
            if is_block_type
            else BlockCrawlerStrategy(config)
        )

    async def crawl(self) -> List[CrawlResult]:
        """尝试主策略，必要时回退到备用策略"""
        try:
            return await self._primary_strategy.crawl()
        except Exception as e:
            self.logger.warning(f"主策略失败，尝试备用策略: {e}")
            return await self._fallback_strategy.crawl()

    async def get_target_list(self) -> List[Dict[str, Any]]:
        """从主策略获取目标列表"""
        return await self._primary_strategy.get_target_list()

    async def crawl_single_target(
        self, target: Dict[str, Any]
    ) -> Optional[CrawlResult]:
        """使用主策略爬取"""
        return await self._primary_strategy.crawl_single_target(target)


class StrategySelector:
    """根据条件选择适当策略"""

    def __init__(self):
        self._strategy_map = {
            DataSourceType.BLOCK_CAPITAL_FLOW: BlockCrawlerStrategy,
            DataSourceType.BLOCK_KLINE: BlockCrawlerStrategy,
            DataSourceType.STOCK_KLINE: StockCrawlerStrategy,
        }

    def select_strategy(self, config: CrawlerConfiguration) -> CrawlerStrategy:
        """根据配置选择策略"""
        strategy_class = self._strategy_map.get(config.source_type)
        if strategy_class:
            return strategy_class(config)
        else:
            raise ValueError(f"源类型没有可用的策略: {config.source_type}")

    def register_strategy(self, source_type: DataSourceType, strategy_class: type):
        """为源类型注册自定义策略"""
        self._strategy_map[source_type] = strategy_class
