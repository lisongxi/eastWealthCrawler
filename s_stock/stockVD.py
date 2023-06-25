"""个股数据校验模型
"""
from pydantic import BaseModel


class StockKlineVD(BaseModel):
    """个股历史价格数据校验模型
    """
    stockCode: str  # 股票代码
    stockName: str  # 股票名称
    s_date: str  # 日期
    open_price: str  # 开盘价
    close_price: str  # 收盘价
    top_price: str  # 最高
    low_price: str  # 最低
    turnover: str  # 成交量
    transaction: str  # 成交额
    amplitude: str  # 振幅
    quote_change: str  # 涨跌幅
    change_amount: str  # 涨跌额
    turnover_rate: str  # 换手率
