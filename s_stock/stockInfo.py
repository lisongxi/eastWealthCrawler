"""
个股基本信息表
"""

from database import mysql1
from peewee import (
    Model,
    BigAutoField,
    CharField,
    DateField,
    IntegerField,
    BooleanField,
)


class StockInfo(Model):
    """个股基本信息"""
    id = BigAutoField(null=False, primary_key=True)
    stock_code = CharField(max_length=12, null=False, verbose_name="股票代码")
    stock_name = CharField(max_length=32, null=False, verbose_name="股票名称")
    
    # 市场：沪市/深市
    market = CharField(max_length=8, null=True, verbose_name="所属市场")
    
    # 股票类型（主板/创业板/科创板等）
    stock_type = CharField(max_length=16, null=True, verbose_name="股票类型")
    
    # 当前价格信息
    current_price = CharField(max_length=16, null=True, verbose_name="当前价格")
    change_pct = CharField(max_length=16, null=True, verbose_name="涨跌幅")
    
    # 状态：1=正常，0=停用
    status = IntegerField(default=1, verbose_name="状态")
    
    # 创建和更新时间
    created_at = DateField(null=False, verbose_name="创建时间")
    updated_at = DateField(null=False, verbose_name="更新时间")

    class Meta:
        database = mysql1
        table_name = "t_stock_info"