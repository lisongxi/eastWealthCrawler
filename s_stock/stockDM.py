"""个股数据库模型
"""
from database import mysql1
from peewee import (
    Model,
    BigAutoField,
    CharField,
    DateField,
    FloatField,
    DecimalField
)


# 个股历史K线数据模型
class StockKline(Model):
    id = BigAutoField(null=False, primary_key=True)
    stockCode = CharField(max_length=24, null=False, verbose_name='股票编号')
    stockName = CharField(max_length=24, null=False, verbose_name='股票名称')
    s_date = DateField(null=False, verbose_name='日期')  # 日期
    open_price = FloatField(null=False, verbose_name='开盘价')
    close_price = FloatField(null=False, verbose_name='收盘价')
    top_price = FloatField(null=False, verbose_name='最高')
    low_price = FloatField(null=False, verbose_name='最低')
    turnover = FloatField(null=False, verbose_name='成交量')
    transaction = FloatField(null=False, verbose_name='成交额')
    amplitude = DecimalField(max_digits=4, decimal_places=2, null=False, verbose_name='振幅')
    quote_change = DecimalField(max_digits=4, decimal_places=2, null=False, verbose_name='涨跌幅')
    change_amount = DecimalField(max_digits=4, decimal_places=2, null=False, verbose_name='涨跌额')
    turnover_rate = DecimalField(max_digits=4, decimal_places=2, null=False, verbose_name='换手率')

    class Meta:
        database = mysql1
        table_name = 't_stock_k'
