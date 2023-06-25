from database import mysql1
from peewee import (
    Model,
    CompositeKey,
    BigAutoField,
    CharField,
    DateField,
    DecimalField,
    FloatField
)


# 板块历史资金流数据模型
class BlockCFHistory(Model):
    id = BigAutoField(null=False, primary_key=True)  # 一个主键，且不可为空,为每个新记录自动生成一个唯一值
    blockCode = CharField(max_length=24, null=False, verbose_name='板块编号')  # 短字符串, 需要指定最大长度
    blockName = CharField(max_length=24, null=False, verbose_name='板块名称')  # 短字符串, 需要指定最大长度
    b_date = DateField(null=False, verbose_name='日期')  # 日期
    main_net_inflow = FloatField(null=False, verbose_name='主力净流入')
    small_net_inflow = FloatField(null=False, verbose_name='小单净流入')
    mid_net_inflow = FloatField(null=False, verbose_name='中单净流入')
    large_net_inflow = FloatField(null=False, verbose_name='大单净流入')
    super_large_net_inflow = FloatField(null=False, verbose_name='超大单净流入')
    main_net_proportion = DecimalField(max_digits=4, decimal_places=2, null=False, verbose_name='主力净占比')
    small_net_proportion = DecimalField(max_digits=4, decimal_places=2, null=False, verbose_name='小单净占比')
    mid_net_proportion = DecimalField(max_digits=4, decimal_places=2, null=False, verbose_name='中单净占比')
    large_net_proportion = DecimalField(max_digits=4, decimal_places=2, null=False, verbose_name='大单净占比')
    super_large_net_proportion = DecimalField(max_digits=4, decimal_places=2, null=False, verbose_name='超大单净占比')
    d_closing_price = FloatField(null=False, verbose_name='当日收盘价')
    d_quote_change = DecimalField(max_digits=4, decimal_places=2, null=False, verbose_name='当日涨跌幅')

    class Meta:
        database = mysql1
        table_name = 't_block_cf_history'


# 板块历史价格数据模型
class BlockPriceHistory(Model):
    id = BigAutoField(null=False, primary_key=True)
    blockCode = CharField(max_length=24, null=False, verbose_name='板块编号')
    blockName = CharField(max_length=24, null=False, verbose_name='板块名称')
    b_date = DateField(null=False, verbose_name='日期')  # 日期
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
        table_name = 't_block_price'
