from playhouse.mysql_ext import JSONField
from config import get_settings
from playhouse.pool import PooledMySQLDatabase
from peewee import (
    Model,
    CompositeKey,
    BigAutoField,
    CharField,
    DateField
)

__MYSQL_PARAMS__ = get_settings().eastWealth.mysql  # 加载数据库配置

# 连接池
mysql1 = PooledMySQLDatabase(
    **__MYSQL_PARAMS__.dict(),
    max_connections=10,  # 最大连接数
    timeout=10,  # 池满阻止秒数
    stale_timeout=300  # 最长连接时间
)


class BlockCFHistory(Model):
    id = BigAutoField(null=False, primary_key=True)  # 一个主键，且不可为空,为每个新记录自动生成一个唯一值
    blockCode = CharField(max_length=24, null=False)  # 短字符串, 需要指定最大长度
    blockName = CharField(max_length=24, null=False)  # 短字符串, 需要指定最大长度
    date = DateField(null=False)  # 日期
    main_net_inflow = CharField(max_length=32, null=False)  # 主力净流入
    small_net_inflow = CharField(max_length=32, null=False)  # 小单净流入
    mid_net_inflow = CharField(max_length=32, null=False)  # 中单净流入
    large_net_inflow = CharField(max_length=32, null=False)  # 大单净流入
    super_large_net_inflow = CharField(max_length=32, null=False)  # 超大单净流入
    main_net_proportion = CharField(max_length=32, null=False)  # 主力净占比
    small_net_proportion = CharField(max_length=32, null=False)  # 小单净占比
    mid_net_proportion = CharField(max_length=32, null=False)  # 中单净占比
    large_net_proportion = CharField(max_length=32, null=False)  # 大单净占比
    super_large_net_proportion = CharField(max_length=32, null=False)  # 超大单净占比
    d_closing_price = CharField(max_length=32, null=False)  # 当日收盘价
    d_quote_change = CharField(max_length=32, null=False)  # 当日涨跌幅

    class Meta:
        database = mysql1
        table_name = 't_block_cf_historys'
