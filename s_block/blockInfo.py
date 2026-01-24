"""
板块基本信息表
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


class BlockInfo(Model):
    """板块基本信息"""
    id = BigAutoField(null=False, primary_key=True)
    block_code = CharField(max_length=24, null=False, verbose_name="板块编号")
    block_name = CharField(max_length=64, null=False, verbose_name="板块名称")
    
    # 板块分类（概念/行业/地域等）
    block_type = CharField(max_length=16, null=True, verbose_name="板块分类")
    
    # 板块热度排名
    hot_rank = IntegerField(null=True, verbose_name="热度排名")
    
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
        table_name = "t_block_info"