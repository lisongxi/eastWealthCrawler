import json
import os.path

from playhouse.pool import PooledMySQLDatabase

from validating import BlockCapitalFlowHistory

from peewee import (
    Model, IntegerField, BigAutoField,
    BigIntegerField, CharField
)

from config import get_settings

__MYSQL_PARAMS__ = get_settings().eastWealth.mysql
__SAVE_DIR__ = './data/'

# 连接池
mysql1 = PooledMySQLDatabase(
    **__MYSQL_PARAMS__.dict(),
    max_connections=10,  # 最大连接数
    timeout=10,  # 池满阻止秒数
    stale_timeout=300  # 最长连接时间
)


class Medins(Model):
    id = BigAutoField(null=False, primary_key=True)
    region_code = IntegerField(null=False)
    medins_code = CharField(max_length=24, null=False)

    class Meta:
        database = mysql1
        table_name = 'medical_institutions'
        # primary_key = 'id'
        indexes = (  # 唯一化约束
            ('region_code', True),  # 后面的逗号不能少
        )


# # 创建表
# mysql1.create_tables([Medins])

tinydict = [
    {'region_code': 12, 'medins_code': 'hah5aha'}, {'region_code': 13, 'medins_code': 'haha546856ha'},
    {'region_code': 14, 'medins_code': 'hah5aha'}, {'region_code': 15, 'medins_code': '674575ggg'},
    {'region_code': 16, 'medins_code': 'hah4aha'}, {'region_code': 17, 'medins_code': 'haha75546ha'},
    {'region_code': 18, 'medins_code': 'haha5785ha'}, {'region_code': 19, 'medins_code': 'hacxsfasdghaha'},
    {'region_code': 20, 'medins_code': 'haha587ha'}, {'region_code': 21, 'medins_code': 'fgfsdhfd'},
    {'region_code': 22, 'medins_code': 'ha54haha'}]

with Medins._meta.database.atomic():
    for dd in tinydict:  # 一次插入1000条. shape返回一个元组，包含Dataframe的行数和列数，所以用索引0来访问行数
        (Medins
         .insert_many([dict(dd)])
         .execute()
         )
