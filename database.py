from config import get_settings
from playhouse.pool import PooledMySQLDatabase

__MYSQL_PARAMS__ = get_settings().eastWealth.mysql  # 加载数据库配置

# 连接池
mysql1 = PooledMySQLDatabase(
    **__MYSQL_PARAMS__.dict(),
    max_connections=10,  # 最大连接数
    timeout=10,  # 池满阻止秒数
    stale_timeout=300  # 最长连接时间
)
