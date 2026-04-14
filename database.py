"""
数据库连接配置
"""

from datetime import datetime

from peewee import MySQLDatabase

from settings.settings import get_settings

# 获取配置信息
settings = get_settings()

# 创建MySQL数据库连接
mysql1 = MySQLDatabase(
    settings.eastWealth.mysql.database,
    host=settings.eastWealth.mysql.host,
    port=settings.eastWealth.mysql.port,
    user=settings.eastWealth.mysql.user,
    password=settings.eastWealth.mysql.password,
)

# 模型导入必须在 mysql1 定义之后，以避免循环导入
from models.block.blockDM import BlockCapitalFlow, BlockKline
from models.block.blockInfo import BlockInfo
from models.common.task import CrawlTask
from models.stock.stockDM import StockKline
from models.stock.stockInfo import StockInfo


def init_db():
    """初始化数据库连接"""
    try:
        mysql1.connect()
        print(f"数据库连接成功: {settings.eastWealth.mysql.database}")
        return True
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return False


def close_db():
    """关闭数据库连接"""
    if not mysql1.is_closed():
        mysql1.close()
        print("数据库连接已关闭")


def create_tables():
    """创建所有数据表"""
    try:
        with mysql1:
            mysql1.create_tables(
                [
                    BlockCapitalFlow,
                    BlockKline,
                    StockKline,
                    BlockInfo,
                    StockInfo,
                    CrawlTask,
                ]
            )
            print("所有数据表创建成功")
            return True
    except Exception as e:
        print(f"创建数据表失败: {e}")
        return False


def ensure_tables_exist():
    """检查表是否存在，不存在则创建（增量模式用）"""
    tables = [BlockCapitalFlow, BlockKline, StockKline, BlockInfo, StockInfo, CrawlTask]
    try:
        if mysql1.is_closed():
            mysql1.connect()
        with mysql1:
            # 检查并创建不存在的表
            for table in tables:
                if not table.table_exists():
                    mysql1.create_tables([table])
            print("数据表检查完成")
        return True
    except Exception as e:
        print(f"检查数据表失败: {e}")
        return False


def ensure_info_tables():
    """确保基本信息表存在，不存在则创建"""
    tables = [BlockInfo, StockInfo]
    try:
        if mysql1.is_closed():
            mysql1.connect()
        with mysql1:
            for table in tables:
                if not table.table_exists():
                    mysql1.create_tables([table])
        return True
    except Exception as e:
        print(f"创建基本信息表失败: {e}")
        return False


# ========== 任务表操作函数 ==========


def is_task_completed(task_type: str, target_code: str, mode_type: str) -> bool:
    """检查任务是否已完成"""
    try:
        if mysql1.is_closed():
            mysql1.connect()

        if not mode_type or not target_code or not task_type:
            raise ValueError("task_type, target_code 和 mode_type 都必须提供")

        query = CrawlTask.select().where(
            CrawlTask.task_type == task_type,
            CrawlTask.target_code == target_code,
            CrawlTask.mode_type == mode_type,
            CrawlTask.status == "completed",
        )

        task = query.first()

        return task is not None
    except Exception as e:
        print(f"检查任务状态失败: {e}")
        return False


def mark_task_completed(task_type: str, target_code: str, mode_type: str) -> bool:
    """标记任务已完成"""
    try:
        if mysql1.is_closed():
            mysql1.connect()

        today = datetime.now().date()

        # 如果未指定mode_type，默认为"full"
        if not mode_type:
            mode_type = "full"

        with mysql1:
            # 检查是否存在记录
            query = CrawlTask.select().where(
                CrawlTask.task_type == task_type,
                CrawlTask.target_code == target_code,
            )

            # 如果指定了mode_type，也需要匹配
            if mode_type:
                query = query.where(CrawlTask.mode_type == mode_type)

            existing = query.first()

            if existing:
                # 更新状态
                CrawlTask.update(status="completed", updated_at=today).where(
                    CrawlTask.task_type == task_type,
                    CrawlTask.target_code == target_code,
                    CrawlTask.mode_type == mode_type,
                ).execute()
            else:
                # 插入新记录
                CrawlTask.create(
                    task_type=task_type,
                    target_code=target_code,
                    mode_type=mode_type,
                    status="completed",
                    created_at=today,
                    updated_at=today,
                )

        return True
    except Exception as e:
        print(f"标记任务完成失败: {e}")
        return False


def reset_task(target_code: str, task_type: str) -> bool:
    """重置任务状态（全量模式前删除数据后调用）"""
    try:
        if mysql1.is_closed():
            mysql1.connect()

        with mysql1:
            if task_type:
                # 重置指定类型任务
                CrawlTask.update(
                    status="pending", updated_at=datetime.now().date()
                ).where(
                    CrawlTask.task_type == task_type,
                    CrawlTask.target_code == target_code,
                ).execute()
            else:
                # 重置该目标所有任务
                CrawlTask.update(
                    status="pending", updated_at=datetime.now().date()
                ).where(CrawlTask.target_code == target_code).execute()

        return True
    except Exception as e:
        print(f"重置任务失败: {e}")
        return False
