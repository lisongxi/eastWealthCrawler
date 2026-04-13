"""通用任务表模型"""

from peewee import BigAutoField, CharField, DateField, Model

from database import mysql1


class CrawlTask(Model):
    """爬虫任务表"""

    id = BigAutoField(null=False, primary_key=True)
    task_type = CharField(max_length=32, null=False, verbose_name="任务类型")
    mode_type = CharField(
        max_length=32, null=False, verbose_name="模式类型（full/incremental）"
    )
    target_code = CharField(max_length=24, null=False, verbose_name="目标代码")
    status = CharField(max_length=16, null=False, verbose_name="状态")
    created_at = DateField(null=False, verbose_name="创建时间")
    updated_at = DateField(null=False, verbose_name="更新时间")

    class Meta:
        database = mysql1
        table_name = "t_crawl_task"
