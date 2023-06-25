"""定时任务
"""
import os

from apscheduler.schedulers.background import BackgroundScheduler
from time import sleep

from crawling import CrawlData

scheduler = BackgroundScheduler()


def CrawEastWealth():
    myCrawl = CrawlData()
    myCrawl.crawler()


scheduler.add_job(
    # 由 trigger 参数指定触发器，来指定调用函数的方式。
    # 1、interval：以固定的时间间隔执行。
    # 2、date：在特定的时间日期执行。
    # 3、cron：指定在某个时间点执行或循环执行。

    CrawEastWealth,
    'interval',
    minutes=5,
    start_date='2023-06-25 16:26:00',
    end_date='2023-06-25 17:00:00',
    timezone='Asia/Shanghai'
)

if __name__ == '__main__':
    # 开始执行任务
    scheduler.start()
