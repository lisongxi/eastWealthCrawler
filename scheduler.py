"""定时任务
"""

from apscheduler.schedulers.background import BackgroundScheduler
from time import sleep

from crawling import CrawlData
from function import saveDate

scheduler = BackgroundScheduler()  # 创建定时任务


def CrawEastWealth():
    myCrawl = CrawlData()
    myCrawl.crawler()  # 爬取数据
    saveDate()  # 保存同步时间


# 由 trigger 参数指定触发器，来指定调用函数的方式。
# 1、interval：以固定的时间间隔执行。
# 2、date：在特定的时间日期执行。
# 3、cron：指定在某个时间点执行或循环执行。

scheduler.add_job(
    CrawEastWealth,
    'interval',
    minutes=5,
    start_date='2023-06-26 17:55:00',
    end_date='2023-06-26 19:00:00',
    timezone='Asia/Shanghai'
)

if __name__ == '__main__':
    # 开始执行任务
    scheduler.start()

    while True:  # 保持进程待命
        sleep(1)
