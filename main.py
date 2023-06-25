"""
爬取东方财富网的股票数据
"""
from datetime import datetime

from crawling import crawler
from function import saveDate

if __name__ == "__main__":
    start = datetime.now()  # 开始时间

    # 爬取数据
    # 已把爬取个股的函数crawlStockKline注释掉了，因为数量太多了，如有需要可以自行去掉注释。
    crawler()

    end = datetime.now()  # 结束时间

    print(f"总耗时{(end - start).seconds}秒")

    # 保存同步时间
    saveDate()

    """
    如果运行没报错，但是爬取数据为空，那就要查看 sync.json 和 data.json 两个文件了
    当sync为true时，只会爬取日期大于date的数据
    """
