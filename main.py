"""
爬取东方财富网的股票数据
"""

from crawling import CrawlData
from function import saveDate

if __name__ == "__main__":
    # 创建实例
    craw = CrawlData()

    # 爬取数据
    craw.crawler()

    # 保存同步时间
    saveDate()
