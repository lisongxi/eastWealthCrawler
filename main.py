"""
爬取东方财富网的股票数据
"""

from crawling import crawler
from function import saveDate

if __name__ == "__main__":
    # 爬取数据
    crawler()

    # 保存同步时间
    saveDate()
