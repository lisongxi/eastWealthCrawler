import json
from yarl import URL
from time import sleep
from random import randint

from config import get_settings, URLs, DataSync
from dataProcessor import to_DB
from proxy import ProxyInfo, testGet_proxyInfo
from errors import ProxyAddrError
from log import LogType, Log
from s_block.blockCrawl import block_cf_crawl, block_price_crawl
from s_block.blockDM import BlockPriceHistory, BlockCFHistory

__SUCCESS_LOG_PATH__ = './logs/success'  # 爬取成功日志
__ERROR_LOG_PATH__ = './logs/errors'  # 错误日志

__SYNC_PATH__ = './settings/sync.json'  # 同步方式

target = URL().build(
    scheme='https',
    host=URLs.t_StockUrl
)

globalSettings = get_settings()


def get_random_sleep():
    """随机睡眠
    """
    sleep(randint(1, 4))


# 使用代理池(暂时没有代理池，下次再用 get_proxyInfo 函数)
try:
    proxyInfo = testGet_proxyInfo()
except ProxyAddrError as err:
    myLog = Log(path=__ERROR_LOG_PATH__, logType=LogType.run_error)
    myLog.add_txt_row(username=globalSettings.sysAdmin, content=err)
    quit()  # 退出进程


# 爬取数据
def crawler():
    """爬取数据
    """
    # 首次运行，默认是全量同步；以后都默认是 增量同步。
    with open(__SYNC_PATH__, 'r', encoding='utf-8') as f1:
        sync = json.load(f1)['sync']

    block_cf_crawl(sync=sync)  # 爬取板块资金流历史数据
    block_price_crawl(sync=sync)  # 爬取板块价格K线图数据

    # 保存到数据库
    to_DB(DB_Model=BlockCFHistory, file_path='板块历史资金流/', sync=sync)
    to_DB(DB_Model=BlockPriceHistory, file_path='板块价格K线数据/', sync=sync)

    # 保存之后执行都是默认 增量同步
    with open(__SYNC_PATH__, 'w', encoding='utf-8') as f2:
        s = {"sync": DataSync.increase}
        json.dump(s, f2)
