import json
from yarl import URL

from config import get_settings, URLs
from dataProcessor import to_DB

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
        s = {"sync": True}
        json.dump(s, f2)
