"""股票板块相关
使用gevent库进行并发
"""
import time

import gevent
from gevent import monkey

monkey.patch_all()  # 注意，这三个gevent相关的一定要放在其他库之前import

from yarl import URL
import requests

from s_block.blockVD import BlockCapitalFlowHistory, BlockPriceHistory, resp_to_dict
from config import URLs, Headers, QueryPayload, get_settings, CrawlStatus
from errors import RequestBlockError, SaveFileError
from log import LogType, Log
from dataProcessor import saveFile

__SUCCESS_LOG_PATH__ = './logs/success'  # 爬取成功日志
__ERROR_LOG_PATH__ = './logs/errors'  # 错误日志

globalSettings = get_settings()  # 获取配置信息


def get_BlockInfo() -> list:
    """查询股票板块信息
    """
    try:
        blockUrl = URL().build(
            scheme='https',
            host=URLs.t_StockUrl
        )
        payload = QueryPayload(pz=500, po=1, pn=1, np=1,
                               fields="f12,f14",
                               fid="f62",
                               fs="m:90+t:2").getDict()
        blockInfoResp = resp_to_dict(
            requests.get(url=str(blockUrl), params=payload, headers=Headers.headers))

        return blockInfoResp['data']['diff']
    except Exception as err:
        raise RequestBlockError(err)


def blockCrawlAndSave(sync: bool, blockUrl: str, blockPayload: dict, blockModel, file_path: str):
    """爬取板块数据,保存文件
    Args:
        sync: 增量同步（True) , 全量同步（False）
        blockUrl: 板块链接
        blockPayload: 请求参数
        blockModel: 板块数据模型
        file_path: 文件保存路径
    """

    # 发起请求
    response = requests.get(url=str(blockUrl), params=blockPayload, headers=Headers.headers)

    blockResp = resp_to_dict(response)

    if blockResp:
        try:
            saveFile(myModel=blockModel, file_path=file_path, file_data=blockResp, sync=sync)
        except SaveFileError as err:
            sfLog = Log(path=__ERROR_LOG_PATH__, logType=LogType.run_error)
            sfLog.add_txt_row(username=globalSettings.sysAdmin, content=err)


def block_cf_crawl(sync: bool):
    """爬取板块历史资金流
    Args:
        sync: 增量同步（True) , 全量同步（False）
    """
    print(CrawlStatus.crawling.value)

    try:
        blockList = get_BlockInfo()
    except RequestBlockError as err:
        blockList = []  # 获取板块列表失败
        myLog = Log(path=__ERROR_LOG_PATH__, logType=LogType.run_error)
        myLog.add_txt_row(username=globalSettings.sysAdmin, content=err)

    bccTasks = []  # 任务列表

    for blockInfo in blockList:
        blockCFPayload = QueryPayload(lmt="0",
                                      klt="101",
                                      secid="90." + blockInfo['f12'],
                                      fields1="f1,f2,f3,f7",
                                      fields2="f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63"
                                      ).getDict()

        # 板块历史资金流链接
        blockCFHUrl = URL.build(
            scheme='https',
            host=URLs.h_StockUrl,
            path='/fflow/daykline/get'
        )

        # 抓取数据，保存文件
        bccTasks.append(
            gevent.spawn(
                blockCrawlAndSave, sync=sync, blockUrl=str(blockCFHUrl), blockPayload=blockCFPayload,
                blockModel=BlockCapitalFlowHistory, file_path="板块历史资金流"
            )
        )

    gevent.joinall(bccTasks)


def block_price_crawl(sync: bool):
    """爬取板块历史价格K线图数据
    Args:
        sync: 增量同步（True) , 全量同步（False）
    """
    print(CrawlStatus.crawling.value)

    try:
        blockList = get_BlockInfo()
    except RequestBlockError as err:
        blockList = []  # 获取板块列表失败
        myLog = Log(path=__ERROR_LOG_PATH__, logType=LogType.run_error)
        myLog.add_txt_row(username=globalSettings.sysAdmin, content=err)

    bpcTasks = []  # 任务列表
    length = len(blockList)
    num = 50  # 放50个协程抓手，不要太多

    for i in range(0, length, num):
        for blockInfo in blockList[i:i + num]:
            blockPayload = QueryPayload(klt="101", fqt="1", end="20500101", lmt="250",
                                        secid="90." + blockInfo['f12'],
                                        fields1="f1,f2,f3,f4,f5,f6",
                                        fields2="f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61").getDict()

            # 板块价格链接
            blockPUrl = URL.build(
                scheme='https',
                host=URLs.h_StockUrl,
                path='/kline/get'
            )

            # 抓取数据，保存文件
            bpcTasks.append(
                gevent.spawn(
                    blockCrawlAndSave, sync=sync, blockUrl=str(blockPUrl), blockPayload=blockPayload,
                    blockModel=BlockPriceHistory, file_path="板块价格K线数据"
                )
            )
        gevent.joinall(bpcTasks)
        time.sleep(2)
