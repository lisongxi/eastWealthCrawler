"""股票板块相关
"""
from yarl import URL
import requests

from validating import BlockCapitalFlowHistory, BlockPriceHistory

from config import URLs, Headers, QueryPayload, get_settings
from validating import resp_to_dict
from errors import RequestBlockError
from log import LogType, Log
from database import saveFile
from proxy import get_proxyInfo

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
        raise RequestBlockError('%s', err)


def block_cf_crawl():
    """爬取板块历史资金流
    """
    try:
        blockList = get_BlockInfo()
    except RequestBlockError as err:
        blockList = []  # 获取板块列表失败
        myLog = Log(path=__ERROR_LOG_PATH__, logType=LogType.run_error.value)
        myLog.add_txt_row(username=globalSettings.sysAdmin, content=err)

    for blockInfo in blockList:
        blockCFPayload = QueryPayload(lmt="0",
                                      klt="101",
                                      secid="90." + blockInfo['f12'],
                                      fields1="f1,f2,f3,f7",
                                      fields2="f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65"
                                      ).getDict()

        # 板块历史资金流链接
        blockCFHUrl = URL.build(
            scheme='https',
            host=URLs.h_StockUrl,
            path='/fflow/daykline/get'
        )

        blockResp = resp_to_dict(requests.get(url=str(blockCFHUrl), params=blockCFPayload, headers=Headers.headers))

        if blockResp:
            saveFile(myModel=BlockCapitalFlowHistory, file_path="板块历史资金流", file_data=blockResp)


def block_price_crawl():
    """爬取板块历史价格K线图数据
    """
    try:
        blockList = get_BlockInfo()
    except RequestBlockError as err:
        blockList = []  # 获取板块列表失败
        myLog = Log(path=__ERROR_LOG_PATH__, logType=LogType.run_error.value)
        myLog.add_txt_row(username=globalSettings.sysAdmin, content=err)

    for blockInfo in blockList:
        blockPayload = QueryPayload(klt="101", fqt="1", end="20500101", lmt="250",
                                    secid="90." + blockInfo['f12'],
                                    fields1="f1,f2,f3,f4,f5,f6",
                                    fields2="f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61").getDict()

        # 板块价格链接
        blockUrl = URL.build(
            scheme='https',
            host=URLs.h_StockUrl,
            path='/kline/get'
        )

        blockPriceResp = resp_to_dict(requests.get(url=str(blockUrl), params=blockPayload, headers=Headers.headers))

        if blockPriceResp:
            saveFile(myModel=BlockPriceHistory, file_path="板块价格K线数据", file_data=blockPriceResp)


if __name__ == "__main__":
    print("hello")
