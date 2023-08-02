"""
爬取个股数据
"""
import requests
from yarl import URL

from config import URLs, QueryPayload, Headers, get_settings, CrawlStatus
from s_block.blockVD import resp_to_dict
from errors import RequestStockError
from log import LogType, Log
from dataProcessor import saveFile
from s_stock.stockVD import StockKlineVD
from errors import SaveFileError

__SUCCESS_LOG_PATH__ = './logs/success'  # 爬取成功日志
__ERROR_LOG_PATH__ = './logs/errors'  # 错误日志

globalSettings = get_settings()  # 获取配置信息


def get_StockInfo() -> list:
    """查询股票代码信息
    """
    try:
        stockUrl = URL().build(
            scheme='https',
            host=URLs.t_StockUrl
        )
        payload = QueryPayload(pn=1, pz=5320, po=1, np=1, fltt=2, invt=2, wbp2u="|0|0|0|web",
                               fields="f12",
                               fid="f20",
                               fs="m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048").getDict()
        stockInfoResp = resp_to_dict(
            requests.get(url=str(stockUrl), params=payload, headers=Headers.headers))

        return stockInfoResp['data']['diff']
    except Exception as err:
        raise RequestStockError(err)


def getSecid(stockCode: str) -> str:
    """
    生成股票代码参数
    :param stockCode:
    :return: str
    """
    # 沪市指数
    if stockCode[:3] == '000':
        return f'0.{stockCode}'
    # 深证指数
    if stockCode[:3] == '399':
        return f'0.{stockCode}'

    if stockCode[0] != '6':
        return f'0.{stockCode}'

    return f'1.{stockCode}'


def crawlStockKline(sync: bool):
    """获取股票K线图
    """
    print(CrawlStatus.crawling.value)
    try:
        stockList = get_StockInfo()
    except RequestStockError as err:
        stockList = []  # 获取板块列表失败
        myLog = Log(path=__ERROR_LOG_PATH__, logType=LogType.run_error)
        myLog.add_txt_row(username=globalSettings.sysAdmin, content=err)

    for stock in stockList:
        stockCode = stock["f12"]

        stockUrl = URL().build(
            scheme='http',
            host=URLs.h_StockUrl,
            path='/kline/get'
        )
        payload = QueryPayload(secid=getSecid(stockCode), fields1="f1,f2,f3,f4,f5,f6",
                               fields2="f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61", klt="101", fqt="1",
                               end="20500101", lmt="1000"
                               ).getDict()

        stockCrawlAndSave(sync=sync, stockUrl=stockUrl, stockPayload=payload, stockModel=StockKlineVD,
                          file_path="个股K线数据")


def stockCrawlAndSave(sync: bool, stockUrl: URL, stockPayload: dict, stockModel, file_path: str) -> None:
    """
    爬取个股数据，并写入数据库
    """
    response = requests.get(url=str(stockUrl), params=stockPayload, headers=Headers.headers)
    stockResp = resp_to_dict(response)

    if stockResp:
        try:
            saveFile(myModel=stockModel, file_path=file_path, file_data=stockResp, sync=sync)
        except SaveFileError as err:
            sfLog = Log(path=__ERROR_LOG_PATH__, logType=LogType.run_error)
            sfLog.add_txt_row(username=globalSettings.sysAdmin, content=err)
