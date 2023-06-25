import requests
from yarl import URL
from time import sleep
from random import randint

from config import get_settings, URLs, QueryPayload, Headers
from database import saveFile
from proxy import get_proxyInfo, ProxyInfo, testGet_proxyInfo
from errors import ProxyAddrError, RequestBlockError
from log import LogType, Log
from stock.block import get_BlockInfo
from validating import resp_to_dict

__SUCCESS_LOG_PATH__ = './logs/success'  # 爬取成功日志
__ERROR_LOG_PATH__ = './logs/errors'  # 错误日志

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
    myLog = Log(path=__ERROR_LOG_PATH__, logType=LogType.run_error.value)
    myLog.add_txt_row(username=globalSettings.sysAdmin, content=err)
    quit()  # 退出进程


class CrawlData:
    def __init__(self):
        pass

    # 爬取数据
    def crawler(self, pInfo: ProxyInfo = proxyInfo):
        """爬取数据
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
                saveFile(file_path="板块历史资金流", file_data=blockResp)


if __name__ == "__main__":
    craw = CrawlData()
    craw.crawler()
