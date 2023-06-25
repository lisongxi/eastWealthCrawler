"""
通过代理池爬取
东方财富网不做反爬机制，无需用到IP代理池，仅作学习编写
"""
import requests
from yarl import URL
from datetime import datetime
from pydantic import BaseModel

from errors import ProxyAddrError

# 代理池参数
proxyAddr = "127.0.0.1"
authKey = ""
authPwd = ""

# 获取代理IP信息的地址
proxyUrl = URL().build(
    scheme='',
    host='',
    path='',
    query={}
)


class ProxyInfo(BaseModel):
    """代理IP详细
    """
    proxy_ip: str
    server: str
    area: str
    isp: str
    deadline: datetime


def get_proxyInfo() -> ProxyInfo:
    """获取代理IP地址
    """
    try:
        resp = requests.get(proxyUrl).json()
    except Exception as err:
        raise ProxyAddrError('%s' % err)

    if resp['code'] == 'SUCCESS':
        return ProxyInfo(**resp['data'])
    else:
        raise ProxyAddrError(resp['code'])


def testGet_proxyInfo():
    """没有代理服务器
    下次再搞
    """
    return 0


if __name__ == "__main__":
    pass
