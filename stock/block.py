"""股票板块相关
"""
from yarl import URL
import requests

from config import URLs, Headers, QueryPayload
from validating import resp_to_dict
from errors import RequestBlockError


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
        blockInfoResp = resp_to_dict(requests.get(url=blockUrl, params=payload, headers=Headers.headers))

        return blockInfoResp['data']['diff']
    except Exception as err:
        raise RequestBlockError('%s', err)


if __name__ == "__main__":
    print(get_BlockInfo())
