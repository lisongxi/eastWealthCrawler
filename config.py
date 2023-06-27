"""
全局配置
"""

from enum import Enum
from pydantic import BaseModel
from cachier import cachier
import yaml
import time
import requests

from validating import dict_del_null

__SETTINGS_PATH__ = './settings/settings.yaml'


# --------------------------------------数据库配置-----------------------------------------
class MysqlParams(BaseModel):
    """MySQL配置
    """
    database: str
    host: str = '127.0.0.1'
    port: int = 3306
    user: str = 'root'
    password: str


# --------------------------------------东方财富配置-----------------------------------------
class User(BaseModel):
    """用户信息
    """
    username: str
    password: str


class EastWealth(BaseModel):
    """东方财富相关配置
    """
    user: User
    mysql: MysqlParams


# --------------------------------------日志配置-----------------------------------------
class Freq(Enum):
    """日志文件频率
    """
    month = 'month'
    day = 'day'
    hour = 'hour'


class LogParams(BaseModel):
    """日志参数模型
    """
    freq: Freq
    useUTC: bool  # 是否使用UTC时间戳


class Settings(BaseModel):
    """配置模型
    """
    sysAdmin: str
    log: LogParams
    eastWealth: EastWealth


@cachier(backend='memory')
def get_settings() -> Settings:
    """获取配置信息
    """
    with open(__SETTINGS_PATH__, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    return Settings(**data)


# --------------------------------------请求体配置-----------------------------------------
class URLs:
    """关键URL清单
    """
    t_StockUrl = "push2.eastmoney.com/api/qt/clist/get"  # 今日股票数据链接
    h_StockUrl = "push2his.eastmoney.com/api/qt/stock"  # 历史股票数据链接


class Headers:
    """请求头
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }


class BasePayloadModel(BaseModel):
    """根请求体参数模型
    """
    cb: str = "jQuery1123011842352019573332_" + str(int(time.time()))
    ut: str = "b2884a393a59ad64002292a3e90d46a5"
    po: int = None
    pn: int = None
    np: int = None
    pz: int = None
    fid: str = None
    fs: str = None


class QueryPayload(BasePayloadModel):
    """板块历史资金流请求体模型
    """
    lmt: str = None  # 交易日数量
    klt: str = None
    fqt: str = None
    end: str = None
    secid: str = None
    fields: str = None
    fields1: str = None
    fields2: str = None
    _: str = str(int(time.time()))

    # 访问 '_' 属性 ,因为在python中，单下划线代表内部属性。偏偏东财又要一个'_'参数
    def getDict(self) -> dict:
        ts = self._
        obj_dict = self.__dict__
        obj_dict["_"] = ts
        return dict_del_null(obj_dict)


if __name__ == "__main__":
    qp = QueryPayload(lmt="0",
                      klt="101",
                      secid="90.",
                      fields1="f1,f2,f3,f7",
                      fields2="f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65"
                      ).getDict()
    print(qp)

    # 注意
    # 如果要直接 print(vars(qp)) ，这是无法访问 _ 的
    # 1. 实例属性的查找顺序是先查实例的 __dict__,然后才是类的 __dict__
    # 2. vars() 只显示实例自己的属性,不显示从类继承而来的属性
    # 3. 调用 getDict() 方法后,_ 属性被添加到了实例的 __dict__ 中,所以被 vars() 显示,并且实例访问 _ 时也会首先在实例的 __dict__ 中查找到该属性。
