"""板块数据校验
"""
import json
import re
from pydantic import BaseModel
from function import judgeDate


class Block(BaseModel):
    blockCode: str
    blockName: str


class BlockCapitalFlowHistory(Block):
    """股票板块历史资金流数据校验模型
    由于东财返回的是字符串，这里定义的类属性顺序不可乱
    """
    b_date: str
    main_net_inflow: str  # 主力净流入
    small_net_inflow: str  # 小单净流入
    mid_net_inflow: str  # 中单净流入
    large_net_inflow: str  # 大单净流入
    super_large_net_inflow: str  # 超大单净流入
    main_net_proportion: str  # 主力净占比
    small_net_proportion: str  # 小单净占比
    mid_net_proportion: str  # 中单净占比
    large_net_proportion: str  # 大单净占比
    super_large_net_proportion: str  # 超大单净占比
    d_closing_price: str  # 当日收盘价
    d_quote_change: str  # 当日涨跌幅


class BlockPriceHistory(Block):
    """板块历史价格数据校验模型
    """
    b_date: str  # 日期
    open_price: str  # 开盘价
    close_price: str  # 收盘价
    top_price: str  # 最高
    low_price: str  # 最低
    turnover: str  # 成交量
    transaction: str  # 成交额
    amplitude: str  # 振幅
    quote_change: str  # 涨跌幅
    change_amount: str  # 涨跌额
    turnover_rate: str  # 换手率


def resp_to_dict(resp) -> dict:
    """将返回数据转换成字典
    """
    response = resp.text
    objLocation = re.compile(r"jQuery.*?\u0028(?P<dataJson>.*?)\u0029;", re.S)
    result = json.loads(objLocation.findall(response)[0])
    return result


def dict_del_null(payload) -> dict:
    """删除字典中值为空的键值对
    """
    return {k: v for k, v in payload.items() if v}


def get_result_dict(model, code: str, name: str, data: str, sync: bool) -> dict:
    """结合校验模型，创建数据字典
    Args:
        model: 模型
        code: 板块编号
        name: 板块名字
        data: 板块数据
        sync: 同步方式
    """
    attributes = data.split(",")

    if sync and judgeDate(attributes[0]):
        return {}

    attributes.insert(0, code)
    attributes.insert(1, name)

    mydict = dict(model.__fields__)

    for key, value in zip(mydict.keys(), attributes):
        mydict[key] = value

    blockDict = model(**mydict).dict()

    return blockDict
