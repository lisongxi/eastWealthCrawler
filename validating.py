"""数据校验
"""
import json
import re
from pydantic import BaseModel
from datetime import datetime


class Block(BaseModel):
    blockCode: str
    blockName: str


class BlockCapitalFlowHistory(Block):
    """股票板块历史资金流数据模型
    由于东财返回的是字符串，这里定义的类属性顺序不可乱
    """
    date: str
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

    @classmethod
    def get_Block_cf(cls, blockCode, blockName, flowData):
        attributes = flowData.split(",")[:-2]  # 去掉后两个元素
        attributes.insert(0, blockCode)
        attributes.insert(1, blockName)

        mydict = dict(cls.__fields__)

        for key, value in zip(mydict.keys(), attributes):
            mydict[key] = value

        block_cf = cls(**mydict)

        return block_cf


def resp_to_dict(resp) -> dict:
    """将返回数据转换成字典
    """
    objLocation = re.compile(r"jQuery.*?\u0028(?P<dataJson>.*?)\u0029;", re.S)
    result = json.loads(objLocation.findall(resp.text)[0])
    return result


def dict_del_null(payload) -> dict:
    """删除字典中值为空的键值对
    """
    return {k: v for k, v in payload.items() if v}
