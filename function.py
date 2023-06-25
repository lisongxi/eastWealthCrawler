"""
自定义的函数
"""

import json
from datetime import date, datetime

__Date_PATH__ = './settings/date.json'


def judgeDate(t_date: str) -> bool:
    """判断日期先后
    """
    with open(__Date_PATH__, 'r', encoding='utf-8') as f:
        file_date = json.load(f)["expiration_date"]

    last_day = datetime.strptime(file_date, "%Y-%m-%d").date()
    t_date = datetime.strptime(t_date, "%Y-%m-%d").date()

    # 计算 股票日期 是否小于等于 数据最后保存日期
    delta = t_date <= last_day

    return delta


def saveDate():
    """保存当天日期
    """
    today = {"expiration_date": str(date.today())}

    with open(__Date_PATH__, 'w', encoding='utf-8') as f:
        json.dump(today, f)
