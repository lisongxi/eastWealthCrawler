"""日期相关
"""
import json
from datetime import date, datetime

__Date_PATH__ = './settings/date.json'


def getDateInterval() -> str:
    """计算日期间隔天数
    """
    with open(__Date_PATH__, 'r', encoding='utf-8') as f:
        file_date = json.load(f)["expiration_date"]

    last_day = datetime.strptime(file_date, "%Y-%m-%d").date()
    today = date.today()

    # 计算时间差
    delta = today - last_day
    delta_days = delta.days

    return str(delta_days)


def saveDate():
    """保存当天日期
    """
    today = {"expiration_date": str(date.today())}

    with open(__Date_PATH__, 'w', encoding='utf-8') as f:
        json.dump(today, f)
