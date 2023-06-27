import json
import os.path
from os import listdir

from DBModels import mysql1, BlockPriceHistory, BlockCFHistory

__SAVE_DIR__ = './data/'  # 设置文件基本路径


def saveFile(myModel, file_path: str, file_data: dict):
    """保存数据到文件
    """
    Code = file_data['data']['code']
    Name = file_data['data']['name']

    data = []

    for crawlData in file_data['data']['klines']:
        result = myModel.get_result_dict(code=Code, name=Name, data=crawlData)
        data.append(result)

    os.makedirs(__SAVE_DIR__ + file_path, exist_ok=True)  # 创建文件路径
    path = os.path.join(__SAVE_DIR__ + file_path, str(Code) + '.json')  # 创建文件

    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            oldData = json.load(f)
        data = oldData + data
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def first_to_sql(DB_Model, file_path: str):
    """首次将所有历史数据保存到数据库
    """
    # 创建表
    mysql1.create_tables([DB_Model])

    listFiles = listdir(__SAVE_DIR__ + file_path)

    # 遍历所有文件
    for file_name in listFiles:

        with open(__SAVE_DIR__ + file_path + file_name, 'r', encoding='utf-8') as f:
            data = json.load(f)

        with mysql1.atomic():
            for i in range(0, len(data), num := 100):  # 一次插入100条
                (DB_Model
                 .insert_many([dict(flow) for flow in data[i: i + num]])
                 .execute()
                 )


def renew_to_sql(DB_Model, data: dict):
    """更新数据到数据库
    """
    with mysql1.atomic():
        for i in range(0, len(data), num := 100):  # 一次插入100条
            (DB_Model
             .insert_many([dict(flow) for flow in data[i: i + num]])
             .execute()
             )


if __name__ == "__main__":
    print("hello")
    first_to_sql(BlockCFHistory, '板块历史资金流/')
