import json
import os.path
from os import listdir

from DBModels import mysql1, BlockPriceHistory, BlockCFHistory
from validating import get_result_dict

__SAVE_DIR__ = './data/'  # 设置文件基本路径


def saveFile(myModel, file_path: str, file_data: dict, sync: bool):
    """保存数据到文件
    Args:
        myModel: 数据模型
        file_path: 文件路径
        file_data: 待保存数据
        sync: 同步类型（增量True，全量False）
    """
    Code = file_data['data']['code']
    Name = file_data['data']['name']

    data = []

    for crawlData in reversed(file_data['data']['klines']):
        result = get_result_dict(model=myModel, code=Code, name=Name, data=crawlData, sync=sync)
        if not result:
            break
        data.append(result)

    os.makedirs(__SAVE_DIR__ + file_path, exist_ok=True)  # 创建文件路径
    path = os.path.join(__SAVE_DIR__ + file_path, str(Code) + '.json')  # 创建文件

    # # 这个是拼接旧数据，需要可以自行使用
    # if os.path.exists(path):
    #     with open(path, 'r', encoding='utf-8') as f:
    #         oldData = json.load(f)
    #     data = oldData + data

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def first_to_sql(DB_Model, file_path: str):
    """首次将所有历史数据保存到数据库
    Args:
        DB_Model: 数据库模型
        file_path: 文件路径
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
    first_to_sql(BlockPriceHistory, '板块价格K线数据/')
