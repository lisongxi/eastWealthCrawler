import json
import os.path

from playhouse.pool import PooledMySQLDatabase

from validating import BlockCapitalFlowHistory

from config import get_settings

__MYSQL_PARAMS__ = get_settings().eastWealth.mysql
__SAVE_DIR__ = './data/'

# 连接池
mysql = PooledMySQLDatabase(
    **__MYSQL_PARAMS__.dict(),
    max_connections=10,  # 最大连接数
    timeout=10,  # 池满阻止秒数
    stale_timeout=300  # 最长连接时间
)


def saveFile(file_path: str, file_data: dict):
    """保存数据到文件
    """
    blockCode = file_data['data']['code']
    blockName = file_data['data']['name']

    data = []

    for blockFlow in file_data['data']['klines']:
        bf = BlockCapitalFlowHistory.get_Block_cf(blockCode=blockCode, blockName=blockName, flowData=blockFlow)
        data.append(bf.dict())

    os.makedirs(__SAVE_DIR__ + file_path, exist_ok=True)  # 创建文件路径
    path = os.path.join(__SAVE_DIR__ + file_path, str(blockCode) + '.json')  # 创建文件

    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            oldData = json.load(f)
        data = oldData + data
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def to_sql():
    print(1)


if __name__ == "__main__":
    print("hello")
