"""
数据库初始化脚本
用于创建数据库和表
"""

import pymysql

from database import close_db, create_tables, init_db, mysql1
from settings.settings import get_settings


def create_database():
    """创建数据库"""
    settings = get_settings()
    mysql_config = settings.eastWealth.mysql

    # 连接到MySQL服务器（不指定数据库）
    connection = pymysql.connect(
        host=mysql_config.host,
        port=mysql_config.port,
        user=mysql_config.user,
        password=mysql_config.password,
    )

    try:
        cursor = connection.cursor()

        # 创建数据库
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS {mysql_config.database} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        print(f"数据库 {mysql_config.database} 创建成功或已存在")

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"创建数据库失败: {e}")
        raise


def main():
    """主函数"""
    print("开始初始化数据库...")

    # 1. 创建数据库
    try:
        create_database()
    except Exception as e:
        print(f"数据库创建失败，请检查配置: {e}")
        return

    # 2. 连接数据库
    if not init_db():
        print("无法连接到数据库，请检查配置")
        return

    # 3. 创建数据表
    if create_tables():
        print("数据库初始化完成！")
    else:
        print("数据表创建失败")

    # 4. 关闭连接
    close_db()


if __name__ == "__main__":
    main()
