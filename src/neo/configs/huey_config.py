"""Huey 配置模块

提供全局的 Huey 实例配置，支持多线程 Consumer 和 Sqlite 后端。
"""

from huey import SqliteHuey
from . import get_config

# 获取配置
config = get_config()
db_path = config.huey.sqlite_path

# 创建 SqliteHuey 实例
huey = SqliteHuey("stock_downloader", filename=db_path, utc=False)
