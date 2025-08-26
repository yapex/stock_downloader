"""Huey 配置模块

提供全局的 Huey 实例配置，支持多线程 Consumer 和 Sqlite 后端。
"""

from huey import SqliteHuey
from . import get_config

# 获取配置
config = get_config()

# 快速队列实例
huey_fast = SqliteHuey(
    name='fast_queue',
    filename=config.huey_fast.sqlite_path,
    utc=False
)

# 慢速队列实例
huey_slow = SqliteHuey(
    name='slow_queue',
    filename=config.huey_slow.sqlite_path,
    utc=False
)
