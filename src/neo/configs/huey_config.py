"""Huey 配置模块

提供全局的 Huey 实例配置，支持多线程 Consumer 和 Sqlite 后端。
"""

import os
from huey import SqliteHuey
from . import get_config

# 获取配置
config = get_config()

# 为数据库连接启用 WAL (Write-Ahead Logging) 模式以提高并发性
pragmas = [
    'PRAGMA journal_mode=wal',
    'PRAGMA synchronous=normal',
]

# 快速队列实例
os.makedirs(os.path.dirname(config.huey_fast.sqlite_path), exist_ok=True)
huey_fast = SqliteHuey(
    name='fast_queue',
    filename=config.huey_fast.sqlite_path,
    utc=False,
)

# 慢速队列实例
os.makedirs(os.path.dirname(config.huey_slow.sqlite_path), exist_ok=True)
huey_slow = SqliteHuey(
    name='slow_queue',
    filename=config.huey_slow.sqlite_path,
    utc=False,
)

# 维护队列实例
os.makedirs(os.path.dirname(config.huey_maint.sqlite_path), exist_ok=True)
huey_maint = SqliteHuey(
    name='maint_queue',
    filename=config.huey_maint.sqlite_path,
    utc=False,
)
