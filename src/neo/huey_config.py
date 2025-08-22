"""Huey 配置模块

提供全局的 MiniHuey 实例配置。
"""

import gevent

from huey.contrib.mini import MiniHuey

# 创建 MiniHuey 实例用于链式任务
huey = MiniHuey('stock_downloader')