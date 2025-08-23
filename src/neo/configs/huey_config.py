"""Huey 配置模块

提供全局的 MemoryHuey 实例配置，支持多线程 Consumer。
"""

from huey import MemoryHuey

# 创建 MemoryHuey 实例用于多线程任务处理
# immediate=False 表示任务异步执行
# utc=False 使用本地时区
huey = MemoryHuey("stock_downloader", immediate=False, utc=False)
