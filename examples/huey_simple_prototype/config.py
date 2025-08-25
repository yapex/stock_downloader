"""Huey 简单原型验证配置"""

from huey import SqliteHuey

# 创建 Huey 实例
huey = SqliteHuey("prototype_test", filename="prototype_tasks.db", utc=False)