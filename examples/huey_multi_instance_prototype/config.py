"""Huey 实例配置文件"""

from huey import SqliteHuey

# 为高速、I/O密集型任务（如模拟的下载）创建的Huey实例
# 使用独立的数据库文件来隔离队列
huey_fast = SqliteHuey(filename="/tmp/prototype_fast.db")

# 为低速、阻塞型任务（如模拟的数据库写入）创建的Huey实例
huey_slow = SqliteHuey(filename="/tmp/prototype_slow.db")
