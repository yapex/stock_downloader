from huey import SqliteHuey
import tempfile
import os

# 共享的 Huey 实例配置 - 使用 SQLite 模式（支持跨进程）
# SQLite 文件队列可以在不同进程间共享任务
temp_dir = tempfile.gettempdir()
db_file = os.path.join(temp_dir, "huey_separated_example.db")
huey = SqliteHuey("separated_example", filename=db_file)

# 如果需要 Redis 模式，可以使用：
# from huey import RedisHuey
# huey = RedisHuey('separated_example', host='localhost', port=6379, db=0)

# 如果需要内存模式（仅适合单进程），可以使用：
# from huey import MemoryHuey
# huey = MemoryHuey('separated_example')
