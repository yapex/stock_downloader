"""Huey 配置 - 使用 SQLite"""

from huey import SqliteHuey
import tempfile
import os

# 使用 SQLite 作为任务队列，适用于跨进程但单机的场景
# 任务会被持久化到临时目录的一个 .db 文件中
temp_dir = tempfile.gettempdir()
db_file = os.path.join(temp_dir, "huey_pipeline_progress.db") # 使用不同的db文件避免冲突

huey = SqliteHuey("pipeline_example_progress", filename=db_file)
