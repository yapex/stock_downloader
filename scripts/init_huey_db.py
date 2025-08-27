# scripts/init_huey_db.py
import os
from neo.configs.huey_config import (
    huey_fast,
    huey_slow,
    huey_maint,
)  # 假设这些都指向同一个sqlite文件或不同的文件但需要初始化

# 这里的关键是触发Huey的存储初始化逻辑
# 对于SqliteHuey，访问huey.storage属性会触发其初始化
# 如果你的huey_fast, huey_slow, huey_maint配置的是同一个sqlite文件，只需初始化一次
# 如果是不同的sqlite文件，则需要分别触发

print("Initializing Huey fast queue database...")
try:
    # 尝试获取存储，这会触发SqliteStorage的__init__和initialize_schema
    _ = huey_fast.storage
    print(f"Fast queue database at {huey_fast.storage.filename} initialized.")
except Exception as e:
    print(f"Error initializing fast queue database: {e}")

print("Initializing Huey slow queue database...")
try:
    _ = huey_slow.storage
    print(f"Slow queue database at {huey_slow.storage.filename} initialized.")
except Exception as e:
    print(f"Error initializing slow queue database: {e}")

print("Initializing Huey maint queue database...")
try:
    _ = huey_maint.storage
    print(f"Maint queue database at {huey_maint.storage.filename} initialized.")
except Exception as e:
    print(f"Error initializing maint queue database: {e}")

# 注意：如果你的huey_config.py中的huey_fast, huey_slow, huey_maint都指向同一个物理SQLite文件
# 那么实际上只需要初始化一次即可，其他的会自动使用已创建的数据库。
# 可以在 huey_config.py 中统一配置 SQLite 路径以确保这一点。

print("Huey database initialization complete.")
