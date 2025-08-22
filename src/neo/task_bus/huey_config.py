"""Huey 配置模块

提供全局的 Huey 实例配置。
"""

from huey import SqliteHuey
from pathlib import Path


def _create_huey_instance() -> SqliteHuey:
    """创建 Huey 实例

    Returns:
        SqliteHuey: 配置好的 Huey 实例
    """
    import os
    
    # 检测是否在测试环境中
    is_testing = (
        "pytest" in os.environ.get("_", "") or
        "PYTEST_CURRENT_TEST" in os.environ or
        any("pytest" in arg for arg in __import__("sys").argv)
    )
    
    if is_testing:
        # 测试环境：使用内存数据库和立即执行模式
        return SqliteHuey(
            filename=":memory:",
            immediate=True,
            utc=True,
        )
    
    try:
        from ..config import get_config

        config = get_config()
        db_file = config.huey.db_file
        immediate = config.huey.get("immediate", False)
    except Exception:
        # 如果配置加载失败，使用默认值
        db_file = "data/task_queue.db"
        immediate = False

    # 确保数据目录存在
    if db_file != ":memory:":
        db_path = Path(db_file)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db_file = str(db_path)

    return SqliteHuey(
        filename=db_file,
        immediate=immediate,
        utc=True,
    )


# 全局 Huey 实例
huey = _create_huey_instance()

# 自动导入任务模块以确保任务被注册到 Huey
try:
    from . import tasks  # 导入任务模块以触发任务注册
except ImportError:
    # 在某些测试环境中可能会失败，这是正常的
    pass
