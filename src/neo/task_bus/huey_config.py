"""Huey 配置模块

提供全局的 Huey 实例配置。
"""

from venv import logger
from huey import MemoryHuey, SqliteHuey, Huey
from pathlib import Path


def _create_huey_instance() -> Huey:
    """创建 Huey 实例

    Returns:
        SqliteHuey: 配置好的 Huey 实例
    """
    try:
        from ..config import get_config

        config = get_config()
        db_file = config.huey.db_file
        immediate = config.huey.get("immediate", False)
    except Exception:
        logger.exception("Failed to load Huey config, using memory mode")
        return MemoryHuey(immediate=True)

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
