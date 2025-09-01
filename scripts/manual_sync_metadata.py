#!/usr/bin/env python3

"""手动元数据同步脚本

直接调用 MetadataSyncManager 来执行一次性的元数据同步。
"""

import logging
import sys
from pathlib import Path

# 将项目根目录添加到 sys.path，以便能够导入 neo 包
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root / "src"))

from neo.tasks.metadata_sync_tasks import MetadataSyncManager
from neo.helpers.utils import setup_logging

# 设置日志
setup_logging("manual_sync", "info")
logger = logging.getLogger(__name__)


def main():
    """执行元数据同步"""
    logger.info("开始手动执行元数据同步...")
    try:
        sync_manager = MetadataSyncManager()
        sync_manager.sync_metadata()
        logger.info("手动元数据同步成功完成。")
    except Exception as e:
        logger.error(f"手动元数据同步失败: {e}", exc_info=True)


if __name__ == "__main__":
    main()
