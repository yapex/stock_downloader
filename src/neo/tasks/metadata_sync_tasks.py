"""元数据同步相关任务

使用 Huey 任务队列实现数据湖元数据的定期同步。
"""

import logging
from huey import crontab
from ..configs.huey_config import huey_maint
from ..helpers.metadata_sync import MetadataSyncManager
from ..configs.app_config import get_config

logger = logging.getLogger(__name__)


@huey_maint.task()
def sync_metadata(force_full_scan: bool = False, mtime_check_minutes: int = 60):
    """同步 DuckDB 元数据与 Parquet 数据湖
    
    Args:
        force_full_scan (bool): 强制执行完整的状态比较
        mtime_check_minutes (int): mtime 检查窗口，0 表示禁用
    """
    try:
        config = get_config()
        metadata_db_path = config.database.metadata_path
        parquet_base_path = config.storage.parquet_base_path
        
        sync_manager = MetadataSyncManager(metadata_db_path, parquet_base_path)
        sync_manager.sync(
            force_full_scan=force_full_scan,
            mtime_check_minutes=mtime_check_minutes
        )
        
        logger.info(f"元数据同步任务完成，force_full_scan={force_full_scan}")
        
    except Exception as e:
        logger.error(f"元数据同步任务失败: {e}")
        raise


def get_sync_metadata_crontab():
    """获取元数据同步的 crontab 调度配置
    
    Returns:
        huey.crontab: 根据配置文件中的调度配置
    """
    config = get_config()
    schedule = config.cron_tasks.sync_metadata_schedule
    
    # 解析 cron 表达式 (简化版，仅支持分钟)
    # 例如："*/10 * * * *" 表示每10分钟执行
    # "0 * * * *" 表示每小时的第0分钟执行
    parts = schedule.split()
    if len(parts) >= 1:
        minute_part = parts[0]
        if minute_part.startswith('*/'):
            # 每N分钟执行
            interval = int(minute_part[2:])
            return crontab(minute=f'*/{interval}')
        else:
            # 固定分钟执行
            return crontab(minute=minute_part)
    
    # 默认每小时执行
    return crontab(minute='0')


# 定期任务：每小时同步一次元数据
@huey_maint.periodic_task(get_sync_metadata_crontab())
def periodic_sync_metadata():
    """定期执行元数据同步的周期性任务"""
    return sync_metadata()
