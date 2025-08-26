"""Huey 任务定义

定义带 @huey_task 装饰器的下载任务函数。
使用中心化的容器实例来获取依赖服务。
"""

import asyncio
import logging

import pandas as pd
from ..configs.huey_config import huey_fast, huey_slow
from ..task_bus.types import TaskType

logger = logging.getLogger(__name__)


def _process_data_sync(task_type: str, data: pd.DataFrame) -> bool:
    """异步处理数据的公共函数

    Args:
        task_type: 任务类型字符串
        data: 要处理的数据

    Returns:
        bool: 处理是否成功
    """
    from ..app import container

    data_processor = container.data_processor()
    try:
        process_success = data_processor.process(task_type, data)
        logger.debug(f"[HUEY] {task_type} 数据处理器返回结果: {process_success}")
        return process_success
    finally:
        # 确保数据处理器正确关闭，刷新所有缓冲区数据
        data_processor.shutdown()


@huey_fast.task()
def download_task(task_type: TaskType, symbol: str):
    """下载股票数据的 Huey 任务 (快速队列)

    下载完成后，直接调用慢速队列的数据处理任务。

    Args:
        task_type: 任务类型枚举
        symbol: 股票代码
    """
    try:
        logger.debug(f"🚀 [HUEY_FAST] 开始执行下载任务: {symbol} ({task_type})")

        # 从中心化的 app.py 获取共享的容器实例
        from ..app import container

        downloader = container.downloader()

        result = downloader.download(task_type, symbol)

        if result is not None and not result.empty:
            logger.debug(f"🚀 [HUEY_FAST] 下载完成: {symbol}, 准备提交到慢速队列...")
            # 手动调用慢速任务，并传递数据
            process_data_task(
                task_type=task_type,
                symbol=symbol,
                data_frame=result.to_dict("records"),
            )
        else:
            logger.warning(
                f"⚠️ [HUEY_FAST] 下载任务完成: {symbol}, 但返回空数据，不提交后续任务"
            )

    except Exception as e:
        logger.error(f"❌ [HUEY_FAST] 下载任务执行失败: {symbol}, 错误: {e}")
        raise e


@huey_slow.task()
def process_data_task(task_type: str, symbol: str, data_frame: list) -> bool:
    """数据处理任务 (慢速队列)

    Args:
        task_type: 任务类型字符串
        symbol: 股票代码
        data_frame: DataFrame 数据 (字典列表形式)

    Returns:
        bool: 处理是否成功
    """
    try:
        # 创建异步数据处理器并运行
        def process_sync():
            try:
                # 将字典列表转换为 DataFrame
                if data_frame and isinstance(data_frame, list) and len(data_frame) > 0:
                    df_data = pd.DataFrame(data_frame)
                    logger.debug(
                        f"🐌 [HUEY_SLOW] 开始异步保存数据: {symbol}_{task_type}, 数据行数: {len(df_data)}"
                    )
                    return _process_data_sync(task_type, df_data)
                else:
                    logger.warning(
                        f"⚠️ [HUEY_SLOW] 数据保存失败，无有效数据: {symbol}_{task_type}, 数据为空或None"
                    )
                    return False
            except Exception as e:
                raise e

        result = process_sync()
        logger.info(f"🏆 [HUEY_SLOW] 最终结果: {symbol}_{task_type}, 成功: {result}")
        return result

    except Exception as e:
        logger.error(f"❌ [HUEY_SLOW] 数据处理任务执行失败: {symbol}, 错误: {e}")
        raise e


# ==========================================================
# 元数据同步任务 (维护队列)
# ==========================================================
import duckdb
from pathlib import Path
from huey import crontab
from ..configs.huey_config import huey_maint
from ..configs import get_config

config = get_config()


def get_sync_metadata_crontab():
    """从配置中读取 cron 表达式"""
    schedule = config.cron_tasks.sync_metadata_schedule
    minute, hour, day, month, day_of_week = schedule.split()
    return crontab(minute, hour, day, month, day_of_week)

@huey_maint.periodic_task(get_sync_metadata_crontab(), name="sync_metadata")
def sync_metadata():
    """
    周期性任务：扫描 Parquet 文件目录，并更新 DuckDB 元数据文件。
    """
    logger.info("🛠️ [HUEY_MAINT] 开始执行元数据同步任务...")
    
    # 获取当前文件所在目录的绝对路径，并找到项目根目录
    # neo/tasks/huey_tasks.py -> neo/tasks -> neo -> src -> project_root
    project_root = Path(__file__).resolve().parents[3]
    logger.info(f"诊断: 项目根目录: {project_root}")

    parquet_base_path = project_root / config.storage.parquet_base_path
    metadata_db_path = project_root / config.database.metadata_path
    logger.info(f"诊断: Parquet 根目录: {parquet_base_path}")
    logger.info(f"诊断: 元数据DB路径: {metadata_db_path}")

    if not parquet_base_path.is_dir():
        logger.warning(f"Parquet 根目录 {parquet_base_path} 不存在，跳过同步。")
        return

    try:
        with duckdb.connect(str(metadata_db_path)) as con:
            logger.info("诊断: 成功连接到元数据DB。")
            # 扫描 Parquet 根目录下的所有子目录，每个子目录代表一个表
            
            found_items = list(parquet_base_path.iterdir())
            if not found_items:
                logger.warning(f"警告: 在 {parquet_base_path} 中没有找到任何条目。")
                return

            logger.info(f"诊断: 在 {parquet_base_path} 中找到以下条目: {[p.name for p in found_items]}")

            for table_dir in found_items:
                if table_dir.is_dir():
                    table_name = table_dir.name
                    # DuckDB 的 hive_partitioning 会自动处理子目录，我们只需提供根路径
                    # 修正：为增强兼容性，我们提供一个更明确的 glob 路径
                    table_glob_path = str(table_dir / '**/*.parquet')
                    
                    logger.info(f"正在为表 {table_name} 从路径 {table_glob_path} 同步元数据...")
                    
                    sql = f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{table_glob_path}', hive_partitioning=1, union_by_name=True);
                    """
                    con.execute(sql)
                    logger.info(f"✅ 表 {table_name} 元数据同步完成。")
                else:
                    logger.info(f"诊断: 跳过非目录条目: {table_dir}")

        logger.info("🛠️ [HUEY_MAINT] 元数据同步任务成功完成。")
    except Exception as e:
        logger.error(f"❌ [HUEY_MAINT] 元数据同步任务失败: {e}")
        raise e
