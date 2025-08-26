"""Huey 任务定义

定义带 @huey_task 装饰器的下载任务函数。
使用中心化的容器实例来获取依赖服务。
"""

import asyncio
import logging

import pandas as pd
from ..configs.huey_config import huey_fast, huey_slow
from ..task_bus.types import TaskType
from ..helpers.utils import get_next_day_str

logger = logging.getLogger(__name__)


@huey_slow.task()
def build_and_enqueue_downloads_task(group_name: str):
    """构建并派发增量下载任务 (慢速队列)

    这是智能增量下载的第一步。
    它会查询数据湖中已有数据的最新日期，计算出需要下载的起始日期，
    然后将具体的下载任务派发到快速队列。

    Args:
        group_name: 在 groups.toml 中定义的任务组名
    """
    from ..app import container
    from collections import defaultdict

    logger.info(f"🛠️ [HUEY_SLOW] 开始构建增量下载任务, 任务组: {group_name}")

    group_handler = container.group_handler()
    db_operator = container.db_operator()
    config = container.config()

    try:
        # 1. 解析任务组成员
        members = group_handler.get_members(group_name)
        if not members:
            logger.warning(f"任务组 '{group_name}' 中没有找到任何成员，任务结束。")
            return

        # 2. 按 task_type 对 symbols 进行分组，为批量查询做准备
        symbols_by_task_type = defaultdict(list)
        for task_type, symbol in members:
            symbols_by_task_type[task_type].append(symbol)

        # 3. 批量查询每个 task_type 下所有 symbols 的最新日期
        max_dates = {}
        for task_type, symbols in symbols_by_task_type.items():
            logger.debug(f"正在为 {task_type} 查询 {len(symbols)} 个股票的最新日期...")
            # 注意：这里我们假设 DBOperator 是连接到元数据DB的
            # 在新架构下，我们需要一个能查询 Parquet 数据湖的 DBOperator
            # 此处暂时使用一个模拟的 ParquetDBOperator
            # TODO: 替换为真实的 ParquetDBOperator
            from ..database.parquet_operator import ParquetDBOperator
            parquet_op = ParquetDBOperator(config.storage.parquet_base_path)
            max_dates.update(parquet_op.get_max_date(task_type, symbols))

        # 4. 循环派发具体的下载任务
        default_start_date = config.downloader.default_start_date
        enqueued_count = 0
        for task_type, symbol in members:
            latest_date = max_dates.get(symbol)
            start_date = get_next_day_str(latest_date) if latest_date else default_start_date
            
            # 派发任务到快速队列
            download_task.call(task_type=task_type, symbol=symbol, start_date=start_date)
            enqueued_count += 1
        
        logger.info(f"✅ [HUEY_SLOW] 成功派发 {enqueued_count} 个增量下载任务。")

    except Exception as e:
        logger.error(f"❌ [HUEY_SLOW] 构建下载任务失败: {e}", exc_info=True)
        raise e


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
def download_task(task_type: TaskType, symbol: str, **kwargs):
    """下载股票数据的 Huey 任务 (快速队列)

    下载完成后，直接调用慢速队列的数据处理任务。

    Args:
        task_type: 任务类型枚举
        symbol: 股票代码
        **kwargs: 额外的下载参数，如 start_date, end_date
    """
    try:
        logger.debug(f"🚀 [HUEY_FAST] 开始执行下载任务: {symbol} ({task_type})")

        # 从中心化的 app.py 获取共享的容器实例
        from ..app import container

        downloader = container.downloader()

        result = downloader.download(task_type, symbol, **kwargs)

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
