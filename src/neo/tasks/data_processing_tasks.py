"""数据处理任务模块

包含股票数据处理相关的 Huey 任务。
"""

import logging
from typing import List, Dict, Any

import pandas as pd
from ..configs.huey_config import huey_slow

logger = logging.getLogger(__name__)


class DataProcessor:
    """数据处理器，负责处理和验证数据"""

    def __init__(self):
        pass

    def _validate_data_frame(
        self, data_frame: List[Dict[str, Any]], task_type: str, symbol: str
    ) -> pd.DataFrame:
        """验证并转换数据格式

        Args:
            data_frame: 字典列表形式的数据
            task_type: 任务类型
            symbol: 股票代码

        Returns:
            pd.DataFrame: 转换后的数据框

        Raises:
            ValueError: 当数据无效时
        """
        if not data_frame or not isinstance(data_frame, list) or len(data_frame) == 0:
            raise ValueError(f"数据为空或格式无效: {symbol}_{task_type}")

        try:
            df_data = pd.DataFrame(data_frame)
            logger.debug(
                f"🐌 [HUEY_SLOW] 数据验证通过: {symbol}_{task_type}, 数据行数: {len(df_data)}"
            )
            return df_data
        except Exception as e:
            raise ValueError(f"数据转换失败: {symbol}_{task_type}, 错误: {e}")

    def _process_with_container(self, task_type: str, df_data: pd.DataFrame) -> bool:
        """使用容器中的数据处理器处理数据

        Args:
            task_type: 任务类型
            df_data: 要处理的数据框

        Returns:
            bool: 处理是否成功
        """
        from ..app import container
        from ..data_processor.data_processor_factory import DataProcessorFactory

        # 使用工厂根据任务类型选择合适的数据处理器
        factory = DataProcessorFactory(container)
        data_processor = factory.create_processor(task_type)
        
        try:
            process_success = data_processor.process(task_type, df_data)
            logger.debug(f"[HUEY] {task_type} 数据处理器返回结果: {process_success}")
            return process_success
        finally:
            # 确保数据处理器正确关闭，刷新所有缓冲区数据
            data_processor.shutdown()

    def process_data(
        self, task_type: str, symbol: str, data_frame: List[Dict[str, Any]]
    ) -> bool:
        """处理数据的主要方法

        Args:
            task_type: 任务类型
            symbol: 股票代码
            data_frame: 字典列表形式的数据

        Returns:
            bool: 处理是否成功
        """
        try:
            # 验证和转换数据
            df_data = self._validate_data_frame(data_frame, task_type, symbol)

            # 处理数据
            logger.debug(
                f"🐌 [HUEY_SLOW] 开始异步保存数据: {symbol}_{task_type}, 数据行数: {len(df_data)}"
            )
            return self._process_with_container(task_type, df_data)

        except ValueError as e:
            logger.warning(f"⚠️ [HUEY_SLOW] 数据处理失败: {e}")
            return False
        except Exception as e:
            logger.error(
                f"❌ [HUEY_SLOW] 数据处理异常: {symbol}_{task_type}, 错误: {e}",
                exc_info=True,
            )
            raise e


def _process_data_sync(task_type: str, data: pd.DataFrame) -> bool:
    """异步处理数据的公共函数（保持向后兼容）

    Args:
        task_type: 任务类型字符串
        data: 要处理的数据

    Returns:
        bool: 处理是否成功
    """
    processor = DataProcessor()
    # 将DataFrame转换为字典列表格式以使用新的处理方法
    data_records = data.to_dict("records")
    return processor.process_data(task_type, "", data_records)


@huey_slow.task()
def process_data_task(
    task_type: str, symbol: str, data_frame: List[Dict[str, Any]]
) -> bool:
    """数据处理任务 (慢速队列)

    Args:
        task_type: 任务类型字符串
        symbol: 股票代码
        data_frame: DataFrame 数据 (字典列表形式)

    Returns:
        bool: 处理是否成功
    """
    try:
        processor = DataProcessor()
        result = processor.process_data(task_type, symbol, data_frame)

        logger.info(f"🏆 [HUEY_SLOW] 最终结果: {symbol}_{task_type}, 成功: {result}")
        return result

    except Exception as e:
        logger.error(f"❌ [HUEY_SLOW] 数据处理任务执行失败: {symbol}, 错误: {e}")
        raise e
