"""简单数据处理器实现

专注数据清洗、转换和验证的数据处理层。
"""

import logging
from typing import Optional, List
import pandas as pd

from ..configs import get_config
from .interfaces import IDataProcessor
from ..writers.interfaces import IParquetWriter
from ..writers.parquet_writer import ParquetWriter

logger = logging.getLogger(__name__)


class SimpleDataProcessor(IDataProcessor):
    """同步数据处理器实现

    将数据清洗、转换后，使用 ParquetWriter 写入磁盘。
    支持根据配置选择增量更新或全量替换策略。
    """

    @classmethod
    def create_default(
        cls,
        parquet_writer: Optional[IParquetWriter] = None,
    ) -> "SimpleDataProcessor":
        """创建默认配置的同步数据处理器"""
        config = get_config()
        if parquet_writer is None:
            base_path = config.get("storage.parquet_base_path", "data/parquet")
            parquet_writer = ParquetWriter(base_path=base_path)

        return cls(parquet_writer=parquet_writer)

    def __init__(
        self,
        parquet_writer: IParquetWriter,
    ):
        """初始化同步数据处理器"""
        self.config = get_config()
        self.parquet_writer = parquet_writer

    def _get_partition_cols(self, task_type: str) -> List[str]:
        """根据任务类型获取用于分区的列名"""
        return ["year"]

    def _get_update_strategy(self, task_type: str) -> str:
        """获取任务类型的更新策略"""
        try:
            return self.config.get(
                f"download_tasks.{task_type}.update_strategy", "incremental"
            )
        except Exception as e:
            logger.warning(f"获取 {task_type} 更新策略失败，使用默认策略: {e}")
            return "incremental"

    def _should_update_by_symbol(self, task_type: str) -> bool:
        """检查是否应按 symbol 进行更新"""
        try:
            return self.config.get(f"download_tasks.{task_type}.update_by_symbol", True)
        except Exception as e:
            logger.warning(f"获取 {task_type} 更新方式失败，默认按 symbol 更新: {e}")
            return True

    def process(self, task_type: str, symbol: str, data: pd.DataFrame) -> bool:
        """同步处理任务结果"""
        try:
            if data is None or data.empty:
                logger.debug("数据为空，跳过处理")
                return False

            logger.debug(
                f"{task_type} 数据维度: {len(data)} 行 x {len(data.columns)} 列"
            )

            update_strategy = self._get_update_strategy(task_type)

            # --- 创建分区列 ---
            if "trade_date" in data.columns:
                data["year"] = data["trade_date"].str[:4]
            elif "end_date" in data.columns:
                data["year"] = data["end_date"].str[:4]
            else:
                # 如果没有日期列，则不进行分区
                if update_strategy == "full_replace":
                    if self._should_update_by_symbol(task_type):
                        self.parquet_writer.write_full_replace_by_symbol(
                            data, task_type, [], symbol
                        )
                    else:
                        self.parquet_writer.write_full_replace(data, task_type, [])
                    logger.debug(f"使用全量替换策略写入 {task_type} 数据（无分区）")
                else:
                    self.parquet_writer.write(data, task_type, [])
                    logger.debug(f"使用增量更新策略写入 {task_type} 数据（无分区）")
                return True

            partition_cols = self._get_partition_cols(task_type)

            # 根据更新策略和表类型选择写入方式
            if update_strategy == "full_replace":
                if self._should_update_by_symbol(task_type):
                    logger.debug(f"为 {task_type} 执行 {symbol} 的定向全量替换")
                    self.parquet_writer.write_full_replace_by_symbol(
                        data, task_type, partition_cols, symbol
                    )
                else:
                    logger.debug(f"为字典表 {task_type} 执行全局全量替换")
                    self.parquet_writer.write_full_replace(
                        data, task_type, partition_cols
                    )
            else:
                self.parquet_writer.write(data, task_type, partition_cols)
                logger.debug(f"使用增量更新策略写入 {task_type} 数据")

            return True

        except Exception as e:
            logger.error(f"💥 同步处理异常: {task_type} - {str(e)}")
            return False

    def shutdown(self) -> None:
        """关闭数据处理器，清理资源"""
        pass
