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
        # 对于大多数时间序列数据，按年分区是一个很好的实践
        # 对于财务报表，按报告期（通常也是年）分区
        return ["year"]

    def process(self, task_type: str, data: pd.DataFrame) -> bool:
        """同步处理任务结果"""
        try:
            if data is None or data.empty:
                logger.debug("数据为空，跳过处理")
                return False

            logger.debug(
                f"{task_type} 数据维度: {len(data)} 行 x {len(data.columns)} 列"
            )

            # --- 创建分区列 ---
            # 确保数据中包含用于分区的列
            if "trade_date" in data.columns:
                # 从 YYYYMMDD 格式中提取年份
                data["year"] = data["trade_date"].str[:4]
            elif "end_date" in data.columns:
                data["year"] = data["end_date"].str[:4]
            else:
                # 如果没有日期列，则不进行分区
                self.parquet_writer.write(data, task_type, [])
                return True

            partition_cols = self._get_partition_cols(task_type)
            self.parquet_writer.write(data, task_type, partition_cols)
            return True

        except Exception as e:
            logger.error(f"💥 同步处理异常: {task_type} - {str(e)}")
            return False

    def shutdown(self) -> None:
        """关闭数据处理器，清理资源"""
        pass
