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
from ..database.interfaces import ISchemaLoader
from ..database.schema_loader import SchemaLoader

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
        schema_loader: Optional[ISchemaLoader] = None,
    ) -> "SimpleDataProcessor":
        """创建默认配置的同步数据处理器"""
        config = get_config()
        if parquet_writer is None:
            base_path = config.get("storage.parquet_base_path", "data/parquet")
            parquet_writer = ParquetWriter(base_path=base_path)
        if schema_loader is None:
            schema_loader = SchemaLoader()

        return cls(parquet_writer=parquet_writer, schema_loader=schema_loader)

    def __init__(
        self,
        parquet_writer: IParquetWriter,
        schema_loader: ISchemaLoader,
    ):
        """初始化同步数据处理器"""
        self.config = get_config()
        self.parquet_writer = parquet_writer
        self.schema_loader = schema_loader

    def _get_update_strategy(self, task_type: str) -> str:
        """获取任务类型的更新策略"""
        try:
            return self.config.download_tasks[task_type].get(
                "update_strategy", "incremental"
            )
        except Exception as e:
            logger.warning(f"获取 {task_type} 更新策略失败，使用默认策略: {e}")
            return "incremental"

    def _should_update_by_symbol(self, task_type: str) -> bool:
        """检查是否应按 symbol 进行更新"""
        try:
            return self.config.download_tasks[task_type].get("update_by_symbol", True)
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

            partition_cols: List[str] = []
            schema = self.schema_loader.load_schema(task_type)

            # --- L1 优化：确定分区列 ---
            date_col = schema.date_col
            if date_col and date_col in data.columns:
                data["year"] = data[date_col].str[:4]
                partition_cols = ["year"]
                logger.debug(f"根据 schema 配置，使用 '{date_col}' 列创建 'year' 分区")
            else:
                logger.debug(f"表 {task_type} 无分区配置或数据中缺少日期列，不进行分区")

            # --- L2 优化：按主键排序 ---
            primary_key = schema.primary_key
            if primary_key:
                # 确保排序键都在数据列中
                sort_keys = [key for key in primary_key if key in data.columns]
                if sort_keys:
                    data.sort_values(by=sort_keys, inplace=True, ignore_index=True)
                    logger.debug(f"数据已按主键 {sort_keys} 排序")

            # --- 执行写入 ---
            update_strategy = self._get_update_strategy(task_type)
            if update_strategy == 'full_replace':
                # 此分支现在只对 stock_basic 等非时间序列、非 by_symbol 的表有效
                self.parquet_writer.write_full_replace(data, task_type, partition_cols)
                logger.debug(f"使用全量替换策略写入 {task_type} 数据")
            else: # incremental
                self.parquet_writer.write(data, task_type, partition_cols, symbol)
                logger.debug(f"使用增量更新策略写入 {task_type} 数据")

            return True

        except KeyError:
            logger.warning(f"未找到 {task_type} 的 schema 定义，跳过处理")
            return False
        except Exception as e:
            logger.error(f"💥 同步处理异常: {task_type} - {str(e)}")
            return False

    def shutdown(self) -> None:
        """关闭数据处理器，清理资源"""
        pass
