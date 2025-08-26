"""简单数据处理器实现

专注数据清洗、转换和验证的数据处理层。
"""

import logging
from typing import Optional, Dict
import pandas as pd

from ..configs import get_config
from .interfaces import IDataProcessor
from ..database.operator import DBOperator
from ..database.interfaces import ISchemaLoader
from ..database.schema_loader import SchemaLoader

logger = logging.getLogger(__name__)


class SimpleDataProcessor(IDataProcessor):
    """同步数据处理器实现

    专注于数据清洗、转换和验证的同步数据处理器。
    """

    @classmethod
    def create_default(
        cls,
        db_operator: Optional[DBOperator] = None,
        schema_loader: Optional[ISchemaLoader] = None,
    ) -> "SimpleDataProcessor":
        """创建默认配置的同步数据处理器"""
        return cls(
            db_operator=db_operator or DBOperator.create_default(),
            schema_loader=schema_loader or SchemaLoader(),
        )

    def __init__(
        self,
        db_operator: Optional[DBOperator] = None,
        schema_loader: Optional[ISchemaLoader] = None,
    ):
        """初始化同步数据处理器"""
        self.config = get_config()
        self.db_operator = db_operator or DBOperator()
        self.schema_loader = schema_loader or SchemaLoader()

    def _get_table_name(self, task_type) -> Optional[str]:
        """根据任务类型获取对应的表名"""
        try:
            type_name = task_type.name if hasattr(task_type, "name") else str(task_type)
            schema = self.schema_loader.load_schema(type_name)
            return schema.table_name
        except KeyError:
            type_name = task_type.name if hasattr(task_type, "name") else str(task_type)
            logger.debug(f"未找到任务类型 '{type_name}' 对应的表配置")
            return None

    def process(self, task_type: str, data: pd.DataFrame) -> bool:
        """同步处理任务结果"""
        try:
            if data is None or data.empty:
                logger.debug("数据为空，跳过处理")
                return False

            logger.debug(
                f"{task_type} 数据维度: {len(data)} 行 x {len(data.columns)} 列"
            )

            success = self._save_data(data, task_type)
            return success

        except Exception as e:
            logger.error(f"💥 同步处理异常: {task_type} - {str(e)}")
            return False

    def _save_data(self, data: pd.DataFrame, task_type: str) -> bool:
        """数据保存"""
        try:
            table_name = self._get_table_name(task_type)
            if not table_name:
                logger.debug(f"未知的任务类型: {task_type}")
                return False

            self.db_operator.upsert(table_name, data)
            logger.info(f"✅ {table_name} 成功保存了 {len(data)} 条数据")
            return True

        except Exception as e:
            logger.error(f"{table_name} 同步数据保存失败: {e}")
            return False

    def shutdown(self) -> None:
        """关闭数据处理器，清理资源"""
        pass