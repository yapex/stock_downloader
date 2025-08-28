"""数据处理器接口定义

定义数据处理相关的接口规范。
"""

from typing import Protocol
import pandas as pd


class IDataProcessor(Protocol):
    """数据处理器接口

    专注于数据清洗、转换和验证，不处理业务逻辑。
    """

    def process(self, task_type: str, data: pd.DataFrame) -> bool:
        """处理任务结果

        Args:
            task_type: 任务类型字符串
            data: 要处理的数据

        Returns:
            bool: 处理是否成功
        """
        ...
