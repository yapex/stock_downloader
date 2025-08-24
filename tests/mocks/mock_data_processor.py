"""数据处理器模拟类

用于测试的数据处理器模拟实现。
"""

from typing import List, Optional
import pandas as pd
from neo.data_processor.interfaces import IDataProcessor


class MockDataProcessor(IDataProcessor):
    """模拟数据处理器

    用于测试，记录所有处理的数据。
    """

    def __init__(self):
        """初始化模拟数据处理器"""
        self.processed_data: List[tuple[str, str, Optional[pd.DataFrame]]] = []
        self.process_count = 0

    def process_data(
        self, task_type: str, symbol: str, data: Optional[pd.DataFrame]
    ) -> None:
        """处理数据

        Args:
            task_type: 任务类型
            symbol: 股票代码
            data: 数据
        """
        self.processed_data.append((task_type, symbol, data))
        self.process_count += 1

    def get_processed_count(self) -> int:
        """获取处理的数据数量"""
        return self.process_count

    def get_processed_data(self) -> List[tuple[str, str, Optional[pd.DataFrame]]]:
        """获取所有处理的数据"""
        return self.processed_data.copy()

    def clear(self) -> None:
        """清空记录的数据"""
        self.processed_data.clear()
        self.process_count = 0
