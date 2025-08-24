"""任务总线模拟类

用于测试的任务总线模拟实现。
"""

from typing import List, Protocol, Optional
import pandas as pd


class ITaskBus(Protocol):
    """任务总线接口"""

    def submit_data(
        self, task_type: str, symbol: str, data: Optional[pd.DataFrame]
    ) -> None:
        """提交数据"""
        ...


class MockTaskBus:
    """模拟任务总线"""

    def __init__(self):
        self.submitted_data: List[tuple[str, str, Optional[pd.DataFrame]]] = []

    def submit_data(
        self, task_type: str, symbol: str, data: Optional[pd.DataFrame]
    ) -> None:
        """提交数据

        Args:
            task_type: 任务类型
            symbol: 股票代码
            data: 数据
        """
        self.submitted_data.append((task_type, symbol, data))

    def clear(self) -> None:
        """清空已提交的数据"""
        self.submitted_data.clear()

    def get_data_count(self) -> int:
        """获取已提交的数据数量

        Returns:
            数据数量
        """
        return len(self.submitted_data)

    def get_submitted_data(self) -> List[tuple[str, str, Optional[pd.DataFrame]]]:
        """获取所有已提交的数据

        Returns:
            数据列表
        """
        return self.submitted_data.copy()
