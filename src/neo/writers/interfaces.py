"""数据写入器接口"""

from typing import List, Protocol
import pandas as pd


class IParquetWriter(Protocol):
    """Parquet 文件写入器接口"""

    def write(
        self, data: pd.DataFrame, task_type: str, partition_cols: List[str]
    ) -> None:
        """将 DataFrame 写入到分区的 Parquet 文件中

        Args:
            data (pd.DataFrame): 要写入的数据
            task_type (str): 任务类型，用于确定存储的根路径
            partition_cols (List[str]): 用于分区的列名列表
        """
        pass

    def write_full_replace(
        self, data: pd.DataFrame, task_type: str, partition_cols: List[str]
    ) -> None:
        """全量替换写入 (用于字典表等非 `symbol` 分区的数据)

        Args:
            data (pd.DataFrame): 要写入的数据
            task_type (str): 任务类型，用于确定存储的根路径
            partition_cols (List[str]): 用于分区的列名列表
        """
        pass

    def write_full_replace_by_symbol(
        self, data: pd.DataFrame, task_type: str, partition_cols: List[str], symbol: str
    ) -> None:
        """按 symbol 全量替换写入 (用于按 `symbol` 分区的数据)

        Args:
            data (pd.DataFrame): 要写入的数据
            task_type (str): 任务类型，用于确定存储的根路径
            partition_cols (List[str]): 用于分区的列名列表
            symbol (str): 股票代码，用于精确定位要替换的数据
        """
        pass
