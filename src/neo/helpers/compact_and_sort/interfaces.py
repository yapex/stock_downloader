"""
数据压缩和排序模块的接口定义
"""

from typing import Protocol, Dict, Any
import ibis


class DataDeduplicationStrategy(Protocol):
    """数据去重策略接口"""

    def deduplicate(
        self, table: ibis.Table, table_config: Dict[str, Any]
    ) -> ibis.Table:
        """
        对表数据进行去重

        Args:
            table: ibis表对象
            table_config: 表配置信息

        Returns:
            去重后的ibis表对象
        """
        ...


class FilenameGenerator(Protocol):
    """文件名生成器接口"""

    def generate_filename(self, table_name: str) -> str:
        """
        生成唯一文件名

        Args:
            table_name: 表名

        Returns:
            唯一文件名（包含UUID后缀）
        """
        ...


class CompactAndSortService:
    """数据压缩和排序服务"""

    def __init__(
        self,
        deduplication_strategy: DataDeduplicationStrategy,
        filename_generator: FilenameGenerator,
    ):
        """
        初始化服务

        Args:
            deduplication_strategy: 数据去重策略
            filename_generator: 文件名生成器
        """
        self.deduplication_strategy = deduplication_strategy
        self.filename_generator = filename_generator

    def compact_and_sort_table(
        self, table_name: str, table_config: Dict[str, Any]
    ) -> None:
        """
        对单个表进行压缩和排序

        Args:
            table_name: 表名
            table_config: 表配置信息
        """
        # 具体实现将在后续补充
        pass

    def generate_unique_filename(self, table_name: str) -> str:
        """
        生成唯一文件名

        Args:
            table_name: 表名

        Returns:
            唯一文件名
        """
        return self.filename_generator.generate_filename(table_name)
