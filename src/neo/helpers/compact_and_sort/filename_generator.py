"""
文件名生成器实现
"""

import uuid
from .interfaces import FilenameGenerator


class UUIDFilenameGenerator(FilenameGenerator):
    """UUID文件名生成器"""

    def generate_filename(self, table_name: str) -> str:
        """
        生成包含UUID的唯一文件名

        Args:
            table_name: 表名

        Returns:
            格式：{table_name}-{8位UUID}.parquet
        """
        unique_id = str(uuid.uuid4())[:8]  # 使用UUID前8位确保唯一性
        return f"{table_name}-{unique_id}.parquet"
