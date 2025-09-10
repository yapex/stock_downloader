"""
数据压缩和排序模块

提供数据压缩整合时的幂等（upsert）操作功能，支持：
- financial三表（balance_sheet,income,cash_flow）的特殊去重策略
- 其他表的通用去重策略
- UUID文件名生成避免覆盖
"""

from .compact_and_sort_service import CompactAndSortServiceImpl
from .deduplication_strategies import HybridDeduplicationStrategy
from .filename_generator import UUIDFilenameGenerator

__all__ = [
    "CompactAndSortServiceImpl",
    "HybridDeduplicationStrategy", 
    "UUIDFilenameGenerator"
]
