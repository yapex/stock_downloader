"""数据库相关类型定义"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum


@dataclass
class TableSchema:
    """表结构配置"""

    table_name: str
    api_method: str
    default_params: Dict[str, Any]
    primary_key: List[str]
    description: str
    date_col: Optional[str] = None
    columns: Optional[List[Dict[str, str]]] = None
    required_params: Dict[str, Any] = None

    def __post_init__(self):
        if self.required_params is None:
            self.required_params = {}
