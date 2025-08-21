"""Schema 加载器接口定义"""

from typing import Protocol, List, Dict, Optional, Union
from pathlib import Path
from downloader.database.types import TableSchema


class ISchemaLoader(Protocol):
    """Schema 加载器接口"""
    
    def get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        """获取指定表的 schema
        
        Args:
            table_name: 表名
            
        Returns:
            表的 schema，如果表不存在则返回 None
        """
        ...
    
    def get_all_table_schemas(self) -> Dict[str, TableSchema]:
        """获取所有表的 schema
        
        Returns:
            所有表的 schema 字典
        """
        ...
    
    def get_table_names(self) -> List[str]:
        """获取所有表名
        
        Returns:
            表名列表
        """
        ...