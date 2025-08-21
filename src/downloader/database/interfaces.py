"""Database 包的接口定义"""

from abc import ABC, abstractmethod
from typing import Protocol, List, Dict, Any, Optional, Union
from pathlib import Path
import pandas as pd
from downloader.database.types import TableSchema


class IDBOperator(Protocol):
    """数据库操作器接口"""
    
    def upsert(self, table_key: str, data: pd.DataFrame) -> None:
        """插入或更新数据
        
        Args:
            table_key: 表键名
            data: 要插入或更新的数据
        """
        ...
    
    def get_all_symbols(self) -> List[str]:
        """获取所有股票代码
        
        Returns:
            股票代码列表
        """
        ...
    
    def create_table(self, table_key: str) -> None:
        """创建表
        
        Args:
            table_key: 表键名
        """
        ...
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在
        
        Args:
            table_name: 表名
            
        Returns:
            表是否存在
        """
        ...


class ISchemaTableCreator(Protocol):
    """Schema 表创建器接口"""
    
    def create_table(self, table_key: str) -> None:
        """创建表
        
        Args:
            table_key: 表键名
        """
        ...
    
    def create_all_tables(self) -> None:
        """创建所有表"""
        ...
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在
        
        Args:
            table_name: 表名
            
        Returns:
            表是否存在
        """
        ...


class ISchemaFileLoader(ABC):
    """Schema 文件加载器抽象基类"""
    
    @abstractmethod
    def load_schema(self, schema_path: Path) -> Dict[str, TableSchema]:
        """加载 schema 文件
        
        Args:
            schema_path: schema 文件路径
            
        Returns:
            表名到 schema 的映射
        """
        pass


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