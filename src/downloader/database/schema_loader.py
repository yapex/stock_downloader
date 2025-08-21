"""Schema 配置加载器

从 stock_schema.toml 文件中读取和解析表结构及任务配置信息。
"""

from functools import lru_cache
import tomllib
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
from box import Box
from downloader.database.types import TableSchema
from downloader.database.interfaces import ISchemaLoader


class SchemaLoader(ISchemaLoader):
    """从 TOML 文件加载 Schema 配置"""

    def __init__(self, schema_file_path: Optional[Union[str, Path]] = None):
        """初始化 Schema 加载器

        Args:
            schema_file_path: schema 文件路径，默认为项目根目录下的 stock_schema.toml
        """
        if schema_file_path is None:
            # 默认路径：从当前文件向上查找项目根目录
            current_dir = Path(__file__).parent
            project_root = current_dir
            while project_root.parent != project_root:
                if (project_root / "stock_schema.toml").exists():
                    break
                project_root = project_root.parent
            schema_file_path = project_root / "stock_schema.toml"

        # 确保 schema_file_path 是 Path 对象
        self.schema_file_path = Path(schema_file_path)
        self._schemas: Optional[Dict[str, TableSchema]] = None

    def _load_schemas(self) -> Dict[str, TableSchema]:
        """加载所有 schema 配置"""
        if not self.schema_file_path.exists():
            raise FileNotFoundError(f"Schema 文件不存在: {self.schema_file_path}")

        with open(self.schema_file_path, "rb") as f:
            toml_data = tomllib.load(f)

        schemas = {}
        for table_name, config in toml_data.items():
            # 使用 Box 包装以便访问
            config_box = Box(config)

            # 验证必需字段
            required_fields = [
                "table_name",
                "api_method",
                "default_params",
                "primary_key",
                "description",
            ]
            for field in required_fields:
                if field not in config_box:
                    raise ValueError(f"表 {table_name} 缺少必需字段: {field}")

            # 创建 TableSchema 对象
            schema = TableSchema(
                table_name=config_box.table_name,
                api_method=config_box.api_method,
                default_params=dict(config_box.default_params),
                primary_key=list(config_box.primary_key),
                description=config_box.description,
                date_col=config_box.get("date_col"),
                columns=config_box.get("columns"),
            )

            schemas[table_name] = schema

        return schemas

    def get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        """获取指定表的 schema 配置"""
        if self._schemas is None:
            self._schemas = self._load_schemas()

        return self._schemas.get(table_name)

    @lru_cache(maxsize=1)
    def get_all_table_schemas(self) -> Dict[str, TableSchema]:
        """获取所有表的 schema 配置"""
        if self._schemas is None:
            self._schemas = self._load_schemas()

        return self._schemas.copy()

    def get_table_names(self) -> List[str]:
        """获取所有表名"""
        if self._schemas is None:
            self._schemas = self._load_schemas()

        return list(self._schemas.keys())

    def reload(self) -> None:
        """重新加载配置文件"""
        self._schemas = None
