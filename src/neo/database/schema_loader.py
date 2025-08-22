"""Schema 配置加载器

从 stock_schema.toml 文件中读取和解析表结构及任务配置信息。
"""

from functools import lru_cache
import tomllib
from pathlib import Path
from typing import Dict, List, Optional, Union
from box import Box
from .types import TableSchema
from .interfaces import ISchemaLoader


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

            # 创建 TableSchema 对象
            schema = TableSchema(
                table_name=config_box.get("table_name", table_name),
                api_method=config_box.get("api_method", ""),
                default_params=config_box.get("default_params", {}),
                primary_key=config_box.get("primary_key", []),
                description=config_box.get("description", ""),
                date_col=config_box.get("date_col"),
                columns=config_box.get("columns"),
                required_params=config_box.get("required_params", {}),
            )
            schemas[table_name] = schema

        return schemas

    @lru_cache(maxsize=None)
    def load_all_schemas(self) -> Dict[str, TableSchema]:
        """加载所有表的Schema配置

        Returns:
            所有表的Schema配置字典
        """
        if self._schemas is None:
            self._schemas = self._load_schemas()
        return self._schemas

    def load_schema(self, table_name: str) -> TableSchema:
        """加载指定表的Schema配置

        Args:
            table_name: 表名

        Returns:
            表的Schema配置
        """
        schemas = self.load_all_schemas()
        if table_name not in schemas:
            raise KeyError(f"表 '{table_name}' 的 Schema 配置不存在")
        return schemas[table_name]

    def get_table_names(self) -> List[str]:
        """获取所有表名

        Returns:
            表名列表
        """
        schemas = self.load_all_schemas()
        return list(schemas.keys())

    def get_table_config(self, table_name: str) -> Box:
        """获取表配置（Box格式，兼容原有代码）

        Args:
            table_name: 表名

        Returns:
            表配置Box对象
        """
        if not self.schema_file_path.exists():
            raise FileNotFoundError(f"Schema 文件不存在: {self.schema_file_path}")

        with open(self.schema_file_path, "rb") as f:
            toml_data = tomllib.load(f)

        if table_name not in toml_data:
            raise KeyError(f"表 '{table_name}' 的配置不存在")

        return Box(toml_data[table_name])

    def reload_schemas(self) -> None:
        """重新加载 schema 配置"""
        self._schemas = None
        self.load_all_schemas.cache_clear()
