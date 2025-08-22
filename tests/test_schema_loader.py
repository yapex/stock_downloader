import pytest
import tempfile
import os
from pathlib import Path
from neo.database.schema_loader import SchemaLoader


class TestSchemaLoader:
    """SchemaLoader 类的测试用例"""

    def setup_method(self):
        """每个测试方法执行前的设置"""
        self.test_toml_content = """
[stock_basic]
table_name = "stock_basic"
primary_key = ["ts_code"]
description = "股票基本信息表字段"
api_method = "stock_basic"
default_params = { ts_code = "600519.SH" }
columns = [
    { name = "ts_code", type = "TEXT" },
    { name = "symbol", type = "TEXT" },
]

[stock_daily]
table_name = "stock_daily"
primary_key = ["ts_code", "trade_date"]
date_col = "trade_date"
description = "股票日线数据字段"
api_method = "daily"
default_params = { ts_code = "600519.SH", trade_date = "20150726" }
columns = [
    { name = "ts_code", type = "TEXT" },
    { name = "trade_date", type = "TEXT" },
]
"""

    def test_init_with_valid_file(self):
        """测试使用有效文件初始化 SchemaLoader"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(self.test_toml_content)
            temp_file = f.name

        try:
            loader = SchemaLoader(temp_file)
            assert loader.schema_file_path == Path(temp_file)
            table_names = loader.get_table_names()
            assert len(table_names) == 2
            assert "stock_basic" in table_names
            assert "stock_daily" in table_names
        finally:
            os.unlink(temp_file)

    def test_init_with_nonexistent_file(self):
        """测试使用不存在的文件初始化 SchemaLoader"""
        loader = SchemaLoader("/nonexistent/path/schema.toml")
        with pytest.raises(FileNotFoundError):
            loader.get_table_names()  # 只有在实际加载时才会抛出异常

    def test_init_with_invalid_toml(self):
        """测试使用无效 TOML 文件初始化 SchemaLoader"""
        invalid_toml = "[invalid toml content"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(invalid_toml)
            temp_file = f.name

        try:
            loader = SchemaLoader(temp_file)
            with pytest.raises(Exception):  # tomllib.TOMLDecodeError 或其他解析错误
                loader.get_table_names()  # 只有在实际加载时才会抛出异常
        finally:
            os.unlink(temp_file)

    def test_get_table_schema_existing_table(self):
        """测试获取存在的表的 schema"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(self.test_toml_content)
            temp_file = f.name

        try:
            loader = SchemaLoader(temp_file)
            schema = loader.load_schema("stock_basic")

            assert schema is not None
            assert schema.table_name == "stock_basic"
            assert schema.api_method == "stock_basic"
            assert schema.default_params == {"ts_code": "600519.SH"}
            assert len(schema.columns) == 2
            assert schema.columns[0]["name"] == "ts_code"
        finally:
            os.unlink(temp_file)

    def test_get_table_schema_nonexistent_table(self):
        """测试获取不存在的表的 schema"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(self.test_toml_content)
            temp_file = f.name

        try:
            loader = SchemaLoader(temp_file)
            try:
                loader.load_schema("nonexistent_table")
                assert False, "应该抛出 KeyError"
            except KeyError:
                pass  # 预期的异常
            # 测试已经通过异常处理验证
        finally:
            os.unlink(temp_file)

    def test_get_all_table_schemas(self):
        """测试获取所有表的 schemas"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(self.test_toml_content)
            temp_file = f.name

        try:
            loader = SchemaLoader(temp_file)
            all_schemas = loader.load_all_schemas()

            assert len(all_schemas) == 2
            assert "stock_basic" in all_schemas
            assert "stock_daily" in all_schemas
            assert all_schemas["stock_basic"].api_method == "stock_basic"
            assert all_schemas["stock_daily"].api_method == "daily"
        finally:
            os.unlink(temp_file)

    def test_get_table_names(self):
        """测试获取所有表名"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(self.test_toml_content)
            temp_file = f.name

        try:
            loader = SchemaLoader(temp_file)
            table_names = loader.get_table_names()

            assert len(table_names) == 2
            assert "stock_basic" in table_names
            assert "stock_daily" in table_names
            assert isinstance(table_names, list)
        finally:
            os.unlink(temp_file)

    def test_schema_with_optional_fields(self):
        """测试包含可选字段的 schema"""
        toml_with_optional = """
[test_table]
table_name = "test_table"
primary_key = ["id"]
description = "测试表"
api_method = "test_api"
default_params = { id = "123" }
date_col = "created_at"
columns = [
    { name = "id", type = "TEXT" },
]
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml_with_optional)
            temp_file = f.name

        try:
            loader = SchemaLoader(temp_file)
            schema = loader.load_schema("test_table")

            assert schema is not None
            assert schema.table_name == "test_table"
            assert schema.date_col == "created_at"
            assert schema.primary_key == ["id"]
            assert schema.api_method == "test_api"
            assert schema.default_params == {"id": "123"}
        finally:
            os.unlink(temp_file)

    def test_schema_without_optional_fields(self):
        """测试不包含可选字段的 schema"""
        minimal_toml = """
[minimal_table]
table_name = "minimal_table"
primary_key = ["id"]
description = "最小表配置"
api_method = "minimal_api"
default_params = { param = "value" }
columns = [
    { name = "col1", type = "TEXT" },
]
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(minimal_toml)
            temp_file = f.name

        try:
            loader = SchemaLoader(temp_file)
            schema = loader.load_schema("minimal_table")

            assert schema is not None
            assert schema.table_name == "minimal_table"
            assert schema.api_method == "minimal_api"
            assert schema.date_col is None
            assert schema.primary_key == ["id"]
        finally:
            os.unlink(temp_file)

    def test_empty_default_params(self):
        """测试空的 default_params"""
        toml_empty_params = """
[empty_params_table]
table_name = "empty_params_table"
primary_key = ["id"]
description = "空参数表"
api_method = "empty_api"
default_params = { }
columns = [
    { name = "col1", type = "TEXT" },
]
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml_empty_params)
            temp_file = f.name

        try:
            loader = SchemaLoader(temp_file)
            schema = loader.load_schema("empty_params_table")

            assert schema is not None
            assert schema.default_params == {}
        finally:
            os.unlink(temp_file)

    def test_complex_default_params(self):
        """测试复杂的 default_params"""
        complex_toml = """
[complex_table]
table_name = "complex_table"
primary_key = ["ts_code"]
description = "复杂参数表"
api_method = "complex_api"
default_params = { ts_code = "600519.SH", adj = "qfq", start_date = "20240101", end_date = "20240131" }
columns = [
    { name = "col1", type = "TEXT" },
]
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(complex_toml)
            temp_file = f.name

        try:
            loader = SchemaLoader(temp_file)
            schema = loader.load_schema("complex_table")

            expected_params = {
                "ts_code": "600519.SH",
                "adj": "qfq",
                "start_date": "20240101",
                "end_date": "20240131",
            }
            assert schema is not None
            assert schema.default_params == expected_params
        finally:
            os.unlink(temp_file)
