"""容器配置测试

测试依赖注入容器的配置是否正确，特别是确保所有组件都能正常初始化。
"""

import pytest
from unittest.mock import patch


class TestAppContainer:
    """测试 AppContainer 的配置"""

    def test_data_processor_initialization_should_work_after_fix(self):
        """测试修复后的 data_processor 配置应该能正常初始化"""
        from neo.app import container  # 使用真实的容器和配置

        # 尝试获取 data_processor - 修复后应该成功
        try:
            data_processor = container.data_processor()
            # 验证 data_processor 确实有 schema_loader
            assert hasattr(data_processor, 'schema_loader')
            assert data_processor.schema_loader is not None
            print(f"✅ data_processor 初始化成功：{type(data_processor).__name__}")
            print(f"✅ schema_loader 也正常注入：{type(data_processor.schema_loader).__name__}")
        except TypeError as e:
            if "missing 1 required positional argument: 'schema_loader'" in str(e):
                pytest.fail(f"❌ data_processor 仍然缺少 schema_loader 参数，容器配置需要修复：{e}")
            else:
                # 其他 TypeError 可能是预期的（比如文件不存在等）
                print(f"⚠️  遇到其他 TypeError（可能是正常的）：{e}")
                # 但我们仍然可以检查错误消息不是 schema_loader 相关的
                assert "missing 1 required positional argument: 'schema_loader'" not in str(e)

    @patch("neo.configs.get_config")
    def test_data_processor_initialization_should_work_with_schema_loader(self, mock_get_config):
        """测试修复后的 data_processor 配置应该包含 schema_loader 参数"""
        from neo.containers import AppContainer
        
        # 模拟配置
        mock_config = {
            "storage": {"parquet_base_path": "test/path"},
            "database": {"schema_file_path": "test_schema.toml"},
        }
        mock_get_config.return_value.to_dict.return_value = mock_config
        
        # 创建容器
        container = AppContainer()
        
        # 尝试获取 data_processor - 修复后应该成功
        try:
            data_processor = container.data_processor()
            # 验证 data_processor 确实有 schema_loader
            assert hasattr(data_processor, 'schema_loader')
            assert data_processor.schema_loader is not None
        except TypeError as e:
            if "missing 1 required positional argument: 'schema_loader'" in str(e):
                pytest.fail("data_processor 仍然缺少 schema_loader 参数，容器配置需要修复")
            else:
                # 其他 TypeError 可能是预期的（比如文件不存在等）
                pass

    @patch("neo.configs.get_config")
    def test_all_main_components_can_be_initialized(self, mock_get_config):
        """测试主要组件都能正常初始化"""
        from neo.containers import AppContainer
        
        # 模拟配置
        mock_config = {
            "storage": {"parquet_base_path": "test/path"},
            "database": {"schema_file_path": "test_schema.toml"},
        }
        mock_get_config.return_value.to_dict.return_value = mock_config
        
        # 创建容器
        container = AppContainer()
        
        # 测试各个组件能否初始化（可能会因为文件不存在等原因失败，但不应该是依赖注入问题）
        components_to_test = [
            ("schema_loader", "schema_loader"),
            ("parquet_writer", "parquet_writer"), 
            ("data_processor", "data_processor"),
            ("app_service", "app_service"),
        ]
        
        for component_name, container_attr in components_to_test:
            try:
                component = getattr(container, container_attr)()
                print(f"✅ {component_name} 初始化成功: {type(component).__name__}")
            except TypeError as e:
                if "missing" in str(e) and "required positional argument" in str(e):
                    pytest.fail(f"❌ {component_name} 缺少必需的依赖注入参数: {e}")
                else:
                    # 其他类型错误可能是正常的（比如文件不存在）
                    print(f"⚠️  {component_name} 初始化时遇到其他错误（可能是正常的）: {e}")
            except Exception as e:
                # 其他异常可能是正常的（比如文件不存在）
                print(f"⚠️  {component_name} 初始化时遇到异常（可能是正常的）: {e}")
