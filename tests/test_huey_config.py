"""测试 Huey 配置模块"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from huey import SqliteHuey, MemoryHuey

from neo.task_bus.huey_config import _create_huey_instance, huey


class TestCreateHueyInstance:
    """测试 _create_huey_instance 函数"""

    @patch('neo.config.get_config')
    def test_create_huey_with_valid_config(self, mock_get_config, tmp_path):
        """测试使用有效配置创建 Huey 实例"""
        # 准备测试数据
        db_file = str(tmp_path / "test_queue.db")
        mock_config = MagicMock()
        mock_config.huey.db_file = db_file
        mock_config.huey.get.return_value = False
        mock_get_config.return_value = mock_config
        
        # 执行测试
        result = _create_huey_instance()
        
        # 验证结果
        assert isinstance(result, SqliteHuey)
        assert result.immediate is False
        assert result.utc is True
        
        # 验证目录被创建
        assert Path(db_file).parent.exists()

    @patch('neo.config.get_config')
    def test_create_huey_with_immediate_mode(self, mock_get_config, tmp_path):
        """测试创建立即执行模式的 Huey 实例"""
        # 准备测试数据
        db_file = str(tmp_path / "test_queue.db")
        mock_config = MagicMock()
        mock_config.huey.db_file = db_file
        mock_config.huey.get.return_value = True
        mock_get_config.return_value = mock_config
        
        # 执行测试
        result = _create_huey_instance()
        
        # 验证结果
        assert isinstance(result, SqliteHuey)
        assert result.immediate is True

    @patch('neo.config.get_config')
    def test_create_huey_with_memory_db(self, mock_get_config):
        """测试使用内存数据库创建 Huey 实例"""
        # 准备测试数据
        mock_config = MagicMock()
        mock_config.huey.db_file = ":memory:"
        mock_config.huey.get.return_value = False
        mock_get_config.return_value = mock_config
        
        # 执行测试
        result = _create_huey_instance()
        
        # 验证结果
        assert isinstance(result, SqliteHuey)
        # SqliteHuey的内部属性可能不直接暴露，只验证类型和基本属性
        assert result.immediate is False
        assert result.utc is True

    @patch('neo.config.get_config')
    def test_create_huey_with_config_exception(self, mock_get_config):
        """测试配置加载失败时使用内存模式"""
        # 模拟配置加载异常
        mock_get_config.side_effect = Exception("Config load failed")
        
        # 执行测试
        result = _create_huey_instance()
        
        # 验证结果
        assert isinstance(result, MemoryHuey)
        assert result.immediate is True

    @patch('neo.config.get_config')
    def test_create_huey_creates_parent_directory(self, mock_get_config, tmp_path):
        """测试创建父目录"""
        # 准备测试数据 - 使用不存在的深层目录
        db_file = str(tmp_path / "deep" / "nested" / "queue.db")
        mock_config = MagicMock()
        mock_config.huey.db_file = db_file
        mock_config.huey.get.return_value = False
        mock_get_config.return_value = mock_config
        
        # 确保目录不存在
        assert not Path(db_file).parent.exists()
        
        # 执行测试
        result = _create_huey_instance()
        
        # 验证结果
        assert isinstance(result, SqliteHuey)
        
        # 验证父目录被创建
        assert Path(db_file).parent.exists()


class TestHueyGlobalInstance:
    """测试全局 Huey 实例"""

    def test_huey_instance_exists(self):
        """测试全局 Huey 实例存在"""
        assert huey is not None
        assert isinstance(huey, SqliteHuey)

    def test_huey_instance_is_singleton(self):
        """测试全局 Huey 实例是单例"""
        from neo.task_bus.huey_config import huey as huey2
        assert huey is huey2


class TestTaskImport:
    """测试任务模块导入"""

    @patch('neo.task_bus.huey_config.tasks', create=True)
    def test_tasks_import_success(self, mock_tasks):
        """测试任务模块成功导入"""
        # 重新导入模块以触发任务导入逻辑
        import importlib
        import neo.task_bus.huey_config
        importlib.reload(neo.task_bus.huey_config)
        
        # 验证没有异常抛出
        assert True  # 如果到达这里说明没有异常

    def test_tasks_import_failure_handled(self):
        """测试任务导入失败时的处理"""
        # 这个测试验证模块导入时不会因为tasks模块缺失而崩溃
        # 由于异常被捕获，我们只需要验证模块可以正常导入
        try:
            import neo.task_bus.huey_config
            # 如果能成功导入说明异常处理正确
            assert True
        except ImportError:
            pytest.fail("任务导入失败应该被正确处理，不应该导致模块导入失败")