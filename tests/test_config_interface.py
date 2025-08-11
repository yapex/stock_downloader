"""配置接口测试

测试ConfigInterface及其实现类的功能。
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch

from src.downloader.config_impl import ConfigManager, create_config_manager
from src.downloader.interfaces import ConfigInterface


@pytest.fixture
def sample_config_data():
    """示例配置数据"""
    return {
        "tushare_token": "test_token",
        "database": {
            "path": "test.db"
        },
        "downloader": {
            "max_producers": 2,
            "max_consumers": 3,
            "producer_queue_size": 500,
            "data_queue_size": 1000,
            "symbols": ["600519", "000001"]
        },
        "consumer": {
            "batch_size": 50,
            "flush_interval": 60,
            "max_retries": 5
        },
        "tasks": {
            "daily_qfq": {
                "name": "日K线数据(前复权)",
                "type": "daily",
                "enabled": True,
                "start_date": "2020-01-01",
                "end_date": "2023-12-31",
                "date_col": "trade_date"
            },
            "financial_income": {
                "name": "财务报表-利润表",
                "type": "financials",
                "enabled": False,
                "statement_type": "income",
                "date_col": "ann_date"
            }
        },
        "groups": {
            "default": {
                "description": "默认组",
                "symbols": ["600519", "000001"],
                "tasks": ["daily_qfq"]
            },
            "all": {
                "description": "全股票组",
                "symbols": "all",
                "tasks": ["daily_qfq", "financial_income"]
            }
        }
    }


@pytest.fixture
def temp_config_file(sample_config_data):
    """创建临时配置文件"""
    import yaml
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(sample_config_data, f, default_flow_style=False, allow_unicode=True)
        temp_path = f.name
    
    yield temp_path
    
    # 清理
    os.unlink(temp_path)


class TestConfigManager:
    """配置管理器测试"""
    
    def test_create_config_manager(self, temp_config_file):
        """测试创建配置管理器"""
        config = create_config_manager(temp_config_file)
        assert isinstance(config, ConfigInterface)
        assert config.validate()
    
    def test_tushare_token_from_config(self, temp_config_file):
        """测试从配置文件获取token"""
        config = ConfigManager(temp_config_file)
        assert config.get_runtime_token() == "test_token"
    
    @patch.dict(os.environ, {'TUSHARE_TOKEN': 'env_token'})
    def test_tushare_token_from_env(self, temp_config_file):
        """测试从环境变量获取token"""
        config = ConfigManager(temp_config_file)
        assert config.get_runtime_token() == "env_token"
    
    def test_database_config(self, temp_config_file):
        """测试数据库配置"""
        config = ConfigManager(temp_config_file)
        db_config = config.database
        assert db_config.path == "test.db"
    
    def test_downloader_config(self, temp_config_file):
        """测试下载器配置"""
        config = ConfigManager(temp_config_file)
        dl_config = config.downloader
        assert dl_config.max_producers == 2
        assert dl_config.max_consumers == 3
        assert dl_config.producer_queue_size == 500
        assert dl_config.data_queue_size == 1000
        assert dl_config.symbols == ["600519", "000001"]
    
    def test_consumer_config(self, temp_config_file):
        """测试消费者配置"""
        config = ConfigManager(temp_config_file)
        consumer_config = config.consumer
        assert consumer_config.batch_size == 50
        assert consumer_config.flush_interval == 60
        assert consumer_config.max_retries == 5
    
    def test_task_config(self, temp_config_file):
        """测试任务配置"""
        config = ConfigManager(temp_config_file)
        
        # 测试存在的任务
        task_config = config.get_task_config("daily_qfq")
        assert task_config is not None
        assert task_config.name == "日K线数据(前复权)"
        assert task_config.type == "daily"
        assert task_config.get("enabled", False) is True
        assert task_config.get("start_date") == "2020-01-01"
        assert task_config.get("end_date") == "2023-12-31"
        assert task_config.date_col == "trade_date"
        
        # 测试不存在的任务
        assert config.get_task_config("nonexistent") is None
    
    def test_group_config(self, temp_config_file):
        """测试任务组配置"""
        config = ConfigManager(temp_config_file)
        
        # 测试存在的组
        group_config = config.get_group_config("default")
        assert group_config is not None
        assert group_config.description == "默认组"
        assert group_config.symbols == ["600519", "000001"]
        assert group_config.tasks == ["daily_qfq"]
        
        # 测试不存在的组
        assert config.get_group_config("nonexistent") is None
    
    def test_get_all_tasks(self, temp_config_file):
        """测试获取所有任务"""
        config = ConfigManager(temp_config_file)
        all_tasks = config.get_all_tasks()
        assert len(all_tasks) == 2
        assert "daily_qfq" in all_tasks
        assert "financial_income" in all_tasks
    
    def test_get_all_groups(self, temp_config_file):
        """测试获取所有任务组"""
        config = ConfigManager(temp_config_file)
        all_groups = config.get_all_groups()
        assert len(all_groups) == 2
        assert "default" in all_groups
        assert "all" in all_groups
    

    
    def test_validate_valid_config(self, temp_config_file):
        """测试验证有效配置"""
        config = ConfigManager(temp_config_file)
        assert config.validate() is True
    
    def test_validate_invalid_config_no_token(self, temp_config_file, sample_config_data):
        """测试验证无效配置 - 缺少token"""
        # 移除token
        sample_config_data.pop("tushare_token")
        
        import yaml
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(sample_config_data, f, default_flow_style=False, allow_unicode=True)
            invalid_config_path = f.name
        
        try:
            config = ConfigManager(invalid_config_path)
            assert config.validate() is False
        finally:
            os.unlink(invalid_config_path)
    
    def test_default_values(self):
        """测试默认值"""
        minimal_config = {
            "tushare_token": "test_token",
            "database": {},
            "downloader": {},
            "consumer": {},
            "tasks": {},
            "groups": {}
        }
        
        import yaml
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(minimal_config, f, default_flow_style=False, allow_unicode=True)
            minimal_config_path = f.name
        
        try:
            config = ConfigManager(minimal_config_path)
            
            # 测试默认值
            assert config.database.path == "data/stock.db"
            assert config.downloader.max_producers == 1
            assert config.downloader.max_consumers == 2
            assert config.consumer.batch_size == 100
            assert config.consumer.flush_interval == 30
            assert config.consumer.max_retries == 3
        finally:
            os.unlink(minimal_config_path)


class TestConfigIntegration:
    """配置接口集成测试"""
    
    def test_protocol_compliance(self, temp_config_file):
        """测试Protocol接口合规性"""
        config: ConfigInterface = create_config_manager(temp_config_file)
        
        # 测试所有接口方法都可以调用
        assert isinstance(config.get_runtime_token(), str)
        assert config.database is not None
        assert config.downloader is not None
        assert config.consumer is not None
        assert isinstance(config.get_all_tasks(), dict)
        assert isinstance(config.get_all_groups(), dict)

        assert isinstance(config.validate(), bool)