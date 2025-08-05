import pytest
import pandas as pd
import tempfile
from unittest.mock import patch, MagicMock
from pathlib import Path
import yaml

from downloader.engine import DownloadEngine
from downloader.fetcher import TushareFetcher
from downloader.storage import ParquetStorage


@pytest.fixture
def temp_config_file():
    """创建一个临时配置文件用于测试"""
    config_content = {
        "storage": {
            "base_path": "{temp_dir}/data"
        },
        "downloader": {
            "symbols": ["000001", "600519"]
        },
        "tasks": [
            {
                "name": "测试股票列表",
                "enabled": True,
                "type": "stock_list",
                "update_strategy": "cooldown",
                "update_interval_hours": 0,  # 禁用冷却期以便测试
                "rate_limit": {
                    "calls_per_minute": 10  # 设置较低的速率限制便于测试
                }
            },
            {
                "name": "测试日线数据",
                "enabled": True,
                "type": "daily",
                "adjust": "none",
                "update_strategy": "incremental",
                "date_col": "trade_date",
                "rate_limit": {
                    "calls_per_minute": 20  # 设置不同的速率限制
                }
            }
        ]
    }
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # 格式化配置中的路径
        config_content["storage"]["base_path"] = f"{temp_dir}/data"
        
        config_file = Path(temp_dir) / "test_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_content, f)
        yield config_file, temp_dir


def test_rate_limit_integration(temp_config_file):
    """测试速率限制在完整下载流程中的集成"""
    config_file, temp_dir = temp_config_file
    
    # 加载配置
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    # Mock Tushare API
    with patch('downloader.fetcher.os.getenv') as mock_getenv, \
         patch('downloader.fetcher.ts') as mock_ts:
        
        # 设置mock
        mock_getenv.return_value = "test_token"
        mock_pro = MagicMock()
        mock_ts.pro_api.return_value = mock_pro
        mock_ts.set_token.return_value = None
        mock_pro.trade_cal.return_value = pd.DataFrame({'date': ['20230101']})
        
        # Mock数据返回
        mock_stock_list = pd.DataFrame({
            'ts_code': ['000001.SZ', '600519.SH'],
            'symbol': ['000001', '600519'],
            'name': ['平安银行', '贵州茅台'],
            'area': ['深圳', '上海'],
            'industry': ['银行', '白酒'],
            'market': ['主板', '主板'],
            'list_date': ['19910403', '20010827']
        })
        mock_pro.stock_basic.return_value = mock_stock_list
        
        mock_daily_data = pd.DataFrame({
            'ts_code': ['000001.SZ', '000001.SZ'],
            'trade_date': ['20230101', '20230102'],
            'open': [10.0, 10.1],
            'high': [10.5, 10.6],
            'low': [9.9, 10.0],
            'close': [10.2, 10.3],
            'vol': [1000000, 1100000]
        })
        mock_ts.pro_bar.return_value = mock_daily_data
        
        # 创建组件并运行引擎
        fetcher = TushareFetcher()
        storage = ParquetStorage(base_path=config["storage"]["base_path"])
        engine = DownloadEngine(config, fetcher, storage)
        engine.run()
        
        # 验证调用次数
        # 股票列表应该被调用一次
        assert mock_pro.stock_basic.call_count == 1
        # 日线数据应该被调用两次（每只股票一次）
        assert mock_ts.pro_bar.call_count == 2


def test_rate_limit_with_no_config():
    """测试没有速率限制配置时的行为"""
    config_content = {
        "storage": {
            "base_path": "{temp_dir}/data"
        },
        "downloader": {
            "symbols": ["000001"]
        },
        "tasks": [
            {
                "name": "无速率限制测试",
                "enabled": True,
                "type": "stock_list",
                "update_strategy": "cooldown",
                "update_interval_hours": 0
                # 故意不设置rate_limit配置
            }
        ]
    }
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # 格式化配置中的路径
        config_content["storage"]["base_path"] = f"{temp_dir}/data"
        
        config_file = Path(temp_dir) / "test_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_content, f)
        
        # 加载配置
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Mock Tushare API
        with patch('downloader.fetcher.os.getenv') as mock_getenv, \
             patch('downloader.fetcher.ts') as mock_ts:
            
            # 设置mock
            mock_getenv.return_value = "test_token"
            mock_pro = MagicMock()
            mock_ts.pro_api.return_value = mock_pro
            mock_ts.set_token.return_value = None
            mock_pro.trade_cal.return_value = pd.DataFrame({'date': ['20230101']})
            mock_pro.stock_basic.return_value = pd.DataFrame({
                'ts_code': ['000001.SZ'],
                'symbol': ['000001'],
                'name': ['平安银行'],
                'area': ['深圳'],
                'industry': ['银行'],
                'market': ['主板'],
                'list_date': ['19910403']
            })
            
            # 创建组件并运行引擎
            fetcher = TushareFetcher()
            storage = ParquetStorage(base_path=config["storage"]["base_path"])
            engine = DownloadEngine(config, fetcher, storage)
            engine.run()
            
            # 验证调用
            assert mock_pro.stock_basic.call_count == 1
