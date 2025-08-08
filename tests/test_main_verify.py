import json
import os
import tempfile
from unittest.mock import Mock, patch
import pytest
from typer.testing import CliRunner

from src.downloader.main import app


class TestVerifyCommand:
    """测试 verify 命令的功能"""

    def setup_method(self):
        """每个测试方法前的设置"""
        self.runner = CliRunner()

    @patch('src.downloader.storage.DuckDBStorage')
    @patch('src.downloader.main.load_config')
    def test_verify_missing_data_detection(self, mock_load_config, mock_storage_class):
        """测试缺失数据检测功能"""
        # 模拟配置
        mock_load_config.return_value = {
            "database": {"path": "test.db"}
        }
        
        # 模拟存储实例
        mock_storage = Mock()
        mock_storage_class.return_value = mock_storage
        
        # 模拟股票列表数据
        import pandas as pd
        stock_df = pd.DataFrame({
            'ts_code': ['600519.SH', '000001.SZ', '000002.SZ']
        })
        mock_storage.query.return_value = stock_df
        
        # 模拟表摘要数据（只有部分股票有数据）
        mock_storage.get_summary.return_value = [
            {"table_name": "daily_600519_SH", "record_count": 100},
            {"table_name": "financials_600519_SH", "record_count": 50},
            {"table_name": "daily_basic_000001_SZ", "record_count": 80},
        ]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = os.path.join(temp_dir, "dead_letter.jsonl")
            
            # 模拟 os.path.exists 返回 True（数据库存在）
            with patch('os.path.exists', return_value=True):
                result = self.runner.invoke(app, ['verify', '--log-path', log_path])
            
            # 验证命令执行成功
            assert result.exit_code == 0
            
            # 验证输出包含统计信息
            assert "总股票数: 3" in result.stdout
            assert "按业务分类缺失统计:" in result.stdout
            
            # 验证死信日志文件被创建
            assert os.path.exists(log_path)
            
            # 验证死信日志内容
            with open(log_path, 'r', encoding='utf-8') as f:
                tasks = [json.loads(line.strip()) for line in f if line.strip()]
            
            # 应该有缺失的任务
            assert len(tasks) > 0
            
            # 验证任务格式
            for task in tasks:
                assert 'task_id' in task
                assert 'task_type' in task
                assert 'symbol' in task
                assert 'business_name' in task
                assert 'error_message' in task
                assert 'timestamp' in task
                assert 'retry_count' in task
                assert task['retry_count'] == 0

    @patch('src.downloader.storage.DuckDBStorage')
    @patch('src.downloader.main.load_config')
    def test_verify_deduplication(self, mock_load_config, mock_storage_class):
        """测试去重功能"""
        # 模拟配置
        mock_load_config.return_value = {
            "database": {"path": "test.db"}
        }
        
        # 模拟存储实例
        mock_storage = Mock()
        mock_storage_class.return_value = mock_storage
        
        # 模拟股票列表数据
        import pandas as pd
        stock_df = pd.DataFrame({
            'ts_code': ['600519.SH', '000001.SZ']
        })
        mock_storage.query.return_value = stock_df
        
        # 模拟表摘要数据（无任何数据表）
        mock_storage.get_summary.return_value = []
        
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = os.path.join(temp_dir, "dead_letter.jsonl")
            
            # 预先创建一个死信日志文件
            existing_task = {
                "task_id": "existing_daily_600519.SH_20250101_000000",
                "task_type": "daily",
                "symbol": "600519.SH",
                "business_name": "daily_qfq",
                "error_message": "Existing task",
                "timestamp": "2025-01-01T00:00:00",
                "retry_count": 1
            }
            
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(json.dumps(existing_task, ensure_ascii=False) + '\n')
            
            # 模拟 os.path.exists 返回 True
            with patch('os.path.exists', return_value=True):
                result = self.runner.invoke(app, ['verify', '--log-path', log_path])
            
            # 验证命令执行成功
            assert result.exit_code == 0
            
            # 验证死信日志文件内容
            with open(log_path, 'r', encoding='utf-8') as f:
                tasks = [json.loads(line.strip()) for line in f if line.strip()]
            
            # 验证去重：相同的 task_type + symbol 组合应该只有一个任务
            task_keys = set()
            for task in tasks:
                key = f"{task['task_type']}_{task['symbol']}"
                assert key not in task_keys, f"发现重复任务: {key}"
                task_keys.add(key)

    @patch('src.downloader.storage.DuckDBStorage')
    @patch('src.downloader.main.load_config')
    def test_verify_no_stock_list_error(self, mock_load_config, mock_storage_class):
        """测试无股票列表时的错误处理"""
        # 模拟配置
        mock_load_config.return_value = {
            "database": {"path": "test.db"}
        }
        
        # 模拟存储实例
        mock_storage = Mock()
        mock_storage_class.return_value = mock_storage
        
        # 模拟查询股票列表失败
        mock_storage.query.side_effect = Exception("Table not found")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = os.path.join(temp_dir, "dead_letter.jsonl")
            
            # 模拟 os.path.exists 返回 True
            with patch('os.path.exists', return_value=True):
                result = self.runner.invoke(app, ['verify', '--log-path', log_path])
            
            # 验证命令执行失败
            assert result.exit_code == 1
            assert "无法获取股票列表" in result.stdout

    @patch('src.downloader.storage.DuckDBStorage')
    @patch('src.downloader.main.load_config')
    def test_verify_empty_stock_list_error(self, mock_load_config, mock_storage_class):
        """测试空股票列表时的错误处理"""
        # 模拟配置
        mock_load_config.return_value = {
            "database": {"path": "test.db"}
        }
        
        # 模拟存储实例
        mock_storage = Mock()
        mock_storage_class.return_value = mock_storage
        
        # 模拟空的股票列表数据
        import pandas as pd
        empty_df = pd.DataFrame(columns=['ts_code'])
        mock_storage.query.return_value = empty_df
        
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = os.path.join(temp_dir, "dead_letter.jsonl")
            
            # 模拟 os.path.exists 返回 True
            with patch('os.path.exists', return_value=True):
                result = self.runner.invoke(app, ['verify', '--log-path', log_path])
            
            # 验证命令执行失败
            assert result.exit_code == 1
            assert "股票列表为空" in result.stdout

    @patch('src.downloader.storage.DuckDBStorage')
    @patch('src.downloader.main.load_config')
    def test_verify_business_type_mapping(self, mock_load_config, mock_storage_class):
        """测试业务类型映射的正确性"""
        # 模拟配置
        mock_load_config.return_value = {
            "database": {"path": "test.db"}
        }
        
        # 模拟存储实例
        mock_storage = Mock()
        mock_storage_class.return_value = mock_storage
        
        # 模拟股票列表数据
        import pandas as pd
        stock_df = pd.DataFrame({
            'ts_code': ['600519.SH']
        })
        mock_storage.query.return_value = stock_df
        
        # 模拟表摘要数据（包含各种业务类型的表）
        mock_storage.get_summary.return_value = [
            {"table_name": "daily_600519_SH", "record_count": 100},
            {"table_name": "daily_basic_600519_SH", "record_count": 50},
            {"table_name": "financials_600519_SH", "record_count": 30},
        ]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = os.path.join(temp_dir, "dead_letter.jsonl")
            
            # 模拟 os.path.exists 返回 True
            with patch('os.path.exists', return_value=True):
                result = self.runner.invoke(app, ['verify', '--log-path', log_path])
            
            # 验证命令执行成功
            assert result.exit_code == 0
            
            # 验证输出包含所有业务分类
            expected_business_types = [
                "daily_qfq", "daily_none", "daily_basic", 
                "financial_income", "financial_balance", "financial_cashflow"
            ]
            
            for business_type in expected_business_types:
                assert business_type in result.stdout

    def test_verify_database_not_exists(self):
        """测试数据库文件不存在时的错误处理"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.yaml")
            db_path = os.path.join(temp_dir, "nonexistent.db")
            log_path = os.path.join(temp_dir, "dead_letter.jsonl")
            
            # 创建配置文件
            config_content = f"""
database:
  path: "{db_path}"
"""
            with open(config_path, 'w') as f:
                f.write(config_content)
            
            result = self.runner.invoke(app, [
                'verify', 
                '--config', config_path,
                '--log-path', log_path
            ])
            
            # 验证命令执行失败
            assert result.exit_code == 1
            assert "数据库文件不存在" in result.stdout