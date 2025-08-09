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

    @patch('src.downloader.utils.get_table_name')
    @patch('os.path.exists')
    @patch('src.downloader.main.get_storage')
    @patch('src.downloader.main.load_config')
    def test_verify_missing_data_detection(self, mock_load_config, mock_get_storage, mock_exists, mock_get_table_name):
        """测试缺失数据检测功能"""
        # 模拟数据库文件存在
        mock_exists.return_value = True
        
        # 模拟配置
        mock_load_config.return_value = {
            "database": {"path": "test.db"}
        }
        
        # 模拟存储实例
        mock_storage = Mock()
        mock_get_storage.return_value = mock_storage
        
        # 模拟股票列表数据
        import pandas as pd
        stock_df = pd.DataFrame({
            'ts_code': ['600519.SH', '000001.SZ', '000002.SZ']
        })
        
        def mock_get_stock_list():
            print(f"DEBUG: get_stock_list called, returning: {stock_df}")
            print(f"DEBUG: DataFrame empty: {stock_df.empty}")
            print(f"DEBUG: DataFrame columns: {stock_df.columns.tolist()}")
            return stock_df
        
        mock_storage.get_stock_list.side_effect = mock_get_stock_list
        
        # 模拟 get_table_name 函数
        def mock_table_name(table_prefix, stock_code):
            return f"{table_prefix}_{stock_code.replace('.', '_')}"
        mock_get_table_name.side_effect = mock_table_name
        
        # RetryLogger 使用真实实现
        
        # 模拟表摘要数据（只有部分股票有数据）
        mock_storage.get_summary.return_value = [
            {"table_name": "daily_600519_SH", "record_count": 100},
            {"table_name": "financials_600519_SH", "record_count": 50},
            {"table_name": "daily_basic_000001_SZ", "record_count": 80},
        ]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = os.path.join(temp_dir, "dead_letter.jsonl")
            result = self.runner.invoke(app, ['verify', '--log-path', log_path])
            
            # 调试信息
            if result.exit_code != 0:
                print(f"Exit code: {result.exit_code}")
                print(f"Stdout: {result.stdout}")
                print(f"Exception: {result.exception}")
                
            # 验证命令执行成功
            assert result.exit_code == 0, f"Command failed with exit code {result.exit_code}. Stdout: {result.stdout}"
            
            # 验证输出包含统计信息
            assert "总股票数: 3" in result.stdout
            assert "按业务分类缺失统计:" in result.stdout
            
            # 验证死信日志文件被创建
            assert os.path.exists(log_path)
            
            # 验证重试日志内容
            from src.downloader.retry_policy import RetryLogger
            retry_logger = RetryLogger(log_path)
            symbols = retry_logger.read_symbols()
            
            # 应该有缺失的股票代码
            assert len(symbols) > 0
            
            # 验证包含预期的缺失股票代码
            expected_symbols = ['000001.SZ', '000002.SZ']
            for symbol in expected_symbols:
                assert symbol in symbols, f"缺失预期的股票代码: {symbol}"

    @patch('os.path.exists')
    @patch('src.downloader.main.get_storage')
    @patch('src.downloader.main.load_config')
    def test_verify_deduplication(self, mock_load_config, mock_get_storage, mock_exists):
        """测试去重功能"""
        # 模拟数据库文件存在
        mock_exists.return_value = True
        
        # 模拟配置
        mock_load_config.return_value = {
            "database": {"path": "test.db"}
        }
        
        # 模拟存储实例
        mock_storage = Mock()
        mock_get_storage.return_value = mock_storage
        
        # 模拟股票列表数据
        import pandas as pd
        stock_df = pd.DataFrame({
            'ts_code': ['600519.SH', '000001.SZ']
        })
        mock_storage.query.return_value = stock_df
        mock_storage.get_stock_list.return_value = pd.DataFrame({
            'ts_code': ['600519.SH', '000001.SZ']
        })
        
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
            
            # 验证重试日志文件内容
            from src.downloader.retry_policy import RetryLogger
            retry_logger = RetryLogger(log_path)
            symbols = retry_logger.read_symbols()
            
            # 验证去重：相同的股票代码应该只出现一次
            assert len(symbols) == len(set(symbols)), "发现重复的股票代码"
            
            # 验证包含预期的缺失股票代码（基于mock的股票列表）
            expected_symbols = ['000001.SZ', '600519.SH']
            for symbol in expected_symbols:
                assert symbol in symbols, f"缺失预期的股票代码: {symbol}"

    @patch('os.path.exists')
    @patch('src.downloader.main.get_storage')
    @patch('src.downloader.main.load_config')
    def test_verify_no_stock_list_error(self, mock_load_config, mock_get_storage, mock_exists):
        """测试无法获取股票列表的错误情况"""
        # 模拟数据库文件存在
        mock_exists.return_value = True
        
        # 模拟配置
        mock_load_config.return_value = {
            "database": {"path": "test.db"}
        }
        
        # 模拟存储实例
        mock_storage = Mock()
        mock_get_storage.return_value = mock_storage
        
        # 模拟查询股票列表失败
        mock_storage.query.side_effect = Exception("Table not found")
        mock_storage.get_stock_list.side_effect = Exception("Table not found")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = os.path.join(temp_dir, "dead_letter.jsonl")
            
            # 模拟 os.path.exists 返回 True
            with patch('os.path.exists', return_value=True):
                result = self.runner.invoke(app, ['verify', '--log-path', log_path])
            
            # 验证命令执行失败
            assert result.exit_code == 1
            assert "无法获取股票列表" in result.stdout

    @patch('os.path.exists')
    @patch('src.downloader.main.get_storage')
    @patch('src.downloader.main.load_config')
    def test_verify_empty_stock_list_error(self, mock_load_config, mock_get_storage, mock_exists):
        """测试空股票列表的错误情况"""
        # 模拟数据库文件存在
        mock_exists.return_value = True
        
        # 模拟配置
        mock_load_config.return_value = {
            "database": {"path": "test.db"}
        }
        
        # 模拟存储实例
        mock_storage = Mock()
        mock_get_storage.return_value = mock_storage
        
        # 模拟空的股票列表数据
        import pandas as pd
        empty_df = pd.DataFrame(columns=['ts_code'])
        mock_storage.query.return_value = empty_df
        mock_storage.get_stock_list.return_value = pd.DataFrame(columns=['ts_code'])
        
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = os.path.join(temp_dir, "dead_letter.jsonl")
            
            # 模拟 os.path.exists 返回 True
            with patch('os.path.exists', return_value=True):
                result = self.runner.invoke(app, ['verify', '--log-path', log_path])
            
            # 验证命令执行失败
            assert result.exit_code == 1
            assert "股票列表为空" in result.stdout

    @patch('os.path.exists')
    @patch('src.downloader.main.get_storage')
    @patch('src.downloader.main.load_config')
    def test_verify_business_type_mapping(self, mock_load_config, mock_get_storage, mock_exists):
        """测试业务类型映射功能"""
        # 模拟数据库文件存在
        mock_exists.return_value = True
        
        # 模拟配置
        mock_load_config.return_value = {
            "database": {"path": "test.db"}
        }
        
        # 模拟存储实例
        mock_storage = Mock()
        mock_get_storage.return_value = mock_storage
        
        # 模拟股票列表数据
        import pandas as pd
        stock_df = pd.DataFrame({
            'ts_code': ['600519.SH']
        })
        mock_storage.query.return_value = stock_df
        mock_storage.get_stock_list.return_value = pd.DataFrame({
            'ts_code': ['600519.SH']
        })
        
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