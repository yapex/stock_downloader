"""
测试 last_run 时间戳跟踪功能

测试完整组运行的判定和 last_run_ts 的更新逻辑
"""

import pytest
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

from downloader.storage import DuckDBStorage
from downloader.engine import DownloadEngine
from downloader.fetcher import TushareFetcher


class TestLastRunTracking:
    """测试 last_run 时间戳跟踪功能"""

    @pytest.fixture
    def storage(self):
        """创建临时存储实例"""
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test_last_run.db"
        storage = DuckDBStorage(db_path)
        yield storage
        # 清理
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def mock_fetcher(self):
        """创建模拟的 fetcher"""
        return Mock(spec=TushareFetcher)

    def test_storage_last_run_methods(self, storage):
        """测试 storage 的 last_run 相关方法"""
        group_name = "test_group"
        
        # 初始状态应该返回 None
        assert storage.get_last_run(group_name) is None
        
        # 设置 last_run
        now = datetime.now()
        storage.set_last_run(group_name, now)
        
        # 获取应该返回设置的时间
        retrieved = storage.get_last_run(group_name)
        assert retrieved is not None
        assert abs((retrieved - now).total_seconds()) < 1  # 允许1秒误差

    def test_full_group_run_condition(self, storage, mock_fetcher):
        """测试完整组运行的判定条件"""
        config = {
            "tasks": [
                {"name": "task1", "type": "daily", "enabled": True},
                {"name": "task2", "type": "daily_basic", "enabled": True},
                {"name": "task3", "type": "stock_list", "enabled": True},
            ],
            "downloader": {
                "symbols": ["600519.SH", "000001.SZ"],  # 指定股票池
            }
        }
        
        engine = DownloadEngine(config, mock_fetcher, storage)
        
        # 模拟所有任务成功完成的情况
        engine.task_registry = {
            "daily": Mock(),
            "daily_basic": Mock(), 
            "stock_list": Mock(),
        }
        
        # 这种情况应该是完整组运行
        configured_symbols = config["downloader"]["symbols"]
        assert engine._is_full_group_run(configured_symbols, ["600519.SH", "000001.SZ"], [])

    def test_partial_run_with_symbols_filter(self, storage, mock_fetcher):
        """测试带 --symbols 过滤的局部运行"""
        config = {
            "tasks": [
                {"name": "task1", "type": "daily", "enabled": True},
                {"name": "task2", "type": "daily_basic", "enabled": True},
            ],
            "downloader": {
                "symbols": ["600519.SH"],  # 配置中的完整股票池
            }
        }
        
        engine = DownloadEngine(config, mock_fetcher, storage)
        
        # 用户通过命令行指定了部分股票，这是局部运行
        # 这种情况下不应该更新 last_run_ts
        # 这里模拟的情况：配置中有更多股票，但实际只运行了一部分
        configured_symbols = ["600519.SH", "000001.SZ"]  # 完整配置
        actual_symbols = ["600519.SH"]  # 实际执行的
        assert not engine._is_full_group_run(configured_symbols, actual_symbols, [])

    def test_partial_run_with_failed_tasks(self, storage, mock_fetcher):
        """测试有任务失败的局部运行"""
        config = {
            "tasks": [
                {"name": "task1", "type": "daily", "enabled": True},
                {"name": "task2", "type": "daily_basic", "enabled": True},
            ],
            "downloader": {
                "symbols": ["600519.SH", "000001.SZ"],
            }
        }
        
        engine = DownloadEngine(config, mock_fetcher, storage)
        
        # 有任务失败的情况下不是完整组运行
        configured_symbols = config["downloader"]["symbols"]
        failed_tasks = ["task1"]
        assert not engine._is_full_group_run(configured_symbols, ["600519.SH", "000001.SZ"], failed_tasks)

    def test_full_group_run_updates_last_run(self, storage, mock_fetcher):
        """测试完整组运行会更新 last_run_ts"""
        config = {
            "tasks": [
                {"name": "stock_list", "type": "stock_list", "enabled": True},
                {"name": "daily", "type": "daily", "enabled": True},
            ],
            "downloader": {
                "symbols": "all",  # 下载全市场
            },
            "defaults": {}
        }
        
        # 创建模拟的任务处理器
        mock_stock_list_handler = Mock()
        mock_daily_handler = Mock()
        
        engine = DownloadEngine(config, mock_fetcher, storage)
        engine.task_registry = {
            "stock_list": Mock(return_value=mock_stock_list_handler),
            "daily": Mock(return_value=mock_daily_handler),
        }
        
        # 模拟股票列表数据
        import pandas as pd
        stock_df = pd.DataFrame({
            'ts_code': ['600519.SH', '000001.SZ'],
            'name': ['贵州茅台', '平安银行']
        })
        storage.save_full(stock_df, "system", "stock_list")
        
        group_name = "default"  # 假设默认组名
        before_run = storage.get_last_run(group_name)
        
        # 执行引擎
        engine.run()
        
        # 验证 last_run_ts 被更新
        after_run = storage.get_last_run(group_name)
        if before_run is None:
            assert after_run is not None
        else:
            assert after_run > before_run

    def test_partial_run_does_not_update_last_run(self, storage, mock_fetcher):
        """测试局部运行不会更新 last_run_ts"""
        # 这个测试直接测试 _is_full_group_run 方法的逻辑
        # 而不是整个 engine.run() 流程
        
        config = {
            "tasks": [
                {"name": "daily", "type": "daily", "enabled": True},
            ],
            "downloader": {
                "symbols": ["600519.SH", "000001.SZ"],  # 配置中的完整股票池
            }
        }
        
        engine = DownloadEngine(config, mock_fetcher, storage)
        
        # 模拟用户通过命令行参数只指定了部分股票
        configured_symbols = ["600519.SH", "000001.SZ"]  # 配置中的完整股票池
        actual_symbols = ["600519.SH"]  # 实际执行的（由于命令行参数）
        failed_tasks = []  # 没有任务失败
        
        # 这应该不被认为是完整组运行，因为实际执行的股票数量少于配置中的
        assert not engine._is_full_group_run(configured_symbols, actual_symbols, failed_tasks)

    def test_command_line_override_prevents_full_run(self, storage, mock_fetcher):
        """测试命令行参数覆盖阻止完整组运行更新"""
        config = {
            "tasks": [
                {"name": "daily", "type": "daily", "enabled": True},
            ],
            "downloader": {
                "symbols": ["600519.SH", "000001.SZ"],  # 配置中的完整股票池
            }
        }
        
        # 模拟命令行参数覆盖的情况
        engine = DownloadEngine(config, mock_fetcher, storage, symbols_overridden=True)
        
        # 即使股票列表一致，由于命令行参数覆盖，也不应该认为是完整组运行
        configured_symbols = ["600519.SH", "000001.SZ"]
        actual_symbols = ["600519.SH", "000001.SZ"]  # 完全一样的列表
        failed_tasks = []
        
        assert not engine._is_full_group_run(configured_symbols, actual_symbols, failed_tasks)

    def test_full_group_updates_timestamp(self, storage, mock_fetcher):
        """测试：不传 --symbols，跑完季度组 → 断言 last_run_ts 更新"""
        
        # 配置一个季度组
        config = {
            "tasks": [
                {"name": "financials", "type": "financials", "enabled": True},
                {"name": "daily", "type": "daily", "enabled": True},
            ],
            "downloader": {
                "symbols": ["600519.SH", "000001.SZ"],  # 明确指定股票池
            },
            "group_name": "quarterly"
        }
        
        # 创建模拟的任务处理器
        mock_financials_handler = Mock()
        mock_daily_handler = Mock()
        
        # 模拟成功执行所有任务
        mock_financials_handler.execute = Mock()
        mock_daily_handler.execute = Mock()
        
        group_name = config.get("group_name", "default")
        
        # 创建引擎（未设置 symbols_overridden，表示没有命令行参数覆盖）
        engine = DownloadEngine(config, mock_fetcher, storage, symbols_overridden=False, group_name=group_name)
        engine.task_registry = {
            "financials": Mock(return_value=mock_financials_handler),
            "daily": Mock(return_value=mock_daily_handler),
        }
        
        # 记录运行前的时间戳
        before_run_ts = storage.get_last_run(group_name)
        
        # 执行引擎
        engine.run()
        
        # 验证 last_run_ts 被更新
        after_run_ts = storage.get_last_run(group_name)
        
        if before_run_ts is None:
            # 首次运行，应该设置时间戳
            assert after_run_ts is not None
        else:
            # 后续运行，时间戳应该被更新
            assert after_run_ts > before_run_ts
        
        # 验证所有任务都被调用
        mock_financials_handler.execute.assert_called_once()
        mock_daily_handler.execute.assert_called_once()

    def test_partial_symbol_does_not_touch_timestamp(self, storage, mock_fetcher):
        """测试：传 --symbols 600519 → 断言 last_run_ts 未变"""
        
        config = {
            "tasks": [
                {"name": "daily", "type": "daily", "enabled": True},
            ],
            "downloader": {
                "symbols": ["600519.SH", "000001.SZ"],  # 配置中的完整股票池
            },
            "group_name": "test_group"
        }
        
        # 模拟任务处理器
        mock_daily_handler = Mock()
        mock_daily_handler.execute = Mock()
        
        group_name = config.get("group_name", "default")
        
        # 创建引擎，设置 symbols_overridden=True 表示命令行参数覆盖
        engine = DownloadEngine(config, mock_fetcher, storage, symbols_overridden=True, group_name=group_name)
        engine.task_registry = {
            "daily": Mock(return_value=mock_daily_handler),
        }
        
        # 先设置一个初始时间戳
        initial_ts = datetime.now() - timedelta(hours=1)
        storage.set_last_run(group_name, initial_ts)
        
        # 执行引擎（模拟用户传了 --symbols 参数）
        engine.run()
        
        # 验证 last_run_ts 未被更新（因为是部分运行）
        final_ts = storage.get_last_run(group_name)
        assert final_ts == initial_ts, "部分运行不应该更新 last_run_ts"
        
        # 验证任务仍然被执行了
        mock_daily_handler.execute.assert_called_once()

    def test_failure_does_not_touch_timestamp(self, storage, mock_fetcher):
        """测试：模拟任务抛异常 → 断言 last_run_ts 未变"""
        config = {
            "tasks": [
                {"name": "daily", "type": "daily", "enabled": True},
                {"name": "financials", "type": "financials", "enabled": True},
            ],
            "downloader": {
                "symbols": ["600519.SH"],
            },
            "group_name": "test_group"
        }
        
        # 模拟任务处理器
        mock_daily_handler = Mock()
        mock_financials_handler = Mock()
        
        # 第一个任务成功执行，第二个任务抛异常
        mock_daily_handler.execute = Mock()
        mock_financials_handler.execute = Mock(side_effect=Exception("模拟任务执行失败"))
        
        group_name = config.get("group_name", "default")
        
        # 创建引擎（未设置 symbols_overridden）
        engine = DownloadEngine(config, mock_fetcher, storage, symbols_overridden=False, group_name=group_name)
        engine.task_registry = {
            "daily": Mock(return_value=mock_daily_handler),
            "financials": Mock(return_value=mock_financials_handler),
        }
        
        # 先设置一个初始时间戳
        initial_ts = datetime.now() - timedelta(hours=1)
        storage.set_last_run(group_name, initial_ts)
        
        # 执行引擎（应该有任务失败）
        engine.run()
        
        # 验证 last_run_ts 未被更新（因为有任务失败）
        final_ts = storage.get_last_run(group_name)
        assert final_ts == initial_ts, "任务失败时不应该更新 last_run_ts"
        
        # 验证成功的任务被调用了，失败的任务也被调用了
        mock_daily_handler.execute.assert_called_once()
        mock_financials_handler.execute.assert_called_once()
