"""
核心模块回归测试
针对核心业务逻辑的回归测试，确保在代码重构或修改后关键功能不会出现问题。
"""
import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock, patch
from pathlib import Path
import tempfile
import os

from downloader.engine import DownloadEngine
from downloader.storage import DuckDBStorage
from downloader.database import DuckDBConnectionFactory
from downloader.logger_interface import LoggerFactory
from downloader.fetcher import TushareFetcher
from downloader.models import DownloadTask, TaskType, Priority
from downloader.domain.category_service import find_missing, find_missing_by_category, group_by_category
from downloader.utils import get_table_name


class TestCoreRegressionEngine:
    """引擎核心功能回归测试"""

    def test_engine_initialization_with_empty_config(self):
        """测试引擎能正确处理空配置"""
        config = {"downloader": {}, "tasks": [], "defaults": {}}
        engine = DownloadEngine(config)
        
        assert engine.config == config
        assert engine.task_registry is not None
        assert engine.execution_stats["total_tasks"] == 0

    def test_engine_task_discovery(self):
        """测试引擎能发现并注册任务处理器"""
        config = {"downloader": {}, "tasks": [], "defaults": {}}
        engine = DownloadEngine(config)
        
        # 验证任务注册表包含核心任务类型
        expected_handlers = ["daily", "stock_list", "daily_basic", "financials"]
        for handler_name in expected_handlers:
            assert handler_name in engine.task_registry

    def test_engine_prepare_target_symbols_with_list(self):
        """测试引擎能正确处理符号列表"""
        symbols = ["000001.SZ", "600519.SH"]
        config = {
            "downloader": {"symbols": symbols},
            "tasks": [{"enabled": True, "type": "daily"}],
            "defaults": {}
        }
        engine = DownloadEngine(config)
        
        enabled_tasks = [task for task in config["tasks"] if task.get("enabled", False)]
        target_symbols = engine._prepare_target_symbols(enabled_tasks)
        assert target_symbols == symbols

    def test_engine_handles_invalid_task_type(self):
        """测试引擎能正确处理无效的任务类型"""
        config = {
            "downloader": {"symbols": ["000001.SZ"]},
            "tasks": [{"enabled": True, "type": "invalid_task"}],
            "defaults": {}
        }
        engine = DownloadEngine(config)
        
        enabled_tasks = [task for task in config["tasks"] if task.get("enabled", False)]
        target_symbols = engine._prepare_target_symbols(enabled_tasks)
        tasks = engine._build_download_tasks(target_symbols, enabled_tasks)
        
        # 无效任务类型应该被跳过
        assert len(tasks) == 0


class TestCoreRegressionStorage:
    """存储模块核心功能回归测试"""

    @pytest.fixture
    def temp_db(self):
        """创建临时数据库文件"""
        with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as f:
            db_path = Path(f.name)
        # 删除临时文件，让DuckDB自己创建
        db_path.unlink()
        yield db_path
        # 清理时删除可能存在的文件
        if db_path.exists():
            db_path.unlink()

    def test_storage_basic_crud_operations(self, temp_db):
        """测试存储模块的基本CRUD操作"""
        db_factory = DuckDBConnectionFactory()
        logger = LoggerFactory.create_logger("test_storage")
        storage = DuckDBStorage(temp_db, db_factory, logger)
        
        # 测试数据
        df = pd.DataFrame({
            'ts_code': ['000001.SZ', '000001.SZ', '000001.SZ'],
            'trade_date': ['20230101', '20230102', '20230103'],
            'close': [100.0, 101.0, 102.0],
            'volume': [1000, 1100, 1200]
        })
        
        # 测试保存
        storage.save_daily_data(df)
        
        # 测试获取最新日期
        latest_date = storage.get_latest_date_by_stock("000001.SZ", "daily")
        assert latest_date == "20230103"
        
        # 测试增量更新
        new_df = pd.DataFrame({
            'ts_code': ['000001.SZ', '000001.SZ'],
            'trade_date': ['20230104', '20230105'],
            'close': [103.0, 104.0],
            'volume': [1300, 1400]
        })
        storage.save_daily_data(new_df)
        
        latest_date = storage.get_latest_date_by_stock("000001.SZ", "daily")
        assert latest_date == "20230105"

    def test_storage_handles_empty_dataframe(self, temp_db):
        """测试存储模块处理空DataFrame的能力"""
        db_factory = DuckDBConnectionFactory()
        logger = LoggerFactory.create_logger("test_storage")
        storage = DuckDBStorage(temp_db, db_factory, logger)
        
        empty_df = pd.DataFrame()
        # 不应该抛出异常
        storage.save_daily_data(empty_df)
        
        # 应该返回None（表不存在）
        latest_date = storage.get_latest_date_by_stock("000001.SZ", "daily")
        assert latest_date is None

    def test_storage_table_name_generation(self):
        """测试表名生成逻辑"""
        assert get_table_name("daily", "000001.SZ") == "daily_000001_SZ"
        assert get_table_name("daily_basic", "600519.SH") == "daily_basic_600519_SH"
        assert get_table_name("system", "stock_list") == "sys_stock_list"


class TestCoreRegressionCategoryService:
    """分类服务回归测试"""

    def test_group_by_category_basic_functionality(self):
        """测试分类服务的基本功能"""
        records = [
            {"category": "Tech", "symbol": "000001.SZ"},
            {"category": "Finance", "symbol": "600036.SH"},
            {"category": "Tech", "symbol": "300059.SZ"},
            {"industry": "Energy", "symbol": "600028.SH"},  # 使用industry字段
            {"symbol": "999999.SZ"}  # 无分类字段，应归入Unknown
        ]
        
        grouped = group_by_category(records)
        
        assert "Tech" in grouped
        assert "Finance" in grouped  
        assert "Energy" in grouped
        assert "Unknown" in grouped
        
        assert len(grouped["Tech"]) == 2
        assert len(grouped["Finance"]) == 1
        assert len(grouped["Energy"]) == 1
        assert len(grouped["Unknown"]) == 1

    def test_find_missing_stocks_functionality(self):
        """测试缺股查找功能"""
        all_stocks = ["000001.SZ", "000002.SZ", "600519.SH", "600036.SH"]
        table1_stocks = ["000001.SZ", "600519.SH"]
        table2_stocks = ["000002.SZ", "600519.SH"]
        
        # 总体缺股（相对于所有表的并集）
        missing = find_missing(all_stocks, table1_stocks, table2_stocks)
        expected_missing = {"600036.SH"}  # 只有这个在两个表都没有
        assert missing == expected_missing

    def test_find_missing_by_category_functionality(self):
        """测试按分类查找缺股功能"""
        all_stocks = ["000001.SZ", "000002.SZ", "600519.SH"]
        tables_by_category = {
            "daily": ["000001.SZ", "600519.SH"],
            "daily_basic": ["000002.SZ"]
        }
        
        missing_by_cat = find_missing_by_category(all_stocks, tables_by_category)
        
        assert "daily" in missing_by_cat
        assert "daily_basic" in missing_by_cat
        
        # daily表缺少000002.SZ
        assert missing_by_cat["daily"] == {"000002.SZ"}
        # daily_basic表缺少000001.SZ和600519.SH
        assert missing_by_cat["daily_basic"] == {"000001.SZ", "600519.SH"}


class TestCoreRegressionTaskHandlers:
    """任务处理器回归测试"""

    @pytest.fixture
    def mock_fetcher(self):
        """模拟fetcher"""
        fetcher = Mock(spec=TushareFetcher)
        fetcher.fetch_daily_history.return_value = pd.DataFrame({
            'trade_date': ['20230101', '20230102'],
            'close': [100.0, 101.0]
        })
        fetcher.fetch_daily_basic.return_value = pd.DataFrame({
            'trade_date': ['20230101', '20230102'],
            'pe': [10.0, 11.0]
        })
        return fetcher

    @pytest.fixture
    def mock_storage(self):
        """模拟storage"""
        storage = Mock(spec=DuckDBStorage)
        storage.get_latest_date_by_stock.return_value = None
        return storage

    def test_task_handlers_registration(self):
        """测试任务处理器能正确注册"""
        config = {"downloader": {}, "tasks": [], "defaults": {}}
        engine = DownloadEngine(config)
        
        # 验证核心任务处理器已注册
        from downloader.tasks.daily import DailyTaskHandler
        from downloader.tasks.daily_basic import DailyBasicTaskHandler
        from downloader.tasks.stock_list import StockListTaskHandler
        from downloader.tasks.financials import FinancialsTaskHandler
        
        assert engine.task_registry["daily"] == DailyTaskHandler
        assert engine.task_registry["daily_basic"] == DailyBasicTaskHandler
        assert engine.task_registry["stock_list"] == StockListTaskHandler
        assert engine.task_registry["financials"] == FinancialsTaskHandler

    def test_daily_task_handler_execution(self, mock_fetcher, mock_storage):
        """测试daily任务处理器执行"""
        from downloader.tasks.daily import DailyTaskHandler
        
        task_config = {
            "name": "Daily Test",
            "type": "daily",
            "date_col": "trade_date"
        }
        
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        handler.execute(target_symbols=["000001.SZ"])
        
        # 验证fetcher被调用
        mock_fetcher.fetch_daily_history.assert_called()
        # 验证storage被调用
        mock_storage.save_daily_data.assert_called()


class TestCoreRegressionModels:
    """数据模型回归测试"""

    def test_download_task_creation(self):
        """测试DownloadTask模型创建"""
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={"start_date": "20230101", "end_date": "20231231"},
            priority=Priority.NORMAL
        )
        
        assert task.symbol == "000001.SZ"
        assert task.task_type == TaskType.DAILY
        assert task.priority == Priority.NORMAL
        assert task.params["start_date"] == "20230101"

    def test_task_type_enum(self):
        """测试TaskType枚举"""
        assert TaskType.DAILY.value == "daily"
        assert TaskType.DAILY_BASIC.value == "daily_basic"
        assert TaskType.STOCK_LIST.value == "stock_list"
        assert TaskType.FINANCIALS.value == "financials"

    def test_priority_enum(self):
        """测试Priority枚举"""
        assert Priority.LOW.value == 1
        assert Priority.NORMAL.value == 5
        assert Priority.HIGH.value == 10


class TestCoreRegressionUtilities:
    """工具函数回归测试"""

    def test_table_name_utility_edge_cases(self):
        """测试表名工具函数的边界情况"""
        # 测试system类型的表名
        assert get_table_name("system", "stock_list") == "sys_stock_list"
        
        # 测试普通表名
        assert get_table_name("daily", "000001.SZ") == "daily_000001_SZ"
        assert get_table_name("daily_basic", "600519.SH") == "daily_basic_600519_SH"
        
        # 测试包含特殊字符的处理（使用有效的股票代码）
        assert get_table_name("financials", "000001.SZ") == "financials_000001_SZ"


# 数据完整性回归测试
class TestCoreRegressionDataIntegrity:
    """数据完整性回归测试"""

    def test_date_handling_consistency(self):
        """测试日期处理的一致性"""
        # 测试日期格式的一致性（目前跳过，因为没有通用的日期标准化函数）
        # 这个测试预留给将来如果需要统一日期处理逻辑
        pass

    def test_symbol_format_consistency(self):
        """测试股票代码格式处理的一致性"""
        from downloader.utils import normalize_stock_code
        
        # 测试股票代码标准化
        assert normalize_stock_code('000001') == '000001.SZ'
        assert normalize_stock_code('600519') == '600519.SH'
        assert normalize_stock_code('000001.SZ') == '000001.SZ'
