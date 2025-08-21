import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from threading import Lock
from neo.downloader.fetcher_builder import (
    TushareApiManager,
    FetcherBuilder
)
from neo.task_bus.types import TaskTemplate, TaskType


class TestTaskTemplate:
    """测试 TaskTemplate 数据类"""
    
    def test_task_template_creation(self):
        """测试任务模板创建"""
        template = TaskTemplate(
            base_object="pro",
            api_method="stock_basic",
            default_params={"exchange": "SSE"}
        )
        assert template.base_object == "pro"
        assert template.api_method == "stock_basic"
        assert template.default_params == {"exchange": "SSE"}
    
    def test_task_template_without_default_params(self):
        """测试没有默认参数的任务模板"""
        template = TaskTemplate(
            base_object="pro",
            api_method="stock_basic"
        )
        assert template.base_object == "pro"
        assert template.api_method == "stock_basic"
        assert template.default_params == {}


class TestTaskType:
    """测试 TaskType 枚举"""
    
    def test_task_type_enum_values(self):
        """测试任务类型枚举值"""
        assert TaskType.STOCK_BASIC.name == "STOCK_BASIC"
        assert TaskType.STOCK_DAILY.name == "STOCK_DAILY"
        assert TaskType.STOCK_ADJ_QFQ.name == "STOCK_ADJ_QFQ"
    
    def test_task_type_templates(self):
        """测试任务类型模板"""
        stock_basic_template = TaskType.STOCK_BASIC.template
        assert stock_basic_template.base_object == "pro"
        assert stock_basic_template.api_method == "stock_basic"
        
        stock_daily_template = TaskType.STOCK_DAILY.template
        assert stock_daily_template.base_object == "pro"
        assert stock_daily_template.api_method == "daily"


class TestTushareApiManager:
    """测试 TushareApiManager 单例类"""
    
    def teardown_method(self):
        """测试后清理单例状态"""
        TushareApiManager._instance = None
        TushareApiManager._lock = Lock()
    
    def test_singleton_pattern(self):
        """测试单例模式"""
        manager1 = TushareApiManager.get_instance()
        manager2 = TushareApiManager.get_instance()
        assert manager1 is manager2
    
    @patch('neo.downloader.fetcher_builder.ts')
    @patch('neo.downloader.fetcher_builder.get_config')
    def test_get_api_function_pro(self, mock_get_config, mock_ts):
        """测试获取 pro API 函数"""
        # Mock 配置
        mock_config = Mock()
        mock_config.tushare.token = "test_token"
        mock_get_config.return_value = mock_config
        
        # Mock pro API
        mock_pro = Mock()
        mock_ts.pro_api.return_value = mock_pro
        mock_api_func = Mock()
        setattr(mock_pro, 'stock_basic', mock_api_func)
        
        # 重置单例实例
        TushareApiManager._instance = None
        TushareApiManager._lock = Lock()
        
        manager = TushareApiManager.get_instance()
        result = manager.get_api_function('pro', 'stock_basic')
        
        assert result is mock_api_func
        mock_ts.set_token.assert_called_once_with("test_token")
        mock_ts.pro_api.assert_called_once()
    
    @patch('neo.downloader.fetcher_builder.ts')
    @patch('neo.downloader.fetcher_builder.get_config')
    def test_get_api_function_direct(self, mock_get_config, mock_ts):
        """测试获取直接 API 函数"""
        # Mock 配置
        mock_config = Mock()
        mock_config.tushare.token = "test_token"
        mock_get_config.return_value = mock_config
        
        # Mock ts API
        mock_api_func = Mock()
        setattr(mock_ts, 'get_hist_data', mock_api_func)
        mock_ts.pro_api.return_value = Mock()
        
        # 重置单例实例
        TushareApiManager._instance = None
        TushareApiManager._lock = Lock()
        
        manager = TushareApiManager.get_instance()
        result = manager.get_api_function('ts', 'get_hist_data')
        
        assert result is mock_api_func


class TestFetcherBuilder:
    """测试 FetcherBuilder 类"""
    
    def setup_method(self):
        """测试前设置"""
        self.builder = FetcherBuilder()
    
    def test_init(self):
        """测试初始化"""
        assert self.builder.api_manager is not None
        assert isinstance(self.builder.api_manager, TushareApiManager)
    
    def test_build_by_task_invalid_type(self):
        """测试无效任务类型"""
        with pytest.raises(ValueError, match="不支持的任务类型"):
            self.builder.build_by_task("invalid_type")
    
    @patch.object(TushareApiManager, 'get_api_function')
    @patch('neo.downloader.fetcher_builder.normalize_stock_code')
    def test_build_by_task_with_symbol(self, mock_normalize, mock_get_api):
        """测试带股票代码的任务构建"""
        mock_normalize.return_value = "600519.SH"
        mock_api_func = Mock(return_value=pd.DataFrame({'data': [1, 2, 3]}))
        mock_get_api.return_value = mock_api_func
        
        fetcher = self.builder.build_by_task(TaskType.STOCK_BASIC, "600519")
        result = fetcher()
        
        mock_normalize.assert_called_once_with("600519")
        mock_get_api.assert_called_once_with("pro", "stock_basic")
        mock_api_func.assert_called_once_with(ts_code="600519.SH")
        assert isinstance(result, pd.DataFrame)
    
    @patch.object(TushareApiManager, 'get_api_function')
    def test_build_by_task_without_symbol(self, mock_get_api):
        """测试不带股票代码的任务构建"""
        mock_api_func = Mock(return_value=pd.DataFrame({'data': [1, 2, 3]}))
        mock_get_api.return_value = mock_api_func
        
        fetcher = self.builder.build_by_task(TaskType.STOCK_BASIC)
        result = fetcher()
        
        mock_get_api.assert_called_once_with("pro", "stock_basic")
        # STOCK_BASIC 没有默认参数，所以不传递任何参数
        mock_api_func.assert_called_once_with()
        assert isinstance(result, pd.DataFrame)
    
    @patch.object(TushareApiManager, 'get_api_function')
    @patch('neo.downloader.fetcher_builder.normalize_stock_code')
    def test_build_by_task_with_overrides(self, mock_normalize, mock_get_api):
        """测试带参数覆盖的任务构建"""
        mock_normalize.return_value = "600519.SH"
        mock_api_func = Mock(return_value=pd.DataFrame({'data': [1, 2, 3]}))
        mock_get_api.return_value = mock_api_func
        
        fetcher = self.builder.build_by_task(
            TaskType.STOCK_BASIC, 
            "600519", 
            exchange="SZSE",
            list_status="L"
        )
        result = fetcher()
        
        mock_normalize.assert_called_once_with("600519")
        mock_get_api.assert_called_once_with("pro", "stock_basic")
        mock_api_func.assert_called_once_with(
            ts_code="600519.SH",
            exchange="SZSE",
            list_status="L"
        )
        assert isinstance(result, pd.DataFrame)
    
    @patch.object(TushareApiManager, 'get_api_function')
    def test_build_by_task_with_default_params(self, mock_get_api):
        """测试带默认参数的任务构建"""
        mock_api_func = Mock(return_value=pd.DataFrame({'data': [1, 2, 3]}))
        mock_get_api.return_value = mock_api_func
        
        # 使用有默认参数的任务类型
        fetcher = self.builder.build_by_task(TaskType.STOCK_ADJ_QFQ)
        result = fetcher()
        
        mock_get_api.assert_called_once_with("ts", "pro_bar")
        # 验证默认参数被使用
        call_args = mock_api_func.call_args[1]
        assert call_args["adj"] == "qfq"
        assert isinstance(result, pd.DataFrame)
    
    @patch.object(TushareApiManager, 'get_api_function')
    def test_execute_function_exception_handling(self, mock_get_api):
        """测试执行函数异常处理"""
        mock_api_func = Mock(side_effect=Exception("API调用失败"))
        mock_get_api.return_value = mock_api_func
        
        fetcher = self.builder.build_by_task(TaskType.STOCK_BASIC)
        
        with pytest.raises(Exception, match="API调用失败"):
            fetcher()
    
    @patch.object(TushareApiManager, 'get_api_function')
    @patch('neo.downloader.fetcher_builder.normalize_stock_code')
    def test_parameter_merging(self, mock_normalize, mock_get_api):
        """测试参数合并逻辑"""
        mock_normalize.return_value = "600519.SH"
        mock_api_func = Mock(return_value=pd.DataFrame({'data': [1]}))
        mock_get_api.return_value = mock_api_func
        
        # 测试运行时参数覆盖默认参数
        fetcher = self.builder.build_by_task(
            TaskType.STOCK_DAILY,
            "600519",
            start_date="20240101",  # 覆盖默认参数
            end_date="20240131"     # 新增参数
        )
        fetcher()
        
        call_args = mock_api_func.call_args[1]
        assert call_args["ts_code"] == "600519.SH"
        assert call_args["start_date"] == "20240101"
        assert call_args["end_date"] == "20240131"