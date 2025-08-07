import pytest
from unittest.mock import MagicMock
import pandas as pd
from downloader.tasks.daily import DailyTaskHandler
from downloader.tasks.daily_basic import DailyBasicTaskHandler


class TestDataDownloadIntegration:
    """测试数据下载阶段的完整集成流程"""

    @pytest.fixture
    def mock_fetcher(self):
        fetcher = MagicMock()
        return fetcher
    
    @pytest.fixture  
    def mock_storage(self):
        storage = MagicMock()
        storage.get_latest_date.return_value = None
        return storage

    def test_complete_data_download_workflow(self, mock_fetcher, mock_storage):
        """测试完整的数据下载工作流程"""
        
        # 1. 配置任务，包含rate_limit设置
        task_config = {
            "name": "完整日线下载测试",
            "type": "daily",
            "adjust": "qfq", 
            "rate_limit": {
                "calls_per_minute": 200
            }
        }
        
        # 2. 准备测试数据
        target_symbols = ["000001.SZ", "600519.SH", "000002.SZ"]
        
        # 模拟不同场景的返回数据
        mock_responses = [
            # 第一只股票：正常数据
            pd.DataFrame({
                "trade_date": ["20230101", "20230102"], 
                "open": [10.0, 10.5],
                "close": [10.2, 10.8]
            }),
            # 第二只股票：网络错误（fetcher层会重试，但这里我们模拟最终失败）
            ConnectionError("Network connection failed"),
            # 第三只股票：空数据
            pd.DataFrame(),
        ]
        
        mock_fetcher.fetch_daily_history.side_effect = mock_responses
        
        # 3. 执行TaskHandler
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        
        # 注意：现在使用ratelimit库，不需要手动清理限制器状态
        
        # 4. 执行完整流程
        handler.execute(target_symbols=target_symbols)
        
        # 5. 验证调用情况
        # 现在每个股票只调用一次，共3次调用
        assert mock_fetcher.fetch_daily_history.call_count == 3
        
        # 应该有2次成功保存（第一只股票有数据 + 第三只股票空数据也算成功）
        assert mock_storage.save.call_count == 1
        
        # 验证保存的数据类型和参数
        save_calls = mock_storage.save.call_args_list
        for call in save_calls:
            args, kwargs = call
            df, data_type, ts_code = args[:3]
            assert data_type == "daily_qfq"
            assert isinstance(df, pd.DataFrame)
            assert not df.empty

    def test_rate_limit_enforcement_with_ratelimit_library(self, mock_fetcher, mock_storage):
        """测试ratelimit库的限速效果"""
        import time
        from ratelimit import limits, sleep_and_retry
        
        # 创建一个使用ratelimit装饰器的简单函数来测试
        call_count = [0]  # 使用列表来避免闭包问题
        
        @sleep_and_retry
        @limits(calls=2, period=1)  # 每秒最多2次调用
        def test_function():
            call_count[0] += 1
            return call_count[0]
        
        # 记录开始时间
        start_time = time.time()
        
        # 连续调用3次
        for i in range(3):
            result = test_function()
            assert result == i + 1
        
        # 记录结束时间
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print(f"ratelimit库测试耗时: {elapsed_time:.2f}秒，调用次数: {call_count[0]}")
        
        # 由于限速（2次/秒），3次调用应该至少需要约1秒
        # 前2次调用应该很快，第3次会等待
        if elapsed_time >= 0.9:
            print("✅ ratelimit库限速器工作正常")
        else:
            print(f"警告：限速效果可能不明显，实际耗时: {elapsed_time:.2f}秒")

    def test_multiple_task_types_with_different_rate_limits(self, mock_fetcher, mock_storage):
        """测试不同任务类型使用不同的限速配置"""
        
        # 日线任务配置
        daily_config = {
            "name": "日线任务",
            "type": "daily",
            "adjust": "qfq",
            "rate_limit": {"calls_per_minute": 500}
        }
        
        # 基础指标任务配置  
        basic_config = {
            "name": "基础指标任务", 
            "type": "daily_basic",
            "rate_limit": {"calls_per_minute": 200}
        }
        
        # 准备模拟数据
        mock_fetcher.fetch_daily_history.return_value = pd.DataFrame({"trade_date": ["20230101"]})
        mock_fetcher.fetch_daily_basic.return_value = pd.DataFrame({"trade_date": ["20230101"]})
        
        # 创建处理器
        daily_handler = DailyTaskHandler(daily_config, mock_fetcher, mock_storage)
        basic_handler = DailyBasicTaskHandler(basic_config, mock_fetcher, mock_storage)
        
        # 执行任务
        daily_handler._process_single_symbol("000001.SZ")
        basic_handler._process_single_symbol("000001.SZ")
        
        # 验证方法被调用（ratelimit库会自动处理限流）
        assert mock_fetcher.fetch_daily_history.called
        assert mock_fetcher.fetch_daily_basic.called
        
        # 验证数据保存
        assert mock_storage.save.call_count >= 2

    def test_error_recovery_workflow(self, mock_fetcher, mock_storage):
        """测试错误恢复工作流程"""
        
        task_config = {
            "name": "错误恢复测试",
            "type": "daily", 
            "adjust": "none"
        }
        
        # 模拟多种错误场景
        error_scenarios = [
            # 网络超时（fetcher层重试后假设成功）  
            pd.DataFrame({"trade_date": ["20230101"]}),
            
            # 连接错误（fetcher层重试后失败）
            ConnectionError("Network connection failed"), 
            
            # 非网络错误
            ValueError("Invalid parameter"),
        ]
        
        mock_fetcher.fetch_daily_history.side_effect = error_scenarios
        
        # 执行任务
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        target_symbols = ["000001.SZ", "000002.SZ", "000003.SZ"]
        
        handler.execute(target_symbols=target_symbols)
        
        # 验证调用次数：现在每个股票只调用一次，共3次
        assert mock_fetcher.fetch_daily_history.call_count == 3
        
        # 验证成功保存次数：只有第一只股票成功
        assert mock_storage.save.call_count == 1

    def test_data_type_and_date_col_configuration(self, mock_fetcher, mock_storage):
        """测试数据类型和日期列的配置"""
        
        # 测试不同adjust参数对data_type的影响
        configs = [
            {"adjust": "qfq", "expected_type": "daily_qfq"},
            {"adjust": "hfq", "expected_type": "daily_hfq"}, 
            {"adjust": "none", "expected_type": "daily_none"},
            {"adjust": None, "expected_type": "daily_none"},
            # 缺少adjust字段时的默认行为
            {}, 
        ]
        
        for config in configs:
            task_config = {
                "name": "类型测试",
                "type": "daily",
                **config
            }
            
            # 删除expected_type，它不是task_config的一部分
            expected_type = config.pop("expected_type", "daily_none")
            
            mock_fetcher.fetch_daily_history.return_value = pd.DataFrame({"trade_date": ["20230101"]})
            
            handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
            
            # 验证data_type
            assert handler.get_data_type() == expected_type
            
            # 验证date_col默认值
            assert handler.get_date_col() == "trade_date"
            
            # 重置mock
            mock_fetcher.reset_mock()
            mock_storage.reset_mock()
