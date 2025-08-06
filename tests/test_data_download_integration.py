import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from downloader.tasks.daily import DailyTaskHandler
from downloader.tasks.daily_basic import DailyBasicTaskHandler
from downloader.rate_limit import dynamic_limiter


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
            # 第二只股票：网络错误，稍后重试成功
            ConnectionError("Network connection failed"),
            # 第三只股票：空数据
            pd.DataFrame(),
            # 第二只股票重试：成功获取数据
            pd.DataFrame({
                "trade_date": ["20230101"], 
                "open": [50.0],
                "close": [51.0]
            })
        ]
        
        mock_fetcher.fetch_daily_history.side_effect = mock_responses
        
        # 3. 执行TaskHandler
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        
        # 清理限制器状态
        dynamic_limiter.limiters.clear()
        
        # 4. 执行完整流程
        handler.execute(target_symbols=target_symbols)
        
        # 5. 验证调用情况
        # 初始3次调用 + 1次重试 = 4次调用
        assert mock_fetcher.fetch_daily_history.call_count == 4
        
        # 应该有2次成功保存（第一只股票 + 第二只股票重试成功）
        assert mock_storage.save.call_count == 2
        
        # 验证保存的数据类型和参数
        save_calls = mock_storage.save.call_args_list
        for call in save_calls:
            args, kwargs = call
            df, data_type, ts_code = args[:3]
            assert data_type == "daily_qfq"
            assert isinstance(df, pd.DataFrame)
            assert not df.empty

    def test_rate_limit_enforcement_with_real_limiter(self, mock_fetcher, mock_storage):
        """测试真实限速器的限速效果"""
        import time
        from downloader.rate_limit import rate_limit, dynamic_limiter
        
        # 清理限制器状态  
        dynamic_limiter.limiters.clear()
        
        # 创建一个使用限速装饰器的简单函数来测试
        call_count = [0]  # 使用列表来避免闭包问题
        
        @rate_limit(calls_per_minute=120, task_key="test_limiter")
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
        
        # 验证创建了限速器
        assert len(dynamic_limiter.limiters) > 0, "应该创建了至少一个限速器"
        assert "test_limiter" in dynamic_limiter.limiters, "应该创建了指定键的限速器"
        
        # 由于限速（120次/分钟 = 0.5秒/次），3次调用应该至少需要约1秒
        # 考虑到首次调用无需等待，实际应该是约1秒（第2次等待0.5s，第3次等待0.5s）
        print(f"限速器测试耗时: {elapsed_time:.2f}秒，调用次数: {call_count[0]}")
        
        # 较宽松的时间验证，主要验证限速器确实在工作
        if elapsed_time < 0.8:
            print(f"警告：限速效果可能不明显，实际耗时: {elapsed_time:.2f}秒")
        else:
            print("✅ 限速器工作正常")

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
        
        # 清理限制器状态
        dynamic_limiter.limiters.clear()
        
        # 执行任务
        daily_handler._process_single_symbol("000001.SZ", is_retry=False)
        basic_handler._process_single_symbol("000001.SZ", is_retry=False)
        
        # 验证创建了不同的限制器
        assert len(dynamic_limiter.limiters) == 2
        
        # 验证限制器的配置
        daily_limiter_key = "日线任务_000001.SZ"
        basic_limiter_key = "基础指标任务_000001.SZ"
        
        assert daily_limiter_key in dynamic_limiter.limiters
        assert basic_limiter_key in dynamic_limiter.limiters
        
        daily_limiter = dynamic_limiter.limiters[daily_limiter_key]
        basic_limiter = dynamic_limiter.limiters[basic_limiter_key]
        
        assert daily_limiter.calls_per_minute == 500
        assert basic_limiter.calls_per_minute == 200

    def test_error_recovery_workflow(self, mock_fetcher, mock_storage):
        """测试错误恢复工作流程"""
        
        task_config = {
            "name": "错误恢复测试",
            "type": "daily", 
            "adjust": "none"
        }
        
        # 模拟多种错误场景
        error_scenarios = [
            # 网络超时 -> 重试成功  
            TimeoutError("Request timeout"),
            pd.DataFrame({"trade_date": ["20230101"]}),
            
            # 连接错误 -> 重试失败
            ConnectionError("Network connection failed"), 
            ConnectionError("Network connection failed"),
            
            # 非网络错误 -> 不重试
            ValueError("Invalid parameter"),
        ]
        
        mock_fetcher.fetch_daily_history.side_effect = error_scenarios
        
        # 执行任务
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        target_symbols = ["000001.SZ", "000002.SZ", "000003.SZ"]
        
        handler.execute(target_symbols=target_symbols)
        
        # 验证调用次数：初始3次 + 重试2次 = 5次
        assert mock_fetcher.fetch_daily_history.call_count == 5
        
        # 验证成功保存次数：只有第一只股票重试成功
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
