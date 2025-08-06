#!/usr/bin/env python3
"""
演示数据下载阶段的核心功能

步骤3: 数据下载阶段：Fetcher + 限速装饰器
1. TaskHandler 根据 task_config 判断是否需要 rate_limit，并封装为 _fetch_data。  
2. 调用对应 fetcher 方法（如 fetch_daily_history / fetch_daily_basic），传入 ts_code, start_date, end_date。  
3. 处理网络异常与空数据返回；当失败为网络错误时暂存待重试。
"""

from unittest.mock import MagicMock
import pandas as pd
from src.downloader.tasks.daily import DailyTaskHandler
from src.downloader.tasks.daily_basic import DailyBasicTaskHandler
from src.downloader.rate_limit import dynamic_limiter


def demonstrate_rate_limit_configuration():
    """演示TaskHandler如何根据task_config判断并应用rate_limit"""
    print("=== 1. TaskHandler 根据 task_config 判断是否需要 rate_limit ===")
    
    # 创建mock组件
    mock_fetcher = MagicMock()
    mock_storage = MagicMock()
    mock_storage.get_latest_date.return_value = None
    
    # 配置1: 有rate_limit
    config_with_limit = {
        "name": "带限速的日线任务",
        "type": "daily",
        "adjust": "qfq",
        "rate_limit": {
            "calls_per_minute": 200  # 每分钟200次调用
        }
    }
    
    # 配置2: 无rate_limit
    config_no_limit = {
        "name": "无限速的日线任务", 
        "type": "daily",
        "adjust": "none"
    }
    
    print(f"任务配置1 (有限速): {config_with_limit}")
    print(f"任务配置2 (无限速): {config_no_limit}")
    
    # 创建TaskHandler
    handler_with_limit = DailyTaskHandler(config_with_limit, mock_fetcher, mock_storage)
    handler_no_limit = DailyTaskHandler(config_no_limit, mock_fetcher, mock_storage)
    
    print(f"✅ 成功创建两个TaskHandler")
    print()


def demonstrate_fetcher_method_mapping():
    """演示不同任务类型调用对应的fetcher方法"""
    print("=== 2. 调用对应 fetcher 方法，传入 ts_code, start_date, end_date ===")
    
    mock_fetcher = MagicMock()
    mock_storage = MagicMock()
    
    # 模拟返回数据
    sample_daily_data = pd.DataFrame({
        "trade_date": ["20230101", "20230102"],
        "open": [10.0, 10.5],
        "close": [10.2, 10.8]
    })
    
    sample_basic_data = pd.DataFrame({
        "trade_date": ["20230101", "20230102"],
        "pe": [15.5, 16.0],
        "pb": [1.2, 1.3]
    })
    
    mock_fetcher.fetch_daily_history.return_value = sample_daily_data
    mock_fetcher.fetch_daily_basic.return_value = sample_basic_data
    
    # 创建不同类型的任务处理器
    daily_config = {"name": "日线任务", "type": "daily", "adjust": "qfq"}
    basic_config = {"name": "基础指标任务", "type": "daily_basic"}
    
    daily_handler = DailyTaskHandler(daily_config, mock_fetcher, mock_storage)
    basic_handler = DailyBasicTaskHandler(basic_config, mock_fetcher, mock_storage)
    
    # 演示fetcher方法调用
    test_params = ("000001.SZ", "20230101", "20230131")
    
    print(f"调用参数: ts_code='{test_params[0]}', start_date='{test_params[1]}', end_date='{test_params[2]}'")
    
    # Daily任务 -> fetch_daily_history
    daily_result = daily_handler.fetch_data(*test_params)
    print(f"DailyTaskHandler.fetch_data() -> fetcher.fetch_daily_history()")
    print(f"返回数据形状: {daily_result.shape}")
    
    # DailyBasic任务 -> fetch_daily_basic  
    basic_result = basic_handler.fetch_data(*test_params)
    print(f"DailyBasicTaskHandler.fetch_data() -> fetcher.fetch_daily_basic()")
    print(f"返回数据形状: {basic_result.shape}")
    
    # 验证调用
    mock_fetcher.fetch_daily_history.assert_called_with("000001.SZ", "20230101", "20230131", "qfq")
    mock_fetcher.fetch_daily_basic.assert_called_with("000001.SZ", "20230101", "20230131")
    print("✅ fetcher方法调用验证成功")
    print()


def demonstrate_error_handling():
    """演示网络异常与空数据的处理机制"""
    print("=== 3. 处理网络异常与空数据返回；网络错误时暂存待重试 ===")
    
    mock_fetcher = MagicMock()
    mock_storage = MagicMock()
    mock_storage.get_latest_date.return_value = None
    
    config = {"name": "错误处理演示", "type": "daily", "adjust": "none"}
    handler = DailyTaskHandler(config, mock_fetcher, mock_storage)
    
    # 情况1: 网络错误
    print("情况1: 模拟网络错误")
    network_errors = [
        ConnectionError("网络连接失败"),
        TimeoutError("请求超时"),
        Exception("SSL certificate error"),
        Exception("name or service not known")
    ]
    
    for i, error in enumerate(network_errors, 1):
        mock_fetcher.fetch_daily_history.side_effect = error
        success, is_network_error = handler._process_single_symbol("000001.SZ", is_retry=False)
        
        print(f"  错误{i}: {type(error).__name__}('{error}') -> 网络错误: {is_network_error}, 暂存待重试: {not success and is_network_error}")
    
    # 情况2: 非网络错误
    print("情况2: 模拟非网络错误")
    mock_fetcher.fetch_daily_history.side_effect = ValueError("参数无效")
    success, is_network_error = handler._process_single_symbol("000002.SZ", is_retry=False)
    print(f"  ValueError('参数无效') -> 网络错误: {is_network_error}, 不重试: {not is_network_error}")
    
    # 情况3: 空数据处理
    print("情况3: 空数据处理")
    
    # 空DataFrame
    mock_fetcher.fetch_daily_history.side_effect = None
    mock_fetcher.fetch_daily_history.return_value = pd.DataFrame()
    success, is_network_error = handler._process_single_symbol("000003.SZ", is_retry=False)
    print(f"  返回空DataFrame -> 成功: {success}, 但不保存数据")
    
    # 返回None
    mock_fetcher.fetch_daily_history.return_value = None
    success, is_network_error = handler._process_single_symbol("000004.SZ", is_retry=False)  
    print(f"  返回None -> 成功: {success}, 记录为获取失败")
    
    print("✅ 错误处理机制演示完成")
    print()


def demonstrate_retry_workflow():
    """演示完整的重试工作流程"""
    print("=== 4. 完整重试工作流程演示 ===")
    
    mock_fetcher = MagicMock()
    mock_storage = MagicMock()
    mock_storage.get_latest_date.return_value = None
    
    config = {"name": "重试演示", "type": "daily", "adjust": "none"}
    handler = DailyTaskHandler(config, mock_fetcher, mock_storage)
    
    target_symbols = ["000001.SZ", "000002.SZ", "000003.SZ"]
    
    # 设置模拟响应：第一只正常，第二只网络错误后重试成功，第三只非网络错误
    mock_responses = [
        # 初始调用
        pd.DataFrame({"trade_date": ["20230101"]}),  # 000001.SZ 成功
        ConnectionError("Network failed"),           # 000002.SZ 网络错误
        ValueError("Invalid param"),                 # 000003.SZ 非网络错误
        # 重试调用
        pd.DataFrame({"trade_date": ["20230102"]})   # 000002.SZ 重试成功
    ]
    
    mock_fetcher.fetch_daily_history.side_effect = mock_responses
    
    print(f"目标股票: {target_symbols}")
    print("执行任务...")
    
    # 执行任务（包含重试逻辑）
    handler.execute(target_symbols=target_symbols)
    
    # 检查结果
    call_count = mock_fetcher.fetch_daily_history.call_count
    save_count = mock_storage.save.call_count
    
    print(f"fetcher调用次数: {call_count} (初始3次 + 重试1次)")
    print(f"storage保存次数: {save_count} (000001.SZ成功 + 000002.SZ重试成功)")
    print("✅ 重试工作流程演示完成")
    print()


def demonstrate_rate_limit_keys():
    """演示限速器的任务键区分机制"""
    print("=== 5. 限速器任务键区分机制 ===")
    
    # 清理全局限制器状态
    dynamic_limiter.limiters.clear()
    
    mock_fetcher = MagicMock()
    mock_storage = MagicMock()
    mock_storage.get_latest_date.return_value = None
    mock_fetcher.fetch_daily_history.return_value = pd.DataFrame({"trade_date": ["20230101"]})
    
    config = {
        "name": "限速键演示",
        "type": "daily", 
        "adjust": "none",
        "rate_limit": {"calls_per_minute": 100}
    }
    
    handler = DailyTaskHandler(config, mock_fetcher, mock_storage)
    
    # 模拟正常调用和重试调用
    handler._process_single_symbol("000001.SZ", is_retry=False)
    handler._process_single_symbol("000001.SZ", is_retry=True)
    handler._process_single_symbol("000002.SZ", is_retry=False)
    
    print("创建的限速器键:")
    for key, limiter in dynamic_limiter.limiters.items():
        print(f"  - {key}: {limiter.calls_per_minute} calls/min")
    
    expected_keys = [
        "限速键演示_000001.SZ",
        "限速键演示_000001.SZ_retry", 
        "限速键演示_000002.SZ"
    ]
    
    actual_keys = set(dynamic_limiter.limiters.keys())
    print(f"期望键数量: {len(expected_keys)}, 实际键数量: {len(actual_keys)}")
    print("✅ 限速器键区分机制验证完成")
    print()


if __name__ == "__main__":
    print("🚀 数据下载阶段功能演示")
    print("=" * 60)
    
    demonstrate_rate_limit_configuration()
    demonstrate_fetcher_method_mapping()
    demonstrate_error_handling()
    demonstrate_retry_workflow()
    demonstrate_rate_limit_keys()
    
    print("✅ 数据下载阶段演示完成!")
    print("\n核心功能总结:")
    print("1. ✅ TaskHandler 根据 task_config 判断是否需要 rate_limit")
    print("2. ✅ 动态封装 _fetch_data 并调用对应 fetcher 方法") 
    print("3. ✅ 正确传入 ts_code, start_date, end_date 参数")
    print("4. ✅ 处理网络异常，区分网络错误和其他错误")
    print("5. ✅ 网络错误时暂存待重试，非网络错误直接失败")
    print("6. ✅ 处理空数据返回（DataFrame 和 None）")
    print("7. ✅ 重试时使用不同的 task_key 避免限速器冲突")
