#!/usr/bin/env python3
"""
测试脚本：验证错误处理和重试机制改进

包含以下测试：
1. 北交所股票代码识别测试
2. 错误分类测试
3. 错误日志分类测试
4. 重试机制测试
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from downloader.utils import normalize_stock_code, record_failed_task
from downloader.error_handler import classify_error, ErrorCategory, RetryStrategy, enhanced_retry
import requests


def test_beijing_stock_exchange():
    """测试北交所股票代码支持"""
    print("=" * 50)
    print("🧪 测试北交所股票代码支持")
    print("=" * 50)
    
    # 测试用例：北交所股票代码
    test_codes = [
        "430017.BJ",  # 完整格式
        "430047",     # 只有数字
        "BJ430090",   # 前缀格式
        "430139BJ",   # 后缀格式
        "839300",     # 8开头的北交所代码
        "920000",     # 9开头的北交所代码
    ]
    
    for code in test_codes:
        try:
            normalized = normalize_stock_code(code)
            print(f"✅ {code} -> {normalized}")
        except Exception as e:
            print(f"❌ {code} -> 错误: {e}")
    
    print()


def test_error_classification():
    """测试错误分类功能"""
    print("=" * 50)
    print("🧪 测试错误分类功能")
    print("=" * 50)
    
    test_errors = [
        (ConnectionError("Connection failed"), ErrorCategory.NETWORK),
        (TimeoutError("Request timeout"), ErrorCategory.NETWORK),
        (ValueError("Invalid parameter"), ErrorCategory.PARAMETER),
        (Exception("参数无效"), ErrorCategory.PARAMETER),
        (Exception("rate limit exceeded"), ErrorCategory.API_LIMIT),
        (Exception("ERROR."), ErrorCategory.DATA_UNAVAILABLE),
        (Exception("test error"), ErrorCategory.TEST),
        (Exception("unknown error"), ErrorCategory.BUSINESS),
    ]
    
    for error, expected_category in test_errors:
        actual_category = classify_error(error)
        status = "✅" if actual_category == expected_category else "❌"
        print(f"{status} {error} -> {actual_category.value} (预期: {expected_category.value})")
    
    print()


def test_error_logging_classification():
    """测试错误日志分类"""
    print("=" * 50)
    print("🧪 测试错误日志分类")
    print("=" * 50)
    
    # 清理可能存在的测试日志文件
    import glob
    for log_file in glob.glob("failed_tasks_*.log"):
        try:
            os.remove(log_file)
            print(f"🧹 清理旧日志文件: {log_file}")
        except:
            pass
    
    # 测试不同类型的错误记录
    test_cases = [
        ("业务任务", "daily_600519.SH", "fetch_failed", "business"),
        ("测试任务", "test_000001.SZ", "参数错误", "test"),
        ("网络任务", "daily_000002.SZ", "Connection timeout", "network"),
        ("参数任务", "daily_430017.BJ", "无法识别的股票代码前缀", "parameter"),
    ]
    
    for task_name, entity_id, reason, category in test_cases:
        record_failed_task(task_name, entity_id, reason, category)
        print(f"📝 记录错误: {category} - {task_name} - {reason}")
    
    # 检查生成的日志文件
    print("\n生成的日志文件:")
    for log_file in glob.glob("failed_tasks*.log"):
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        print(f"📄 {log_file}: {len(lines)} 条记录")
        if lines:
            print(f"   示例: {lines[-1].strip()}")
    
    print()


@enhanced_retry(strategy=RetryStrategy(max_retries=2, base_delay=0.1), task_name="测试重试")
def test_retry_function(should_fail: bool = True, fail_type: str = "network"):
    """测试重试机制的示例函数"""
    if should_fail:
        if fail_type == "network":
            raise ConnectionError("模拟网络连接失败")
        elif fail_type == "parameter":
            raise ValueError("模拟参数错误")
        elif fail_type == "api_limit":
            raise Exception("rate limit exceeded")
    return "成功!"


def test_retry_mechanism():
    """测试重试机制"""
    print("=" * 50)
    print("🧪 测试重试机制")
    print("=" * 50)
    # 测试网络错误重试
    print("1. 测试网络错误重试 (应该重试):")
    result = test_retry_function(should_fail=True, fail_type="network")
    print(f"结果: {result}")
    print()
    
    # 测试参数错误不重试
    print("2. 测试参数错误不重试 (应该立即失败):")
    try:
        result = test_retry_function(should_fail=True, fail_type="parameter")
        print(f"结果: {result}")
    except ValueError as e:
        print(f"正确捕获参数错误: {e}")
    print()
    
    # 测试成功情况
    print("3. 测试成功情况 (第一次就成功):")
    result = test_retry_function(should_fail=False)
    print(f"结果: {result}")
    print()
    print()


def main():
    """主测试函数"""
    print("🚀 开始测试错误处理改进")
    print("=" * 60)
    
    try:
        test_beijing_stock_exchange()
        test_error_classification() 
        test_error_logging_classification()
        test_retry_mechanism()
        
        print("✅ 所有测试完成!")
        print("=" * 60)
        
        # 显示生成的日志文件摘要
        import glob
        log_files = glob.glob("failed_tasks*.log")
        if log_files:
            print(f"📊 生成了 {len(log_files)} 个分类日志文件:")
            for log_file in log_files:
                with open(log_file, 'r', encoding='utf-8') as f:
                    line_count = len(f.readlines())
                print(f"   • {log_file}: {line_count} 条记录")
        
    except Exception as e:
        print(f"❌ 测试过程中出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
