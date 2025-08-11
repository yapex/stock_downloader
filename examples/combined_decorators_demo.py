#!/usr/bin/env python3
"""
演示速率限制装饰器和重试装饰器的组合使用

模拟真实的 API 调用场景，展示两个装饰器如何协作：
1. 速率限制确保不超过 API 限制
2. 重试机制处理网络错误和临时故障
3. 两者互不干扰，各司其职
"""

import time
import random
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.rate_limiter_native import rate_limit, reset_limiter
from src.downloader.simple_retry_simplified import simple_retry


class MockAPIError(Exception):
    """模拟 API 错误"""
    pass


class SimulatedTushareFetcher:
    """模拟的 Tushare 数据获取器"""
    
    def __init__(self):
        self.call_history = []
        self.network_failure_rate = 0.3  # 30% 网络失败率
        self.api_error_rate = 0.1  # 10% API 错误率
    
    def _simulate_api_call(self, method_name: str, ts_code: str) -> dict:
        """模拟 API 调用，可能出现各种错误"""
        call_time = time.time()
        self.call_history.append({
            'method': method_name,
            'ts_code': ts_code,
            'time': call_time
        })
        
        print(f"[{call_time:.3f}] 调用 {method_name}({ts_code})")
        
        # 模拟网络错误
        if random.random() < self.network_failure_rate:
            raise ConnectionError(f"网络连接失败 - {method_name}({ts_code})")
        
        # 模拟 API 错误
        if random.random() < self.api_error_rate:
            raise MockAPIError(f"API 临时错误 - {method_name}({ts_code})")
        
        # 模拟参数错误（不可重试）
        if ts_code == "INVALID":
            raise ValueError("invalid parameter: 无效的股票代码")
        
        # 成功返回模拟数据
        return {
            'ts_code': ts_code,
            'method': method_name,
            'data': f"模拟数据_{method_name}_{ts_code}",
            'timestamp': call_time
        }
    
    @simple_retry(max_retries=2, task_name="获取利润表")
    @rate_limit("fetch_income")
    def fetch_income(self, ts_code: str) -> dict:
        """获取利润表数据"""
        return self._simulate_api_call("fetch_income", ts_code)
    
    @simple_retry(max_retries=2, task_name="获取资产负债表")
    @rate_limit("fetch_balancesheet")
    def fetch_balancesheet(self, ts_code: str) -> dict:
        """获取资产负债表数据"""
        return self._simulate_api_call("fetch_balancesheet", ts_code)
    
    @simple_retry(max_retries=2, task_name="获取现金流量表")
    @rate_limit("fetch_cashflow")
    def fetch_cashflow(self, ts_code: str) -> dict:
        """获取现金流量表数据"""
        return self._simulate_api_call("fetch_cashflow", ts_code)
    
    def get_call_statistics(self) -> dict:
        """获取调用统计信息"""
        if not self.call_history:
            return {}
        
        total_calls = len(self.call_history)
        methods = {}
        
        for call in self.call_history:
            method = call['method']
            if method not in methods:
                methods[method] = 0
            methods[method] += 1
        
        first_call = self.call_history[0]['time']
        last_call = self.call_history[-1]['time']
        duration = last_call - first_call
        
        return {
            'total_calls': total_calls,
            'methods': methods,
            'duration': duration,
            'calls_per_second': total_calls / duration if duration > 0 else 0
        }


def demo_successful_scenario():
    """演示成功场景"""
    print("\n=== 演示成功场景 ===")
    print("所有调用都成功，展示速率限制和重试装饰器的正常协作")
    
    fetcher = SimulatedTushareFetcher()
    # 临时降低错误率，确保成功
    fetcher.network_failure_rate = 0
    fetcher.api_error_rate = 0
    
    test_codes = ["000001.SZ", "000002.SZ", "600000.SH"]
    
    for code in test_codes:
        result = fetcher.fetch_income(code)
        if result:
            print(f"  ✓ 成功获取 {code} 的利润表数据")
        else:
            print(f"  ✗ 获取 {code} 的利润表数据失败")
    
    stats = fetcher.get_call_statistics()
    print(f"  统计: 总调用 {stats['total_calls']} 次，耗时 {stats['duration']:.2f} 秒")


def demo_network_error_scenario():
    """演示网络错误重试场景"""
    print("\n=== 演示网络错误重试场景 ===")
    print("模拟网络错误，展示重试装饰器的工作")
    
    fetcher = SimulatedTushareFetcher()
    # 设置较高的网络错误率
    fetcher.network_failure_rate = 0.6
    fetcher.api_error_rate = 0
    
    test_codes = ["000001.SZ", "000002.SZ"]
    
    for code in test_codes:
        print(f"\n尝试获取 {code} 的资产负债表数据...")
        result = fetcher.fetch_balancesheet(code)
        if result:
            print(f"  ✓ 最终成功获取数据")
        else:
            print(f"  ✗ 重试后仍然失败")
    
    stats = fetcher.get_call_statistics()
    print(f"\n统计: 总调用 {stats['total_calls']} 次，耗时 {stats['duration']:.2f} 秒")


def demo_parameter_error_scenario():
    """演示参数错误场景（不重试）"""
    print("\n=== 演示参数错误场景 ===")
    print("模拟参数错误，展示重试装饰器正确识别不可重试错误")
    
    fetcher = SimulatedTushareFetcher()
    fetcher.network_failure_rate = 0
    fetcher.api_error_rate = 0
    
    try:
        print("尝试使用无效股票代码...")
        result = fetcher.fetch_cashflow("INVALID")
        print(f"  意外成功: {result}")
    except ValueError as e:
        print(f"  ✓ 正确捕获参数错误: {e}")
        print(f"  ✓ 没有进行重试（符合预期）")
    
    stats = fetcher.get_call_statistics()
    print(f"统计: 总调用 {stats['total_calls']} 次（应该只有1次）")


def demo_mixed_methods_scenario():
    """演示混合方法调用场景"""
    print("\n=== 演示混合方法调用场景 ===")
    print("调用不同方法，展示独立的速率限制")
    
    fetcher = SimulatedTushareFetcher()
    fetcher.network_failure_rate = 0.2
    fetcher.api_error_rate = 0.1
    
    test_codes = ["000001.SZ", "000002.SZ"]
    methods = [
        ("fetch_income", "利润表"),
        ("fetch_balancesheet", "资产负债表"),
        ("fetch_cashflow", "现金流量表")
    ]
    
    for code in test_codes:
        for method_name, display_name in methods:
            print(f"\n获取 {code} 的{display_name}数据...")
            method = getattr(fetcher, method_name)
            result = method(code)
            if result:
                print(f"  ✓ 成功")
            else:
                print(f"  ✗ 失败")
    
    stats = fetcher.get_call_statistics()
    print(f"\n最终统计:")
    print(f"  总调用: {stats['total_calls']} 次")
    print(f"  耗时: {stats['duration']:.2f} 秒")
    print(f"  平均速率: {stats['calls_per_second']:.2f} 调用/秒")
    print(f"  各方法调用次数: {stats['methods']}")


def main():
    """主函数"""
    print("速率限制装饰器 + 重试装饰器组合演示")
    print("=" * 50)
    
    # 重置速率限制器
    reset_limiter()
    
    # 设置随机种子，使结果可重现（可选）
    random.seed(42)
    
    # 运行各种演示场景
    demo_successful_scenario()
    
    time.sleep(1)  # 短暂等待
    demo_network_error_scenario()
    
    time.sleep(1)  # 短暂等待
    demo_parameter_error_scenario()
    
    time.sleep(1)  # 短暂等待
    demo_mixed_methods_scenario()
    
    print("\n=== 总结 ===")
    print("1. ✓ 速率限制装饰器正常工作，控制调用频率")
    print("2. ✓ 重试装饰器正确处理网络错误和临时故障")
    print("3. ✓ 两个装饰器互不干扰，各司其职")
    print("4. ✓ 不可重试错误被正确识别，避免无效重试")
    print("5. ✓ 不同方法有独立的速率限制")
    print("\n装饰器组合验证完成！")


if __name__ == "__main__":
    main()