#!/usr/bin/env python3
"""
演示如何在 fetcher 中使用原生速率限制器
"""

import time
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.downloader.rate_limiter_native import rate_limit


class SimpleFetcher:
    """简化的数据获取器，使用原生速率限制器"""
    
    @rate_limit("income")
    def fetch_income(self, ts_code: str) -> dict:
        """获取利润表数据"""
        print(f"Fetching income data for {ts_code} at {time.time()}")
        # 模拟 API 调用
        return {
            "ts_code": ts_code,
            "revenue": 1000000,
            "net_profit": 100000
        }
    
    @rate_limit("balancesheet")
    def fetch_balancesheet(self, ts_code: str) -> dict:
        """获取资产负债表数据"""
        print(f"Fetching balance sheet data for {ts_code} at {time.time()}")
        # 模拟 API 调用
        return {
            "ts_code": ts_code,
            "total_assets": 5000000,
            "total_liab": 3000000
        }
    
    @rate_limit("cashflow")
    def fetch_cashflow(self, ts_code: str) -> dict:
        """获取现金流量表数据"""
        print(f"Fetching cash flow data for {ts_code} at {time.time()}")
        # 模拟 API 调用
        return {
            "ts_code": ts_code,
            "operating_cf": 200000,
            "investing_cf": -50000
        }


def main():
    """主函数"""
    print("演示原生速率限制器在 fetcher 中的使用")
    print("="*50)
    
    fetcher = SimpleFetcher()
    
    # 测试股票代码
    test_codes = ["000001.SZ", "000002.SZ", "600000.SH"]
    
    print("\n=== 测试利润表数据获取 ===")
    for code in test_codes:
        data = fetcher.fetch_income(code)
        print(f"Income data for {code}: revenue={data['revenue']}")
    
    print("\n=== 测试资产负债表数据获取 ===")
    for code in test_codes:
        data = fetcher.fetch_balancesheet(code)
        print(f"Balance sheet data for {code}: assets={data['total_assets']}")
    
    print("\n=== 测试现金流量表数据获取 ===")
    for code in test_codes:
        data = fetcher.fetch_cashflow(code)
        print(f"Cash flow data for {code}: operating_cf={data['operating_cf']}")
    
    print("\n=== 混合调用测试 ===")
    # 混合调用不同的方法
    for i, code in enumerate(test_codes):
        if i % 3 == 0:
            data = fetcher.fetch_income(code)
            print(f"Mixed call - Income: {code}")
        elif i % 3 == 1:
            data = fetcher.fetch_balancesheet(code)
            print(f"Mixed call - Balance: {code}")
        else:
            data = fetcher.fetch_cashflow(code)
            print(f"Mixed call - Cashflow: {code}")
    
    print("\n演示完成！")
    print("\n优势总结：")
    print("1. 使用 pyrate-limiter 原生装饰器，代码更简洁")
    print("2. 每个方法有独立的速率限制")
    print("3. 内置阻塞机制，无需手动实现等待逻辑")
    print("4. 避免重复造轮子，利用成熟的库功能")


if __name__ == "__main__":
    main()