#!/usr/bin/env python3
"""
演示DeadLetterLogger.log_missing_symbols方法的使用示例

展示如何批量记录缺失的股票代码到死信日志
"""

import sys
import tempfile
import json
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.downloader.retry_policy import DeadLetterLogger


def main():
    """演示log_missing_symbols方法的使用"""
    print("=== DeadLetterLogger.log_missing_symbols 使用示例 ===")
    
    # 创建临时日志文件用于演示
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.jsonl') as temp_file:
        temp_path = temp_file.name
    
    try:
        # 初始化DeadLetterLogger
        logger = DeadLetterLogger(log_path=temp_path)
        
        print(f"\n1. 创建死信日志记录器，日志文件: {temp_path}")
        
        # 示例1: 批量记录日线数据的缺失股票
        print("\n2. 批量记录daily类型的缺失股票...")
        daily_missing_symbols = [
            "000001.SZ",  # 平安银行
            "000002.SZ",  # 万科A  
            "600000.SH",  # 浦发银行
            "600036.SH",  # 招商银行
        ]
        
        logger.log_missing_symbols("daily", daily_missing_symbols)
        print(f"   记录了 {len(daily_missing_symbols)} 个daily缺失股票")
        
        # 示例2: 记录财务数据的缺失股票  
        print("\n3. 记录financials类型的缺失股票...")
        financials_missing_symbols = [
            "ST退市",     # 特殊字符股票
            "*ST海润",    # 风险警示股票
        ]
        
        logger.log_missing_symbols("financials", financials_missing_symbols) 
        print(f"   记录了 {len(financials_missing_symbols)} 个financials缺失股票")
        
        # 示例3: 记录基本面数据的缺失股票
        print("\n4. 记录daily_basic类型的缺失股票...")
        basic_missing_symbols = [
            "300001.SZ",  # 特锐德
        ]
        
        logger.log_missing_symbols("daily_basic", basic_missing_symbols)
        print(f"   记录了 {len(basic_missing_symbols)} 个daily_basic缺失股票")
        
        # 示例4: 空列表测试（不会写入任何记录）
        print("\n5. 测试空股票列表...")
        logger.log_missing_symbols("stock_list", [])
        print("   空列表不会写入任何记录")
        
        # 读取并显示生成的记录
        print("\n6. 读取生成的死信记录:")
        print("-" * 60)
        
        with open(temp_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                if line.strip():
                    record = json.loads(line)
                    print(f"记录 {i}:")
                    print(f"  股票代码: {record['symbol']}")
                    print(f"  任务类型: {record['task_type']}")  
                    print(f"  错误类型: {record['error_type']}")
                    print(f"  错误信息: {record['error_message']}")
                    print(f"  重试次数: {record['retry_count']}")
                    print(f"  最大重试: {record['max_retries']}")
                    print(f"  失败时间: {record['failed_at']}")
                    print()
        
        # 示例5: 使用现有方法读取记录
        print("7. 使用DeadLetterLogger读取统计信息:")
        print("-" * 60)
        
        stats = logger.get_statistics()
        print(f"总记录数: {stats['total_count']}")
        print(f"按任务类型统计: {stats['by_task_type']}")
        print(f"按错误类型统计: {stats['by_error_type']}")
        
        print(f"\n最近失败记录:")
        for failure in stats['recent_failures'][:3]:  # 只显示前3条
            print(f"  {failure['symbol']} ({failure['task_type']}) - {failure['error_type']}")
        
        # 示例6: 错误处理演示
        print("\n8. 错误处理演示:")
        print("-" * 60)
        
        try:
            logger.log_missing_symbols("invalid_task_type", ["000001.SZ"])
        except ValueError as e:
            print(f"捕获到错误: {e}")
        
        print("\n=== 演示完成 ===")
        
    finally:
        # 清理临时文件
        Path(temp_path).unlink(missing_ok=True)
        print(f"已清理临时文件: {temp_path}")


if __name__ == "__main__":
    main()
