#!/usr/bin/env python3
"""性能分析报告脚本

分析 pysnooper 生成的性能日志文件
"""

import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple

def parse_elapsed_time(log_content: str) -> float:
    """解析日志中的执行时间"""
    elapsed_pattern = r"Elapsed time: (\d{2}:\d{2}:\d{2}\.\d+)"
    match = re.search(elapsed_pattern, log_content)
    if match:
        time_str = match.group(1)
        # 解析时间格式 HH:MM:SS.microseconds
        parts = time_str.split(':')
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds_parts = parts[2].split('.')
        seconds = int(seconds_parts[0])
        microseconds = int(seconds_parts[1])
        
        total_seconds = hours * 3600 + minutes * 60 + seconds + microseconds / 1000000
        return total_seconds
    return 0.0

def count_operations(log_content: str) -> int:
    """统计操作次数（行数）"""
    lines = log_content.strip().split('\n')
    # 过滤掉非执行行（如 Source path, Starting var 等）
    execution_lines = [line for line in lines if 'line' in line and 'call' not in line and 'return' not in line]
    return len(execution_lines)

def analyze_log_file(log_path: Path) -> Dict:
    """分析单个日志文件"""
    if not log_path.exists():
        return {"error": f"文件不存在: {log_path}"}
    
    try:
        content = log_path.read_text(encoding='utf-8')
        elapsed_time = parse_elapsed_time(content)
        operation_count = count_operations(content)
        file_size = log_path.stat().st_size
        
        return {
            "file": log_path.name,
            "elapsed_time": elapsed_time,
            "operation_count": operation_count,
            "file_size_bytes": file_size,
            "avg_time_per_operation": elapsed_time / operation_count if operation_count > 0 else 0
        }
    except Exception as e:
        return {"error": f"解析文件 {log_path} 时出错: {e}"}

def generate_performance_report():
    """生成性能分析报告"""
    log_dir = Path("logs/performance")
    
    if not log_dir.exists():
        print("❌ 性能日志目录不存在")
        return
    
    print("📊 性能分析报告")
    print("=" * 50)
    
    log_files = list(log_dir.glob("*.log"))
    if not log_files:
        print("❌ 未找到性能日志文件")
        return
    
    results = []
    total_time = 0.0
    
    for log_file in sorted(log_files):
        result = analyze_log_file(log_file)
        if "error" not in result:
            results.append(result)
            total_time += result["elapsed_time"]
    
    # 按执行时间排序
    results.sort(key=lambda x: x["elapsed_time"], reverse=True)
    
    print(f"\n🔍 分析了 {len(results)} 个日志文件")
    print(f"⏱️  总执行时间: {total_time:.6f} 秒")
    print("\n📈 性能详情:")
    print("-" * 80)
    print(f"{'文件名':<25} {'执行时间(秒)':<12} {'操作数':<8} {'平均时间/操作':<15} {'文件大小(KB)':<12}")
    print("-" * 80)
    
    for result in results:
        print(f"{result['file']:<25} "
              f"{result['elapsed_time']:<12.6f} "
              f"{result['operation_count']:<8} "
              f"{result['avg_time_per_operation']:<15.6f} "
              f"{result['file_size_bytes']/1024:<12.1f}")
    
    # 性能瓶颈分析
    print("\n🎯 性能瓶颈分析:")
    print("-" * 50)
    
    if results:
        slowest = results[0]
        print(f"⚠️  最耗时的操作: {slowest['file']} ({slowest['elapsed_time']:.6f}秒)")
        
        # 分析具体瓶颈
        if "load_task_types" in slowest['file']:
            print("   - Schema 加载和解析是主要瓶颈")
            print("   - 建议: 实现 Schema 缓存机制")
        elif "enum_generation" in slowest['file']:
            print("   - 动态枚举生成耗时较多")
            print("   - 建议: 缓存生成的枚举类")
        elif "api_manager_init" in slowest['file']:
            print("   - API 管理器初始化耗时")
            print("   - 建议: 延迟初始化或连接池")
        elif "build_by_task" in slowest['file']:
            print("   - 任务构建过程耗时")
            print("   - 建议: 优化参数合并逻辑")
    
    # 缓存建议
    print("\n💡 缓存优化建议:")
    print("-" * 50)
    print("1. SchemaLoader 缓存: 缓存解析后的 schema 对象")
    print("2. TaskType 枚举缓存: 避免重复生成动态枚举")
    print("3. API 函数缓存: 缓存 API 函数引用")
    print("4. 配置缓存: 缓存配置文件解析结果")
    
    print("\n✅ 分析完成!")

if __name__ == "__main__":
    generate_performance_report()