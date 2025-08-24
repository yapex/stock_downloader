"""Huey配置"""
from huey import MemoryHuey
from dataclasses import dataclass


@dataclass
class HueyPerformanceConfig:
    """Huey性能测试配置"""
    # 测试数据配置
    stock_count: int = 30
    data_points_per_stock: int = 50
    
    # 网络延迟模拟
    min_network_delay: float = 0.1
    max_network_delay: float = 0.5
    
    # 测试配置
    test_iterations: int = 2
    warmup_iterations: int = 1
    batch_size: int = 5
    
    # Huey配置
    huey_name: str = "performance_test"
    worker_count: int = 4  # Consumer工作线程数
    
    # 输出配置
    output_dir: str = "huey_performance_results"


# 默认配置
default_config = HueyPerformanceConfig()

# 创建MemoryHuey实例，支持多线程并发
huey = MemoryHuey(
    default_config.huey_name,
    blocking=True  # 使用blocking模式，由Consumer控制并发
)