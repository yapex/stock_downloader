#!/usr/bin/env python3
"""配置接口使用示例

演示如何使用新的配置接口系统，替代直接访问配置字典。
"""

import os
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.downloader.config_impl import create_config_manager
from src.downloader.interfaces import ConfigInterface


def demonstrate_config_usage():
    """演示配置接口的使用"""
    print("=== 配置接口使用演示 ===")
    
    # 创建配置管理器
    config: ConfigInterface = create_config_manager("config.yaml")
    
    print("\n1. 基本配置信息:")
    token = config.get_runtime_token()
    print(f"   Tushare Token: {token[:10]}..." if token else "   Tushare Token: 未设置")
    print(f"   数据库路径: {config.database.path}")
    print(f"   配置验证: {'✓ 通过' if config.validate() else '✗ 失败'}")
    
    print("\n2. 下载器配置:")
    dl_config = config.downloader
    print(f"   最大生产者数: {dl_config.max_producers}")
    print(f"   最大消费者数: {dl_config.max_consumers}")
    print(f"   生产者队列大小: {dl_config.producer_queue_size}")
    print(f"   数据队列大小: {dl_config.data_queue_size}")
    if dl_config.symbols:
        print(f"   指定股票数量: {len(dl_config.symbols)}")
    
    print("\n3. 消费者配置:")
    consumer_config = config.consumer
    print(f"   批处理大小: {consumer_config.batch_size}")
    print(f"   刷新间隔: {consumer_config.flush_interval}秒")
    print(f"   最大重试次数: {consumer_config.max_retries}")
    
    print("\n4. 任务配置:")
    all_tasks = config.get_all_tasks()
    enabled_tasks = [task for task in all_tasks.values() if task.get('enabled', False)]
    print(f"   总任务数: {len(all_tasks)}")
    print(f"   启用任务数: {len(enabled_tasks)}")
    
    print("\n   启用的任务:")
    for task in enabled_tasks:
        print(f"   - {task.name} ({task.type})")
        start_date = task.get('start_date')
        end_date = task.get('end_date')
        if start_date:
            print(f"     日期范围: {start_date} ~ {end_date or '现在'}")
    
    print("\n5. 任务组配置:")
    all_groups = config.get_all_groups()
    print(f"   总组数: {len(all_groups)}")
    
    for group_name, group in all_groups.items():
        print(f"\n   组: {group_name}")
        print(f"   描述: {group.description}")
        print(f"   股票: {group.symbols if isinstance(group.symbols, str) else f'{len(group.symbols)}只股票'}")
        print(f"   任务: {', '.join(group.tasks)}")


def demonstrate_task_specific_config():
    """演示特定任务的配置访问"""
    print("\n=== 特定任务配置演示 ===")
    
    config: ConfigInterface = create_config_manager("config.yaml")
    
    # 查看特定任务配置
    task_names = ["daily_qfq", "financial_income", "update_stock_list"]
    
    for task_name in task_names:
        task_config = config.get_task_config(task_name)
        if task_config:
            print(f"\n任务: {task_name}")
            print(f"  名称: {task_config.name}")
            print(f"  类型: {task_config.type}")
            print(f"  启用: {'是' if task_config.get('enabled', False) else '否'}")
            start_date = task_config.get('start_date')
            if start_date:
                print(f"  开始日期: {start_date}")
            end_date = task_config.get('end_date')
            if end_date:
                print(f"  结束日期: {end_date}")
            if task_config.date_col:
                print(f"  日期列: {task_config.date_col}")
            if task_config.statement_type:
                print(f"  报表类型: {task_config.statement_type}")
        else:
            print(f"\n任务: {task_name} - 未找到配置")


def demonstrate_group_specific_config():
    """演示特定任务组的配置访问"""
    print("\n=== 特定任务组配置演示 ===")
    
    config: ConfigInterface = create_config_manager("config.yaml")
    
    # 查看特定任务组配置
    group_names = ["default", "daily", "financial", "quick"]
    
    for group_name in group_names:
        group_config = config.get_group_config(group_name)
        if group_config:
            print(f"\n任务组: {group_name}")
            print(f"  描述: {group_config.description}")
            
            if isinstance(group_config.symbols, str):
                print(f"  股票范围: {group_config.symbols}")
            else:
                print(f"  指定股票: {len(group_config.symbols)}只")
                if len(group_config.symbols) <= 5:
                    print(f"    股票代码: {', '.join(group_config.symbols)}")
            
            print(f"  包含任务: {', '.join(group_config.tasks)}")
            
            # 验证任务组中的任务是否都存在
            missing_tasks = []
            for task_name in group_config.tasks:
                if not config.get_task_config(task_name):
                    missing_tasks.append(task_name)
            
            if missing_tasks:
                print(f"  ⚠️  缺失任务: {', '.join(missing_tasks)}")
            else:
                print(f"  ✓ 所有任务配置完整")
        else:
            print(f"\n任务组: {group_name} - 未找到配置")


def demonstrate_environment_integration():
    """演示环境变量集成"""
    print("\n=== 环境变量集成演示 ===")
    
    config: ConfigInterface = create_config_manager("config.yaml")
    
    print("Token获取优先级:")
    print("1. 环境变量 TUSHARE_TOKEN")
    print("2. 配置文件 tushare_token")
    
    env_token = os.getenv("TUSHARE_TOKEN")
    if env_token:
        print(f"\n✓ 从环境变量获取Token: {env_token[:10]}...")
    else:
        print(f"\n- 环境变量未设置，使用配置文件Token")
    
    current_token = config.get_runtime_token()
    print(f"\n当前使用的Token: {current_token[:10]}..." if current_token else "未设置Token")


def demonstrate_validation():
    """演示配置验证"""
    print("\n=== 配置验证演示 ===")
    
    config: ConfigInterface = create_config_manager("config.yaml")
    
    print("配置验证检查项:")
    print("- Tushare Token 是否存在")
    print("- 数据库路径是否配置")
    print("- 任务配置是否完整")
    print("- 任务组引用的任务是否存在")
    
    is_valid = config.validate()
    print(f"\n配置验证结果: {'✓ 通过' if is_valid else '✗ 失败'}")
    
    if not is_valid:
        print("\n请检查以下配置项:")
        if not config.get_runtime_token():
            print("- 缺少 Tushare Token")
        if not config.database.path:
            print("- 缺少数据库路径配置")


if __name__ == "__main__":
    try:
        demonstrate_config_usage()
        demonstrate_task_specific_config()
        demonstrate_group_specific_config()
        demonstrate_environment_integration()
        demonstrate_validation()
        
        print("\n=== 演示完成 ===")
        print("\n配置接口的优势:")
        print("✓ 类型安全的配置访问")
        print("✓ 统一的配置接口")
        print("✓ 环境变量集成")
        print("✓ 配置验证")
        print("✓ 面向接口编程")
        print("✓ 易于测试和模拟")
        
    except FileNotFoundError as e:
        print(f"错误: {e}")
        print("请确保在项目根目录运行此脚本，并且 config.yaml 文件存在。")
    except Exception as e:
        print(f"运行时错误: {e}")
        import traceback
        traceback.print_exc()