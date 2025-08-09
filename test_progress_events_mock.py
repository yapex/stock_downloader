#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试新的进度事件系统 - 模拟版本
"""

import logging
import time
import threading
from src.downloader.progress_events import (
    progress_event_manager, start_phase, end_phase, task_started, 
    task_completed, task_failed, batch_completed, update_total, 
    send_message, ProgressPhase
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('progress_events_mock_test.log'),
        logging.StreamHandler()
    ]
)

def simulate_download_process():
    """模拟下载过程"""
    print("=== 模拟下载过程 ===")
    
    # 启动进度事件管理器
    progress_event_manager.start()
    
    try:
        # 1. 初始化阶段
        start_phase(ProgressPhase.INITIALIZATION)
        send_message("正在初始化下载系统...")
        time.sleep(0.5)
        
        # 2. 准备阶段
        start_phase(ProgressPhase.PREPARATION, total=6)
        send_message("正在准备下载任务...")
        time.sleep(0.3)
        
        # 模拟准备6个任务
        symbols = ['000001.SZ', '000002.SZ', '600519.SH']
        task_types = ['daily', 'daily_basic']
        total_tasks = len(symbols) * len(task_types)
        
        update_total(total_tasks, ProgressPhase.DOWNLOADING)
        
        # 3. 下载阶段
        start_phase(ProgressPhase.DOWNLOADING, total=total_tasks)
        
        # 模拟下载任务
        for symbol in symbols:
            for task_type in task_types:
                task_id = f"{symbol}-{task_type}"
                
                # 任务开始
                task_started(
                    task_id=task_id,
                    symbol=symbol,
                    message=f"下载 {task_type} 数据"
                )
                
                # 模拟下载时间
                time.sleep(0.2)
                
                # 模拟成功/失败
                if symbol == '000002.SZ' and task_type == 'daily_basic':
                    # 模拟一个失败的任务
                    task_failed(
                        task_id=task_id,
                        symbol=symbol,
                        message="网络连接超时"
                    )
                else:
                    # 成功完成
                    task_completed(
                        task_id=task_id,
                        symbol=symbol
                    )
        
        # 4. 保存阶段
        start_phase(ProgressPhase.SAVING)
        send_message("正在保存数据到数据库...")
        
        # 模拟批次保存
        for i in range(3):
            time.sleep(0.3)
            batch_completed(
                count=1,
                message=f"保存批次 {i+1} 完成"
            )
        
        # 5. 完成阶段
        start_phase(ProgressPhase.COMPLETED)
        send_message("所有任务已完成！")
        
    finally:
        # 等待一下确保所有事件都被处理
        time.sleep(1)
        
        # 停止进度事件管理器
        progress_event_manager.stop()

def main():
    """主测试函数"""
    print("=== 测试新的进度事件系统 (模拟版本) ===")
    
    print("\n开始前的进度状态:")
    print(f"进度事件管理器状态: {progress_event_manager.get_stats()}")
    
    try:
        start_time = time.time()
        
        # 运行模拟下载
        simulate_download_process()
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n模拟下载完成，耗时: {duration:.2f} 秒")
        
        print("\n完成后的进度状态:")
        final_stats = progress_event_manager.get_stats()
        print(f"进度事件管理器最终状态: {final_stats}")
        
        # 显示详细统计
        print("\n=== 详细统计信息 ===")
        print(f"当前阶段: {final_stats['current_phase']}")
        print(f"总任务数: {final_stats['overall_total']}")
        print(f"已完成: {final_stats['overall_completed']}")
        print(f"失败数: {final_stats['overall_failed']}")
        print(f"成功率: {final_stats['success_rate']:.1f}%")
        
        print("\n=== 各阶段统计 ===")
        for phase, total in final_stats['phase_totals'].items():
            completed = final_stats['phase_completed'].get(phase, 0)
            failed = final_stats['phase_failed'].get(phase, 0)
            print(f"{phase}: 总数={total}, 完成={completed}, 失败={failed}")
        
        # 验证结果
        print("\n=== 验证结果 ===")
        expected_completed = 5  # 6个任务中有1个失败
        expected_failed = 1
        
        if final_stats['overall_completed'] == expected_completed:
            print("✅ 完成任务数正确")
        else:
            print(f"❌ 完成任务数错误: 期望 {expected_completed}, 实际 {final_stats['overall_completed']}")
            
        if final_stats['overall_failed'] == expected_failed:
            print("✅ 失败任务数正确")
        else:
            print(f"❌ 失败任务数错误: 期望 {expected_failed}, 实际 {final_stats['overall_failed']}")
            
        if final_stats['current_phase'] == 'completed':
            print("✅ 最终阶段正确")
        else:
            print(f"❌ 最终阶段错误: 期望 'completed', 实际 '{final_stats['current_phase']}'")
        
    except Exception as e:
        print(f"\n运行过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    main()