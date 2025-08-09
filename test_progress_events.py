#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试新的进度事件系统
"""

import logging
import time
from src.downloader.app import DownloaderApp
from src.downloader.progress_events import progress_event_manager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('progress_events_test.log'),
        logging.StreamHandler()
    ]
)

def main():
    """测试进度事件系统"""
    print("=== 测试新的进度事件系统 ===")
    
    # 创建下载应用
    app = DownloaderApp()
    
    print("\n开始下载前的进度状态:")
    print(f"进度事件管理器状态: {progress_event_manager.get_stats()}")
    
    try:
        # 运行下载
        print("\n开始运行下载任务...")
        start_time = time.time()
        
        success = app.run_download(
            config_path='config.yaml',
            group_name='default',
            symbols=['000001.SZ'],
            force=True
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n下载完成，成功: {success}，耗时: {duration:.2f} 秒")
        
        # 等待一下确保所有事件都被处理
        time.sleep(1)
        
        print("\n下载完成后的进度状态:")
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
        
    except Exception as e:
        print(f"\n运行过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    main()