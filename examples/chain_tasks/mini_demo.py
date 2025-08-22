#!/usr/bin/env python3

import time
from config import huey
from tasks import download_task

def main():
    print("🎯 MiniHuey 链式任务演示")
    print()
    
    # 启动 MiniHuey 调度器
    print("🚀 启动 MiniHuey 调度器...")
    huey.start()
    
    try:
        print("📋 提交链式任务...")
        print()
        
        # 提交多个任务
        symbols = ['AAPL', 'TSLA', 'GOOGL']
        results = []
        
        for symbol in symbols:
            print(f"📤 提交任务: {symbol}")
            result = download_task(symbol)
            results.append((symbol, result))
        
        print("\n⏳ 等待任务执行完成...")
        print()
        
        # 等待所有任务完成
        for symbol, result in results:
            try:
                # 获取任务结果（会阻塞直到任务完成）
                task_result = result()
                print(f"✅ 任务完成: {symbol}")
            except Exception as e:
                print(f"❌ 任务失败: {symbol} - {e}")
        
        print("\n" + "="*40)
        print("✅ 所有链式任务执行完成!")
        
    finally:
        # 停止调度器
        print("\n🛑 停止 MiniHuey 调度器...")
        huey.stop()
        print("👋 演示结束")

if __name__ == '__main__':
    main()