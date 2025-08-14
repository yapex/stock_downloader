#!/usr/bin/env python3
"""简化的 Blinker 事件总线演示脚本

演示股票下载器的核心事件流程
"""

import sys
import os
import time
import random
from typing import List

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.downloader2.blinker_event_bus import BlinkerEventBus


class ProgressMonitor:
    """进度监控器"""
    
    def __init__(self):
        self.total_processed = 0
        self.successful = 0
        self.failed = 0
    
    def on_download_started(self, data):
        print(f"📊 开始下载 {data['total']} 只股票")
    
    def on_symbol_success(self, data):
        self.successful += 1
        print(f"✅ {data['symbol']} 下载成功 ({data['records']} 条记录)")
    
    def on_symbol_failed(self, data):
        self.failed += 1
        print(f"❌ {data['symbol']} 下载失败: {data['reason']}")
    
    def on_progress_update(self, data):
        print(f"📈 进度: {data['completed']}/{data['total']} ({data['percentage']:.1f}%)")
    
    def on_download_completed(self, data):
        print(f"🎉 下载完成! 成功: {data['successful']}, 失败: {data['failed']}, 耗时: {data['duration']:.2f}秒")


class StockDownloader:
    """股票下载器"""
    
    def __init__(self, event_bus: BlinkerEventBus):
        self.event_bus = event_bus
        self.start_time = None
    
    def download_stocks(self, symbols: List[str]):
        """下载股票数据"""
        self.start_time = time.time()
        
        # 发布开始事件
        self.event_bus.publish("download.started", {
            "total": len(symbols),
            "timestamp": self.start_time
        })
        
        successful = 0
        failed = 0
        
        for i, symbol in enumerate(symbols):
            # 模拟下载过程
            if self._download_symbol(symbol):
                successful += 1
                self.event_bus.publish("symbol.success", {
                    "symbol": symbol,
                    "records": random.randint(200, 1000),
                    "timestamp": time.time()
                })
            else:
                failed += 1
                self.event_bus.publish("symbol.failed", {
                    "symbol": symbol,
                    "reason": random.choice(["网络超时", "API限制", "数据格式错误"]),
                    "timestamp": time.time()
                })
            
            # 发布进度更新
            completed = i + 1
            self.event_bus.publish("progress.update", {
                "completed": completed,
                "total": len(symbols),
                "percentage": (completed / len(symbols)) * 100,
                "timestamp": time.time()
            })
            
            time.sleep(0.1)  # 模拟处理时间
        
        # 发布完成事件
        duration = time.time() - self.start_time
        self.event_bus.publish("download.completed", {
            "successful": successful,
            "failed": failed,
            "duration": duration,
            "timestamp": time.time()
        })
    
    def _download_symbol(self, symbol: str) -> bool:
        """模拟下载单个股票（85% 成功率）"""
        return random.random() > 0.15


def demo_stock_download():
    """演示股票下载流程"""
    print("\n=== 股票下载演示 ===")
    
    # 创建事件总线
    event_bus = BlinkerEventBus()
    
    # 创建进度监控器
    monitor = ProgressMonitor()
    
    # 订阅事件
    event_bus.subscribe("download.started", monitor.on_download_started)
    event_bus.subscribe("symbol.success", monitor.on_symbol_success)
    event_bus.subscribe("symbol.failed", monitor.on_symbol_failed)
    event_bus.subscribe("progress.update", monitor.on_progress_update)
    event_bus.subscribe("download.completed", monitor.on_download_completed)
    
    # 创建下载器
    downloader = StockDownloader(event_bus)
    
    # 模拟下载一些股票
    symbols = ["600519", "000001", "000002", "600036", "600000", "000858", "002415"]
    
    print("开始股票数据下载...")
    downloader.download_stocks(symbols)
    
    print(f"\n📋 最终统计: 成功 {monitor.successful} 只, 失败 {monitor.failed} 只")
    
    # 清理
    event_bus.clear()


def main():
    """主函数"""
    print("📈 股票下载器事件系统演示")
    print("=" * 40)

    try:
        demo_stock_download()
        print("\n✨ 演示完成！")

    except Exception as e:
        print(f"❌ 演示过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
