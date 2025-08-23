#!/usr/bin/env python3
"""高级进度跟踪演示 - 展示复杂的 tqdm 进度条使用场景"""

import asyncio
import time
from tqdm.asyncio import tqdm
from huey.contrib.asyncio import aget_result
from huey.consumer import Consumer
from config import huey
from tasks_tqdm import (
    batch_download_task,
    data_analysis_task,
    error_prone_task,
    download_task_with_progress
)

# 全局 consumer 变量
consumer = None
consumer_task = None


async def start_consumer():
    """启动 Huey consumer"""
    global consumer, consumer_task

    def run_consumer_sync():
        consumer = Consumer(huey, workers=6, worker_type="thread")
        consumer.run()

    loop = asyncio.get_event_loop()
    consumer_task = loop.run_in_executor(None, run_consumer_sync)
    await asyncio.sleep(0.5)


async def stop_consumer():
    """停止 Huey consumer"""
    global consumer, consumer_task
    if consumer:
        consumer.stop()
    if consumer_task:
        try:
            consumer_task.cancel()
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass


async def demo_nested_progress_bars():
    """演示嵌套进度条 - 批量处理多个股票组"""
    print("🎯 演示嵌套进度条 - 批量处理")
    print()
    
    # 定义股票组
    stock_groups = {
        "科技股": ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"],
        "金融股": ["JPM", "BAC", "WFC", "GS", "MS"],
        "消费股": ["AMZN", "WMT", "HD", "MCD", "NKE"],
        "医疗股": ["JNJ", "PFE", "UNH", "ABBV", "MRK"]
    }
    
    all_results = {}
    
    # 外层进度条：处理股票组
    with tqdm(total=len(stock_groups), desc="📊 处理股票组", position=0) as group_pbar:
        for group_name, symbols in stock_groups.items():
            group_pbar.set_postfix_str(f"当前组: {group_name}")
            
            # 内层进度条：处理单个组内的股票
            with tqdm(total=len(symbols), desc=f"  📈 {group_name}", position=1, leave=False) as stock_pbar:
                group_tasks = []
                
                # 提交该组的所有任务
                for symbol in symbols:
                    task = download_task_with_progress(symbol)
                    group_tasks.append(task)
                    stock_pbar.set_postfix_str(f"提交: {symbol}")
                    stock_pbar.update(1)
                    await asyncio.sleep(0.1)
                
                # 等待该组所有任务完成
                stock_pbar.reset(total=len(group_tasks))
                stock_pbar.set_description(f"  ⏳ {group_name}")
                
                group_results = []
                for coro in asyncio.as_completed([aget_result(task) for task in group_tasks]):
                    try:
                        result = await coro
                        group_results.append(result)
                        stock_pbar.set_postfix_str(f"完成: {result['symbol']}")
                        stock_pbar.update(1)
                    except Exception as e:
                        stock_pbar.set_postfix_str(f"失败: {str(e)[:15]}")
                        stock_pbar.update(1)
                
                all_results[group_name] = group_results
            
            group_pbar.update(1)
            await asyncio.sleep(0.2)  # 组间间隔
    
    print("\n📊 批量处理结果:")
    for group_name, results in all_results.items():
        success_count = len(results)
        total_count = len(stock_groups[group_name])
        success_rate = success_count / total_count * 100
        print(f"  {group_name}: {success_count}/{total_count} ({success_rate:.1f}%)")


async def demo_error_handling_with_progress():
    """演示带错误处理的进度条"""
    print("\n🎯 演示错误处理进度条")
    print()
    
    # 创建不同失败率的任务
    failure_rates = [0.1, 0.2, 0.3, 0.4, 0.5]
    tasks = []
    
    # 提交任务
    with tqdm(total=len(failure_rates), desc="📤 提交错误测试任务") as submit_pbar:
        for i, rate in enumerate(failure_rates):
            task = error_prone_task(rate)
            tasks.append((task, rate))
            submit_pbar.set_postfix_str(f"失败率: {rate*100:.0f}%")
            submit_pbar.update(1)
            await asyncio.sleep(0.1)
    
    # 执行任务并处理错误
    success_count = 0
    error_count = 0
    results = []
    
    with tqdm(total=len(tasks), desc="⚡ 执行错误测试") as exec_pbar:
        for coro in asyncio.as_completed([aget_result(task) for task, _ in tasks]):
            try:
                result = await coro
                results.append({"status": "success", "result": result})
                success_count += 1
                exec_pbar.set_postfix_str(f"成功: {success_count}, 失败: {error_count}")
            except Exception as e:
                results.append({"status": "error", "error": str(e)})
                error_count += 1
                exec_pbar.set_postfix_str(f"成功: {success_count}, 失败: {error_count}")
            
            exec_pbar.update(1)
    
    print("\n📊 错误处理统计:")
    print(f"  ✅ 成功: {success_count}/{len(tasks)} ({success_count/len(tasks)*100:.1f}%)")
    print(f"  ❌ 失败: {error_count}/{len(tasks)} ({error_count/len(tasks)*100:.1f}%)")
    
    # 显示错误详情
    if error_count > 0:
        print("\n❌ 错误详情:")
        for i, result in enumerate(results):
            if result["status"] == "error":
                rate = failure_rates[i] if i < len(failure_rates) else "未知"
                print(f"  任务{i+1} (失败率{rate*100:.0f}%): {result['error']}")


async def demo_pipeline_progress():
    """演示流水线进度条 - 下载 -> 分析 -> 报告"""
    print("\n🎯 演示流水线进度条")
    print()
    
    # 阶段1: 批量下载
    symbols_batch = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX"]
    
    print("📊 流水线处理: 下载 -> 分析 -> 报告")
    print()
    
    # 总体进度条
    with tqdm(total=3, desc="🔄 流水线进度", position=0) as pipeline_pbar:
        
        # 阶段1: 批量下载
        pipeline_pbar.set_postfix_str("阶段1: 批量下载")
        download_task = batch_download_task(symbols_batch)
        
        # 模拟下载进度监控
        with tqdm(total=100, desc="  📥 下载进度", position=1, leave=False) as download_pbar:
            start_time = time.time()
            estimated_time = len(symbols_batch) * 1.0  # 估算时间
            
            while True:
                elapsed = time.time() - start_time
                progress = min(int(elapsed / estimated_time * 100), 99)
                
                download_pbar.n = progress
                download_pbar.set_postfix_str(f"已用时: {elapsed:.1f}s")
                download_pbar.refresh()
                
                # 检查任务是否完成
                try:
                    download_result = await asyncio.wait_for(aget_result(download_task), timeout=0.1)
                    download_pbar.n = 100
                    download_pbar.set_postfix_str("下载完成")
                    download_pbar.refresh()
                    break
                except asyncio.TimeoutError:
                    await asyncio.sleep(0.5)
                except Exception as e:
                    download_pbar.set_postfix_str(f"下载失败: {str(e)[:20]}")
                    download_result = {"results": [], "errors": []}
                    break
        
        pipeline_pbar.update(1)
        
        # 阶段2: 数据分析
        pipeline_pbar.set_postfix_str("阶段2: 数据分析")
        analysis_task = data_analysis_task(download_result)
        
        with tqdm(total=100, desc="  📊 分析进度", position=1, leave=False) as analysis_pbar:
            start_time = time.time()
            estimated_time = len(download_result.get("results", [])) * 0.15
            
            while True:
                elapsed = time.time() - start_time
                progress = min(int(elapsed / max(estimated_time, 0.1) * 100), 99)
                
                analysis_pbar.n = progress
                analysis_pbar.set_postfix_str(f"分析中: {elapsed:.1f}s")
                analysis_pbar.refresh()
                
                try:
                    analysis_result = await asyncio.wait_for(aget_result(analysis_task), timeout=0.1)
                    analysis_pbar.n = 100
                    analysis_pbar.set_postfix_str("分析完成")
                    analysis_pbar.refresh()
                    break
                except asyncio.TimeoutError:
                    await asyncio.sleep(0.3)
                except Exception as e:
                    analysis_pbar.set_postfix_str(f"分析失败: {str(e)[:20]}")
                    analysis_result = {"status": "error"}
                    break
        
        pipeline_pbar.update(1)
        
        # 阶段3: 生成报告
        pipeline_pbar.set_postfix_str("阶段3: 生成报告")
        
        with tqdm(total=100, desc="  📄 报告生成", position=1, leave=False) as report_pbar:
            # 模拟报告生成过程
            report_steps = ["收集数据", "计算指标", "生成图表", "格式化输出", "保存文件"]
            
            for i, step in enumerate(report_steps):
                report_pbar.set_postfix_str(step)
                await asyncio.sleep(0.5)
                report_pbar.n = int((i + 1) / len(report_steps) * 100)
                report_pbar.refresh()
        
        pipeline_pbar.update(1)
    
    print("\n📊 流水线处理结果:")
    print(f"  📥 下载结果: {download_result.get('success', 0)}/{download_result.get('total', 0)} 成功")
    if analysis_result.get("status") == "completed":
        analysis = analysis_result.get("analysis", {})
        print(f"  📊 分析结果: 平均价格 ${analysis.get('avg_price', 0)}, 总成交量 {analysis.get('total_volume', 0):,}")
    print("  📄 报告状态: 已生成")


async def main():
    """主函数"""
    print("🚀 高级 tqdm 进度条演示")
    print("=" * 70)

    # 启动 consumer
    await start_consumer()
    print("🚀 Huey Consumer 已启动 (6个工作线程)\n")

    try:
        # 演示1: 嵌套进度条
        await demo_nested_progress_bars()
        await asyncio.sleep(1)
        
        # 演示2: 错误处理进度条
        await demo_error_handling_with_progress()
        await asyncio.sleep(1)
        
        # 演示3: 流水线进度条
        await demo_pipeline_progress()

    finally:
        # 停止 consumer
        await stop_consumer()
        print("\n🛑 Huey Consumer 已停止")

    print("\n" + "=" * 70)
    print("✅ 高级进度条演示完成!")
    print("👋 演示结束")


if __name__ == "__main__":
    asyncio.run(main())