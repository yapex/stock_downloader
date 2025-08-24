"""Huey性能测试脚本"""
import time
import statistics
import json
import os
import threading
from typing import Dict, List, Any
from dataclasses import dataclass, asdict
from huey.consumer import Consumer

from huey_config import HueyPerformanceConfig, default_config, huey
from sync_tasks import SyncTaskManager
from async_tasks import AsyncTaskManager


@dataclass
class TestMetrics:
    """测试指标"""
    total_time: float
    avg_task_time: float
    min_task_time: float
    max_task_time: float
    throughput: float  # 每秒处理的股票数
    success_rate: float
    total_data_points: int
    failed_tasks: int
    buffer_stats: Dict[str, int]


@dataclass
class HueyComparisonResult:
    """Huey对比结果"""
    sync_metrics: TestMetrics
    async_metrics: TestMetrics
    performance_improvement: Dict[str, float]
    test_config: Dict[str, Any]
    huey_stats: Dict[str, Any]


class HueyPerformanceTestRunner:
    """Huey性能测试运行器"""
    
    def __init__(self, config: HueyPerformanceConfig = None):
        self.config = config or default_config
        self.sync_manager = SyncTaskManager()
        self.async_manager = AsyncTaskManager()
        self.consumer = None
        self.consumer_thread = None
    
    def _calculate_metrics(self, results: List[Any], total_time: float, buffer_stats: Dict[str, int]) -> TestMetrics:
        """计算测试指标"""
        if not results:
            return TestMetrics(
                total_time=total_time,
                avg_task_time=0.0,
                min_task_time=0.0,
                max_task_time=0.0,
                throughput=0.0,
                success_rate=0.0,
                total_data_points=0,
                failed_tasks=0,
                buffer_stats=buffer_stats
            )
        
        # 展平结果列表（处理批次任务）
        flat_results = []
        for result in results:
            if isinstance(result, list):
                flat_results.extend(result)
            else:
                flat_results.append(result)
        
        # 解析字符串结果: "mode:symbol:data_count:processing_time[:ERROR:error_msg]"
        task_times = []
        successful_tasks = 0
        failed_tasks = 0
        total_data_points = 0
        
        for result_str in flat_results:
            if isinstance(result_str, str):
                parts = result_str.split(':')
                if len(parts) >= 4:
                    try:
                        data_count = int(parts[2])
                        processing_time = float(parts[3])
                        task_times.append(processing_time)
                        total_data_points += data_count
                        
                        if len(parts) > 4 and parts[4] == 'ERROR':
                            failed_tasks += 1
                        else:
                            successful_tasks += 1
                    except (ValueError, IndexError):
                        failed_tasks += 1
                else:
                    failed_tasks += 1
            else:
                failed_tasks += 1
        
        total_tasks = successful_tasks + failed_tasks
        
        return TestMetrics(
            total_time=total_time,
            avg_task_time=statistics.mean(task_times) if task_times else 0.0,
            min_task_time=min(task_times) if task_times else 0.0,
            max_task_time=max(task_times) if task_times else 0.0,
            throughput=total_data_points / total_time if total_time > 0 else 0.0,
            success_rate=(successful_tasks / total_tasks) * 100 if total_tasks > 0 else 0.0,
            total_data_points=total_data_points,
            failed_tasks=failed_tasks,
            buffer_stats=buffer_stats
        )
    
    def start_consumer(self):
        """启动Huey消费者"""
        if self.consumer is None:
            # 启动多线程Consumer，支持真正的并发执行
            self.consumer = Consumer(huey, workers=self.config.worker_count, worker_type="thread")
            self.consumer_thread = threading.Thread(target=self.consumer.run, daemon=True)
            self.consumer_thread.start()
            print(f"🚀 Huey消费者已启动 ({self.config.worker_count}个工作线程)")
            time.sleep(1)  # 等待消费者启动
    
    def stop_consumer(self):
        """停止Huey消费者"""
        if self.consumer:
            self.consumer.stop()
            if self.consumer_thread:
                self.consumer_thread.join(timeout=5)
            self.consumer = None
            self.consumer_thread = None
            print("Huey消费者已停止")
    
    def test_sync_tasks(self) -> TestMetrics:
        """测试同步任务"""
        print("开始Huey同步任务测试...")
        
        # 启动消费者
        self.start_consumer()
        
        symbols = [f"STOCK_{i:03d}" for i in range(self.config.stock_count)]
        
        start_time = time.time()
        
        # 提交任务
        tasks = self.sync_manager.submit_tasks(
            symbols, 
            self.config.data_points_per_stock, 
            self.config.batch_size
        )
        
        print(f"已提交 {len(tasks)} 个批次任务")
        
        # 等待结果
        results = self.sync_manager.wait_for_results(tasks)
        
        total_time = time.time() - start_time
        buffer_stats = self.sync_manager.get_buffer_stats()
        
        metrics = self._calculate_metrics(results, total_time, buffer_stats)
        
        print(f"同步任务完成: {len(results)}个任务, 耗时: {total_time:.2f}秒")
        print(f"成功率: {metrics.success_rate:.2%}, 吞吐量: {metrics.throughput:.2f}任务/秒")
        print(f"缓冲器统计: {buffer_stats}")
        
        return metrics
    
    def test_async_tasks(self) -> TestMetrics:
        """测试异步任务性能"""
        print("\n开始异步任务测试...")
        
        # 启动消费者
        self.start_consumer()
        
        # 生成测试用股票符号
        symbols = [f"STOCK_{i:03d}" for i in range(self.config.stock_count)]
        
        # 预热
        print(f"预热运行 {self.config.warmup_iterations} 次...")
        for i in range(self.config.warmup_iterations):
            warmup_tasks = self.async_manager.submit_tasks(
                symbols[:5], 
                self.config.data_points_per_stock, 
                batch_size=2
            )
            self.async_manager.wait_for_results(warmup_tasks, timeout=30.0)
        
        # 正式测试
        print(f"正式测试运行 {self.config.test_iterations} 次...")
        all_results = []
        total_start_time = time.time()
        
        for iteration in range(self.config.test_iterations):
            print(f"  第 {iteration + 1}/{self.config.test_iterations} 次迭代")
            
            # 提交任务
            tasks = self.async_manager.submit_tasks(
                symbols, 
                self.config.data_points_per_stock,
                batch_size=self.config.batch_size
            )
            
            # 等待结果
            results = self.async_manager.wait_for_results(tasks, timeout=300.0)
            all_results.extend(results)
        
        total_end_time = time.time()
        total_time = total_end_time - total_start_time
        
        # 获取缓冲器统计
        buffer_stats = self.async_manager.get_buffer_stats()
        
        # 停止消费者
        self.stop_consumer()
        
        return self._calculate_metrics(all_results, total_time, buffer_stats)
    
    def _calculate_performance_improvement(self, sync_metrics: TestMetrics, async_metrics: TestMetrics) -> Dict[str, float]:
        """计算性能提升"""
        improvements = {}
        
        if sync_metrics.total_time > 0:
            improvements['total_time'] = (sync_metrics.total_time - async_metrics.total_time) / sync_metrics.total_time * 100
        
        if sync_metrics.throughput > 0:
            improvements['throughput'] = (async_metrics.throughput - sync_metrics.throughput) / sync_metrics.throughput * 100
        
        if sync_metrics.avg_task_time > 0:
            improvements['avg_task_time'] = (sync_metrics.avg_task_time - async_metrics.avg_task_time) / sync_metrics.avg_task_time * 100
        
        return improvements
    
    def _get_huey_stats(self) -> Dict[str, Any]:
        """获取Huey统计信息"""
        return {
            'huey_name': huey.name,
            'storage_type': type(huey.storage).__name__,
            'immediate_mode': getattr(huey, 'immediate', False),
            'pending_count': len(huey.pending()),
            'scheduled_count': len(huey.scheduled())
        }
    
    def run_comparison_test(self) -> HueyComparisonResult:
        """运行对比测试"""
        print(f"开始Huey性能对比测试...")
        print(f"测试配置: {self.config.stock_count}只股票, 每只{self.config.data_points_per_stock}个数据点")
        print(f"批次大小: {self.config.batch_size}, 测试迭代: {self.config.test_iterations}")
        print(f"Huey配置: {type(huey.storage).__name__}")
        print("-" * 60)
        
        # 执行多次测试取平均值
        sync_metrics_list = []
        async_metrics_list = []
        
        for i in range(self.config.test_iterations):
            print(f"执行第 {i+1}/{self.config.test_iterations} 轮测试")
            
            # 清理Huey队列
            huey.flush()
            
            # 测试同步任务
            sync_metrics = self.test_sync_tasks()
            sync_metrics_list.append(sync_metrics)
            
            # 停止消费者
            self.stop_consumer()
            
            # 等待一下再测试异步任务
            time.sleep(2)
            
            # 清理Huey队列
            huey.flush()
            
            # 测试异步任务
            async_metrics = self.test_async_tasks()
            async_metrics_list.append(async_metrics)
            
            # 停止消费者
            self.stop_consumer()
            
            print(f"第 {i+1} 轮完成")
            print("-" * 40)
        
        # 计算平均指标
        def average_metrics(metrics_list: List[TestMetrics]) -> TestMetrics:
            return TestMetrics(
                total_time=statistics.mean([m.total_time for m in metrics_list]),
                avg_task_time=statistics.mean([m.avg_task_time for m in metrics_list]),
                min_task_time=statistics.mean([m.min_task_time for m in metrics_list]),
                max_task_time=statistics.mean([m.max_task_time for m in metrics_list]),
                throughput=statistics.mean([m.throughput for m in metrics_list]),
                success_rate=statistics.mean([m.success_rate for m in metrics_list]),
                total_data_points=int(statistics.mean([m.total_data_points for m in metrics_list])),
                failed_tasks=int(statistics.mean([m.failed_tasks for m in metrics_list])),
                buffer_stats=metrics_list[-1].buffer_stats  # 使用最后一次的缓冲器统计
            )
        
        avg_sync_metrics = average_metrics(sync_metrics_list)
        avg_async_metrics = average_metrics(async_metrics_list)
        
        # 计算性能提升
        improvements = self._calculate_performance_improvement(avg_sync_metrics, avg_async_metrics)
        
        # 获取Huey统计信息
        huey_stats = self._get_huey_stats()
        
        return HueyComparisonResult(
            sync_metrics=avg_sync_metrics,
            async_metrics=avg_async_metrics,
            performance_improvement=improvements,
            test_config=asdict(self.config),
            huey_stats=huey_stats
        )
    
    def save_results(self, result: HueyComparisonResult, filename: str = None) -> str:
        """保存测试结果"""
        if not filename:
            timestamp = int(time.time())
            filename = f"huey_performance_test_{timestamp}.json"
        
        # 确保输出目录存在
        os.makedirs(self.config.output_dir, exist_ok=True)
        filepath = os.path.join(self.config.output_dir, filename)
        
        # 转换为可序列化的格式
        result_dict = asdict(result)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(result_dict, f, indent=2, ensure_ascii=False)
        
        return filepath
    
    def print_results(self, result: HueyComparisonResult) -> None:
        """打印测试结果"""
        print("\n" + "=" * 80)
        print("Huey性能测试结果汇总")
        print("=" * 80)
        
        print(f"\n🔧 Huey配置:")
        for key, value in result.huey_stats.items():
            print(f"  {key}: {value}")
        
        print("\n📊 同步任务指标:")
        print(f"  总耗时: {result.sync_metrics.total_time:.2f}秒")
        print(f"  平均任务时间: {result.sync_metrics.avg_task_time:.4f}秒")
        print(f"  吞吐量: {result.sync_metrics.throughput:.2f}任务/秒")
        print(f"  成功率: {result.sync_metrics.success_rate:.2%}")
        print(f"  处理数据点: {result.sync_metrics.total_data_points:,}")
        print(f"  缓冲器统计: {result.sync_metrics.buffer_stats}")
        
        print("\n🚀 异步任务指标:")
        print(f"  总耗时: {result.async_metrics.total_time:.2f}秒")
        print(f"  平均任务时间: {result.async_metrics.avg_task_time:.4f}秒")
        print(f"  吞吐量: {result.async_metrics.throughput:.2f}任务/秒")
        print(f"  成功率: {result.async_metrics.success_rate:.2%}")
        print(f"  处理数据点: {result.async_metrics.total_data_points:,}")
        print(f"  缓冲器统计: {result.async_metrics.buffer_stats}")
        
        print("\n📈 性能提升:")
        for metric, improvement in result.performance_improvement.items():
            symbol = "📈" if improvement > 0 else "📉"
            print(f"  {symbol} {metric}: {improvement:+.1f}%")
        
        print("\n" + "=" * 80)


def main():
    """主函数"""
    # 配置测试参数
    config = HueyPerformanceConfig(
        stock_count=30,  # 减少股票数量以便快速测试
        data_points_per_stock=50,
        batch_size=5,
        test_iterations=2,
        worker_count=4    # 使用4个工作线程
    )
    
    runner = HueyPerformanceTestRunner(config)
    
    try:
        result = runner.run_comparison_test()
        runner.print_results(result)
        
        # 保存结果
        filepath = runner.save_results(result)
        print(f"\n结果已保存到: {filepath}")
        
    except KeyboardInterrupt:
        print("\n测试被用户中断")
        runner.stop_consumer()
    except Exception as e:
        print(f"\n测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        runner.stop_consumer()
    finally:
        # 确保消费者被停止
        runner.stop_consumer()


if __name__ == "__main__":
    main()