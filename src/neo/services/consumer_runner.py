"""
消费者运行器服务
"""
import sys
from huey.consumer import Consumer

from ..configs import get_config

class ConsumerRunner:
    """数据处理器运行工具类"""

    def run(self, queue_name: str) -> None:
        """独立运行 Huey 消费者

        在主线程中启动多线程 Consumer，适用于独立的消费者进程。
        """
        # 根据名字动态选择要启动的huey实例
        if queue_name == 'fast':
            from ..configs.huey_config import huey_fast as huey
            max_workers = get_config().huey_fast.max_workers
            print(f"🚀 正在启动快速队列消费者 (fast_queue) ，配置 {max_workers} 个 workers...")
        elif queue_name == 'slow':
            from ..configs.huey_config import huey_slow as huey
            max_workers = get_config().huey_slow.max_workers
            print(f"🐌 正在启动慢速队列消费者 (slow_queue)，配置 {max_workers} 个 workers...")
        elif queue_name == 'maint':
            from ..configs.huey_config import huey_maint as huey
            max_workers = get_config().huey_maint.max_workers
            print(f"🛠️ 正在启动维护队列消费者 (maint_queue)，配置 {max_workers} 个 workers...")
        else:
            print(f"❌ 错误：无效的队列名称 '{queue_name}'。请使用 'fast', 'slow', 或 'maint'。", file=sys.stderr)
            sys.exit(1)


        # 重要：导入任务模块，让 Consumer 能够识别和执行任务
        import neo.tasks.huey_tasks  # noqa: F401

        try:
            # 创建 Consumer 实例，配置多线程
            consumer = Consumer(
                huey,
                workers=max_workers,
                worker_type="thread",
            )
            print("数据处理器已启动，按 Ctrl+C 停止...")
            consumer.run()
        except KeyboardInterrupt:
            print(f"\n数据处理器 ({queue_name}) 已停止")
        except Exception as e:
            print(f"Consumer ({queue_name}) 运行异常: {e}")
            sys.exit(1)
