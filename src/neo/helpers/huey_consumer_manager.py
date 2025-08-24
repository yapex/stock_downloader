"""Huey Consumer管理器

专门负责Huey Consumer的生命周期管理，包括启动、停止、监控等。
"""

import asyncio
import concurrent.futures
import signal
import sys
from typing import Optional

from neo.configs import get_config


class HueyConsumerManager:
    """Huey Consumer管理器
    
    负责Consumer的创建、启动、停止和监控。
    """
    
    # 类变量：保存Consumer实例
    _consumer_instance: Optional["Consumer"] = None

    @classmethod
    def setup_signal_handlers(cls):
        """设置信号处理器"""

        def signal_handler(signum, frame):
            print("\n数据处理器已停止")
            import logging

            logger = logging.getLogger(__name__)
            logger.info("数据处理器收到停止信号，正在优雅关闭...")
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    @classmethod
    def setup_huey_logging(cls):
        """配置 Huey 日志"""
        import logging

        # 配置日志 - 保持简洁
        logging.basicConfig(
            level=logging.WARNING,  # 只显示警告和错误
            format="%(message)s",
        )

        # 设置 Huey 日志级别
        huey_logger = logging.getLogger("huey")
        huey_logger.setLevel(logging.ERROR)

    @classmethod
    def start_consumer(cls) -> "Consumer":
        """启动Consumer并保存到类变量中
        
        Returns:
            Consumer: 启动的Consumer实例
        """
        from huey.consumer import Consumer
        from neo.configs import huey
        
        if cls._consumer_instance is not None:
            return cls._consumer_instance
            
        # 从配置文件读取工作线程数
        config = get_config()
        max_workers = config.huey.max_workers
        
        # 创建Consumer实例
        cls._consumer_instance = Consumer(
            huey,
            workers=max_workers,
            worker_type="thread",
        )
        
        return cls._consumer_instance
    
    @classmethod
    def stop_consumer(cls) -> bool:
        """停止Consumer实例
        
        Returns:
            bool: 是否成功停止
        """
        if cls._consumer_instance is not None:
            try:
                cls._consumer_instance.stop()
                cls._consumer_instance = None
                return True
            except Exception:
                return False
        return True
    
    @classmethod
    def get_consumer_instance(cls) -> Optional["Consumer"]:
        """获取当前Consumer实例
        
        Returns:
            Optional[Consumer]: 当前Consumer实例，如果没有则返回None
        """
        return cls._consumer_instance
    
    @classmethod
    def stop_consumer_if_running(cls) -> bool:
        """如果Consumer正在运行则优雅停止
        
        Returns:
            bool: 是否成功停止或Consumer未运行
        """
        if cls._consumer_instance is not None:
            return cls.stop_consumer()
        return True

    @classmethod
    def run_consumer_standalone(cls):
        """独立运行 Huey 消费者
        
        在主线程中启动多线程 Consumer，适用于独立的消费者进程。
        """
        from huey.consumer import Consumer
        from neo.configs import huey

        def start_consumer():
            """启动 Consumer 的同步函数"""
            try:
                # 从配置文件读取工作线程数
                config = get_config()
                max_workers = config.huey.max_workers

                # 创建 Consumer 实例，配置多线程
                consumer = Consumer(
                    huey,
                    workers=max_workers,  # 从配置文件读取工作线程数
                    worker_type="thread",  # 使用线程而不是进程
                )
                print("数据处理器已启动（多线程模式），按 Ctrl+C 停止...")
                consumer.run()
            except Exception as e:
                print(f"Consumer 运行异常: {e}")
                raise

        def stop_consumer():
            """停止 Consumer 的同步函数"""
            print("正在停止数据处理器...")

        try:
            # 在主线程的 executor 中运行 Consumer
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                # 在 executor 中启动 Consumer
                future = executor.submit(start_consumer)

                try:
                    # 等待 Consumer 完成
                    future.result()
                except KeyboardInterrupt:
                    print("\n收到停止信号，正在优雅关闭...")
                    stop_consumer()
                    future.cancel()

        except KeyboardInterrupt:
            print("\n数据处理器已停止")
        except Exception as e:
            print(f"启动失败: {e}")
            sys.exit(1)
        finally:
            if "loop" in locals():
                loop.close()

    @classmethod
    async def wait_for_all_tasks_completion(cls, max_wait_time: int = 30) -> None:
        """等待所有任务（包括数据处理任务）完成
        
        Args:
            max_wait_time: 最大等待时间（秒）
        """
        from neo.configs import huey
        
        print("⏳ 等待数据处理任务完成...")
        
        # 等待配置的最大时间，同时检查队列和活跃任务
        check_interval = 0.5  # 秒
        elapsed_time = 0
        
        # 首先等待一小段时间，让所有被触发的任务都被正确加入队列
        await asyncio.sleep(1.0)
        
        # 记录上一次的 pending 数量，用于检测稳定状态
        last_pending_count = float('inf')
        stable_count = 0  # 连续稳定的次数
        
        while elapsed_time < max_wait_time:
            # 检查是否还有待处理的任务
            pending_count = huey.pending_count()
            
            # 获取 Consumer 实例，检查正在运行的任务数
            consumer = cls.get_consumer_instance()
            active_tasks = 0
            if consumer and hasattr(consumer, '_pool') and consumer._pool:
                # 计算正在执行的任务数（工作线程池中的活跃线程数）
                try:
                    active_tasks = consumer._pool._threads - len(consumer._pool._idle)
                except AttributeError:
                    # 如果无法获取活跃任务数，假设有任务在运行如果 pending_count > 0
                    active_tasks = 1 if pending_count > 0 else 0
            
            total_tasks = pending_count + active_tasks
            
            # 如果没有任务了，检查是否稳定
            if total_tasks == 0:
                if last_pending_count == 0:
                    stable_count += 1
                    if stable_count >= 3:  # 连续3次检查都是0，认为真正完成
                        print("✅ 所有任务已完成")
                        break
                else:
                    stable_count = 1
            else:
                stable_count = 0
                
            last_pending_count = total_tasks
            
            # 等待一段时间再检查
            await asyncio.sleep(check_interval)
            elapsed_time += check_interval
            
            # 每3秒显示一次进度
            if int(elapsed_time * 2) % 6 == 0:  # 每3秒显示
                if total_tasks > 0:
                    print(f"⏳ 还有 {pending_count} 个等待任务，{active_tasks} 个活跃任务... ({elapsed_time:.1f}s)")
        
        if elapsed_time >= max_wait_time:
            remaining = huey.pending_count()
            print(f"⚠️  超时等待，仍有 {remaining} 个任务未完成，强制停止")

    @classmethod
    async def start_consumer_async(cls) -> asyncio.Task:
        """异步启动 Huey Consumer
        
        Returns:
            asyncio.Task: Consumer运行任务
        """
        def run_consumer_sync():
            """同步运行 consumer"""
            # 使用类方法启动Consumer
            consumer = cls.start_consumer()
            consumer.run()

        # 在 executor 中运行 consumer，避免阻塞主线程
        loop = asyncio.get_event_loop()
        consumer_task = loop.run_in_executor(None, run_consumer_sync)

        # 从配置文件读取工作线程数
        config = get_config()
        max_workers = config.huey.max_workers
        print(f"🚀 Huey Consumer 已启动 ({max_workers}个工作线程)")
        
        # 给 consumer 一点时间启动
        await asyncio.sleep(0.5)
        
        return consumer_task

    @classmethod
    async def stop_consumer_async(cls, consumer_task: Optional[asyncio.Task] = None) -> None:
        """异步停止 Huey Consumer
        
        Args:
            consumer_task: Consumer运行任务，可选
        """
        if consumer_task:
            try:
                consumer_task.cancel()
                await asyncio.sleep(0.1)  # 给一点时间让任务清理
            except asyncio.CancelledError:
                pass
        
        # 停止Consumer实例
        cls.stop_consumer_if_running()
        print("🚫 Huey Consumer 已停止")
