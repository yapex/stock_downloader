import asyncio
from collections import defaultdict


class AsyncBuffer:
    """
    一个基于 asyncio 的天然无锁数据缓存池。
    所有操作都在同一个事件循环中调度，避免了并发问题。
    """

    def __init__(self, flush_interval_seconds=10.0):
        self._buffers = defaultdict(list)
        self._callbacks = {}
        self._max_sizes = {}
        self._flush_interval = flush_interval_seconds
        # 创建一个后台任务来处理定时刷新
        self._timer_task = asyncio.create_task(self._timed_flush_loop())

    def register_type(self, data_type: str, callback: callable, max_size: int = 100):
        self._callbacks[data_type] = callback
        self._max_sizes[data_type] = max_size
        print(f"Async: 已注册类型 '{data_type}', 大小: {max_size}")

    async def add(self, data_type: str, item: any):
        if data_type not in self._callbacks:
            return

        self._buffers[data_type].append(item)

        if len(self._buffers[data_type]) >= self._max_sizes[data_type]:
            # 使用 create_task 使得刷新操作不会阻塞当前的 add 调用
            asyncio.create_task(self.flush(data_type))

    async def _flush_buffer(self, data_type: str):
        items_to_process = self._buffers.pop(data_type, [])
        if not items_to_process:
            return

        print(f"--- Async 刷新: 类型='{data_type}', 数量={len(items_to_process)} ---")
        try:
            # 如果回调是协程函数，就 await 它
            if asyncio.iscoroutinefunction(self._callbacks[data_type]):
                await self._callbacks[data_type](data_type, items_to_process)
            else:  # 否则在事件循环的线程池中运行它，避免阻塞
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None, self._callbacks[data_type], data_type, items_to_process
                )
        except Exception as e:
            print(f"错误: 执行回调 '{data_type}' 时异常: {e}")

    async def flush(self, data_type: str = None):
        if data_type:
            await self._flush_buffer(data_type)
        else:
            # 创建一组刷新任务并等待它们全部完成
            flush_tasks = [self._flush_buffer(dt) for dt in list(self._buffers.keys())]
            await asyncio.gather(*flush_tasks)

    async def _timed_flush_loop(self):
        while True:
            await asyncio.sleep(self._flush_interval)
            # print("\n*** Async: 定时器触发 ***")
            await self.flush()

    async def shutdown(self):
        print("Async: 正在关闭...")
        self._timer_task.cancel()
        await self.flush()
        print("Async: 关闭完成。")


# --- 示例用法 (Asyncio) ---
async def main():
    async def async_process_data_batch(data_type, items):
        print(f"[Async 回调] 正在处理 '{data_type}' 的 {len(items)} 个项目...")
        await asyncio.sleep(0.01)  # 模拟异步 I/O

    buffer = AsyncBuffer(flush_interval_seconds=5)
    buffer.register_type("logs", async_process_data_batch, max_size=5)
    buffer.register_type("metrics", async_process_data_batch, max_size=3)

    async def async_producer(data_type, count):
        for i in range(count):
            await buffer.add(data_type, f"Async {data_type} item #{i}")
            await asyncio.sleep(0.05)

    print("\n--- 启动异步生产者任务 ---\n")
    # 并发运行两个生产者
    await asyncio.gather(async_producer("logs", 8), async_producer("metrics", 7))

    print("\n--- 生产者完成，关闭缓存池 ---\n")
    await buffer.shutdown()


if __name__ == "__main__":
    # 切换到运行方案一的示例
    # ... (方案一的代码和 if __name__ 块) ...

    # 运行方案二的示例
    print("\n\n--- 运行 asyncio 示例 ---")
    asyncio.run(main())
