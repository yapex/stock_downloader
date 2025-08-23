"""使用标准 asyncio 的 Huey 配置"""

from huey import MemoryHuey

# 使用 MemoryHuey 实现真正的并发执行，不使用 gevent
# MemoryHuey 支持多线程并发，可以实现真正的并行处理
# 注意：MemoryHuey 的并发由 Consumer 的 workers 参数控制
huey = MemoryHuey("asyncio_prototype", blocking=True)
