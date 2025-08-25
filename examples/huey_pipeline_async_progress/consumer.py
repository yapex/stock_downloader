"""任务消费者 - 执行队列中的任务"""

from config import huey
from tasks import tprint

# 非常重要：必须导入 tasks 模块，
# 这样 Huey Consumer 才能在当前进程中注册和识别 @huey.task() 装饰的任务。
# 如果不导入，Consumer 会因为找不到任务定义而反序列化失败。
import tasks

def main():
    tprint("=== Huey Pipeline Consumer 启动 ===")
    tprint("等待任务... (按 Ctrl+C 停止)")

    try:
        from huey.consumer import Consumer
        consumer = Consumer(huey)
        consumer.run()  # 启动消费者，开始处理队列中的任务
    except KeyboardInterrupt:
        print("\n=== Consumer 停止 ===")
    except Exception as e:
        tprint(f"!!! Consumer 运行出错: {e} !!!")


if __name__ == "__main__":
    main()

