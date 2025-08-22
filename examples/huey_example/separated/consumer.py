#!/usr/bin/env python3
"""Consumer - 任务消费者"""

from config import huey
from tasks import add, slow_task  # 导入任务定义


def main():
    print("=== Huey Consumer 启动 ===")
    print("等待任务...")
    print("按 Ctrl+C 停止 Consumer")
    
    try:
        # 使用 Consumer 类启动消费者
        from huey.consumer import Consumer
        consumer = Consumer(huey)
        consumer.run()
    except KeyboardInterrupt:
        print("\n=== Consumer 停止 ===")
    except Exception as e:
        print(f"Consumer 运行出错: {e}")


if __name__ == '__main__':
    main()