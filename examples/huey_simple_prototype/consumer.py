"""任务消费者 - 处理队列中的任务"""

from config import huey

# 必须导入 tasks 模块，这样 Consumer 才能识别任务
import tasks


def main():
    print("=== Huey 简单原型 Consumer 启动 ===")
    print("等待任务... (按 Ctrl+C 停止)")
    
    try:
        from huey.consumer import Consumer
        consumer = Consumer(huey)
        consumer.run()
    except KeyboardInterrupt:
        print("\n=== Consumer 停止 ===")
    except Exception as e:
        print(f"!!! Consumer 运行出错: {e} !!!")


if __name__ == "__main__":
    main()