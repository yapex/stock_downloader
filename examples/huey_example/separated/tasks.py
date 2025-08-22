from config import huey
import time


@huey.task()
def add(a, b):
    """简单的加法任务"""
    print(f"执行任务: add({a}, {b})")
    return a + b


@huey.task()
def slow_task(duration):
    """模拟耗时任务"""
    print(f"开始执行耗时任务，将等待 {duration} 秒")
    time.sleep(duration)
    print(f"耗时任务完成")
    return f"任务完成，耗时 {duration} 秒"