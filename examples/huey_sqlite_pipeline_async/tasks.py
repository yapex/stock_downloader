"""任务定义模块"""

from config import huey
import time

def tprint(s):
    """带时间戳的打印函数，方便观察任务执行"""
    print(f"[{time.strftime('%H:%M:%S')}] {s}")


@huey.task()
def add(a, b):
    tprint(f"  -> [Executing add] {a} + {b}")
    return a + b


@huey.task()
def multiply(a, b):
    tprint(f"  -> [Executing multiply] {a} * {b}")
    return a * b
