"""任务定义模块"""

from config import huey
import time


def tprint(s):
    """带时间戳的打印函数，方便观察任务执行"""
    print(f"[{time.strftime('%H:%M:%S')}] {s}")


@huey.task()
def add(a, b):
    # 模拟耗时，让进度条有时间显示
    time.sleep(1)
    tprint(f"  -> [Executing add] {a} + {b}")
    result = a + b
    tprint(f"  -> [add result] {result}")
    # 返回元组：(操作类型, 结果值)
    return (result, "add")


@huey.task()
def multiply(b, actual_value, operation_type):
    # 模拟耗时，让进度条有时间显示
    time.sleep(1)

    # 根据观察，参数传递是: (2, 'add', 15)
    # 即: (.then 中的参数, 前一个任务返回元组的第一个元素, 前一个任务返回元组的第二个元素)
    tprint(
        f"  -> [Executing multiply] 接收到 {operation_type} 结果: {actual_value}, 乘以 {b}"
    )
    result = actual_value * b

    tprint(f"  -> [multiply result] {result}")
    return result
