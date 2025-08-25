import time
import logging
from huey import MemoryHuey

# --- 初始化 ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
huey = MemoryHuey(immediate=True)


# --- 任务定义 (修改后) ---


@huey.task()
def download_task(task_type: str, symbol: str) -> dict:
    """
    模拟的下载任务。
    返回的字典的键名现在与 process_data_task 的参数名对齐。
    """
    print(
        f"\n[任务 1: download_task] ==> 开始执行: 类型='{task_type}', 股票代码='{symbol}'"
    )
    print(f"[任务 1: download_task]     正在模拟网络下载...")
    time.sleep(1)

    downloaded_data = {
        "symbol": symbol,
        "task_type": task_type,  # <-- 修改点 1: 将 'type' 改为 'task_type'
        "payload": f"这是 {symbol} 的原始数据",
        "download_timestamp": time.time(),
    }

    print(f"[任务 1: download_task] ==> 执行完毕. 返回数据: {downloaded_data}")
    return downloaded_data


@huey.task()
def process_data_task(symbol: str, task_type: str, payload: str, **kwargs) -> bool:
    """
    模拟的数据处理任务 (修改后)。
    - 函数签名现在直接对应上一个任务返回的字典的键。
    - 使用 **kwargs 来优雅地接收我们不直接使用的额外字段 (如 download_timestamp)。
    """
    print(
        f"\n[任务 2: process_data_task] ==> 开始执行: 类型='{task_type}', 股票代码='{symbol}'"
    )
    print(f"[任务 2: process_data_task]     自动解包收到的参数: payload='{payload}'")
    print(f"[任务 2: process_data_task]     其他未使用的参数: {kwargs}")
    print(f"[任务 2: process_data_task]     正在模拟数据处理...")
    time.sleep(1)

    if payload:
        print("[任务 2: process_data_task] ==> 执行完毕. 数据处理成功。")
        return True
    else:
        print("[任务 2: process_data_task] ==> 执行完毕. 数据处理失败。")
        return False


# --- 主程序入口 (修改后) ---

if __name__ == "__main__":
    print("--- Huey 任务流水线演示 (修复后) ---")

    task_type_param = "历史股价"
    symbol_param = "AAPL"

    print(f"\n第 1 步: 准备为 '{symbol_param}' 构建任务流水线...")

    download_signature = download_task.s(task_type_param, symbol_param)

    # <-- 修改点 2: .then() 现在非常简洁，不再需要重复传递参数。
    # Huey 会自动将 download_task 返回的字典解包，并匹配给 process_data_task 的参数。
    pipeline = download_signature.then(process_data_task)

    print("第 2 步: 流水线构建完成。结构如下:")
    print(
        f"         1. download_task(task_type='{task_type_param}', symbol='{symbol_param}')"
    )
    print(f"         2. process_data_task( **(上一步的结果) )")  # ** 代表字典解包

    print("\n第 3 步: 将整个流水线作为一个任务入队执行...")
    final_result_handle = huey.enqueue(pipeline)

    print("\n--- 任务开始执行 (因为 immediate=True) ---")

    final_status = final_result_handle.get(blocking=True)

    print("\n--- 流水线执行完毕 ---")
    print(f"\n第 4 步: 收到流水线的最终结果 (来自 process_data_task): {final_status}")
