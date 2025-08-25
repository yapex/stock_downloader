"任务生产者 - 创建并提交一个任务链 (异步版本)"

import asyncio
from config import huey
from tasks import add, multiply, tprint
from huey.contrib.asyncio import aget_result_group


async def main():
    tprint("=== Huey Async Pipeline Producer 启动 ===")

    # 1. 定义一个任务链 (pipeline)
    tprint("1. 构建任务链: add(5, 10).then(multiply, 2)")
    pipeline = add.s(5, 10).then(multiply, 2)

    # 2. 提交任务链到队列
    result_group = huey.enqueue(pipeline)
    tprint("2. 任务链已提交。")

    # 3. 异步等待最终结果
    tprint("3. 异步等待最终结果...")
    try:
        # 使用 aget_result_group 异步等待结果
        all_results = await aget_result_group(result_group)
        tprint("\n=== 结果验证 ===")
        tprint(f"任务链执行成功! 中间结果列表: {all_results}")

        # 链条的最终结果是列表中的最后一个值
        final_value = all_results[-1]
        tprint(f"链条的最终结果: {final_value}")

        assert final_value == 30
        tprint("最终结果值 (30) 正确!")
        tprint("\n[结论] Huey 链式任务在 asyncio + SQLite 模式下工作正常。")

    except Exception as e:
        tprint("\n!!! 测试失败 !!!")
        tprint(f"错误类型: {type(e).__name__}")
        tprint(f"错误详情: {e}")
        tprint("请检查代码逻辑或 Consumer 的状态。")

    tprint("\n=== Producer 完成 ===")


if __name__ == "__main__":
    asyncio.run(main())
