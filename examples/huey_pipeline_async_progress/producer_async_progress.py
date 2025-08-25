"任务生产者 - 创建并提交一个任务链 (异步版本，带进度条模拟)"

import asyncio
from config import huey
from tasks import add, multiply, tprint
from huey.contrib.asyncio import aget_result_group


async def progress_reporter():
    """模拟进度条，每隔一段时间打印一次"""
    counter = 0
    while True:
        tprint(f"  ... 模拟进度条: {'.' * (counter % 4 + 1)}")
        counter += 1
        await asyncio.sleep(0.5)  # 模拟进度条刷新间隔


async def main():
    tprint("=== Huey Async Pipeline Producer (带进度条模拟) 启动 ===")

    # 1. 定义一个任务链 (pipeline)
    # 预期计算流程: add(5, 10) -> 15, 然后 multiply(15, 2) -> 30
    # 每个任务内部有 time.sleep(1)，所以总耗时至少 2 秒
    tprint("1. 构建任务链: add(5, 10).then(multiply, 2)")
    pipeline = add.s(5, 10).then(multiply, 2)

    # 2. 提交任务链到队列
    result_group = huey.enqueue(pipeline)
    tprint("2. 任务链已提交。")

    # 3. 异步等待最终结果，同时启动进度条模拟
    tprint("3. 异步等待结果，同时模拟进度条...")

    # 创建并启动进度条任务
    reporter_task = asyncio.create_task(progress_reporter())

    try:
        # 等待主任务链的结果
        # 设置一个合理的超时时间，确保任务有足够时间完成
        all_results = await asyncio.wait_for(
            aget_result_group(result_group), timeout=10
        )

        # 任务完成后，取消进度条任务
        reporter_task.cancel()
        try:
            await reporter_task  # 等待任务真正取消
        except asyncio.CancelledError:
            tprint("  ... 进度条任务已取消。")

        tprint("\n=== 结果验证 ===")
        tprint(f"任务链执行成功! 所有结果: {all_results}")

        # 链条的最终结果是列表中的最后一个值
        final_value = all_results[-1]
        tprint(f"链条的最终结果: {final_value}")

        assert final_value == 30
        tprint("最终结果值 (30) 正确!")
        tprint(
            "\n[结论] Huey 链式任务在 asyncio + SQLite 模式下，与模拟进度条可以正常并发工作。"
        )

    except asyncio.TimeoutError:
        reporter_task.cancel()
        try:
            await reporter_task
        except asyncio.CancelledError:
            pass
        tprint("\n!!! 测试失败 !!!")
        tprint("错误类型: asyncio.TimeoutError")
        tprint(
            "错误详情: 等待任务结果超时。请检查 Consumer 是否已启动并能正确处理任务。"
        )
    except Exception as e:
        reporter_task.cancel()
        try:
            await reporter_task
        except asyncio.CancelledError:
            pass
        tprint("\n!!! 测试失败 !!!")
        tprint(f"错误类型: {type(e).__name__}")
        tprint(f"错误详情: {e}")
        tprint("请检查代码逻辑或 Consumer 的状态。")

    tprint("\n=== Producer 完成 ===")


if __name__ == "__main__":
    asyncio.run(main())
