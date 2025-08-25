# Huey 链式任务（Pipeline）+ Asyncio + SQLite 验证示例

本示例是同步版本的扩展，旨在验证 Huey 的链式任务（Pipeline）功能在与 `asyncio` 结合使用时是否能正常工作。

## 目的

本示例将生产者获取结果的方式从同步阻塞 (`result.get(blocking=True)`) 切换为异步非阻塞 (`await aget_result_group(result_group)`)，从而更贴近现代异步应用的真实场景。

通过本示例可以验证 `asyncio` 与 Huey 的集成是否顺畅，为解决主项目中遇到的复杂交互问题提供线索。

## 文件结构

- `config.py`: 配置 `SqliteHuey` 实例。
- `tasks.py`: 定义基础任务（与同步版本相同）。
- `consumer.py`: 任务消费者（与同步版本相同）。
- `producer_async.py`: **异步任务生产者**。它创建并提交任务链，然后使用 `await aget_result_group()` 异步等待并验证最终结果。

## 如何运行

与同步版本类似，你需要打开两个终端窗口。

### 1. 在终端 1: 启动 Consumer

消费者不需要任何改动，它像之前一样运行即可。

```bash
uv run examples/huey_sqlite_pipeline_async/consumer.py
```

### 2. 在终端 2: 运行 Async Producer

运行新的异步生产者脚本。

```bash
uv run examples/huey_sqlite_pipeline_async/producer_async.py
```

### 预期输出

- **Consumer 终端** 的输出与同步版本完全相同，它会按顺序执行任务。
- **Producer 终端** 会显示异步等待的日志，并在几秒钟后，打印出成功获取到最终结果 `30` 的日志。

如果 Producer 成功打印出 `"[结论] Huey 链式任务在 asyncio + SQLite 模式下工作正常。"`，则证明本次验证成功。
