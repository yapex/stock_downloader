# Huey 分离式 Producer-Consumer 示例

这个示例展示了如何将 Huey 的 Producer 和 Consumer 分离到不同的进程中运行。

## 文件结构

- `config.py` - Huey 实例配置（使用 SQLite 模式支持跨进程）
- `tasks.py` - 任务定义
- `producer.py` - 任务生产者（提交任务）
- `consumer.py` - 任务消费者（执行任务）

## 运行方式

### 1. 启动 Consumer（在终端1中）
```bash
cd examples/huey_example/separated
uv run python consumer.py
```

### 2. 运行 Producer（在终端2中）
```bash
cd examples/huey_example/separated
uv run python producer.py
```

## 注意事项

- 使用 `SqliteHuey`，支持跨进程任务队列，Producer 和 Consumer 可以在不同进程中运行
- 如果需要跨机器部署，请修改 `config.py` 使用 `RedisHuey`
- Consumer 需要先启动，否则 Producer 提交的任务会排队等待
- SQLite 数据库文件位于系统临时目录，任务会持久化直到被处理