# Huey 多实例原型

本原型旨在验证通过使用独立的Huey实例和专用的消费者进程，可以有效隔离快速任务（如网络I/O）和慢速任务（如数据库写入），从而防止慢速任务阻塞快速任务的执行，提升系统整体吞吐量。

## 文件结构

- `config.py`: 定义两个独立的Huey实例 (`huey_fast`, `huey_slow`)。
- `tasks.py`: 定义 `download_task` (快速) 和 `process_data_task` (慢速)。
- `producer.py`: 任务生产者，用于向 `huey_fast` 队列提交大量任务。
- `run_consumers.sh`: 用于一键启动两个专用消费者进程的脚本。
- `consumer_fast.log`: 快速消费者的日志输出。
- `consumer_slow.log`: 慢速消费者的日志输出。

## 如何运行

1.  **赋予脚本执行权限**

    首先，需要给 `run_consumers.sh` 添加执行权限：
    ```bash
    chmod +x examples/huey_multi_instance_prototype/run_consumers.sh
    ```

2.  **启动消费者进程**

    在项目根目录下运行脚本来启动两个后台消费者进程：
    ```bash
    ./examples/huey_multi_instance_prototype/run_consumers.sh
    ```

3.  **运行生产者**

    打开**另一个**终端窗口，在项目根目录下运行生产者，它会提交1200个任务：
    ```bash
    uv run python -m examples.huey_multi_instance_prototype.producer
    ```

## 如何观察结果

你可以通过 `tail` 命令实时监控两个消费者的日志：

- **监控快速消费者**:
  ```bash
  tail -f examples/huey_multi_instance_prototype/consumer_fast.log
  ```

- **监控慢速消费者**:
  ```bash
  tail -f examples/huey_multi_instance_prototype/consumer_slow.log
  ```

## 预期结果

1.  **快速消费者的日志 (`consumer_fast.log`)**:
    - 你会看到 `[FAST_WORKER] ...` 的日志以极快的速度滚动，几乎没有停顿。
    - 所有1200个下载任务应该在**大约1分钟**左右全部完成（1200 * 0.05秒 = 60秒）。

2.  **慢速消费者的日志 (`consumer_slow.log`)**:
    - 你会看到 `[SLOW_WORKER] ...` 的日志以较慢的速度输出，每2秒才完成一个任务。

**核心验证点**: 快速消费者的完成速度**完全不受**慢速消费者的影响。这证明了通过独立的队列和worker池，我们成功地将瓶颈隔离，使得下载能力可以达到其理论上限。
