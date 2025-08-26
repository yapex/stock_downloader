#!/bin/bash
# 该脚本用于在后台启动两个独立的消费者进程

# 清理之前的日志和数据库文件，确保从干净的状态开始
rm -f examples/huey_multi_instance_prototype/consumer_fast.log examples/huey_multi_instance_prototype/consumer_slow.log /tmp/prototype_fast.db /tmp/prototype_slow.db

echo "🚀 开始启动消费者进程..."
echo "查看 examples/huey_multi_instance_prototype/consumer_*.log 来监控输出。"

# 启动“快速”消费者，使用50个线程，并将其输出重定向到日志文件
# 使用 uv run 并设置 PYTHONPATH=. 来保证 huey_consumer 能正确找到模块
# 注意：这里的 `config.huey_fast` 是指向我们定义的Huey实例的Python路径
uv run -- env PYTHONPATH=. huey_consumer examples.huey_multi_instance_prototype.main.huey_fast -w 50 -k thread > examples/huey_multi_instance_prototype/consumer_fast.log 2>&1 &
FAST_PID=$!

# 等待1秒，以避免在启动时因同时初始化数据库而产生的锁竞争
sleep 1

# 启动“慢速”消费者，使用4个线程
uv run -- env PYTHONPATH=. huey_consumer examples.huey_multi_instance_prototype.main.huey_slow -w 4 -k thread > examples/huey_multi_instance_prototype/consumer_slow.log 2>&1 &
SLOW_PID=$!

echo "✅ 快速消费者已启动, PID: $FAST_PID"
echo "✅ 慢速消费者已启动, PID: $SLOW_PID"
echo ""
echo "👉 使用 'kill $FAST_PID $SLOW_PID' 命令来停止两个消费者进程。"
