# Huey 简单原型验证

这是一个最简单的 Huey pipeline 原型验证，用于测试任务链式调用功能。

## 文件说明

- `config.py` - Huey 配置
- `tasks.py` - 任务定义（两个简单的模拟任务）
- `consumer.py` - 任务消费者
- `producer.py` - 任务生产者和测试脚本

## 任务说明

### download_task
- 模拟下载任务
- 返回一个包含模拟股票数据的 DataFrame
- 接口与主项目中的 `download_task` 保持一致

### process_data_task
- 模拟数据处理任务
- 接收 DataFrame 并打印处理结果
- 接口与主项目中的 `process_data_task` 保持一致

## 使用方法

### 1. 启动 Consumer

```bash
cd examples/huey_simple_prototype
python consumer.py
```

### 2. 运行测试

在另一个终端中：

```bash
cd examples/huey_simple_prototype
python producer.py
```

## 预期结果

如果 pipeline 工作正常，你应该看到：

1. Consumer 启动并等待任务
2. Producer 提交 pipeline 任务
3. 第一个任务（download_task）执行并返回模拟数据
4. 第二个任务（process_data_task）接收数据并打印处理结果
5. Pipeline 完成

## 调试信息

每个任务都会打印详细的调试信息，包括：
- 任务开始执行
- 接收到的参数
- 数据内容和形状
- 处理结果
- 任务完成状态