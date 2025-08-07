# 队列与消息模型实现说明

## 概述

本项目实现了基于 `queue.Queue` 的两条管道队列管理系统，包含 `DownloadTask` 和 `DataBatch` 数据类，支持优先级调度和智能重试机制。

## 核心组件

### 1. 数据模型 (models.py)

#### DownloadTask
下载任务数据类，支持以下功能：
- **symbol**: 股票代码或其他标识符
- **task_type**: 任务类型 (DAILY, DAILY_BASIC, FINANCIALS, STOCK_LIST)
- **params**: 任务参数字典
- **priority**: 优先级 (HIGH=10, NORMAL=5, LOW=1)
- **retry_count**: 当前重试次数
- **max_retries**: 最大重试次数 (默认3次)
- **task_id**: 唯一标识符 (自动生成UUID)
- **created_at**: 创建时间

主要方法：
```python
# 检查是否可以重试
task.can_retry() -> bool

# 增加重试次数并返回新实例
task.increment_retry() -> DownloadTask

# 序列化/反序列化
task.to_dict() -> dict
DownloadTask.from_dict(data) -> DownloadTask
```

#### DataBatch
数据批次数据类，支持以下功能：
- **df**: pandas DataFrame 包含的数据
- **meta**: 元数据信息字典
- **task_id**: 关联的任务ID
- **symbol**: 关联的股票代码
- **batch_id**: 唯一标识符 (自动生成UUID)
- **created_at**: 创建时间

主要方法：
```python
# 数据大小和状态
batch.size -> int
batch.is_empty -> bool
batch.columns -> list

# 数据转换
batch.to_records() -> list
DataBatch.from_records(records, meta, task_id, symbol) -> DataBatch
DataBatch.empty(task_id, symbol, meta) -> DataBatch

# 序列化 (不包含DataFrame)
batch.to_dict() -> dict
```

### 2. 增强队列管理器 (enhanced_queue_manager.py)

#### EnhancedQueueManager
基于 `queue.Queue` 实现的队列管理器，提供：

**两条管道队列**：
- 任务队列：`queue.PriorityQueue` 支持优先级调度
- 数据队列：`queue.Queue` 管理数据批次
- 重试队列：`queue.PriorityQueue` 延迟重试任务

**优先级支持**：
- HIGH 优先级任务优先执行
- 按优先级值排序 (HIGH=10, NORMAL=5, LOW=1)
- 相同优先级按时间戳排序

**智能重试机制**：
- 根据优先级设置不同重试延迟
  - HIGH: [1s, 2s, 4s]
  - NORMAL: [2s, 5s, 10s]
  - LOW: [5s, 15s, 30s]
- 最大重试次数限制
- 指数退避策略

**主要API**：
```python
manager = EnhancedQueueManager(
    task_queue_size=1000,
    data_queue_size=5000,
    queue_timeout=30.0,
    enable_smart_retry=True
)

# 生命周期管理
manager.start()
manager.stop()

# 任务队列操作
manager.put_task(task) -> bool
manager.get_task(timeout) -> Optional[DownloadTask]
manager.task_done()

# 数据队列操作
manager.put_data(data_batch) -> bool
manager.get_data(timeout) -> Optional[DataBatch]
manager.data_done()

# 重试和统计
manager.schedule_retry(task, error) -> bool
manager.report_task_completed(task)
manager.report_task_failed(task, error)
manager.metrics -> dict
```

**性能指标**：
- 任务提交/完成/失败数
- 重试次数统计
- 数据批次处理数
- 队列利用率
- 成功率计算
- 运行时长

## 使用示例

### 基本使用
```python
from src.downloader.models import DownloadTask, DataBatch, TaskType, Priority
from src.downloader.enhanced_queue_manager import EnhancedQueueManager

# 创建队列管理器
manager = EnhancedQueueManager()
manager.start()

# 创建任务
task = DownloadTask(
    symbol="000001.SZ",
    task_type=TaskType.DAILY,
    params={"start_date": "2024-01-01"},
    priority=Priority.HIGH
)

# 添加任务到队列
manager.put_task(task)

# 获取并处理任务
task = manager.get_task()
if task:
    # 模拟数据获取
    df = get_stock_data(task.symbol, task.params)
    
    # 创建数据批次
    batch = DataBatch(
        df=df,
        meta={"source": "tushare"},
        task_id=task.task_id,
        symbol=task.symbol
    )
    
    # 添加到数据队列
    manager.put_data(batch)
    manager.task_done()

# 处理数据
data_batch = manager.get_data()
if data_batch:
    # 存储数据
    save_to_database(data_batch.df)
    manager.data_done()

manager.stop()
```

### 重试处理
```python
try:
    # 处理任务
    process_task(task)
    manager.report_task_completed(task)
except Exception as e:
    # 自动重试
    manager.report_task_failed(task, str(e))
```

### 优先级调度
```python
# 不同优先级的任务
high_priority_task = DownloadTask("000001.SZ", TaskType.DAILY, {}, Priority.HIGH)
normal_task = DownloadTask("000002.SZ", TaskType.DAILY, {}, Priority.NORMAL)
low_priority_task = DownloadTask("000003.SZ", TaskType.DAILY, {}, Priority.LOW)

# 按顺序添加
manager.put_task(low_priority_task)
manager.put_task(normal_task)
manager.put_task(high_priority_task)

# 按优先级获取：HIGH -> NORMAL -> LOW
task1 = manager.get_task()  # high_priority_task
task2 = manager.get_task()  # normal_task
task3 = manager.get_task()  # low_priority_task
```

## 特性优势

1. **简单可靠**：基于标准库 `queue.Queue`，线程安全
2. **优先级调度**：支持任务优先级，重要任务优先处理
3. **智能重试**：指数退避 + 优先级相关延迟
4. **监控指标**：完整的性能和状态监控
5. **灵活配置**：队列大小、超时、重试策略可配置
6. **类型安全**：完整的类型注解和数据验证
7. **易于测试**：全面的单元测试覆盖

## 测试覆盖

- ✅ DownloadTask 数据类测试
- ✅ DataBatch 数据类测试
- ✅ 队列管理器生命周期测试
- ✅ 优先级调度测试
- ✅ 重试机制测试
- ✅ 队列满载处理测试
- ✅ 超时处理测试
- ✅ 并发访问测试
- ✅ 边界条件和异常测试
- ✅ 指标统计测试

## 文件结构

```
src/downloader/
├── models.py                    # 数据模型定义
├── enhanced_queue_manager.py    # 增强队列管理器
└── interfaces.py               # 原有接口（保持兼容）

tests/
└── test_enhanced_queue_manager.py  # 完整测试套件

examples/
└── queue_manager_example.py       # 使用示例

docs/
└── queue_implementation.md        # 本说明文档
```

## 向后兼容

为保持向后兼容，在 `models.py` 中提供了别名：
```python
TaskMessage = DownloadTask
DataMessage = DataBatch
```

现有代码可以无缝迁移到新的实现。
