# Consumer 架构设计方案

## 概述

Consumer 部分负责从 Huey 任务队列中读取数据处理结果，按类型分组后批量保存到 DuckDB。设计采用多线程模式提高效率，参考现有的 Producer 协同模式。

## 架构组件

### 1. ConsumerManager（消费管理器）

**职责：**

- 整体消费流程的协调和管理
- 线程池管理和任务分发
- 监控和统计信息收集
- 优雅关闭处理
- 配置加载

**核心功能：**

- 启动和停止消费进程
- 管理多个 DataProcessor 实例
- 处理系统信号和异常情况
- 提供消费状态监控接口

### 2. DataProcessor（数据处理器）

**职责：**

- 从 Huey 队列中获取 TaskResult
- 数据验证和转换
- 按任务类型分组数据
- 调用 BatchSaver 进行批量保存

**核心功能：**

- 持续监听队列中的任务结果
- 数据格式验证和清洗
- 实现数据分组和缓冲机制
- 错误处理和重试逻辑

### 3. BatchSaver（批量保存器）

**职责：**

- 接收分组后的数据
- 执行批量数据库操作
- 处理数据库连接和事务
- 优化批量插入性能

**核心功能：**

- 利用现有的 DBOperator.upsert 方法
- 实现批量数据的事务处理
- 处理数据库异常和重试
- 提供保存状态反馈

## 技术实现要点

### 任务分组策略

```python
# 重用 Producer 中的 TaskType 定义
from src.downloader.producer.fetcher_builder import TaskType

# 按任务类型分组数据
data_groups = {
    task_type: [] for task_type in TaskType
}

# 或者显式指定需要处理的任务类型
data_groups = {
    TaskType.STOCK_BASIC: [],
    TaskType.STOCK_DAILY: [],
    TaskType.DAILY_BAR_QFQ: [],
    TaskType.DAILY_BAR_NONE: [],
    TaskType.BALANCESHEET: [],
    TaskType.INCOME: [],
    TaskType.CASHFLOW: [],
}
```

### 多线程处理

- 使用 ThreadPoolExecutor 管理消费线程
- 每个线程运行一个 DataProcessor 实例
- 线程间通过队列进行数据传递
- 实现线程安全的数据分组和批量处理

### 批量处理优化

- 设置合适的批量大小（如 1000 条记录）
- 实现超时机制，避免数据积压
- 支持动态调整批量大小
- 优化内存使用，避免数据堆积

## 接口设计

### IConsumerManager

```python
from typing import Protocol
from abc import abstractmethod

class IConsumerManager(Protocol):
    @abstractmethod
    def start(self) -> None:
        """启动消费进程"""
        pass

    @abstractmethod
    def stop(self) -> None:
        """停止消费进程"""
        pass

    @abstractmethod
    def get_status(self) -> dict:
        """获取消费状态"""
        pass
```

### IDataProcessor

```python
class IDataProcessor(Protocol):
    @abstractmethod
    def process_task_result(self, task_result: TaskResult) -> None:
        """处理单个任务结果"""
        pass

    @abstractmethod
    def flush_pending_data(self) -> None:
        """刷新待处理数据"""
        pass
```

### IBatchSaver

```python
class IBatchSaver(Protocol):
    @abstractmethod
    def save_batch(self, task_type: TaskType, data_batch: List[dict]) -> bool:
        """批量保存数据"""
        pass
```

## 配置与监控

### 配置参数

```python
@dataclass
class ConsumerConfig:
    worker_threads: int = 4
    batch_size: int = 1000
    batch_timeout: int = 30  # 秒
    max_retries: int = 3
    retry_delay: int = 5  # 秒
    queue_check_interval: float = 0.1  # 秒
```

### 监控指标

- 消费速率（条/秒）
- 队列积压情况
- 批量保存成功率
- 错误统计和分类
- 线程状态监控

## 错误处理与重试

### 错误分类

1. **数据格式错误**：记录日志，丢弃数据
2. **数据库连接错误**：重试机制，指数退避
3. **数据约束错误**：记录详细信息，人工处理
4. **系统资源错误**：暂停处理，等待恢复

### 重试策略

- 实现指数退避算法
- 设置最大重试次数
- 区分可重试和不可重试错误
- 提供死信队列机制

## 与现有系统集成

### 与 Huey 集成

- 复用现有的 SqliteHuey 实例
- 监听 `process_fetched_data` 任务的结果
- 处理任务执行状态和异常情况

### 与数据库层集成

- 使用现有的 DBOperator 类
- 复用数据库连接池
- 利用现有的表结构和索引

### 与配置系统集成

- 扩展现有的配置文件
- 支持运行时配置更新
- 提供配置验证机制

## 部署和运维

### 启动方式

```python
# 独立进程启动
python -m src.downloader.consumer.main

# 或集成到主应用
from src.downloader.consumer import ConsumerManager
consumer = ConsumerManager(config)
consumer.start()
```

### 监控和日志

- 结构化日志输出
- 性能指标收集
- 健康检查接口
- 告警机制集成

## 性能优化建议

1. **批量大小调优**：根据数据类型和系统负载动态调整
2. **连接池优化**：合理设置数据库连接池大小
3. **内存管理**：及时释放处理完的数据，避免内存泄漏
4. **并发控制**：平衡线程数量和系统资源消耗
5. **数据预处理**：在保存前进行数据去重和验证

## 测试策略

### 单元测试

- 各组件的独立功能测试
- Mock 外部依赖（数据库、队列）
- 边界条件和异常情况测试

### 集成测试

- 端到端数据流测试
- 多线程并发测试
- 数据库事务测试

### 性能测试

- 高并发场景测试
- 大批量数据处理测试
- 内存和 CPU 使用率监控

## 扩展性考虑

1. **水平扩展**：支持多实例部署
2. **插件机制**：支持自定义数据处理器
3. **配置热更新**：运行时调整处理参数
4. **监控集成**：支持 Prometheus、Grafana 等监控系统

这个架构设计充分考虑了系统的可扩展性、可维护性和性能要求，同时与现有的 Producer 架构保持一致的设计理念。
