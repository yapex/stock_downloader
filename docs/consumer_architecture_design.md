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

# 或者显式指定需要处理的任务类型（基于 stock_schema.toml 中的表定义）
data_groups = {
    TaskType.STOCK_BASIC: [],        # 股票基本信息
    TaskType.STOCK_DAILY: [],        # 股票日线数据
    TaskType.STOCK_ADJ_QFQ: [],      # 复权行情数据
    TaskType.DAILY_BASIC: [],        # 每日基本面指标
    TaskType.INCOME_STATEMENT: [],   # 利润表
    TaskType.BALANCE_SHEET: [],      # 资产负债表
    TaskType.CASH_FLOW: [],          # 现金流量表
}
```

### 多线程处理

- 使用 ThreadPoolExecutor 管理消费线程
- 每个线程运行一个 DataProcessor 实例
- 线程间通过队列进行数据传递
- 实现线程安全的数据分组和批量处理

### 批量处理优化

- 设置合适的批量大小（如 100 条记录）, 从配置文件中读取

## 接口设计

### IConsumerManager

```python
from typing import Protocol

class IConsumerManager(Protocol):
    def start(self) -> None:
        """启动消费进程"""
        pass

    def stop(self) -> None:
        """停止消费进程"""
        pass

    def get_status(self) -> dict:
        """获取消费状态"""
        pass
```

### IDataProcessor

```python
class IDataProcessor(Protocol):
    def process_task_result(self, task_result: TaskResult) -> None:
        """处理单个任务结果"""
        pass

    def flush_pending_data(self) -> None:
        """刷新待处理数据"""
        pass
```

### IBatchSaver

```python
class IBatchSaver(Protocol):
    def save_batch(self, task_type: TaskType, data_batch: List[dict]) -> bool:
        """批量保存数据"""
        pass
```

## 错误处理与重试

### 错误分类

1. **数据格式错误**：记录日志，丢弃数据
2. **数据约束错误**：记录详细信息，人工处理
3. **系统资源错误**：暂停处理，等待恢复

## 与现有系统集成

### 与 Huey 集成

- 独立进程运行 SqliteHuey 实例
- 处理任务执行状态和异常情况

### 与数据库层集成

- 使用现有的 DBOperator 类
- 复用数据库连接池
- 利用现有的表结构和索引

### 与配置系统集成

- 扩展现有的配置文件
- 支持运行时配置更新
- 提供配置验证机制

## 测试策略

### 单元测试

- 各组件的独立功能测试
- 使用内存数据库进行测试
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
