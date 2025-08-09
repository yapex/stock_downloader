# 依赖注入重构计划

## 重构目标

1. **构造函数依赖注入**: 所有类对其它类的依赖只能通过构造函数注入，不得直接引用
2. **基于Protocol编程**: 面向接口，确保每个重要的业务类都能被测试
3. **工厂模式**: 复杂的类创建过程应该被封装到工厂类中

## 当前问题分析

### 直接依赖问题

1. **DownloaderApp** 直接导入和使用具体实现类:
   - `from .fetcher import TushareFetcher`
   - `from .storage import PartitionedStorage`
   - 在方法中直接调用工厂函数

2. **DownloadEngine** 直接依赖具体实现:
   - 直接使用 `TushareFetcher` 和 `DuckDBStorage`
   - 在运行时创建依赖对象

3. **Producer** 和 **ConsumerPool** 直接依赖:
   - Producer 直接使用 `TushareFetcher`
   - ConsumerWorker 直接使用 `DuckDBStorage`

4. **工厂类缺乏抽象**:
   - 工厂类直接返回具体实现
   - 缺乏统一的依赖注入容器

### 缺少Protocol接口

当前只有部分接口定义在 `interfaces.py` 中，需要为以下组件创建Protocol:
- IFetcher (数据获取接口)
- IStorage (存储接口) 
- ITaskProcessor (任务处理接口)
- IProgressManager (进度管理接口)
- IErrorHandler (错误处理接口)

## 重构步骤

### 第一阶段: 创建Protocol接口

1. **创建核心业务Protocol**
   - `IFetcher`: 数据获取接口
   - `IStorage`: 存储接口
   - `ITaskProcessor`: 任务处理接口
   - `IProgressManager`: 进度管理接口

2. **扩展现有interfaces.py**
   - 添加新的Protocol定义
   - 保持向后兼容性

### 第二阶段: 重构核心类

1. **重构TushareFetcher**
   - 实现IFetcher接口
   - 保持现有功能不变

2. **重构Storage类**
   - 实现IStorage接口
   - 统一存储接口

3. **重构TaskProcessor**
   - 实现ITaskProcessor接口
   - 通过构造函数注入IFetcher

### 第三阶段: 重构应用层

1. **重构DownloaderApp**
   - 通过构造函数注入所有依赖
   - 移除直接的工厂调用
   - 面向接口编程

2. **重构DownloadEngine**
   - 通过构造函数注入依赖
   - 移除运行时依赖创建

3. **重构Producer和Consumer**
   - 通过构造函数注入依赖
   - 面向接口编程

### 第四阶段: 创建依赖注入容器

1. **创建DI容器**
   - 统一管理对象创建
   - 支持单例和原型模式
   - 支持配置驱动的依赖注入

2. **重构工厂类**
   - 基于DI容器重构
   - 支持接口注入

### 第五阶段: 更新测试

1. **创建Mock实现**
   - 为所有Protocol创建Mock实现
   - 支持单元测试

2. **更新现有测试**
   - 使用依赖注入
   - 提高测试覆盖率

## 实施细节

### Protocol定义示例

```python
from typing import Protocol, Optional
import pandas as pd

class IFetcher(Protocol):
    """数据获取接口"""
    
    def fetch_stock_list(self) -> Optional[pd.DataFrame]:
        """获取股票列表"""
        ...
    
    def fetch_daily_history(self, ts_code: str, start_date: str, 
                          end_date: str, adjust: str) -> Optional[pd.DataFrame]:
        """获取日K线数据"""
        ...

class IStorage(Protocol):
    """存储接口"""
    
    def save_data(self, data: pd.DataFrame, table_name: str) -> bool:
        """保存数据"""
        ...
    
    def get_latest_date(self, table_name: str, symbol: str) -> Optional[str]:
        """获取最新日期"""
        ...
```

### 依赖注入示例

```python
class DownloaderApp:
    def __init__(self, 
                 fetcher: IFetcher,
                 storage: IStorage,
                 progress_manager: IProgressManager,
                 logger: Optional[logging.Logger] = None):
        self._fetcher = fetcher
        self._storage = storage
        self._progress_manager = progress_manager
        self.logger = logger or logging.getLogger(__name__)
```

### DI容器示例

```python
class DIContainer:
    def __init__(self):
        self._services = {}
        self._singletons = {}
    
    def register_singleton(self, interface: type, implementation: type):
        """注册单例服务"""
        self._services[interface] = (implementation, 'singleton')
    
    def register_transient(self, interface: type, implementation: type):
        """注册瞬态服务"""
        self._services[interface] = (implementation, 'transient')
    
    def resolve(self, interface: type):
        """解析服务"""
        if interface not in self._services:
            raise ValueError(f"Service {interface} not registered")
        
        implementation, lifetime = self._services[interface]
        
        if lifetime == 'singleton':
            if interface not in self._singletons:
                self._singletons[interface] = self._create_instance(implementation)
            return self._singletons[interface]
        else:
            return self._create_instance(implementation)
```

## 风险控制

1. **渐进式重构**: 每次只重构一个模块，确保系统始终可运行
2. **测试驱动**: 每个重构步骤都要有对应的测试
3. **向后兼容**: 保持现有API的兼容性
4. **回滚机制**: 每个阶段完成后创建备份点

## 验收标准

1. **依赖注入**: 所有类都通过构造函数注入依赖
2. **接口编程**: 所有依赖都是基于Protocol接口
3. **工厂封装**: 复杂对象创建都通过工厂或DI容器
4. **测试覆盖**: 所有重构的类都有对应的单元测试
5. **功能完整**: 重构后功能与重构前完全一致