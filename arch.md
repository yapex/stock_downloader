# 架构概览

本文档旨在阐述股票数据下载器的系统架构。这是一个基于 Python 的应用程序，专为从 Tushare Pro API 下载金融数据而设计。该系统遵循模块化、可扩展和配置驱动的原则。

## 核心设计原则

- **配置驱动 (Configuration-Driven):** 所有下载操作均由 `config.yaml` 文件控制。该文件定义了需要下载哪些数据集、针对哪些股票以及使用何种参数。这使得用户无需修改任何代码即可自定义下载器的行为。
- **关注点分离 (Separation of Concerns):** 应用程序被划分为多个独立的组件，每个组件都有单一的职责（例如，获取数据、存储数据、执行任务）。这使得系统更易于理解、维护和测试。
- **基于任务的可扩展性 (Task-Based Extensibility):** 系统围绕"任务"进行设计。每个任务代表一个特定的下载作业（例如，下载日线行情、下载财务报表）。通过**插件化**的机制，可以轻松添加新的任务处理器（Task Handler）类来支持新的数据类型。
- **增量更新 (Incremental Updates):** 下载器经过优化，仅获取新增或更新的数据，从而最大限度地减少冗余的 API 调用并节省网络带宽。它会为每支股票和每个数据集跟踪最新已下载的数据点。

## 插件化架构：`entry_points` 详解

本项目的核心是其基于 `entry_points` 的插件化架构，这是实现高度可扩展性的关键。它允许开发者在不修改核心引擎代码的情况下，添加新的下载功能。

### 1. 工作原理

`entry_points` 是 Python 打包标准（由 `setuptools` 或类似的构建工具支持）的一部分。它允许一个包（本项目）向其他包（或自身）"宣告"它提供某些可插拔的组件。

在本应用中，`DownloadEngine` 在初始化时，会执行以下操作：

```python
# downloader/engine.py
from importlib.metadata import entry_points

# ...
class DownloadEngine:
    def _discover_task_handlers(self) -> dict:
        registry = {}
        # 查找所有在 'stock_downloader.task_handlers' 组中注册的插件
        handlers_eps = entry_points(group="stock_downloader.task_handlers")
        for ep in handlers_eps:
            # 加载插件类并存入注册表
            registry[ep.name] = ep.load()
        return registry
```

1.  **发现:** `DownloadEngine` 使用 `importlib.metadata.entry_points` 查找所有在名为 `stock_downloader.task_handlers` 的特定组中注册的插件。
2.  **加载:** 它遍历找到的每个入口点，`ep.name` 是我们在配置中定义的任务类型（如 `daily`），`ep.load()` 则会动态地导入并返回处理器类本身（如 `DailyTaskHandler`）。
3.  **注册:** 引擎将任务类型和对应的处理器类存储在一个字典（`task_registry`）中。

当 `config.yaml` 中的任务被执行时，引擎只需在这个注册表中查找与任务 `type` 匹配的处理器类即可。

### 2. 如何配置

插件的"注册"发生在 `pyproject.toml` 文件中。这是连接任务类型、处理器代码和核心引擎的桥梁。

```toml
# pyproject.toml

[project.entry-points."stock_downloader.task_handlers"]
# 格式: "任务类型标识符" = "模块路径:处理器类名"
daily       = "downloader.tasks.daily:DailyTaskHandler"
stock_list  = "downloader.tasks.stock_list:StockListTaskHandler"
daily_basic = "downloader.tasks.daily_basic:DailyBasicTaskHandler"
financials  = "downloader.tasks.financials:FinancialsTaskHandler"
```

- **`[project.entry-points."stock_downloader.task_handlers"]`**: 定义了一个名为 `stock_downloader.task_handlers` 的入口点组。
- **`daily = "..."`**: 定义了一个名为 `daily` 的插件。这个 `daily` 就是你在 `config.yaml` 中 `type` 字段需要填写的值。
- **`"downloader.tasks.daily:DailyTaskHandler"`**: 指定了当请求 `daily` 类型的任务时，引擎应该从 `downloader.tasks.daily` 模块加载 `DailyTaskHandler` 这个类。

### 3. 如何验证插件是否加载成功

为了方便调试，应用在启动时会打印所有成功加载的插件。你可以通过查看 `downloader.log` 文件或控制台输出来确认。

```log
INFO - 发现了 4 个任务处理器入口点。
INFO -   - 已注册处理器: 'daily'
INFO -   - 已注册处理器: 'stock_list'
INFO -   - 已注册处理器: 'daily_basic'
INFO -   - 已注册处理器: 'financials'
```

如果你的新插件没有出现在这里，通常意味着 `pyproject.toml` 配置有误，或者项目没有被正确地以"可编辑模式"安装。

### 4. 如何添加一个新的任务处理器（示例）

假设你想添加一个下载"指数日线行情"的新功能。

1.  **创建处理器类:**
    在 `src/downloader/tasks/` 目录下创建一个新文件 `index_daily.py`，内容如下：
    ```python
    # src/downloader/tasks/index_daily.py
    from .base import IncrementalTaskHandler
    
    class IndexDailyTaskHandler(IncrementalTaskHandler):
        def get_data_type(self) -> str:
            return "index_daily" # 定义数据类型，用于存储路径
    
        def get_date_col(self) -> str:
            return "trade_date"
    
        def fetch_data(self, ts_code, start_date, end_date):
            # 调用 fetcher 中获取指数行情的方法 (可能需要先在 fetcher 中添加)
            return self.fetcher.fetch_index_daily(ts_code, start_date, end_date)
    ```

2.  **注册新的入口点:**
    打开 `pyproject.toml` 并添加一行：
    ```toml
    [project.entry-points."stock_downloader.task_handlers"]
    # ... 其他处理器
    index_daily = "downloader.tasks.index_daily:IndexDailyTaskHandler"
    ```

3.  **更新项目安装:**
    为了让 `entry_points` 的变更生效，你必须在项目根目录下重新运行安装命令。这会更新项目的元数据。
    ```bash
    # 使用 uv
    uv pip install -e .
    
    # 或使用 pip
    pip install -e .
    ```
    `-e` 表示"可编辑模式"，这意味着你对代码的修改会立刻生效，无需重新安装。

4.  **在 `config.yaml` 中使用:**
    现在你可以在配置文件中添加新任务了：
    ```yaml
    tasks:
      - name: "下载指数日线"
        enabled: true
        type: "index_daily" # 使用你在 toml 中定义的名字
        update_strategy: "incremental"
        date_col: "trade_date"
    ```

通过这套机制，项目的功能可以被无限扩展，而无需触及 `DownloadEngine` 的核心逻辑。

## 组件解析

系统由以下关键组件构成：

### 1. 入口点 (`main.py`)
- **职责:** 初始化应用程序，解析命令行参数（如 `--force`），设置日志系统，并协调主要组件的运行。
- **功能:** 加载配置，实例化 `Fetcher`、`Storage` 和 `Engine`，然后调用 `Engine` 来启动下载流程。

### 2. 下载引擎 (`downloader/engine.py`)
- **职责:** 作为下载流程的中央协调器。
- **功能:**
    - **动态发现插件:** 通过 `entry_points` 机制自动发现并注册所有可用的任务处理器。
    - **解析配置:** 从 `config.yaml` 中读取 `tasks` 列表。
    - **确定目标:** 确定本次运行所需处理的最终股票列表（优先使用命令行参数，其次使用配置文件，最后可从本地 stock_list 文件加载）。
    - **任务分派:** 将每个启用的任务分派给其对应的、已注册的处理器。
    - **错误处理协调:** 捕获任务执行过程中的异常并进行适当的处理

### 3. Tushare 数据获取器 (`downloader/fetcher.py`)
- **职责:** 处理与外部 Tushare Pro API 的所有通信。
- **功能:**
    - 初始化 Tushare API。
    - 提供一个清晰的接口，用于获取不同类型的数据（例如 `fetch_daily_history`）。
    - 抽象 Tushare 库的底层细节，返回标准化的 Pandas DataFrame。
    - 支持 API 调用的速率限制

### 4. Parquet 存储器 (`downloader/storage.py`)
- **职责:** 处理所有文件系统的读写操作。
- **功能:**
    - 使用高效的 Parquet 格式保存数据。
    - 将数据组织在结构化的目录中：`base_path/data_type/STOCK_CODE/data.parquet`。
    - 实现了增量 `save` 和全量 `overwrite` 两种操作。
    - 提供 `get_latest_date` 方法，对于增量更新至关重要。

### 5. 任务处理器 (`downloader/tasks/`)
- **职责:** 封装单个特定下载作业的全部逻辑，是插件的最终实现。
- **结构:**
    - `base.py`: 定义了 `BaseTaskHandler` 和 `IncrementalTaskHandler`，为插件开发提供了标准模板。
    - 具体实现 (例如 `daily.py`): 继承基类，实现数据获取和配置的特定逻辑。
- **错误处理:**
    - `IncrementalTaskHandler` 实现了网络错误的自动检测和重试机制
    - 网络相关错误（如超时、连接失败等）会被捕获并暂存
    - 在任务结束时，系统会统一重试所有失败的股票
    - 如果重试仍然失败，错误会被记录到 `failed_tasks.log` 文件中

### 6. 速率限制 (`downloader/rate_limit.py`)
- **职责:** 防止 API 调用频率超过限制。
- **功能:**
    - 使用装饰器模式实现 API 调用的速率限制。
    - 支持按任务类型配置不同的速率限制。

## 数据流

1. 用户启动程序，通过命令行参数或配置文件指定要下载的股票列表。
2. 程序加载配置，初始化核心组件（Fetcher、Storage、Engine）。
3. Engine 通过 entry_points 机制发现并注册所有可用的任务处理器。
4. Engine 执行所有配置为启用的任务：
   - 先执行 stock_list 任务（如果启用），确保股票列表是最新的。
   - 然后根据配置或命令行参数确定目标股票列表。
   - 依次执行所有其他启用的任务，使用确定的目标股票列表。

## 架构图

```mermaid
graph TD
    subgraph 用户与配置
        A[main.py]
        F[config.yaml]
        I[pyproject.toml]
    end

    subgraph Application Core
        B(Download Engine)
        C{Task Handlers (Plugins)}
        D[Fetcher]
        E[Storage]
        J[Rate Limiter]
    end

    subgraph External / Filesystem
        G[(Tushare API)]
        H[/data/*]
    end

    I -- Defines --> C
    A -- Starts --> B
    B -- Reads --> F
    B -- Discovers & Registers --> C
    B -- Dispatches to --> C
    C -- Uses --> D
    C -- Uses --> E
    C -- Uses --> J
    D -- Fetches from --> G
    G -- Returns data to --> D
    E -- Reads/Writes to --> H
```