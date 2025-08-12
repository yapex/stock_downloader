# Stock Downloader - 架构设计

本文档详细阐述了 Stock Downloader 的系统架构、设计原则和核心组件，旨在帮助开发者快速理解项目内部工作原理并进行二次开发。

## 1. 核心设计哲学

本项目遵循以下几个核心设计原则：

- **配置驱动 (Configuration-Driven):** 系统的所有行为，如数据源、下载目标和任务参数，都由 `config.yaml` 驱动。开发者和用户无需修改代码即可定制下载流程。
- **插件化与可扩展 (Plugin-based & Extensible):** 每一种数据下载任务（如日线、财务报表）都被实现为一个独立的“任务处理器”插件。通过 Python 的 `entry_points` 机制，可以轻松添加新的数据类型，而无需改动核心引擎。
- **关注点分离 (Separation of Concerns):** 系统被划分为职责单一的组件（引擎、获取器、存储器、任务处理器），使得代码更易于理解、测试和维护。
- **健壮性优先 (Robustness First):** 内置了增量更新、错误分类和自动重试机制，确保了数据同步的效率和在不���定网络环境下的可靠性。

## 2. 架构图

下图展示了系统的核心组件及其交互关系：

```mermaid
graph TD
    subgraph "用户层 (User Layer)"
        A[CLI (dl)]
        F[config.yaml]
        I[pyproject.toml]
    end

    subgraph "应用核心 (Application Core)"
        B(Download Engine)
        C{Task Handlers (Plugins)}
        D[Tushare Fetcher]
        E[DuckDB Storage]
    end

    subgraph "外部依赖 (External)"
        G[(Tushare API)]
        H[(data/stock.db)]
    end

    A -- "启动" --> B
    B -- "读取配置" --> F
    B -- "发现插件" --> I
    B -- "加载并分派任务给" --> C
    C -- "使用" --> D
    C -- "使用" --> E
    D -- "从...获取数据" --> G
    E -- "读/写" --> H
```

## 3. 系统组件详解

#### `main.py` (应用入口)

- **职责:** 作为用户与系统交互的命令行接口 (CLI)。
- **功能:**
    - 使用 `Typer` 构建用户友好的命令行界面（例如 `dl`, `dl --group daily`）。
    - 解析命令行参数（如 `--force`, `--group`）。
    - 初始化日志系统。
    - 启动 `DownloaderApp`，开始业务流程。

#### `DownloadEngine` (下载引擎)

- **职责:** 整个下载流程的中央协调器，是系统的大脑。
- **功能:**
    - **插件发现:** 在启动时，通过 `importlib.metadata.entry_points` 自动发现所有在 `pyproject.toml` 中注册的任务处理器插件。
    - **配置解析:** 读取并解析 `config.yaml`，理解用户想要执行哪个任务组 (`group`)，以及组内包含哪些任务 (`tasks`) 和股票 (`symbols`)。
    - **任务调度:** 根据配置，创建任务队列，并将每个任务分派给对应的任务处理器实例。
    - **流程控制:** 管理任务的并发执行，并收集每个任务的执行结果。
    - **错误协调:** 捕获并协调处理在任务执行期间发生的错误，例如触发全局重试。

#### `Task Handlers` (任务处理器 / 插件)

- **职责:** 封装特定数据类型下载的全部逻辑。每个处理器就是一个独立的“工人”。
- **核心设计:**
    - **`Base` (基类):** 定义了所有任务处理器必须遵守的接口。
    - **`IncrementalTaskHandler` (模板方法模式):** 这是设计的精髓所在。它为所有需要“增量更新”的任务提供了一个统一、健壮的算法骨架。它处理了检查本地最新数据、计算需下载的日期范围、调用数��获取、保存数据、错误分类和自动重试等所有通用逻辑。
- **具体实现 (如 `DailyTaskHandler`):**
    - 继承 `IncrementalTaskHandler`。
    - 只需实现几个特定的抽象方法，如 `get_data_type()` (返回数据表名) 和 `fetch_data()` (调用 `Fetcher` 获取具体数据)。
    - 开发者无需关心复杂的增量和重试逻辑，只需聚焦于“获取哪种数据”这一个问题。

#### `TushareFetcher` (数据获取器)

- **职责:** 封装与 Tushare Pro API 的所有网络通信。
- **功能:**
    - 初始化 Tushare Pro SDK。
    - 提供一系列清晰、具体的方法（如 `fetch_daily`, `fetch_financials`）供任务处理器调用。
    - 将 Tushare 返回的数据统一处理为 Pandas DataFrame。

#### `DuckDBStorage` (数据存储器)

- **职责:** 负责所有数据库的读写操作。
- **功能:**
    - 使用 DuckDB 作为底层存储，所有数据都保存在一个单一的文件中 (`data/stock.db`)。
    - **数据分区:** 在 DuckDB 内部，数据按 `data_type` (任务类型) 和 `ts_code` (股票代码) 进行分区存储，极大地提高了查询效率。
    - **增量写入 (`save`):** 将新数据追加到对应的表中。
    - **全量覆写 (`overwrite`):** 删除旧表，用新数据创建新表。
    - **查询最新日期 (`get_latest_date`):** 高效查询某支股票某个数据类型的最新日期，为增量更新提供支持。

## 4. 插件化系统：如何扩展新功能

这是本项目的核心优势。假设您想添加一个下载“指数日线行情”的新功能，只需遵循以下步骤：

#### 第1步：创建任务处理器类

在 `src/downloader/tasks/` 目录下创建新文件 `index_daily.py`。代码如下：

```python
# src/downloader/tasks/index_daily.py
from .base import IncrementalTaskHandler
import pandas as pd

class IndexDailyTaskHandler(IncrementalTaskHandler):
    def get_data_type(self) -> str:
        # 定义数据将存储的表名
        return "index_daily"

    def get_date_col(self) -> str:
        # 定义用于增量更新的日期列
        return "trade_date"

    def fetch_data(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        # 实现具体的数据获取逻辑
        # 注意：可能需要先在 TushareFetcher 中添加一个 fetch_index_daily 方法
        return self.fetcher.fetch_index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
```

#### 第2步：注册插件

打开 `pyproject.toml` 文件，在 `[project.entry-points."stock_downloader.task_handlers"]` 部分，添加一行来“宣告”你的新插件：

```toml
# pyproject.toml

[project.entry-points."stock_downloader.task_handlers"]
# ... 其他已注册的处理器
# 格式: "任务类型标识符" = "模块路径:处理器类名"
index_daily = "downloader.tasks.index_daily:IndexDailyTaskHandler"
```
- `index_daily`: 是这个插件的唯一标识符。你将在 `config.yaml` 中使用它。
- `downloader.tasks.index_daily:IndexDailyTaskHandler`: 指明了当引擎需要 `index_daily` 类型的处理器时，应该去哪里加载它。

#### 第3步：更新项目安装

为了让 `entry_points` 的变更生效，必须在项目根目录重新运行安装命令。这会更新项目的元数据。

```bash
# 使用 uv (推荐)
uv pip install -e .

# 或者使用 pip
pip install -e .
```
> **提示:** `-e` 表示“可编辑模式”，安装后，你对代码的任何修改都会立刻生效，无需再次安装。

#### 第4步：在 `config.yaml` 中使用

现在，你就可以像使用内置任务一样，在 `config.yaml` 中配置和使用你的新任务了。

```yaml
# config.yaml

tasks:
  # ... 其他任务模板
  index_daily_data:
    name: "下载指数日线"
    type: "index_daily"  # 使用你在 toml 中定义的标识符

groups:
  index_group:
    description: "专门下载指数数据的任务组"
    symbols: ["000001.SH", "399001.SZ"] # 指数代码
    tasks: ["index_daily_data"]
```

现在，只需运行 `dl --group index_group`，你的新插件就会被自动加载并执行。
