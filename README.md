# Stock Downloader - 个人量化数据下载器

本项目是一个基于 Python 和 Tushare Pro API 构建的、强大且可扩展的A股数据下载器。它旨在为价值投资者和量化分析师提供一个稳定、高效、自动化的本地数据仓库解决方案。

经过多次迭代和架构优化，该项目具备了专业级数据管理系统的诸多特性，包括任务驱动的调度、实体级的智能冷却期管理、以及健壮的错误处理机制。

## 核心特性

- 
- **专业数据源**: 完全基于 [Tushare Pro](https://www.google.com/url?sa=E&q=https%3A%2F%2Ftushare.pro%2F) 专业版财经数据接口，保证了数据的稳定性和规范性。
- **任务驱动架构**: 所有下载操作都被定义为可配置的任务，易于管理和扩展（例如，未来可轻松添加财务报表、指数数据等任务）。
- **智能增量更新**:
  - 
  - **实体级冷却期**: 对每一个“数据实体”（如贵州茅台的前复权日K线）进行独立的冷却期管理，避免了对已是最新数据的重复请求。
  - **自动差异下载**: 当您在全局股票池中增加新的股票时，系统能自动识别并仅为新股票下载完整的历史数据，而对老股票继续进行增量更新。
- **高性能存储**: 使用 **Apache Parquet** 列式存储格式保存核心数据，极大地提升了数据分析时的读取性能和存储效率。
- **健壮的元数据管理**: 利用 **diskcache** (基于SQLite) 高效管理所有数据实体的元数据（如最后更新时间），保证了状态的持久化和快速查询。
- **灵活的配置**: 所有行为均由一个清晰的 config.yaml 文件驱动，无需修改任何代码即可调整下载的股票池、任务类型和更新频率。
- **完善的监控与日志**:
  - 
  - 详细的运行日志 (downloader.log)，记录每一个关键步骤。
  - 独立的失败任务日志 (failed_tasks.log)，方便追踪和重试失败的操作。
- **命令行友好**: 支持 --force 参数，可强制刷新所有数据，无视冷却期。

## 项目架构

本项目的架构经过精心设计，遵循了“高内聚，低耦合”的原则，让每个模块各司其职。

Generated code

```
/stock_downloader/
├── .env                  # 【关键】存储Tushare Token，需手动创建
├── config.yaml           # 【关键】项目主配置文件，定义任务和股票池
├── main.py               # 程序主入口，负责任务调度和执行
|
├── downloader/           # 核心Python包
│   ├── fetcher.py        # 数据获取器：封装所有Tushare API调用
│   └── storage.py        # 存储器：负责Parquet文件的读写
│
├── cache/                # Diskcache的缓存目录 (自动生成)
├── data/                 # Parquet数据存储目录 (自动生成)
|
├── tests/                # Pytest测试目录
│   ├── test_fetcher.py
│   └── test_storage.py
|
├── verify_data.ipynb     # Jupyter Notebook：用于人工数据校验和可视化
└── requirements.in       # 项目依赖声明
```

Use code [with caution](https://support.google.com/legal/answer/13505487). 

### 模块职责

- 
- **main.py**: 项目的大脑。负责解析配置、执行任务调度（区分系统任务和数据任务）、管理主循环，并将工作分派给Fetcher和Storage。
- **downloader/fetcher.py**: 项目的“手”。唯一与外部数据源（Tushare Pro）交互的模块。负责API认证、数据请求和基本的格式统一。
- **downloader/storage.py**: 项目的“仓库管理员”。负责将Fetcher获取的数据以Parquet格式高效地写入磁盘，并提供增量更新和数据读取的能力。
- **downloader/cache.py (隐式)**: 通过diskcache库实现。是项目的“记忆系统”，记录每个数据实体的最后更新时间，为智能调度提供决策依据。

## 安装与配置

### 1. 环境准备

本项目使用 [uv](https://www.google.com/url?sa=E&q=https%3A%2F%2Fgithub.com%2Fastral-sh%2Fuv) 进行包管理，它是一个极速的Python包安装器。

Generated bash

```
# 克隆项目
git clone <your-repo-url>
cd stock_downloader

# 使用uv创建并激活虚拟环境
uv venv
source .venv/bin/activate  # macOS / Linux
# .venv\Scripts\activate  # Windows

# 安装所有依赖
uv pip sync requirements.txt
```

Use code [with caution](https://support.google.com/legal/answer/13505487). Bash

### 2. 配置 Tushare Token **(最重要的一步)**

Tushare Pro 需要个人Token才能访问。

- 

- 复制 .env.example (如果提供) 或手动创建一个名为 .env 的文件。

- 在 .env 文件中填入您的Token：

  Generated code

  ```
  TUSHARE_TOKEN="这里替换为你的真实Tushare Pro Token"
  ```

  Use code [with caution](https://support.google.com/legal/answer/13505487). 

- **警告**: .env 文件包含敏感信息，已默认添加到 .gitignore 中，**切勿**将其提交到任何版本控制系统。

### 3. 配置下载任务

打开 config.yaml 文件，根据您的需求进行配置。

- 
- **downloader.symbols**: 设置您想要下载的股票池。
  - 
  - 可以是一个具体的 ts_code 列表，例如 ["000001.SZ", "600519.SH"]。
  - 也可以设置为 "all"，程序会自动执行“更新A股列表”任务，并下载全市场的股票。
- **tasks**: 启用或禁用不同的下载任务。enabled: true 表示执行该任务。

## 使用方法

### 1. 自动下载

直接在项目根目录运行主程序：

Generated bash

```
python main.py
```

Use code [with caution](https://support.google.com/legal/answer/13505487). Bash

程序会自动检查每个数据实体的冷却期，只下载需要更新的数据。

### 2. 强制刷新

如果您想忽略冷却期，强制更新所有数据，请使用 -f 或 --force 参数：

Generated bash

```
python main.py --force
```

Use code [with caution](https://support.google.com/legal/answer/13505487). Bash

### 3. 人工数据校验

我们提供了一个强大的Jupyter Notebook脚本用于数据校验。

Generated bash

```
# 确保已安装Jupyter
uv pip install jupyterlab

# 启动Jupyter
jupyter lab
```

Use code [with caution](https://support.google.com/legal/answer/13505487). Bash

在Jupyter环境中，打开 verify_data.ipynb 并运行所有单元格。它会提供系统健康总览、元数据审查和个股深度分析图表。

### 4. 运行单元测试

为了保证代码质量，您可以随时运行pytest测试套件：

Generated bash

```
uv run pytest
```

Use code [with caution](https://support.google.com/legal/answer/13505487). Bash

------



## ⚠️ 不能遗忘的重要事项

这份备忘录记录了项目设计中的关键决策和未来维护的注意事项。

1. 
2. **数据源的纯粹性**: 我们决定**不修改**从Tushare获取的任何原始列名（如ts_code, trade_date）。这保证了数据的可追溯性。所有下游代码（main.py, storage.py）都依赖于这些原始列名。
3. **“实体级”冷却逻辑**: 本项目的核心智能在于**以“数据实体”（股票代码 + 数据类型）为单位**进行冷却期管理。这是通过diskcache实现的。如果系统出现奇怪的重复下载或不更新行为，首先应检查cache/目录的状态是否正常。
4. **状态一致性**: data/目录（Parquet文件）是最终的真相来源，cache/目录是其状态索引。在极少数情况下（如程序被强制中断），两者可能出现不一致。如果遇到难以解释的问题，**最可靠的恢复方法是删除cache/和failed_tasks.log文件**，然后重新运行一次完整的下载（可以使用--force参数）。系统会根据data/目录中的文件重新进行增量更新，并重建一个干净的cache。
5. **symbols: "all" 的依赖**: 当配置为下载全市场股票时，main.py的执行**依赖于“更新A股列表”任务**。请确保该任务在config.yaml中是启用的，并且其update_interval_hours设置合理。
6. **关于复权因子**: 当前版本的main.py下载了日线数据（daily）和复权因子（daily_adj_factor），但存储后并未在下载器中直接使用复权因子计算复权价。这是一个**有意的设计**，将计算过程留给了未来的数据分析阶段，让下载器职责更纯粹。如果您需要在分析时使用复权价，计算公式为：
   - 
   - **前复权价** = 不复权价 * 当日复权因子 / 最新交易日的复权因子
   - **后复权价** = 不复权价 * 当日复权因子
7. **Tushare积分**: Tushare Pro的调用受积分限制。虽然本程序已尽可能高效，但在首次下载全市场数据时，仍可能消耗大量积分。请密切关注您在Tushare官网的积分使用情况。

这份文档为您后续的使用、维护和二次开发提供了坚实的基础。