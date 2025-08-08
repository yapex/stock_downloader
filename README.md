# Stock Downloader

一个灵活、高效且可扩展的A股市场数据下载工具，专为量化分析师和开发者设计。

本项目基于 [Tushare Pro](https://tushare.pro/home) API，将数据高效地存储在 [DuckDB](https://duckdb.org/) 数据库中，并采用插件化架构，让您可以轻松扩展和定制自己的数据任务。

## 核心特性

- **🔌 插件化架构**: 每种数据（日线、财务报表等）都是一个独立的任务插件，易于维护和扩展。
- **🚀 高效存储与同步**: 使用 DuckDB 和 Parquet 格式，支持增量更新，自动跳过已下载数据，节省时间和API积分。
- **⚙️ 配置驱动**: 通过 `config.yaml` 文件灵活定义下载任务组，轻松切换不同的数据下载方案。
- **💪 健壮可靠**: 内置网络错误自动重试机制，确保在不稳定网络环境下的下载成功率。
- ** CLI友好**: 提供简洁的命令行工具，方便执行、调试和管理下载任务。

## 快速上手

跟随以下步骤，在5分钟内开始您的第一次数据下载。

### 1. 环境准备

- **Python**: 确保您的环境是 Python 3.13 或更高版本。
- **Tushare Pro 账户**: 您需要一个 [Tushare Pro](https://tushare.pro/home) 账户以获取 API Token。

### 2. 安装

首先，克隆项目到本地，然后使用 `uv` (推荐) 或 `pip` 安装依赖。

```bash
# 克隆项目
git clone https://github.com/your-username/stock_downloader.git
cd stock_downloader

# 使用 uv (推荐)
uv pip install -e .

# 或者使用 pip
pip install -e .
```

### 3. 配置

#### a. 设置 API Token

项目通过 `.env` 文件管理您的 Tushare API Token。

```bash
# 从模板复制配置文件
cp .env.sample .env
```

然后，编辑 `.env` 文件，将 `your Tushare token here` 替换为您自己的 Token。

```dotenv
# .env
TUSHARE_TOKEN="这里替换成你的真实Token"
```

#### b. 理解 `config.yaml`

这是项目的核心配置文件，您可以在此定义所有下载行为。文件主要由两部分组成：`tasks` 和 `groups`。

- `tasks`: 定义了“可以做什么”。这里是所有可用任务的模板，例如下载日线、下载财务报表等。
- `groups`: 定义了“实际做什么”。每个组引用一系列 `tasks`，并指定要为哪些股票代码 (`symbols`) 执行这些任务。

**示例 `config.yaml`:**
```yaml
storage:
  db_path: "data/stock.db"

# 任务模板库：定义了所有可用的数据下载类型
tasks:
  update_stock_list:
    name: "更新A股列表"
    type: "stock_list"
    
  daily_qfq:
    name: "日K线-前复权"
    type: "daily"
    adjust: "qfq"
    date_col: "trade_date"
    
  financial_income:
    name: "财务报表-利润表"
    type: "financials"
    statement_type: "income"
    date_col: "ann_date"

# 执行组：定义了具体的下载计划
groups:
  # 默认组：下载少量股票的常用数据，适合初次运行和测试
  default:
    description: "默认组：下载少量示例股票的日K线和财务数据"
    symbols: ["600519", "000001", "000858", "600276", "000333"]
    max_concurrent_tasks: 3
    tasks: ["update_stock_list", "daily_qfq", "financial_income"]
    
  # 日线组：下载所有A股的日线相关数据
  daily_all:
    description: "下载所有A股的日K线和每日指标"
    symbols: "all" # "all" 表示下载全部A股
    max_concurrent_tasks: 3
    tasks: ["update_stock_list", "daily_qfq"]
```

### 4. 运行下载

本项目注册了一个命令行工具 `dl`，方便您执行操作。

```bash
# 执行默认组 (default) 的下载任务
uv run dl
```

你会看到一个简洁的进度条，实时显示下载进度和任务状态：

```bash
处理 default 组任务: 100%|████████████████| 5/5 [00:15<00:00, 3.2task/s]
ℹ️  所有 5 个任务处理完成 (成功: 5, 失败: 0)
```

下载完成后，所有数据都会保存在 `data/stock.db` 文件中。详细的下载日志可以在 `logs/downloader.log` 中查看。

## 常用命令

#### 执行指定的任务组

您可以轻松执行在 `config.yaml` 中定义的任何组。

```bash
# 执行 'daily_all' 组
uv run dl --group daily_all
```

#### 强制更新数据

忽略增量更新检查，强制重新下载所有数据。

```bash
# 强制执行默认组
uv run dl --force
```

#### 按股票过滤

```bash
# 只下载贵州茅台
uv run dl --symbol 600519
```

## 验证与排错

### 验证数据库状态（uv run dl verify）

命令：

```bash
uv run dl verify [--config PATH] [--show-missing/--no-missing] [--log-path PATH]
```

参数说明：
- --config, -c: 配置文件路径（默认 config.yaml）
- --show-missing/--no-missing: 是否显示按业务分类的缺口汇总（默认显示）
- --log-path: 死信日志路径（默认 logs/dead_letter.jsonl）

示例输出：
```
总股票数: 5024 | 已覆盖: 4987 | 缺失: 37
日线: 缺 12
基础面: 缺 25
失败任务: 8
```

### Dead-letter 重试流程

1) 生成/合并重试候选：
```bash
# 可选：扫描业务表缺失的股票，写入缺失日志（覆盖）
uv run dl scan_missing --config config.yaml --missing-log logs/missing_symbols.jsonl
```

2) 预演（不执行，只看将要重试哪些任务）：
```bash
uv run dl retry --dry-run \
  --task-type daily \
  --symbol 6005 \
  --limit 50 \
  --log-path logs/dead_letter.jsonl \
  --missing-log logs/missing_symbols.jsonl
```

3) 执行重试：
```bash
uv run dl retry \
  --task-type daily \
  --log-path logs/dead_letter.jsonl \
  --missing-log logs/missing_symbols.jsonl
```

说明：
- 会读取死信日志与缺失符号日志，按 (symbol, task_type) 去重后重试。
- 预演模式仅打印即将重试的任务，不会执行。
- 当前不会自动归档已成功处理的死信记录，如需清理可手动处理日志文件。

### 用测试验证变更

运行测试：
```bash
uv test
```

`pyproject.toml` 已配置 `tool.uv.test`，可直接调用。

## 架构与开发

如果您想深入了解项目或贡献代码，请参考以下信息。

### 文件结构

```
stock_downloader/
├── src/downloader/        # 核心模块
│   ├── main.py           # 命令行入口
│   ├── engine.py         # 下载引擎
│   ├── fetcher.py        # 数据获取器 (Tushare)
│   ├── storage.py        # 存储处理器 (DuckDB)
│   └── tasks/            # 任务处理器插件
│       ├── base.py       # 基础任务类
│       └── ...           # 各种数据任务实现
├── config.yaml            # 核心配置文件
├── pyproject.toml         # 项目配置与依赖
├── tests/                 # 测试代码
└── data/                  # 数据存储目录
```

### 如何添加新的任务处理器

本项目的核心优势是其插件化设计。要添加一个新的数据下载任务（例如“分析师评级”），只需：

1.  在 `src/downloader/tasks/` 目录下创建一个新的 Python 文件（例如 `analyst_rating.py`）。
2.  在该文件中，创建一个继承自 `BaseTaskHandler` 的类，并实现数据获取逻辑。
3.  在 `pyproject.toml` 的 `[project.entry-points."stock_downloader.task_handlers"]` 部分，注册您的新任务处理器。

## 更新日志

所有重要的功能更新和重大变更都会记录在 [CHANGELOG.md](CHANGELOG.md) 文件中。

## 许可证

[根据实际情况添加许可证信息]
