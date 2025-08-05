# Stock Downloader

一个基于 Tushare Pro API 的股票数据下载工具，支持插件化任务处理和高效的 Parquet 存储格式。

## 特性

- **插件化架构**: 通过 entry_points 机制支持动态任务处理器
- **增量更新**: 智能的增量数据下载，避免重复获取
- **高效存储**: 使用 Parquet 格式进行数据存储和压缩
- **自动股票代码标准化**: 支持多种股票代码输入格式，自动标准化
- **向后兼容**: 支持旧版本文件路径格式的自动迁移
- **速率限制**: 支持对 API 调用进行速率限制，防止被限制访问
- **灵活配置**: 支持通过命令行参数或配置文件指定目标股票
- **网络错误处理**: 自动识别和重试网络相关错误，提高下载稳定性

## 最近更新

### 文件路径结构变更

从最新版本开始，文件路径结构已简化：
- **旧格式**: `data_type/entity=STOCK_CODE/data.parquet`
- **新格式**: `data_type/STOCK_CODE/data.parquet`

### 自动股票代码标准化

系统现在自动标准化各种股票代码格式：
- `600519` → `600519.SH`
- `SH600519` → `600519.SH` 
- `000001` → `000001.SZ`
- `sz000001` → `000001.SZ`

## 安装

```bash
# 克隆项目
git clone <repository-url>
cd stock_downloader

# 安装依赖 (推荐使用 uv)
uv pip install -e .

# 或使用 pip
pip install -e .
```

## 配置

1. 创建配置文件 `config.yaml`：
```yaml
tushare:
  token: "your_tushare_token_here"

storage:
  base_path: "./data"

downloader:
  symbols: 
    - "all"  # 或者指定具体股票代码列表

tasks:
  - name: "股票列表"
    enabled: true
    type: "stock_list"
    update_strategy: "overwrite"
    update_interval_hours: 23
  
  - name: "日线行情(前复权)"
    enabled: true
    type: "daily"
    adjust: "qfq"  # qfq-前复权, hfq-后复权, none-不复权
    update_strategy: "incremental"
    date_col: "trade_date"
    
  - name: "每日指标"
    enabled: true
    type: "daily_basic"
    update_strategy: "incremental"
    date_col: "trade_date"
    
  - name: "财务报表-资产负债表"
    enabled: true
    type: "financials"
    statement_type: "balancesheet"
    update_strategy: "incremental"
    date_col: "ann_date"
    
  - name: "财务报表-利润表"
    enabled: true
    type: "financials"
    statement_type: "income"
    update_strategy: "incremental"
    date_col: "ann_date"
    
  - name: "财务报表-现金流量表"
    enabled: true
    type: "financials"
    statement_type: "cashflow"
    update_strategy: "incremental"
    date_col: "ann_date"
```

## 使用方法

```bash
# 运行数据下载（使用配置文件中的股票列表）
python main.py

# 强制更新所有数据（无视冷却期）
python main.py --force

# 指定特定股票下载
python main.py 600519.SH 000001.SZ

# 下载所有A股数据
python main.py all
```

## 错误处理

本系统实现了智能的错误处理机制：

1. **网络错误自动重试**: 当遇到网络连接超时、连接失败等网络相关错误时，系统会自动记录这些股票代码
2. **批量重试**: 在任务完成时，系统会统一重试所有因网络错误而失败的股票
3. **错误日志记录**: 如果重试仍然失败，错误信息会被记录到 `failed_tasks.log` 文件中，便于后续分析和处理

这种机制大大提高了在不稳定网络环境下数据下载的稳定性和成功率。

## 数据迁移

如果您有使用旧版本创建的数据，请参考 [CHANGELOG.md](CHANGELOG.md) 中的迁移说明。系统支持自动向后兼容，但建议按照迁移指南进行数据格式更新。

## 文件结构

```
stock_downloader/
├── main.py                 # 程序入口点
├── config.yaml            # 配置文件
├── src/downloader/        # 核心模块
│   ├── engine.py         # 下载引擎
│   ├── fetcher.py        # 数据获取器
│   ├── storage.py        # 存储处理器
│   ├── utils.py          # 工具函数
│   ├── rate_limit.py     # 速率限制器
│   └── tasks/            # 任务处理器
│       ├── base.py       # 基础任务类
│       ├── daily.py      # 日线数据任务
│       ├── stock_list.py # 股票列表任务
│       └── ...
├── tests/                # 测试文件
└── data/                 # 数据存储目录
    └── [data_type]/      # 按数据类型分类
        └── [stock_code]/ # 按股票代码分类
            └── data.parquet
```

## 开发

### 添加新的任务处理器

1. 在 `src/downloader/tasks/` 中创建新的处理器文件
2. 在 `pyproject.toml` 中注册 entry_point
3. 重新安装项目以更新 entry_points

详细说明请参考 [arch.md](arch.md) 中的架构文档。

## 许可证

[根据实际情况添加许可证信息]