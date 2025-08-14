# 基于Schema的动态表创建原型

这个原型演示了如何基于TOML配置文件动态创建DuckDB表结构，并进行数据操作。

## 文件说明

### 核心文件

- `schema_driven_table_creator.py` - 核心的Schema驱动表创建器
- `schema_data_operations.py` - 数据操作示例，演示插入、查询等功能
- `test_schema_table_creator.py` - 单元测试文件

### 功能特性

1. **动态表创建** - 根据TOML schema配置自动生成CREATE TABLE语句
2. **数据类型映射** - 自动将schema中的字段映射为DuckDB数据类型
3. **主键支持** - 支持单字段和复合主键
4. **数据操作** - 提供数据插入、查询、统计等功能
5. **错误处理** - 完善的错误处理和日志输出

## 使用方法

### 1. 运行基本的表创建示例

```bash
uv run python examples/schema_driven_table_creator.py
```

这将：
- 加载 `stock_schema.toml` 配置文件
- 连接到内存DuckDB数据库
- 创建所有配置的表
- 显示生成的SQL语句

### 2. 运行数据操作示例

```bash
uv run python examples/schema_data_operations.py
```

这将演示：
- 表的动态创建
- 示例数据的插入
- 数据查询和统计
- 表关联查询

### 3. 运行测试

```bash
uv run python examples/test_schema_table_creator.py
```

## 核心类说明

### SchemaTableCreator

主要的表创建器类，提供以下功能：

- `load_schema()` - 加载TOML配置文件
- `connect_db()` - 连接DuckDB数据库
- `generate_create_table_sql()` - 生成CREATE TABLE SQL
- `create_table()` - 创建单个表
- `create_all_tables()` - 创建所有表

### SchemaDataOperator

数据操作器类，提供以下功能：

- `initialize()` - 初始化数据库和表
- `insert_sample_data()` - 插入示例数据
- `query_data()` - 查询数据
- `get_table_stats()` - 获取表统计信息

## Schema配置格式

基于 `stock_schema.toml` 文件，每个表的配置格式如下：

```toml
[table_name]
table_name = "actual_table_name"
primary_key = ["field1", "field2"]  # 支持复合主键
description = "表描述"
columns = [
    "field1",
    "field2",
    # ...
]
```

## 设计特点

1. **配置驱动** - 所有表结构都通过TOML配置文件定义
2. **类型安全** - 使用Python类型提示和数据验证
3. **可扩展性** - 易于添加新的表配置和数据类型
4. **测试覆盖** - 包含完整的单元测试
5. **错误处理** - 友好的错误信息和异常处理

## 示例输出

运行数据操作示例后，你将看到：

```
🚀 基于Schema的数据操作示例
==================================================
🚀 初始化数据操作器...
✅ 加载了 7 个表配置
✅ 数据库连接成功
✅ 成功创建 7/7 个表

📝 插入股票基本信息数据...
✅ 成功插入 3 条记录到表 stock_basic

📝 插入股票日线数据...
✅ 成功插入 3 条记录到表 stock_daily

📊 查询股票基本信息...
📊 从表 stock_basic 查询到 3 条记录
  ts_code symbol name area industry cnspell market list_date       act_name act_ent_type
000001.SZ 000001 平安银行   深圳       银行    PAYH     主板  19910403     平安银行股份有限公司       股份有限公司
000002.SZ 000002  万科A   深圳      房地产     WKA     主板  19910129     万科企业股份有限公司       股份有限公司
600000.SH 600000 浦发银行   上海       银行    PFYH     主板  19991110 上海浦东发展银行股份有限公司       股份有限公司
```

## 下一步计划

这个原型为后续开发奠定了基础，可以进一步扩展为：

1. **集成到主项目** - 将原型集成到主要的数据下载系统中
2. **增量更新** - 实现基于时间戳的增量数据更新
3. **数据验证** - 添加数据质量检查和验证
4. **性能优化** - 批量插入和索引优化
5. **监控告警** - 添加数据同步监控和异常告警