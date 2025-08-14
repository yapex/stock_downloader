好的，没有问题。这是一个按照标准需求文档格式编写的、专门为 AI 模型设计的任务描述。这份文档清晰、结构化、无歧义，旨在让 AI 能够准确理解并生成高质量的解决方案。

---

### **需求文档：通用的、元数据驱动的数据库增量加载系统**

**1.0 项目标题**

通用的、元数据驱动的数据库增量加载系统 (Automated Metadata-Driven Incremental Data Loading System)

**2.0 项目概述**

**2.1 背景与目标**
当前任务是为后端为 DuckDB 的数据分析平台实现一个数据增量加载功能。数据源是 Pandas DataFrame。存在多个数据表，部分表的字段数量非常多（例如超过70个）。为每个表编写独立的、硬编码的 Python 查询和插入脚本是低效、不可维护且容易出错的。

本项目旨在开发一个通用的、可重用的 Python 解决方案，该方案通过读取外部的 schema 配置文件来动态执行增量加载（查询、合并、新增）操作，从而消除对特定表的代码依赖。

**2.2 核心需求**
系统必须能够根据指定的 schema 信息，自动将源 DataFrame 中的数据“合并（MERGE）”到 DuckDB 中的目标表中。合并逻辑定义为：基于主键匹配，如果记录已存在则更新，如果不存在则插入。

**3.0 功能需求 (Functional Requirements)**

**3.1 元数据驱动 (Metadata-Driven)**
*   系统的所有数据库操作逻辑（表名、字段列表、主键）必须由一个外部的 schema 配置文件驱动。
*   修改或增加对新表的处理，不应需要修改 Python 源代码，仅需更新 schema 配置文件。
*   schema 文件请参考 stock_schema.toml

**3.2 动态 SQL 生成 (Dynamic SQL Generation)**
*   系统必须在运行时根据加载的元数据动态生成 DuckDB 的 `MERGE` SQL 语句。
*   禁止在代码中使用针对特定表的硬编码 SQL 字符串。

**3.3 增量加载逻辑 (Incremental Load Logic - Upsert/Merge)**
*   系统必须实现“更新或插入”（Upsert）逻辑。
*   **匹配逻辑**: 使用 schema 文件中定义的 `primary_key` 来匹配源数据与目标表中的记录。支持单一主键和复合主键。
*   **更新操作 (WHEN MATCHED)**: 当源数据记录的主键在目标表中找到时，使用源数据记录的值更新目标表中对应的记录。所有非主键字段都应被更新。
*   **插入操作 (WHEN NOT MATCHED)**: 当源数据记录的主键在目标表中未找到时，将该条新记录完整插入到目标表中。

**3.4 数据源处理 (Data Source Handling)**
*   系统必须接受 Pandas DataFrame 作为输入源数据。
*   为保证性能，应将源 DataFrame 注册为 DuckDB 的临时视图（View），而不是逐行迭代。

**4.0 技术栈与环境约束 (Technical Stack & Constraints)**

*   **数据库**: DuckDB
*   **编程语言**: Python (版本 3.8+)
*   **核心库**:
    *   `duckdb`: 用于数据库连接和操作。
    *   `pandas`: 用于处理源数据。
    *   `toml`: 用于解析 schema 配置文件。
    *   `python-box`: 用于以点符号（dot notation）的便捷方式访问解析后的 schema 配置。
*   **Schema 配置文件格式**: TOML

**5.0 输入与输出规范 (Input/Output Specification)**

**5.1 输入 1: Schema 配置文件 (`schema.toml`)**
*   文件格式为 TOML。
*   每个表是一个独立的 TOML table (e.g., `[products]`)。
*   每个表的配置必须包含以下键：
    *   `table_name` (string): 数据库中的目标表名。
    *   `primary_key` (list of strings): 用于匹配记录的一个或多个主键字段名。
    *   `columns` (list of strings): 表中所有字段的完整列表，顺序无关。

*   **示例 `schema.toml`**:
    ```toml
    [products]
    table_name = "products"
    primary_key = ["product_id"]
    columns = [
      "product_id", "product_name", "category", "price", "last_updated"
    ]

    [sales_records]
    table_name = "sales_records"
    primary_key = ["record_id"]
    columns = [
      "record_id", "product_id", "sale_date", "quantity", "total_amount" 
      # ... and 65+ other columns
    ]
    ```

**5.2 输入 2: 源数据 (Source Pandas DataFrame)**
*   一个 Pandas DataFrame 对象。
*   DataFrame 的列名必须与 `schema.toml` 中对应表的 `columns` 列表中的字段名完全匹配。

**5.3 输出: 更新后的 DuckDB 表**
*   操作的最终结果是 DuckDB 中目标表的数据被正确地更新和插入。无直接文件或对象返回，副作用发生在数据库中。

**6.0 系统核心逻辑流程 (Core System Logic Flow)**

1.  **初始化**: 建立到 DuckDB 的连接。
2.  **加载配置**: 读取 `schema.toml` 文件，使用 `toml` 库解析，并使用 `python-box` 将其转换为 `Box` 对象以便于访问。
3.  **准备数据**: 接收一个 Pandas DataFrame，并使用 `duckdb.register()` 方法将其注册为一个唯一的临时视图。
4.  **构建 MERGE 语句**:
    *   从 `Box` 对象中提取 `table_name`, `primary_key`, 和 `columns`。
    *   构建 `ON` 子句：`t.pk1 = s.pk1 AND t.pk2 = s.pk2 ...`
    *   构建 `UPDATE SET` 子句：`col1 = s.col1, col2 = s.col2 ...` (针对所有非主键列)。
    *   构建 `INSERT` 子句：`INSERT (col1, pk1, ...)` 和 `VALUES (s.col1, s.pk1, ...)` (针对所有列)。
    *   将上述部分组装成一个完整的 `MERGE` SQL 语句字符串。
5.  **执行**: 执行动态生成的 `MERGE` 语句。
6.  **清理**: (可选) 操作完成后，注销临时视图。

**7.0 非功能性需求 (Non-Functional Requirements)**

*   **性能 (Performance)**: 必须将所有数据密集型操作（连接、比较、更新、插入）委托给 DuckDB 数据库引擎执行。严禁在 Python 中对数据进行行级循环处理。
*   **灵活性与可维护性 (Flexibility & Maintainability)**: 系统的设计必须确保在添加新表或修改现有表结构时，开发者只需修改 `schema.toml` 文件，而无需触碰 Python 业务逻辑代码。

**8.0 验收标准 (Acceptance Criteria)**

1.  **记录更新**: 当源 DataFrame 中的一条记录（基于主键）已存在于目标表中时，该记录在目标表中的所有非主键字段被成功更新为 DataFrame 中的值。
2.  **记录插入**: 当源 DataFrame 中的一条记录（基于主键）不存在于目标表中时，该记录被成功插入到目标表中。
3.  **记录保留**: 目标表中存在但源 DataFrame 中不存在的记录，保持原样，不受影响。
4.  **通用性验证**: 编写的通用函数可以成功地对 `schema.toml` 中定义的任意一个表（例如，先对 `products` 表，再对 `sales_records` 表）执行增量加载，而无需任何代码修改。
5.  **复合主键支持**: 如果一个表的 `primary_key` 包含多个字段，合并逻辑依然能正确工作。