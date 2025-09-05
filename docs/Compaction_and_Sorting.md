您提出的问题非常实际，是数据工程中一个经典且常见的问题：“如何在数据持续、乱序到达的情况下，维持数据仓库的查询性能？”

全部下载完再在内存里排序，确实不是一个可行的方案。好消息是，完全**不需要**这样做。我们可以在数据文件已经落盘后，再进行高效的“离线”排序和优化。

这个过程通常被称为**“数据重整” (Compaction and Sorting)**。

### 最佳实践：使用 DuckDB 自身进行高效的核外排序 (Out-of-Core Sorting)

最优雅、最高效的方法，就是利用 DuckDB 强大的 `COPY` 命令来完成这个任务。DuckDB 的 `COPY` 命令支持流式处理，这意味着它可以在处理远超内存大小的数据时，只占用极少的内存。

这个过程的思路是：

1.  用 DuckDB 读取所有乱序、零散的 Parquet 文件。
2.  告诉 DuckDB 在读取流中对数据进行排序。
3.  让 DuckDB 将排序后的结果，重新写回分区化的 Parquet 文件中。

整个过程是“核外”的（Out-of-Core），DuckDB 会智能地使用临时磁盘空间来处理超过内存容量的数据，你完全不用担心内存爆炸。

---

### 行动方案：创建一个数据维护脚本

你可以创建一个独立的 Python 脚本（例如 `scripts/compact_and_sort.py`），定期运行它（比如每天凌晨）来整理前一天的数据，或者在需要时手动运行。

以下是这个脚本的核心逻辑和代码实现：

**`scripts/compact_and_sort.py`:**

```python
import duckdb
import os

# 定义数据存储的根目录
DATA_DIR = "data/parquet"
# 定义一个临时目录，用于存放重写后的数据
TEMP_DIR = "data/parquet_temp"

# 需要进行排序和重整的表名列表
# 你可以只选择性地对大表进行操作
TABLES_TO_OPTIMIZE = [
    "income",
    "daily_basic",
    "balance_sheet",
    "cash_flow",
    # ... 添加其他你需要优化的表
]

def optimize_table(con, table_name):
    """
    对单个表的数据进行排序、分区和重写。
    """
    source_path = os.path.join(DATA_DIR, table_name)
    target_path = os.path.join(TEMP_DIR, table_name)

    if not os.path.exists(source_path):
        print(f"源目录不存在，跳过表: {table_name}")
        return

    print(f"开始优化表: {table_name}...")

    # 这是核心命令
    # 1. 从源路径读取所有 Parquet 文件
    # 2. 按照 year 和 ts_code 排序 (这是关键!)
    # 3. 将排序后的结果以分区的形式写出到目标路径
    sql_command = f"""
    COPY (
        SELECT *
        FROM read_parquet('{source_path}/**/*.parquet', hive_partitioning=0) -- 先读所有文件
        ORDER BY year, ts_code -- 在数据流中进行排序
    )
    TO '{target_path}'
    (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_IGNORE 1);
    """

    try:
        con.execute(sql_command)
        print(f"表 {table_name} 成功优化到临时目录。")

        # 验证成功后，用优化后的数据替换旧数据
        # 这是一个原子操作，非常快
        print(f"正在替换旧数据: {source_path}")
        os.rename(source_path, source_path + "_backup") # 先备份
        os.rename(target_path, source_path) # 将新数据移到原位
        print(f"数据替换完成。")
        # 你可以稍后手动删除 _backup 目录，或者写脚本自动清理
        # import shutil; shutil.rmtree(source_path + "_backup")

    except Exception as e:
        print(f"优化表 {table_name} 时发生错误: {e}")
        # 如果出错，清理临时目录
        if os.path.exists(target_path):
            import shutil; shutil.rmtree(target_path)


if __name__ == "__main__":
    # 使用一个内存数据库来执行这个操作，因为它只是一个“计算引擎”
    # 不需要持久化任何状态
    with duckdb.connect(database=':memory:') as con:
        if not os.path.exists(TEMP_DIR):
            os.makedirs(TEMP_DIR)

        for table in TABLES_TO_OPTIMIZE:
            optimize_table(con, table)

        print("\n所有选定的表已优化完成！")
        # 最后别忘了更新你的元数据视图
        print("请运行 'manual_sync_metadata.py' 来更新视图以指向新的文件。")
```

### 如何执行这个流程

1.  **正常下载数据**：你的下载程序保持不变，继续将零散、乱序的 Parquet 文件写入 `data/parquet/{table_name}/year=XXXX/` 目录。
2.  **定期运行优化脚本**：
    ```bash
    uv run python scripts/compact_and_sort.py
    ```
    这个脚本会：
    a. 读取某个表（如 `income`）下的所有 Parquet 文件。
    b. 高效地对它们的数据进行排序。
    c. 将结果写出为一组全新的、内部有序的 Parquet 文件，同时保持 `year` 分区结构。
    d. 安全地替换掉旧的、无序的文件。
3.  **更新元数据**：优化脚本完成后，Parquet 文件的物理路径和数量可能发生了变化。你需要重新运行 `manual_sync_metadata.py` 脚本来更新 `metadata.db` 中的视图，让它指向这些新的、优化过的文件。
    ```bash
    uv run python scripts/manual_sync_metadata.py
    ```

### 总结：为什么这个方法是优选？

- **低内存占用**：这是最重要的优点。整个过程是流式的，DuckDB 负责处理所有脏活累活，你的脚本内存占用会非常低。
- **解耦**：数据下载过程和数据优化过程完全分离。下载时追求速度，可以并发、乱序；优化时追求规整和性能，定期整理即可。
- **利用专用工具**：你没有手动用 Pandas 去做 `read -> concat -> sort -> write`，而是把这个任务交给了为此而生的专业分析引擎 DuckDB，效率和稳定性都更高。
- **幂等性 (Idempotent)**：由于使用了 `OVERWRITE_OR_IGNORE 1`，这个优化脚本可以反复安全地运行。
- **保留现有架构**：你不需要改变 `metadata.db` 是视图集合的核心架构，只需在数据层增加一个维护步骤，就能获得巨大的性能收益。

这个“下载 -> 离线重整 -> 更新元数据”的模式是现代数据湖架构中非常标准的维护流程，它完美地解决了你所面临的挑战。
