## 目标：完成数据压缩整合时的幂等（upsert）操作
### 支持三张finacial表比较特殊的的去重策略：
- 当两条数据内容相同时，取 update_flag = 1 的那条，表示后更新的
```python
# ibis 的实现
def unique_by_update_flag(table: ibis.Table, key: str = "ts_code") -> ibis.Table:
    # 1. 定义窗口 (Window)
    #    - 按公司和年份进行分组。你需要将 'company_id' 替换为你的表中真正代表公司的字段名。
    #    - 在每个组内，按 update_flag 降序排列（1 在前，0 在后）。
    window = ibis.window(
        group_by=[key, _.year],  # 关键：按业务主键分组
        order_by=ibis.desc("update_flag"),
    )
    # 2. 应用窗口函数添加行号
    #    - 我们在原始表上增加一个临时列 'row_num'。
    #    - 在每个分组中，update_flag 最大的记录（即我们想要的记录），其 row_num 将为 0。
    ranked_table = table.mutate(row_num=ibis.row_number().over(window))
    # 3. 过滤出每个分组的第一行
    #    - 这样就实现了"有 1 用 1，没 1 用 0"的效果。
    return ranked_table.filter(_.row_num == 0)
```

### 合理的结构，单一职责，compat_and_sort.py 只应该是入口，我们需要在 neo.helps 下添加一个新模块 compact_and_sort，里面以良好的结构真实实现业务代码。

### 其它表的去重
通过 schema_loader, 按主键去重

### 验证逻辑
多次下载的数据，在整合后，数据库中原有数据不变，只新增。

### 重要信息
- @stock_schema.toml 数据库的元数据，primary_key 里标明了主键
- 保留现有的逻辑，不要随意删改