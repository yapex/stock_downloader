# 描述配置抽离实现计划

## 业务迭代计划

### 迭代 1: 创建字段描述配置文件
- 从 `docs/stock_schema.toml` 中提取所有字段的描述信息
- 创建独立的 `stock_column_descriptions.toml` 配置文件
- 配置文件结构：按表名组织，包含字段名和描述的映射

### 迭代 2: 修改 create_schema.py 支持描述配置
- 在 `generate_combined_schema_toml` 函数中添加描述合并逻辑
- 从 `stock_column_descriptions.toml` 读取描述信息
- 在生成 schema 时自动将描述信息合并到字段定义中

### 迭代 3: 测试和验证
- 运行 `create_schema.py` 生成新的 `stock_schema.toml`
- 验证生成的文件包含完整的描述信息
- 确保与原始 `docs/stock_schema.toml` 描述信息一致

## 技术实现方案

### 架构规范
- 遵循现有的依赖注入模式
- 使用 `typing.Protocol` 定义接口
- 保持现有代码结构和风格一致

### 核心依赖
- 使用 Python 标准库 `toml` 解析配置文件
- 复用现有的 `Box` 对象处理配置

### 接口定义
```python
def load_column_descriptions(file_path: Path) -> Dict[str, Dict[str, str]]:
    """加载字段描述配置文件"""
    
def merge_descriptions_with_schema(
    schema_data: Dict[str, Any], 
    descriptions: Dict[str, Dict[str, str]]
) -> Dict[str, Any]:
    """将描述信息合并到 schema 数据中"""
```

### 实现细节
1. **描述配置文件格式**:
   ```toml
   [stock_basic]
   ts_code = "TS代码"
   symbol = "股票代码"
   name = "股票名称"
   # ... 其他字段
   ```

2. **合并逻辑**:
   - 读取现有的 schema 数据
   - 从描述配置中查找对应的表和字段描述
   - 将描述信息添加到字段的 `desc` 属性中

3. **向后兼容**:
   - 如果描述配置文件不存在，跳过描述合并
   - 如果某个字段没有描述，保持原有结构不变