# 分区表架构设计与实现方案

## 1. 背景与问题分析

### 1.1 当前架构问题

当前系统采用**每股票每数据类型一张表**的设计模式：
- 表命名规则：`{data_type}_{stock_code}`（如 `daily_000001_SZ`, `financial_income_600519_SH`）
- 预估表数量：5000只股票 × 7种数据类型 = 35,000张表

**核心问题：**
1. **策略分析查询性能瓶颈**：查询"最近5年ROE平均值高于15%的所有股票"需要扫描数千张表
2. **跨表JOIN复杂**：需要大量UNION ALL操作合并数据
3. **索引效率低**：无法建立跨股票的复合索引
4. **维护成本高**：元数据管理复杂，缓存命中率低

### 1.2 目标查询场景

```sql
-- 典型的策略分析查询
SELECT stock_code, AVG(roe) as avg_roe, AVG(roa) as avg_roa
FROM financial_data 
WHERE end_date >= '2019-01-01' 
  AND report_type = 'A'  -- 年报数据
  AND roe IS NOT NULL
  AND roa IS NOT NULL
GROUP BY stock_code
HAVING AVG(roe) > 0.15 AND AVG(roa) > 0.08
ORDER BY avg_roe DESC
LIMIT 50;
```

## 2. 新架构设计

### 2.1 核心设计原则

1. **按数据类型分区表**：每种数据类型一张大表，按股票代码分区
2. **统一数据模型**：标准化字段命名和数据类型
3. **高效索引策略**：支持多维度查询的复合索引
4. **向后兼容**：保持现有API接口不变

### 2.2 表结构设计

#### 2.2.1 日线数据表

```sql
CREATE TABLE daily_data (
    -- 主键字段
    stock_code VARCHAR(12) NOT NULL,
    trade_date DATE NOT NULL,
    
    -- 价格数据
    open_price DECIMAL(10,3),
    high_price DECIMAL(10,3),
    low_price DECIMAL(10,3),
    close_price DECIMAL(10,3),
    pre_close DECIMAL(10,3),
    
    -- 成交数据
    volume BIGINT,
    amount DECIMAL(15,2),
    
    -- 复权数据
    adj_factor DECIMAL(10,6),
    
    -- 技术指标（预计算）
    ma5 DECIMAL(10,3),
    ma10 DECIMAL(10,3),
    ma20 DECIMAL(10,3),
    ma60 DECIMAL(10,3),
    
    -- 分区键
    partition_key VARCHAR(3) GENERATED ALWAYS AS (SUBSTRING(stock_code, 1, 3)) STORED,
    
    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (stock_code, trade_date)
) PARTITION BY LIST (partition_key);
```

#### 2.2.2 财务数据表（核心优化目标）

```sql
CREATE TABLE financial_data (
    -- 主键字段
    stock_code VARCHAR(12) NOT NULL,
    end_date DATE NOT NULL,
    ann_date DATE,
    report_type VARCHAR(10) NOT NULL, -- Q1,Q2,Q3,A
    
    -- 盈利能力指标
    roe DECIMAL(8,4),              -- 净资产收益率
    roa DECIMAL(8,4),              -- 总资产收益率
    gross_margin DECIMAL(8,4),     -- 毛利率
    net_margin DECIMAL(8,4),       -- 净利率
    operating_margin DECIMAL(8,4), -- 营业利率
    
    -- 成长性指标
    revenue_growth DECIMAL(8,4),   -- 营收增长率
    profit_growth DECIMAL(8,4),    -- 净利润增长率
    eps_growth DECIMAL(8,4),       -- EPS增长率
    
    -- 财务健康指标
    debt_ratio DECIMAL(8,4),       -- 资产负债率
    current_ratio DECIMAL(8,4),    -- 流动比率
    quick_ratio DECIMAL(8,4),      -- 速动比率
    
    -- 估值指标
    pe_ratio DECIMAL(8,2),         -- 市盈率
    pb_ratio DECIMAL(8,2),         -- 市净率
    ps_ratio DECIMAL(8,2),         -- 市销率
    
    -- 现金流指标
    operating_cf DECIMAL(15,2),    -- 经营现金流
    free_cf DECIMAL(15,2),         -- 自由现金流
    
    -- 分区键
    partition_key VARCHAR(3) GENERATED ALWAYS AS (SUBSTRING(stock_code, 1, 3)) STORED,
    
    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (stock_code, end_date, report_type)
) PARTITION BY LIST (partition_key);
```

#### 2.2.3 基本面数据表

```sql
CREATE TABLE fundamental_data (
    stock_code VARCHAR(12) NOT NULL,
    trade_date DATE NOT NULL,
    
    -- 市场数据
    total_mv DECIMAL(15,2),        -- 总市值
    circ_mv DECIMAL(15,2),         -- 流通市值
    turnover_rate DECIMAL(8,4),    -- 换手率
    
    -- 估值数据
    pe DECIMAL(8,2),
    pe_ttm DECIMAL(8,2),
    pb DECIMAL(8,2),
    ps DECIMAL(8,2),
    ps_ttm DECIMAL(8,2),
    
    -- 分区键
    partition_key VARCHAR(3) GENERATED ALWAYS AS (SUBSTRING(stock_code, 1, 3)) STORED,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (stock_code, trade_date)
) PARTITION BY LIST (partition_key);
```

### 2.3 索引策略

#### 2.3.1 财务数据表索引

```sql
-- 1. 策略筛选专用索引
CREATE INDEX idx_financial_strategy_screen 
ON financial_data (roe, roa, end_date, report_type, stock_code)
WHERE roe IS NOT NULL AND roa IS NOT NULL;

-- 2. 时间序列查询索引
CREATE INDEX idx_financial_time_series 
ON financial_data (stock_code, end_date, report_type);

-- 3. 行业对比索引（需要结合股票基础信息）
CREATE INDEX idx_financial_industry_compare 
ON financial_data (end_date, report_type, roe, debt_ratio);

-- 4. 成长性分析索引
CREATE INDEX idx_financial_growth 
ON financial_data (revenue_growth, profit_growth, end_date, stock_code)
WHERE revenue_growth IS NOT NULL AND profit_growth IS NOT NULL;
```

#### 2.3.2 日线数据表索引

```sql
-- 1. 技术分析索引
CREATE INDEX idx_daily_technical 
ON daily_data (stock_code, trade_date, close_price, volume);

-- 2. 价格区间查询索引
CREATE INDEX idx_daily_price_range 
ON daily_data (trade_date, close_price, stock_code);

-- 3. 成交量分析索引
CREATE INDEX idx_daily_volume 
ON daily_data (trade_date, volume, amount, stock_code)
WHERE volume > 0;
```

## 3. 实现方案

### 3.1 数据迁移架构

#### 3.1.1 迁移器基类

```python
from abc import ABC, abstractmethod
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime
import logging

class TableMigrator(ABC):
    """表迁移器基类"""
    
    def __init__(self, source_storage: DuckDBStorage, target_storage: DuckDBStorage):
        self.source_storage = source_storage
        self.target_storage = target_storage
        self.logger = logging.getLogger(self.__class__.__name__)
        
    @abstractmethod
    def get_source_tables(self) -> List[str]:
        """获取需要迁移的源表列表"""
        pass
        
    @abstractmethod
    def create_target_table(self) -> None:
        """创建目标分区表"""
        pass
        
    @abstractmethod
    def transform_data(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """数据转换逻辑"""
        pass
        
    def migrate(self, batch_size: int = 10000) -> Dict[str, Any]:
        """执行迁移"""
        start_time = datetime.now()
        stats = {
            'total_tables': 0,
            'migrated_tables': 0,
            'total_rows': 0,
            'failed_tables': [],
            'start_time': start_time
        }
        
        try:
            # 1. 创建目标表
            self.create_target_table()
            
            # 2. 获取源表列表
            source_tables = self.get_source_tables()
            stats['total_tables'] = len(source_tables)
            
            # 3. 批量迁移
            for table_name in source_tables:
                try:
                    self._migrate_single_table(table_name, batch_size)
                    stats['migrated_tables'] += 1
                except Exception as e:
                    self.logger.error(f"迁移表 {table_name} 失败: {e}")
                    stats['failed_tables'].append(table_name)
                    
        except Exception as e:
            self.logger.error(f"迁移过程失败: {e}")
            raise
            
        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']
        
        return stats
        
    def _migrate_single_table(self, table_name: str, batch_size: int) -> None:
        """迁移单个表"""
        self.logger.info(f"开始迁移表: {table_name}")
        
        # 分批读取源数据
        offset = 0
        while True:
            query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
            df = self.source_storage.conn.execute(query).df()
            
            if df.empty:
                break
                
            # 数据转换
            transformed_df = self.transform_data(df, table_name)
            
            # 写入目标表
            self._insert_to_target(transformed_df)
            
            offset += batch_size
            self.logger.debug(f"已迁移 {offset} 行数据")
            
    @abstractmethod
    def _insert_to_target(self, df: pd.DataFrame) -> None:
        """插入数据到目标表"""
        pass
```

#### 3.1.2 财务数据迁移器

```python
class FinancialDataMigrator(TableMigrator):
    """财务数据迁移器"""
    
    def get_source_tables(self) -> List[str]:
        """获取所有财务相关的源表"""
        all_tables = self.source_storage.list_tables()
        financial_prefixes = ['financial_income', 'financial_balance', 'financial_cashflow']
        
        source_tables = []
        for table in all_tables:
            for prefix in financial_prefixes:
                if table.startswith(prefix):
                    source_tables.append(table)
                    break
                    
        return source_tables
        
    def create_target_table(self) -> None:
        """创建财务数据分区表"""
        create_sql = """
        CREATE TABLE IF NOT EXISTS financial_data (
            stock_code VARCHAR(12) NOT NULL,
            end_date DATE NOT NULL,
            ann_date DATE,
            report_type VARCHAR(10) NOT NULL,
            
            -- 盈利能力指标
            roe DECIMAL(8,4),
            roa DECIMAL(8,4),
            gross_margin DECIMAL(8,4),
            net_margin DECIMAL(8,4),
            operating_margin DECIMAL(8,4),
            
            -- 成长性指标
            revenue_growth DECIMAL(8,4),
            profit_growth DECIMAL(8,4),
            eps_growth DECIMAL(8,4),
            
            -- 财务健康指标
            debt_ratio DECIMAL(8,4),
            current_ratio DECIMAL(8,4),
            quick_ratio DECIMAL(8,4),
            
            -- 估值指标
            pe_ratio DECIMAL(8,2),
            pb_ratio DECIMAL(8,2),
            ps_ratio DECIMAL(8,2),
            
            -- 现金流指标
            operating_cf DECIMAL(15,2),
            free_cf DECIMAL(15,2),
            
            -- 分区键
            partition_key VARCHAR(3) GENERATED ALWAYS AS (SUBSTRING(stock_code, 1, 3)) STORED,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            PRIMARY KEY (stock_code, end_date, report_type)
        );
        """
        
        self.target_storage.conn.execute(create_sql)
        self.logger.info("财务数据分区表创建完成")
        
    def transform_data(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """转换财务数据格式"""
        # 从表名提取股票代码
        stock_code = self._extract_stock_code(table_name)
        
        # 标准化列名映射
        column_mapping = {
            'end_date': 'end_date',
            'ann_date': 'ann_date',
            'f_ann_date': 'ann_date',
            'report_type': 'report_type',
            
            # 盈利能力指标
            'roe': 'roe',
            'roe_waa': 'roe',  # 加权平均ROE
            'roa': 'roa',
            'gross_margin': 'gross_margin',
            'netprofit_margin': 'net_margin',
            'operate_profit_margin': 'operating_margin',
            
            # 成长性指标
            'revenue_yoy': 'revenue_growth',
            'op_yoy': 'profit_growth',
            'eps_yoy': 'eps_growth',
            
            # 财务健康指标
            'debt_to_assets': 'debt_ratio',
            'current_ratio': 'current_ratio',
            'quick_ratio': 'quick_ratio',
            
            # 现金流指标
            'n_cashflow_act': 'operating_cf',
            'fcff': 'free_cf',
        }
        
        # 应用列名映射
        transformed_df = pd.DataFrame()
        transformed_df['stock_code'] = stock_code
        
        for target_col, source_col in column_mapping.items():
            if source_col in df.columns:
                transformed_df[target_col] = df[source_col]
            else:
                transformed_df[target_col] = None
                
        # 数据类型转换
        if 'end_date' in transformed_df.columns:
            transformed_df['end_date'] = pd.to_datetime(transformed_df['end_date'])
        if 'ann_date' in transformed_df.columns:
            transformed_df['ann_date'] = pd.to_datetime(transformed_df['ann_date'])
            
        # 设置默认报告类型
        if 'report_type' not in transformed_df.columns or transformed_df['report_type'].isna().all():
            transformed_df['report_type'] = 'A'  # 默认年报
            
        return transformed_df
        
    def _extract_stock_code(self, table_name: str) -> str:
        """从表名提取股票代码"""
        # 表名格式: financial_income_000001_SZ
        parts = table_name.split('_')
        if len(parts) >= 4:
            return f"{parts[-2]}.{parts[-1]}"
        return "UNKNOWN"
        
    def _insert_to_target(self, df: pd.DataFrame) -> None:
        """插入数据到财务数据表"""
        if df.empty:
            return
            
        # 使用DuckDB的高效插入
        self.target_storage.conn.execute(
            "INSERT INTO financial_data SELECT * FROM df"
        )
```

### 3.2 兼容性适配层

#### 3.2.1 存储适配器

```python
class PartitionedStorageAdapter:
    """分区表存储适配器，提供向后兼容的接口"""
    
    def __init__(self, storage: DuckDBStorage, enable_legacy_mode: bool = False):
        self.storage = storage
        self.enable_legacy_mode = enable_legacy_mode
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def query(self, data_type: str, entity_id: str, columns: List[str] = None) -> pd.DataFrame:
        """兼容原有的查询接口"""
        if self.enable_legacy_mode:
            return self._query_legacy(data_type, entity_id, columns)
        else:
            return self._query_partitioned(data_type, entity_id, columns)
            
    def _query_partitioned(self, data_type: str, entity_id: str, columns: List[str] = None) -> pd.DataFrame:
        """从分区表查询数据"""
        table_mapping = {
            'daily': 'daily_data',
            'daily_basic': 'fundamental_data',
            'financial_income': 'financial_data',
            'financial_balance': 'financial_data',
            'financial_cashflow': 'financial_data'
        }
        
        target_table = table_mapping.get(data_type)
        if not target_table:
            self.logger.warning(f"未知的数据类型: {data_type}")
            return pd.DataFrame()
            
        # 构建查询
        if columns:
            cols_str = ", ".join(columns)
        else:
            cols_str = "*"
            
        query = f"""
        SELECT {cols_str}
        FROM {target_table}
        WHERE stock_code = ?
        ORDER BY 
            CASE 
                WHEN '{target_table}' = 'daily_data' THEN trade_date
                WHEN '{target_table}' = 'financial_data' THEN end_date
                WHEN '{target_table}' = 'fundamental_data' THEN trade_date
            END DESC
        """
        
        return self.storage.conn.execute(query, [entity_id]).df()
        
    def _query_legacy(self, data_type: str, entity_id: str, columns: List[str] = None) -> pd.DataFrame:
        """从传统表结构查询数据"""
        return self.storage.query(data_type, entity_id, columns)
        
    def save(self, df: pd.DataFrame, data_type: str, entity_id: str, date_col: str = None) -> None:
        """兼容原有的保存接口"""
        if self.enable_legacy_mode:
            self.storage.save(df, data_type, entity_id, date_col)
        else:
            self._save_to_partitioned(df, data_type, entity_id, date_col)
            
    def _save_to_partitioned(self, df: pd.DataFrame, data_type: str, entity_id: str, date_col: str = None) -> None:
        """保存数据到分区表"""
        if df.empty:
            return
            
        # 添加股票代码列
        df_copy = df.copy()
        df_copy['stock_code'] = entity_id
        
        table_mapping = {
            'daily': 'daily_data',
            'daily_basic': 'fundamental_data',
            'financial_income': 'financial_data',
            'financial_balance': 'financial_data',
            'financial_cashflow': 'financial_data'
        }
        
        target_table = table_mapping.get(data_type)
        if not target_table:
            self.logger.warning(f"未知的数据类型: {data_type}，回退到传统模式")
            self.storage.save(df, data_type, entity_id, date_col)
            return
            
        # 使用UPSERT语义
        self.storage.conn.execute(f"INSERT OR REPLACE INTO {target_table} SELECT * FROM df_copy")
```

### 3.3 分析查询引擎

#### 3.3.1 策略筛选引擎

```python
class StrategyScreeningEngine:
    """策略筛选引擎"""
    
    def __init__(self, storage: DuckDBStorage):
        self.storage = storage
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def screen_by_financial_metrics(self, 
                                   criteria: Dict[str, Any],
                                   years: int = 5,
                                   report_type: str = 'A') -> pd.DataFrame:
        """基于财务指标筛选股票"""
        
        # 构建WHERE条件
        where_conditions = []
        params = []
        
        # 时间范围
        end_date_threshold = datetime.now().replace(year=datetime.now().year - years)
        where_conditions.append("end_date >= ?")
        params.append(end_date_threshold.strftime('%Y-%m-%d'))
        
        # 报告类型
        where_conditions.append("report_type = ?")
        params.append(report_type)
        
        # 动态构建筛选条件
        for metric, condition in criteria.items():
            if isinstance(condition, dict):
                if 'min' in condition:
                    where_conditions.append(f"{metric} >= ?")
                    params.append(condition['min'])
                if 'max' in condition:
                    where_conditions.append(f"{metric} <= ?")
                    params.append(condition['max'])
            else:
                where_conditions.append(f"{metric} >= ?")
                params.append(condition)
                
        where_clause = " AND ".join(where_conditions)
        
        # 构建查询
        query = f"""
        WITH financial_metrics AS (
            SELECT 
                stock_code,
                COUNT(*) as report_count,
                AVG(roe) as avg_roe,
                AVG(roa) as avg_roa,
                AVG(gross_margin) as avg_gross_margin,
                AVG(net_margin) as avg_net_margin,
                AVG(debt_ratio) as avg_debt_ratio,
                AVG(current_ratio) as avg_current_ratio,
                STDDEV(roe) as roe_volatility,
                MIN(roe) as min_roe,
                MAX(roe) as max_roe
            FROM financial_data
            WHERE {where_clause}
              AND roe IS NOT NULL
            GROUP BY stock_code
            HAVING COUNT(*) >= ?
        )
        SELECT 
            fm.*,
            -- 计算稳定性评分
            CASE 
                WHEN fm.roe_volatility = 0 THEN 100
                ELSE GREATEST(0, 100 - (fm.roe_volatility / fm.avg_roe * 100))
            END as stability_score
        FROM financial_metrics fm
        ORDER BY fm.avg_roe DESC, stability_score DESC
        """
        
        # 添加最小报告数量参数
        params.append(max(1, years - 1))  # 至少要有years-1个报告期
        
        try:
            result = self.storage.conn.execute(query, params).df()
            self.logger.info(f"筛选出 {len(result)} 只符合条件的股票")
            return result
        except Exception as e:
            self.logger.error(f"财务指标筛选失败: {e}")
            return pd.DataFrame()
            
    def screen_by_technical_indicators(self, 
                                     criteria: Dict[str, Any],
                                     days: int = 252) -> pd.DataFrame:
        """基于技术指标筛选股票"""
        
        # 计算技术指标的查询
        query = f"""
        WITH technical_metrics AS (
            SELECT 
                stock_code,
                COUNT(*) as trading_days,
                AVG(close_price) as avg_price,
                STDDEV(close_price) as price_volatility,
                AVG(volume) as avg_volume,
                SUM(amount) as total_amount,
                
                -- 价格趋势
                (LAST_VALUE(close_price) OVER (PARTITION BY stock_code ORDER BY trade_date 
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - 
                 FIRST_VALUE(close_price) OVER (PARTITION BY stock_code ORDER BY trade_date 
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / 
                FIRST_VALUE(close_price) OVER (PARTITION BY stock_code ORDER BY trade_date 
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) * 100 as price_change_pct,
                
                -- 成交活跃度
                AVG(volume * close_price) as avg_turnover
                
            FROM daily_data
            WHERE trade_date >= CURRENT_DATE - INTERVAL '{days} days'
            GROUP BY stock_code
            HAVING COUNT(*) >= ?
        )
        SELECT *
        FROM technical_metrics
        WHERE 1=1
        """
        
        # 添加筛选条件
        where_conditions = []
        params = [max(1, days // 2)]  # 至少要有一半的交易日数据
        
        for metric, condition in criteria.items():
            if isinstance(condition, dict):
                if 'min' in condition:
                    where_conditions.append(f"{metric} >= ?")
                    params.append(condition['min'])
                if 'max' in condition:
                    where_conditions.append(f"{metric} <= ?")
                    params.append(condition['max'])
            else:
                where_conditions.append(f"{metric} >= ?")
                params.append(condition)
                
        if where_conditions:
            query += " AND " + " AND ".join(where_conditions)
            
        query += " ORDER BY price_change_pct DESC"
        
        try:
            result = self.storage.conn.execute(query, params).df()
            self.logger.info(f"技术指标筛选出 {len(result)} 只股票")
            return result
        except Exception as e:
            self.logger.error(f"技术指标筛选失败: {e}")
            return pd.DataFrame()
```

## 4. 性能测试方案

### 4.1 基准测试

```python
class PerformanceBenchmark:
    """性能基准测试"""
    
    def __init__(self, legacy_storage: DuckDBStorage, partitioned_storage: DuckDBStorage):
        self.legacy_storage = legacy_storage
        self.partitioned_storage = partitioned_storage
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def benchmark_financial_screening(self, iterations: int = 10) -> Dict[str, Any]:
        """财务筛选性能测试"""
        
        test_criteria = {
            'roe': 0.15,
            'roa': 0.08,
            'debt_ratio': {'max': 0.6}
        }
        
        # 测试传统架构
        legacy_times = []
        for i in range(iterations):
            start_time = time.time()
            self._legacy_financial_screening(test_criteria)
            legacy_times.append(time.time() - start_time)
            
        # 测试分区表架构
        partitioned_times = []
        screening_engine = StrategyScreeningEngine(self.partitioned_storage)
        for i in range(iterations):
            start_time = time.time()
            screening_engine.screen_by_financial_metrics(test_criteria)
            partitioned_times.append(time.time() - start_time)
            
        return {
            'legacy_avg_time': sum(legacy_times) / len(legacy_times),
            'partitioned_avg_time': sum(partitioned_times) / len(partitioned_times),
            'performance_improvement': (sum(legacy_times) / len(legacy_times)) / (sum(partitioned_times) / len(partitioned_times)),
            'legacy_times': legacy_times,
            'partitioned_times': partitioned_times
        }
        
    def _legacy_financial_screening(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """传统架构的财务筛选（模拟）"""
        # 模拟传统架构需要扫描大量表的过程
        all_tables = self.legacy_storage.list_tables()
        financial_tables = [t for t in all_tables if t.startswith('financial_')]
        
        results = []
        for table in financial_tables[:100]:  # 限制测试规模
            try:
                df = self.legacy_storage.conn.execute(f"SELECT * FROM {table}").df()
                # 应用筛选条件
                filtered_df = self._apply_criteria(df, criteria)
                if not filtered_df.empty:
                    results.append(filtered_df)
            except Exception:
                continue
                
        return pd.concat(results, ignore_index=True) if results else pd.DataFrame()
        
    def _apply_criteria(self, df: pd.DataFrame, criteria: Dict[str, Any]) -> pd.DataFrame:
        """应用筛选条件"""
        mask = pd.Series([True] * len(df))
        
        for metric, condition in criteria.items():
            if metric not in df.columns:
                continue
                
            if isinstance(condition, dict):
                if 'min' in condition:
                    mask &= (df[metric] >= condition['min'])
                if 'max' in condition:
                    mask &= (df[metric] <= condition['max'])
            else:
                mask &= (df[metric] >= condition)
                
        return df[mask]
```

### 4.2 压力测试

```python
class StressTest:
    """压力测试"""
    
    def __init__(self, storage: DuckDBStorage):
        self.storage = storage
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def concurrent_query_test(self, num_threads: int = 10, queries_per_thread: int = 100) -> Dict[str, Any]:
        """并发查询压力测试"""
        import threading
        import queue
        
        results_queue = queue.Queue()
        
        def worker():
            screening_engine = StrategyScreeningEngine(self.storage)
            thread_times = []
            
            for _ in range(queries_per_thread):
                start_time = time.time()
                try:
                    criteria = {
                        'roe': random.uniform(0.1, 0.2),
                        'roa': random.uniform(0.05, 0.15)
                    }
                    screening_engine.screen_by_financial_metrics(criteria)
                    thread_times.append(time.time() - start_time)
                except Exception as e:
                    self.logger.error(f"查询失败: {e}")
                    
            results_queue.put(thread_times)
            
        # 启动线程
        threads = []
        start_time = time.time()
        
        for _ in range(num_threads):
            thread = threading.Thread(target=worker)
            thread.start()
            threads.append(thread)
            
        # 等待完成
        for thread in threads:
            thread.join()
            
        total_time = time.time() - start_time
        
        # 收集结果
        all_times = []
        while not results_queue.empty():
            thread_times = results_queue.get()
            all_times.extend(thread_times)
            
        return {
            'total_queries': len(all_times),
            'total_time': total_time,
            'avg_query_time': sum(all_times) / len(all_times) if all_times else 0,
            'queries_per_second': len(all_times) / total_time if total_time > 0 else 0,
            'min_query_time': min(all_times) if all_times else 0,
            'max_query_time': max(all_times) if all_times else 0
        }
```

## 5. 部署与配置

### 5.1 配置文件扩展

```yaml
# config.yaml
storage:
  # 架构模式
  architecture: "partitioned"  # legacy | partitioned | hybrid
  
  # 迁移配置
  migration:
    enabled: true
    batch_size: 10000
    backup_before_migration: true
    
  # 分区配置
  partitioning:
    strategy: "by_stock_prefix"  # by_stock_prefix | by_market | by_industry
    partition_size: 1000  # 每个分区的预期表数量
    
  # 性能优化
  performance:
    enable_query_cache: true
    cache_size_mb: 512
    enable_parallel_queries: true
    max_parallel_workers: 4
    
  # 兼容性
  compatibility:
    enable_legacy_api: true
    legacy_fallback: true
    
# 分析引擎配置
analytics:
  screening:
    default_years: 5
    min_data_points: 3
    enable_caching: true
    cache_ttl_minutes: 30
```

### 5.2 部署脚本

```python
#!/usr/bin/env python3
"""分区表架构部署脚本"""

import argparse
import logging
from pathlib import Path
from src.downloader.storage import DuckDBStorage
from src.downloader.migration import FinancialDataMigrator, DailyDataMigrator

def main():
    parser = argparse.ArgumentParser(description='部署分区表架构')
    parser.add_argument('--source-db', required=True, help='源数据库路径')
    parser.add_argument('--target-db', required=True, help='目标数据库路径')
    parser.add_argument('--data-types', nargs='+', default=['financial', 'daily'], 
                       help='要迁移的数据类型')
    parser.add_argument('--batch-size', type=int, default=10000, help='批处理大小')
    parser.add_argument('--backup', action='store_true', help='迁移前备份')
    parser.add_argument('--verify', action='store_true', help='迁移后验证')
    
    args = parser.parse_args()
    
    # 配置日志
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    try:
        # 初始化存储
        source_storage = DuckDBStorage(args.source_db)
        target_storage = DuckDBStorage(args.target_db)
        
        # 备份
        if args.backup:
            backup_path = f"{args.source_db}.backup.{int(time.time())}"
            logger.info(f"备份数据库到: {backup_path}")
            shutil.copy2(args.source_db, backup_path)
            
        # 执行迁移
        migrators = {
            'financial': FinancialDataMigrator(source_storage, target_storage),
            'daily': DailyDataMigrator(source_storage, target_storage)
        }
        
        for data_type in args.data_types:
            if data_type in migrators:
                logger.info(f"开始迁移 {data_type} 数据")
                migrator = migrators[data_type]
                stats = migrator.migrate(args.batch_size)
                logger.info(f"{data_type} 迁移完成: {stats}")
            else:
                logger.warning(f"未知的数据类型: {data_type}")
                
        # 验证
        if args.verify:
            logger.info("开始验证迁移结果")
            verify_migration(source_storage, target_storage)
            
        logger.info("分区表架构部署完成")
        
    except Exception as e:
        logger.error(f"部署失败: {e}")
        raise
        
def verify_migration(source_storage: DuckDBStorage, target_storage: DuckDBStorage):
    """验证迁移结果"""
    # 实现验证逻辑
    pass
    
if __name__ == '__main__':
    main()
```

## 6. 监控与维护

### 6.1 性能监控

```python
class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self, storage: DuckDBStorage):
        self.storage = storage
        self.metrics = {}
        
    def collect_query_metrics(self) -> Dict[str, Any]:
        """收集查询性能指标"""
        # 查询统计
        query_stats = self.storage.conn.execute("""
            SELECT 
                COUNT(*) as total_queries,
                AVG(query_time) as avg_query_time,
                MAX(query_time) as max_query_time
            FROM query_log 
            WHERE timestamp >= NOW() - INTERVAL '1 hour'
        """).fetchone()
        
        # 表统计
        table_stats = self.storage.conn.execute("""
            SELECT 
                table_name,
                COUNT(*) as row_count,
                pg_size_pretty(pg_total_relation_size(table_name)) as table_size
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """).fetchall()
        
        return {
            'query_stats': query_stats,
            'table_stats': table_stats,
            'timestamp': datetime.now()
        }
        
    def generate_performance_report(self) -> str:
        """生成性能报告"""
        metrics = self.collect_query_metrics()
        
        report = f"""
        # 数据库性能报告
        
        ## 查询性能
        - 总查询数: {metrics['query_stats'][0]}
        - 平均查询时间: {metrics['query_stats'][1]:.2f}ms
        - 最大查询时间: {metrics['query_stats'][2]:.2f}ms
        
        ## 表统计
        """
        
        for table_name, row_count, table_size in metrics['table_stats']:
            report += f"- {table_name}: {row_count:,} 行, {table_size}\n"
            
        return report
```

## 7. 总结

### 7.1 预期收益

1. **查询性能提升**: 复杂分析查询从分钟级降至秒级
2. **存储效率**: 减少元数据开销，提升压缩率
3. **维护成本**: 表数量从35,000降至10以内
4. **扩展性**: 支持更复杂的多维度分析

### 7.2 风险控制

1. **渐进式迁移**: 支持legacy和partitioned模式并存
2. **数据备份**: 迁移前自动备份
3. **回滚机制**: 支持快速回滚到原架构
4. **兼容性保证**: 保持现有API接口不变

### 7.3 实施计划

- **第1周**: 实现迁移工具和兼容性适配层
- **第2周**: 小规模数据迁移测试
- **第3周**: 性能测试和优化
- **第4周**: 全量迁移和上线

这个方案将为量化分析提供强大的数据基础，显著提升策略研究的效率。