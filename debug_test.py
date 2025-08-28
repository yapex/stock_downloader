#!/usr/bin/env python3

import pandas as pd
from unittest.mock import Mock
from src.neo.data_processor.full_replace_data_processor import FullReplaceDataProcessor
from src.neo.writers.interfaces import IParquetWriter
from src.neo.database.interfaces import IDBQueryer
from src.neo.database.schema_loader import SchemaLoader

# 创建 mock 对象
mock_parquet_writer = Mock(spec=IParquetWriter)
mock_parquet_writer.write_full_replace = Mock(return_value=None)
mock_parquet_writer.base_path = "/tmp/test"

mock_db_queryer = Mock(spec=IDBQueryer)
mock_schema_loader = Mock(spec=SchemaLoader)

# 创建处理器
processor = FullReplaceDataProcessor(
    parquet_writer=mock_parquet_writer,
    db_queryer=mock_db_queryer,
    schema_loader=mock_schema_loader
)

# 创建测试数据
df = pd.DataFrame({
    'ts_code': ['000001.SZ', '000002.SZ'],
    'symbol': ['000001', '000002'],
    'name': ['平安银行', '万科A']
})

print("Testing _write_to_temp_table...")
try:
    result = processor._write_to_temp_table('test_temp', df)
    print(f"_write_to_temp_table result: {result}")
    print(f"write_full_replace called: {mock_parquet_writer.write_full_replace.called}")
    if mock_parquet_writer.write_full_replace.called:
        print(f"call_args: {mock_parquet_writer.write_full_replace.call_args}")
except Exception as e:
    print(f"Exception in _write_to_temp_table: {e}")
    import traceback
    traceback.print_exc()

print("\nTesting process method...")
try:
    result = processor.process('stock_basic', df)
    print(f"process result: {result}")
except Exception as e:
    print(f"Exception in process: {e}")
    import traceback
    traceback.print_exc()