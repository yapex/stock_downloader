#!/usr/bin/env python3
"""测试 PyArrow write_to_dataset 在无分区情况下的行为"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
import shutil
from pathlib import Path

def test_write_to_dataset_behavior():
    """测试 write_to_dataset 在有分区和无分区情况下的行为差异"""
    
    # 创建测试数据
    data = pd.DataFrame({
        'ts_code': ['000001.SZ', '000002.SZ', '600000.SH'],
        'name': ['平安银行', '万科A', '浦发银行'],
        'industry': ['银行', '房地产', '银行'],
        'list_date': ['19910403', '19910129', '19990810']
    })
    
    table = pa.Table.from_pandas(data)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        print("=== 测试1: 无分区列的情况 ===")
        no_partition_path = temp_path / "no_partition"
        pq.write_to_dataset(
            table,
            root_path=str(no_partition_path),
            partition_cols=[],  # 空分区列
            basename_template="part-{i}-test.parquet",
        )
        
        print(f"无分区写入结果:")
        for item in no_partition_path.rglob('*'):
            print(f"  {item.relative_to(temp_path)}")
            if item.is_file():
                print(f"    文件类型: {item.suffix or '(无扩展名)'}")
                print(f"    文件大小: {item.stat().st_size} bytes")
        
        print("\n=== 测试2: 有分区列的情况 ===")
        # 添加分区列
        data_with_partition = data.copy()
        data_with_partition['year'] = '2024'
        table_with_partition = pa.Table.from_pandas(data_with_partition)
        
        with_partition_path = temp_path / "with_partition"
        pq.write_to_dataset(
            table_with_partition,
            root_path=str(with_partition_path),
            partition_cols=['year'],
            basename_template="part-{i}-test.parquet",
        )
        
        print(f"有分区写入结果:")
        for item in with_partition_path.rglob('*'):
            print(f"  {item.relative_to(temp_path)}")
            if item.is_file():
                print(f"    文件类型: {item.suffix or '(无扩展名)'}")
                print(f"    文件大小: {item.stat().st_size} bytes")
        
        print("\n=== 测试3: 验证无分区文件的读取 ===")
        # 找到无分区情况下生成的文件
        files = list(no_partition_path.rglob('*'))
        data_files = [f for f in files if f.is_file()]
        
        if data_files:
            test_file = data_files[0]
            print(f"尝试读取文件: {test_file}")
            
            try:
                # 尝试作为 parquet 文件读取
                df = pd.read_parquet(test_file)
                print(f"成功读取 {len(df)} 行数据:")
                print(df.head())
            except Exception as e:
                print(f"读取失败: {e}")
        
        print("\n=== 测试4: 模拟当前代码的行为 ===")
        # 模拟当前 stock_basic 的写入行为
        stock_basic_path = temp_path / "stock_basic"
        
        # 删除目录（如果存在）
        if stock_basic_path.exists():
            shutil.rmtree(stock_basic_path)
        
        # 写入数据
        pq.write_to_dataset(
            table,
            root_path=str(stock_basic_path),
            partition_cols=[],  # 无分区
            basename_template=f"part-{{i}}-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.parquet",
        )
        
        print(f"模拟 stock_basic 写入结果:")
        for item in stock_basic_path.rglob('*'):
            print(f"  {item.relative_to(temp_path)}")
            if item.is_file():
                print(f"    文件类型: {item.suffix or '(无扩展名)'}")
                # 使用 file 命令检查文件类型
                import subprocess
                try:
                    result = subprocess.run(['file', str(item)], capture_output=True, text=True)
                    print(f"    file命令输出: {result.stdout.strip()}")
                except:
                    print(f"    无法执行file命令")

if __name__ == "__main__":
    test_write_to_dataset_behavior()
