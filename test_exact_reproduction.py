#!/usr/bin/env python3
"""精确重现 stock_basic 写入行为的测试"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
from pathlib import Path

def reproduce_stock_basic_write():
    """精确重现 ParquetWriter.write_full_replace 的行为"""
    
    # 模拟 stock_basic 数据
    data = pd.DataFrame({
        'ts_code': ['000001.SZ', '000002.SZ', '600000.SH'],
        'symbol': ['000001', '000002', '600000'],
        'name': ['平安银行', '万科A', '浦发银行'],
        'area': ['深圳', '深圳', '上海'],
        'industry': ['银行', '全国地产', '银行'],
        'cnspell': ['payh', 'wka', 'pfyh'],
        'market': ['主板', '主板', '主板'],
        'list_date': ['19910403', '19910129', '19990810'],
        'act_name': ['无实际控制人', '深圳市人民政府国有资产监督管理委员会', '上海国际集团有限公司'],
        'act_ent_type': ['无', '地方国企', '地方国企']
    })
    
    # 模拟 ParquetWriter 的行为
    base_path = Path("test_output")
    task_type = "stock_basic"
    partition_cols = []  # 空分区列
    
    target_path = base_path / task_type
    
    print(f"目标路径: {target_path}")
    print(f"分区列: {partition_cols}")
    print(f"数据形状: {data.shape}")
    
    try:
        # 删除现有数据目录（如果存在）
        if target_path.exists():
            shutil.rmtree(target_path)
            print(f"已删除现有目录: {target_path}")
        
        # 写入新数据 - 精确复制 ParquetWriter.write_full_replace 的逻辑
        table = pa.Table.from_pandas(data)
        
        print(f"表结构: {table.schema}")
        print(f"表行数: {len(table)}")
        
        # 使用与代码中完全相同的参数
        pq.write_to_dataset(
            table,
            root_path=str(target_path),  # 注意这里使用了 str() 转换
            partition_cols=partition_cols,
            basename_template=f"part-{{i}}-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.parquet",
        )
        
        print(f"写入完成")
        
        # 检查生成的文件
        print(f"\n生成的文件结构:")
        if target_path.exists():
            for item in target_path.rglob('*'):
                print(f"  {item}")
                if item.is_file():
                    print(f"    文件类型: {item.suffix or '(无扩展名)'}")
                    print(f"    文件大小: {item.stat().st_size} bytes")
                    
                    # 使用 file 命令检查
                    import subprocess
                    try:
                        result = subprocess.run(['file', str(item)], capture_output=True, text=True)
                        print(f"    file命令: {result.stdout.strip()}")
                    except:
                        print(f"    无法执行file命令")
        
        # 尝试读取生成的文件
        print(f"\n尝试读取生成的文件:")
        files = list(target_path.rglob('*'))
        data_files = [f for f in files if f.is_file()]
        
        for data_file in data_files:
            try:
                df = pd.read_parquet(data_file)
                print(f"  成功读取 {data_file.name}: {len(df)} 行")
            except Exception as e:
                print(f"  读取 {data_file.name} 失败: {e}")
    
    except Exception as e:
        print(f"写入失败: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 清理
        if target_path.exists():
            shutil.rmtree(target_path)

def test_different_scenarios():
    """测试不同情况下的 write_to_dataset 行为"""
    
    data = pd.DataFrame({
        'ts_code': ['000001.SZ', '000002.SZ'],
        'name': ['平安银行', '万科A']
    })
    
    table = pa.Table.from_pandas(data)
    
    scenarios = [
        {
            'name': '场景1: 正常的文件名模板',
            'root_path': 'test1',
            'partition_cols': [],
            'basename_template': 'part-{i}.parquet'
        },
        {
            'name': '场景2: 文件名模板没有扩展名',
            'root_path': 'test2', 
            'partition_cols': [],
            'basename_template': 'part-{i}'  # 没有 .parquet 扩展名
        },
        {
            'name': '场景3: 使用时间戳的文件名',
            'root_path': 'test3',
            'partition_cols': [],
            'basename_template': f"part-{{i}}-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.parquet"
        },
        {
            'name': '场景4: 写入目录而不是文件',
            'root_path': 'test4.parquet',  # 目录名带扩展名
            'partition_cols': [],
            'basename_template': 'data'  # 文件名没有扩展名
        }
    ]
    
    for scenario in scenarios:
        print(f"\n=== {scenario['name']} ===")
        try:
            # 清理之前的测试
            test_path = Path(scenario['root_path'])
            if test_path.exists():
                shutil.rmtree(test_path)
            
            pq.write_to_dataset(
                table,
                root_path=scenario['root_path'],
                partition_cols=scenario['partition_cols'],
                basename_template=scenario['basename_template']
            )
            
            # 检查结果
            for item in test_path.rglob('*'):
                print(f"  生成: {item}")
                if item.is_file():
                    print(f"    扩展名: {item.suffix or '(无)'}")
            
        except Exception as e:
            print(f"  失败: {e}")
        
        finally:
            # 清理
            if test_path.exists():
                shutil.rmtree(test_path)

if __name__ == "__main__":
    print("=== 精确重现测试 ===")
    reproduce_stock_basic_write()
    
    print("\n" + "="*50)
    print("=== 不同场景测试 ===")
    test_different_scenarios()
