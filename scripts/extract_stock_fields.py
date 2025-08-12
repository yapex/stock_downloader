#!/usr/bin/env python3
"""
提取Fetcher接口字段信息的脚本

该脚本通过 fetcher 获取各个方法返回的 DataFrame，
并将结果的字段名、数据样例、统计信息等写入 docs 目录下的文件中。

支持的方法：
- fetch_stock_list: 股票列表
- fetch_daily_history: 日K线数据
- fetch_daily_basic: 每日指标
- fetch_income: 利润表
- fetch_balancesheet: 资产负债表
- fetch_cashflow: 现金流量表
"""

import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
from typing import Dict, Optional, Callable, Any
from dotenv import load_dotenv

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

# 加载环境变量
load_dotenv(project_root / ".env")

from downloader.fetcher_factory import get_singleton
from downloader.config_impl import create_config_manager
from downloader.interfaces.fetcher import IFetcher

class DataFrameAnalyzer:
    """DataFrame字段分析器"""
    
    @staticmethod
    def analyze_dataframe(df: pd.DataFrame, title: str) -> list[str]:
        """分析DataFrame并生成Markdown格式的字段信息
        
        Args:
            df: 要分析的DataFrame
            title: 文档标题
            
        Returns:
            list[str]: Markdown格式的行列表
        """
        output_lines = []
        output_lines.append(f"# {title}")
        output_lines.append("")
        output_lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output_lines.append("")
        
        # 基本统计信息
        output_lines.extend(DataFrameAnalyzer._generate_basic_stats(df))
        
        # 字段列表
        output_lines.extend(DataFrameAnalyzer._generate_field_list(df))
        
        # 字段详细信息
        output_lines.extend(DataFrameAnalyzer._generate_field_details(df))
        
        # 数据预览
        output_lines.extend(DataFrameAnalyzer._generate_data_preview(df))
        
        return output_lines
    
    @staticmethod
    def _generate_basic_stats(df: pd.DataFrame) -> list[str]:
        """生成基本统计信息"""
        lines = []
        lines.append("## 基本统计信息")
        lines.append("")
        lines.append(f"- 总记录数: {len(df)}")
        lines.append(f"- 字段数: {len(df.columns)}")
        lines.append("")
        return lines
    
    @staticmethod
    def _generate_field_list(df: pd.DataFrame) -> list[str]:
        """生成字段列表"""
        lines = []
        lines.append("## 字段列表")
        lines.append("")
        for i, col in enumerate(df.columns, 1):
            lines.append(f"{i}. `{col}`")
        lines.append("")
        return lines
    
    @staticmethod
    def _generate_field_details(df: pd.DataFrame) -> list[str]:
        """生成字段详细信息"""
        lines = []
        lines.append("## 字段详细信息")
        lines.append("")
        
        for col in df.columns:
            lines.append(f"### {col}")
            lines.append("")
            
            # 数据类型
            dtype = str(df[col].dtype)
            lines.append(f"- **数据类型**: {dtype}")
            
            # 非空值数量
            non_null_count = df[col].count()
            null_count = len(df) - non_null_count
            lines.append(f"- **非空值数量**: {non_null_count}")
            if null_count > 0:
                lines.append(f"- **空值数量**: {null_count}")
            
            # 唯一值数量
            unique_count = df[col].nunique()
            lines.append(f"- **唯一值数量**: {unique_count}")
            
            # 数据样例（前5个非空值）
            sample_values = df[col].dropna().head(5).tolist()
            if sample_values:
                lines.append(f"- **数据样例**: {sample_values}")
            
            lines.append("")
        
        return lines
    
    @staticmethod
    def _generate_data_preview(df: pd.DataFrame) -> list[str]:
        """生成数据预览"""
        lines = []
        lines.append("## 数据预览（前5行）")
        lines.append("")
        lines.append("```")
        lines.append(df.head().to_string())
        lines.append("```")
        lines.append("")
        return lines


class FetcherMethodConfig:
    """Fetcher方法配置"""
    
    def __init__(self, name: str, title: str, method_call: Callable[[IFetcher], Optional[pd.DataFrame]], 
                 filename: str, requires_symbol: bool = False):
        self.name = name
        self.title = title
        self.method_call = method_call
        self.filename = filename
        self.requires_symbol = requires_symbol


class FetcherDataExtractor:
    """Fetcher数据提取器"""
    
    def __init__(self, fetcher: IFetcher):
        self.fetcher = fetcher
        self.test_symbol = "000001.SZ"  # 用于测试的股票代码
        self.test_start_date = "20240101"
        self.test_end_date = "20240131"
        # 财务数据使用更大的时间范围，因为财务数据是季度发布
        self.financial_start_date = "20230101"
        self.financial_end_date = "20241231"
    
    def get_method_configs(self) -> list[FetcherMethodConfig]:
        """获取所有方法配置"""
        return [
            FetcherMethodConfig(
                "fetch_stock_list",
                "股票列表字段信息",
                lambda f: f.fetch_stock_list(),
                "股票列表字段信息.md"
            ),
            FetcherMethodConfig(
                 "fetch_daily_history",
                 "日K线数据字段信息",
                 lambda f: f.fetch_daily_history(self.test_symbol, self.test_start_date, self.test_end_date, "qfq"),
                 "日K线数据字段信息.md",
                 requires_symbol=True
             ),
            FetcherMethodConfig(
                "fetch_daily_basic",
                "每日指标字段信息",
                lambda f: f.fetch_daily_basic(self.test_symbol, self.test_start_date, self.test_end_date),
                "每日指标字段信息.md",
                requires_symbol=True
            ),
            FetcherMethodConfig(
                 "fetch_income",
                 "利润表字段信息",
                 lambda f: f.fetch_income(self.test_symbol, self.financial_start_date, self.financial_end_date),
                 "利润表字段信息.md",
                 requires_symbol=True
             ),
             FetcherMethodConfig(
                 "fetch_balancesheet",
                 "资产负债表字段信息",
                 lambda f: f.fetch_balancesheet(self.test_symbol, self.financial_start_date, self.financial_end_date),
                 "资产负债表字段信息.md",
                 requires_symbol=True
             ),
             FetcherMethodConfig(
                 "fetch_cashflow",
                 "现金流量表字段信息",
                 lambda f: f.fetch_cashflow(self.test_symbol, self.financial_start_date, self.financial_end_date),
                 "现金流量表字段信息.md",
                 requires_symbol=True
             )
        ]
    
    def extract_method_data(self, config: FetcherMethodConfig) -> Optional[pd.DataFrame]:
        """提取指定方法的数据"""
        try:
            print(f"正在获取{config.title}...")
            if config.requires_symbol:
                if "财务" in config.title or config.name in ["fetch_income", "fetch_balancesheet", "fetch_cashflow"]:
                    print(f"使用测试股票代码: {self.test_symbol}, 时间范围: {self.financial_start_date} - {self.financial_end_date}")
                else:
                    print(f"使用测试股票代码: {self.test_symbol}, 时间范围: {self.test_start_date} - {self.test_end_date}")
            
            df = config.method_call(self.fetcher)
            
            if df is None or df.empty:
                print(f"警告：{config.name} 未返回数据")
                return None
            
            print(f"成功获取到 {len(df)} 条{config.title}")
            return df
            
        except Exception as e:
            print(f"获取{config.title}时出错: {e}")
            return None
    
    def save_analysis_to_file(self, lines: list[str], filename: str) -> None:
        """保存分析结果到文件"""
        docs_dir = project_root / "docs"
        docs_dir.mkdir(exist_ok=True)
        
        output_file = docs_dir / filename
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        
        print(f"字段信息已保存到: {output_file}")


def main():
    """主函数"""
    try:
        # 创建配置管理器
        config_manager = create_config_manager()
        
        # 获取 fetcher 实例
        fetcher = get_singleton()
        
        # 创建数据提取器
        extractor = FetcherDataExtractor(fetcher)
        
        # 获取所有方法配置
        method_configs = extractor.get_method_configs()
        
        successful_extractions = 0
        
        for config in method_configs:
            print(f"\n{'='*50}")
            print(f"处理方法: {config.name}")
            print(f"{'='*50}")
            
            # 提取数据
            df = extractor.extract_method_data(config)
            
            if df is not None:
                # 分析数据并生成文档
                analysis_lines = DataFrameAnalyzer.analyze_dataframe(df, config.title)
                
                # 保存到文件
                extractor.save_analysis_to_file(analysis_lines, config.filename)
                successful_extractions += 1
            else:
                print(f"跳过 {config.name}")
        
        print(f"\n{'='*50}")
        print(f"提取完成！成功处理 {successful_extractions}/{len(method_configs)} 个方法")
        print(f"{'='*50}")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()