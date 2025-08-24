#!/usr/bin/env python3
"""创建数据库表的脚本

使用项目的依赖注入容器获取 DBOperator，根据 schema 配置创建数据库表。
支持选择性删除现有表和创建指定的表。
"""

import sys
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from neo.containers import AppContainer

console = Console()
app = typer.Typer(help="创建数据库表的工具")


def display_results(operation: str, results: dict, success_color: str = "green", fail_color: str = "red") -> None:
    """显示操作结果
    
    Args:
        operation: 操作名称（如"删除"、"创建"）
        results: 操作结果字典 {table_name: success}
        success_color: 成功时的颜色
        fail_color: 失败时的颜色
    """
    successful = [table for table, success in results.items() if success]
    failed = [table for table, success in results.items() if not success]
    
    if successful:
        console.print(f"[{success_color}]成功{operation} {len(successful)} 个表: {', '.join(successful)}[/{success_color}]")
    
    if failed:
        console.print(f"[{fail_color}]{operation}失败 {len(failed)} 个表: {', '.join(failed)}[/{fail_color}]")


@app.command()
def main(
    tables: Optional[List[str]] = typer.Option(
        None,
        "--table",
        "-t",
        help="指定要创建的表名，可多次使用此选项指定多个表。不指定则创建所有表"
    ),
    drop_first: bool = typer.Option(
        False,
        "--drop",
        "-d",
        help="创建表之前先删除现有表"
    ),
    list_tables: bool = typer.Option(
        False,
        "--list",
        "-l",
        help="列出所有可用的表名"
    ),
) -> None:
    """创建数据库表
    
    这个工具会:
    1. 从项目的依赖注入容器获取 DBOperator
    2. 根据 schema 配置创建指定的数据库表
    3. 可选择在创建前删除现有表
    4. 支持创建所有表或指定的表
    
    示例:
         创建所有表: python create_tables.py
         先删除再创建所有表: python create_tables.py --drop
         创建指定表: python create_tables.py --table stock_basic --table stock_daily
         列出所有表: python create_tables.py --list
    """
    try:
        # 获取依赖注入容器和 DBOperator
        from neo.app import container
        db_operator = container.db_operator()
        schema_loader = container.schema_loader()
        
        # 获取所有可用的表名
        all_table_names = schema_loader.get_table_names()
        
        if list_tables:
            # 显示所有可用的表名
            table = Table(title="可用的数据库表")
            table.add_column("表名", style="cyan")
            table.add_column("描述", style="green")
            
            for table_name in all_table_names:
                try:
                    config = schema_loader.get_table_config(table_name)
                    description = config.get("description", "无描述")
                except Exception:
                    description = "无描述"
                table.add_row(table_name, description)
            
            console.print(table)
            return
        
        # 确定要操作的表
        target_tables = tables if tables else all_table_names
        
        # 验证指定的表名是否存在
        if tables:
            invalid_tables = [t for t in tables if t not in all_table_names]
            if invalid_tables:
                console.print(f"[red]错误: 以下表名不存在于 schema 中: {', '.join(invalid_tables)}[/red]")
                console.print(f"[yellow]可用的表名: {', '.join(all_table_names)}[/yellow]")
                raise typer.Exit(1)
        
        console.print(f"[blue]准备操作 {len(target_tables)} 个表: {', '.join(target_tables)}[/blue]")
        
        # 删除表（如果指定）
        if drop_first:
            console.print("[blue]开始删除现有表...[/blue]")
            
            if tables:
                # 只删除指定的表
                drop_results = {}
                for table_name in target_tables:
                    success = db_operator.drop_table(table_name)
                    drop_results[table_name] = success
            else:
                # 删除所有表
                drop_results = db_operator.drop_all_tables()
            
            display_results("删除", drop_results, "green", "yellow")
        
        # 创建表
        console.print("[blue]开始创建表...[/blue]")
        
        if tables:
            # 只创建指定的表
            create_results = {}
            for table_name in target_tables:
                success = db_operator.create_table(table_name)
                create_results[table_name] = success
        else:
            # 创建所有表
            create_results = db_operator.create_all_tables()
        
        display_results("创建", create_results)
        
        # 统计结果
        successful_creates = [table for table, success in create_results.items() if success]
        failed_creates = [table for table, success in create_results.items() if not success]
        
        if failed_creates:
            console.print(f"[red]❌ 部分表创建失败! 成功: {len(successful_creates)}, 失败: {len(failed_creates)}[/red]")
            raise typer.Exit(1)
        else:
            console.print(f"[green]✅ 所有表创建成功! 共创建 {len(successful_creates)} 个表[/green]")
    
    except Exception as e:
        console.print(f"[red]执行过程中发生错误: {e}[/red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
