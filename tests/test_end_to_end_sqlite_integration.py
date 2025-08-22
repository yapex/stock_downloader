#!/usr/bin/env python3
"""端到端 SQLite 集成测试

测试从任务提交到消费的完整流程：
1. SimpleDownloader 提交任务到 HueyTaskBus
2. process_task_result 消费任务并处理数据
3. 数据正确写入数据库

使用依赖注入和 Mock 对象，避免使用 patch。
"""

import tempfile
import sqlite3
import pandas as pd
from pathlib import Path
from unittest.mock import Mock

from neo.task_bus.huey_task_bus import HueyTaskBus
from neo.task_bus.types import DownloadTaskConfig, TaskType, TaskPriority
from neo.downloader.simple_downloader import SimpleDownloader
from neo.data_processor.simple_data_processor import SimpleDataProcessor
from neo.database.operator import DBOperator


class TestEndToEndSqliteIntegration:
    """端到端 SQLite 集成测试类"""

    def setup_method(self):
        """每个测试方法执行前的设置"""
        # 创建临时 Huey SQLite 数据库
        self.huey_temp_db = tempfile.NamedTemporaryFile(suffix="_huey.db", delete=False)
        self.huey_temp_db_path = self.huey_temp_db.name
        self.huey_temp_db.close()

        # 创建测试用的数据库操作器（使用内存数据库）
        from src.neo.database.connection import get_memory_conn

        self.test_db_operator = DBOperator(conn=get_memory_conn)

        # 创建必要的数据库表
        self.test_db_operator.create_table("stock_basic")

        # 创建测试用的数据处理器，禁用批量处理模式以便立即保存数据
        self.test_data_processor = SimpleDataProcessor(
            db_operator=self.test_db_operator, enable_batch=False
        )

        # 创建测试用的配置对象
        self.test_config = Mock()
        self.test_config.huey = Mock()
        self.test_config.huey.db_file = str(self.huey_temp_db_path)
        self.test_config.huey.immediate = True  # 使用同步模式进行测试

        # 创建测试用的任务总线
        self.test_task_bus = HueyTaskBus(
            config=self.test_config, data_processor=self.test_data_processor
        )

    def teardown_method(self):
        """每个测试方法执行后的清理"""
        # 清理临时文件
        temp_file = Path(self.huey_temp_db_path)
        if temp_file.exists():
            temp_file.unlink()

    def get_huey_queue_task_count(self):
        """获取 Huey 队列中的任务数量"""

        try:
            with sqlite3.connect(self.huey_temp_db_path) as conn:
                cursor = conn.cursor()
                # 检查表是否存在
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='task'"
                )
                if not cursor.fetchone():
                    return 0
                # 查询任务数量
                cursor.execute("SELECT COUNT(*) FROM task")
                return cursor.fetchone()[0]
        except Exception:
            return 0

    def get_data_table_row_count(self, table_name: str) -> int:
        """获取数据表中的行数"""
        try:
            from src.neo.database.connection import get_memory_conn

            with get_memory_conn() as conn:
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                return result[0] if result else 0
        except Exception:
            return 0

    def execute_huey_tasks(self):
        """执行 Huey 队列中的任务"""
        executed_count = 0
        max_attempts = 10  # 减少最大尝试次数

        for _ in range(max_attempts):
            task = self.test_huey.dequeue()
            if task is None:
                break

            try:
                task.execute()
                executed_count += 1
            except Exception:
                break

        return executed_count

    def test_end_to_end_task_flow(self):
        """测试端到端任务流程：提交 -> 队列 -> 消费 -> 数据库"""
        # 准备测试数据
        test_data = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "symbol": ["000001"],
                "name": ["平安银行"],
                "area": ["深圳"],
                "industry": ["银行"],
                "cnspell": ["payh"],
                "market": ["主板"],
                "list_date": ["19910403"],
                "act_name": ["中国平安保险（集团）股份有限公司"],
                "act_ent_type": ["境内非国有法人"],
            }
        )

        # 创建 Mock FetcherBuilder
        mock_fetcher = Mock(return_value=test_data)
        mock_builder_instance = Mock()
        mock_builder_instance.build_by_task.return_value = mock_fetcher

        # 创建下载器，注入测试任务总线和 Mock FetcherBuilder
        downloader = SimpleDownloader(task_bus=self.test_task_bus)
        downloader.fetcher_builder = mock_builder_instance

        # 创建下载任务配置
        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.stock_basic,
            priority=TaskPriority.HIGH,
            max_retries=3,
        )

        # 步骤1：执行下载任务，应该提交到队列
        initial_queue_count = self.get_huey_queue_task_count()

        result = downloader.download(config)

        # 验证下载成功
        assert result.success is True
        assert result.data is not None
        assert not result.data.empty

        # 在 immediate 模式下，任务会立即执行，不会放入队列
        # 所以我们直接验证数据是否已写入数据库
        if self.test_config.huey.immediate:
            # immediate 模式：任务立即执行，队列计数不变
            final_queue_count = self.get_huey_queue_task_count()
            assert final_queue_count == initial_queue_count
        else:
            # 非 immediate 模式：验证任务已提交到队列
            final_queue_count = self.get_huey_queue_task_count()
            assert final_queue_count == initial_queue_count + 1

            # 步骤2：执行队列中的任务
            executed_count = self.execute_huey_tasks()
            assert executed_count >= 1

        # 步骤3：验证数据已写入数据库
        final_data_count = self.get_data_table_row_count("stock_basic")
        assert final_data_count >= 1

        # 验证队列已清空
        remaining_queue_count = self.get_huey_queue_task_count()
        assert remaining_queue_count == 0

    def test_multiple_tasks_end_to_end(self):
        """测试多个任务的端到端流程"""
        # 准备测试数据
        test_data_1 = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "symbol": ["000001"],
                "name": ["平安银行"],
                "area": ["深圳"],
                "industry": ["银行"],
                "cnspell": ["payh"],
                "market": ["主板"],
                "list_date": ["19910403"],
                "act_name": ["中国平安保险（集团）股份有限公司"],
                "act_ent_type": ["境内非国有法人"],
            }
        )

        test_data_2 = pd.DataFrame(
            {
                "ts_code": ["000002.SZ"],
                "symbol": ["000002"],
                "name": ["万科A"],
                "area": ["深圳"],
                "industry": ["房地产开发"],
                "cnspell": ["wka"],
                "market": ["主板"],
                "list_date": ["19910129"],
                "act_name": ["深圳市地铁集团有限公司"],
                "act_ent_type": ["国有法人"],
            }
        )

        # 创建 Mock FetcherBuilder 返回不同数据
        mock_fetcher_1 = Mock(return_value=test_data_1)
        mock_fetcher_2 = Mock(return_value=test_data_2)
        mock_builder_instance = Mock()
        mock_builder_instance.build_by_task.side_effect = [
            mock_fetcher_1,
            mock_fetcher_2,
        ]

        # 创建下载器并注入测试任务总线和 Mock FetcherBuilder
        downloader = SimpleDownloader(task_bus=self.test_task_bus)
        downloader.fetcher_builder = mock_builder_instance

        # 创建两个下载任务
        configs = [
            DownloadTaskConfig(
                symbol="000001.SZ",
                task_type=TaskType.stock_basic,
                priority=TaskPriority.HIGH,
                max_retries=3,
            ),
            DownloadTaskConfig(
                symbol="000002.SZ",
                task_type=TaskType.stock_basic,
                priority=TaskPriority.HIGH,
                max_retries=3,
            ),
        ]

        # 执行多个下载任务
        initial_queue_count = self.get_huey_queue_task_count()
        results = []

        for config in configs:
            result = downloader.download(config)
            results.append(result)
            assert result.success is True
            assert result.data is not None
            assert not result.data.empty

        # 在 immediate 模式下，任务会立即执行，不会放入队列
        if self.test_config.huey.immediate:
            # immediate 模式：任务立即执行，队列计数不变
            final_queue_count = self.get_huey_queue_task_count()
            assert final_queue_count == initial_queue_count
        else:
            # 非 immediate 模式：验证所有任务都已提交到队列
            final_queue_count = self.get_huey_queue_task_count()
            expected_count = initial_queue_count + len(configs)
            assert final_queue_count == expected_count

            # 执行队列中的所有任务
            executed_count = self.execute_huey_tasks()
            assert executed_count >= len(configs)

        # 验证数据已写入数据库
        row_count = self.get_data_table_row_count("stock_basic")
        assert row_count >= len(configs)

    def test_failed_download_no_database_write(self):
        """测试下载失败时不应写入数据库"""
        # 创建 Mock FetcherBuilder 抛出异常
        mock_fetcher = Mock(side_effect=Exception("网络连接失败"))
        mock_builder_instance = Mock()
        mock_builder_instance.build_by_task.return_value = mock_fetcher

        # 创建下载器并注入 Mock FetcherBuilder
        downloader = SimpleDownloader(task_bus=self.test_task_bus)
        downloader.fetcher_builder = mock_builder_instance

        # 创建下载任务配置
        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.stock_basic,
            priority=TaskPriority.HIGH,
            max_retries=3,
        )

        # 执行下载任务（应该失败）
        initial_queue_count = self.get_huey_queue_task_count()
        initial_row_count = self.get_data_table_row_count("stock_basic")

        result = downloader.download(config)

        # 验证下载失败
        assert result.success is False
        assert result.error is not None

        # 验证没有任务提交到队列（因为下载失败）
        final_queue_count = self.get_huey_queue_task_count()
        assert final_queue_count == initial_queue_count, "下载失败时不应提交任务到队列"

        # 验证数据库没有新数据
        final_row_count = self.get_data_table_row_count("stock_basic")
        assert final_row_count == initial_row_count, "下载失败时不应写入数据库"

        print("✅ 失败场景测试成功：下载失败时没有提交任务或写入数据库")
