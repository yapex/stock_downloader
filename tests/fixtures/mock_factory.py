"""Mock 工厂类

用于创建和管理测试中的 Mock 对象，避免重复代码和 patch 的过度使用。
"""

from unittest.mock import Mock
from typing import Dict, List, Any, Optional


class MockFactory:
    """Mock 对象工厂类"""

    @staticmethod
    def create_container_mock() -> Mock:
        """创建一个配置好的 container mock
        
        Returns:
            配置好的 container mock 对象
        """
        container_mock = Mock()
        
        # 创建基础组件的 Mock
        container_mock.task_builder.return_value = Mock()
        container_mock.group_handler.return_value = Mock()
        
        return container_mock

    @staticmethod
    def create_app_service_mock(
        task_mapping: Optional[Dict[str, List[str]]] = None
    ) -> Mock:
        """创建一个配置好的 app_service mock
        
        Args:
            task_mapping: 任务映射字典，默认为基本的股票任务映射
            
        Returns:
            配置好的 app_service mock 对象
        """
        if task_mapping is None:
            task_mapping = {"stock_daily": ["000001.SZ", "000002.SZ"]}
            
        app_service_mock = Mock()
        app_service_mock.build_task_stock_mapping_from_group.return_value = task_mapping
        app_service_mock.run_data_processor.return_value = None
        
        return app_service_mock

    @staticmethod
    def create_task_result_mock(task_id: str = "test-task-id") -> Mock:
        """创建一个任务结果 mock
        
        Args:
            task_id: 任务ID，默认为 'test-task-id'
            
        Returns:
            配置好的任务结果 mock 对象
        """
        task_result_mock = Mock()
        task_result_mock.id = task_id
        
        return task_result_mock

    @staticmethod
    def create_logging_mock() -> Mock:
        """创建一个日志 mock
        
        Returns:
            配置好的日志 mock 对象
        """
        return Mock()

    @classmethod
    def create_complete_dl_mocks(
        cls,
        task_mapping: Optional[Dict[str, List[str]]] = None,
        task_id: str = "test-task-id"
    ) -> Dict[str, Any]:
        """为 dl 命令创建完整的 mock 集合
        
        Args:
            task_mapping: 任务映射字典
            task_id: 任务ID
            
        Returns:
            包含所有必需 mock 对象的字典
        """
        container_mock = cls.create_container_mock()
        app_service_mock = cls.create_app_service_mock(task_mapping)
        task_result_mock = cls.create_task_result_mock(task_id)
        logging_mock = cls.create_logging_mock()
        
        # 将 app_service 绑定到 container
        container_mock.app_service.return_value = app_service_mock
        
        return {
            "container": container_mock,
            "app_service": app_service_mock,
            "task_result": task_result_mock,
            "logging": logging_mock,
            "task_function": Mock(return_value=task_result_mock)
        }

    @classmethod
    def create_complete_dp_mocks(cls) -> Dict[str, Any]:
        """为 dp 命令创建完整的 mock 集合
        
        Returns:
            包含所有必需 mock 对象的字典
        """
        container_mock = cls.create_container_mock()
        app_service_mock = cls.create_app_service_mock()
        logging_mock = cls.create_logging_mock()
        
        # 将 app_service 绑定到 container
        container_mock.app_service.return_value = app_service_mock
        
        return {
            "container": container_mock,
            "app_service": app_service_mock,
            "logging": logging_mock
        }

    @staticmethod
    def create_downloader_mock(download_data="default") -> Mock:
        """创建一个配置好的 downloader mock
        
        Args:
            download_data: 下载器返回的数据，默认为包含测试数据的 DataFrame
                         如果传入 None，则设置为 None
                         如果传入 "default"，则使用默认测试数据
            
        Returns:
            配置好的 downloader mock 对象
        """
        import pandas as pd
        
        if isinstance(download_data, str) and download_data == "default":
            download_data = pd.DataFrame({"test": [1, 2, 3]})
        # 其他情况（包括 None, DataFrame 等）直接使用
            
        downloader_mock = Mock()
        downloader_mock.download.return_value = download_data
        
        return downloader_mock

    @staticmethod 
    def create_data_processor_mock(process_result: bool = True) -> Mock:
        """创建一个配置好的 data processor mock
        
        Args:
            process_result: 处理结果，默认为 True
            
        Returns:
            配置好的 data processor mock 对象
        """
        processor_mock = Mock()
        processor_mock.process_data.return_value = process_result
        
        return processor_mock

    @staticmethod
    def create_config_mock() -> Mock:
        """创建一个配置好的 config mock
        
        Returns:
            配置好的 config mock 对象
        """
        config_mock = Mock()
        
        # 创建下载任务配置
        download_tasks_mock = Mock()
        download_tasks_mock.default_start_date = "19900101"
        
        # 创建股票日线任务配置
        stock_daily_mock = Mock()
        stock_daily_mock.update_strategy = "incremental"
        download_tasks_mock.stock_daily = stock_daily_mock
        
        # 创建存储配置
        storage_mock = Mock()
        storage_mock.parquet_base_path = "data/parquet"
        
        # 创建数据库配置
        database_mock = Mock()
        database_mock.metadata_path = "data/metadata.db"
        
        # 创建定时任务配置
        cron_tasks_mock = Mock()
        cron_tasks_mock.sync_metadata_schedule = "0 0 * * *"
        
        # 绑定到主配置对象
        config_mock.download_tasks = download_tasks_mock
        config_mock.storage = storage_mock
        config_mock.database = database_mock
        config_mock.cron_tasks = cron_tasks_mock
        
        return config_mock

    @staticmethod
    def create_db_queryer_mock(
        latest_trading_day: str = "20240115",
        max_dates: Optional[Dict[str, str]] = None
    ) -> Mock:
        """创建一个配置好的 db queryer mock
        
        Args:
            latest_trading_day: 最新交易日，默认为 '20240115'
            max_dates: 股票代码到最大日期的映射，默认为基本测试数据
            
        Returns:
            配置好的 db queryer mock 对象
        """
        if max_dates is None:
            max_dates = {"000001.SZ": "20240110"}
            
        db_queryer_mock = Mock()
        db_queryer_mock.get_latest_trading_day.return_value = latest_trading_day
        db_queryer_mock.get_max_date.return_value = max_dates
        
        return db_queryer_mock

    @staticmethod
    def create_schema_loader_mock(date_col: str = "trade_date") -> Mock:
        """创建一个配置好的 schema loader mock
        
        Args:
            date_col: 日期列名，默认为 'trade_date'
            
        Returns:
            配置好的 schema loader mock 对象
        """
        schema_loader_mock = Mock()
        table_config_mock = Mock()
        table_config_mock.date_col = date_col
        schema_loader_mock.get_table_config.return_value = table_config_mock
        
        return schema_loader_mock

    @staticmethod
    def create_metadata_sync_manager_mock() -> Mock:
        """创建一个配置好的 metadata sync manager mock
        
        Returns:
            配置好的 metadata sync manager mock 对象
        """
        sync_manager_mock = Mock()
        sync_manager_mock.sync.return_value = None
        
        return sync_manager_mock

    @classmethod
    def create_complete_huey_container_mock(
        cls,
        downloader_data="default",
        latest_trading_day: str = "20240115",
        max_dates: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """为 Huey 任务创建完整的容器 mock 集合
        
        Args:
            downloader_data: 下载器返回的数据，默认为测试数据
            latest_trading_day: 最新交易日
            max_dates: 股票代码到最大日期的映射
            
        Returns:
            包含所有必需 mock 对象的字典
        """
        container_mock = Mock()
        downloader_mock = cls.create_downloader_mock(downloader_data)
        db_queryer_mock = cls.create_db_queryer_mock(latest_trading_day, max_dates)
        schema_loader_mock = cls.create_schema_loader_mock()
        
        # 绑定到容器
        container_mock.downloader.return_value = downloader_mock
        container_mock.db_queryer.return_value = db_queryer_mock
        container_mock.schema_loader.return_value = schema_loader_mock
        
        return {
            "container": container_mock,
            "downloader": downloader_mock,
            "db_queryer": db_queryer_mock,
            "schema_loader": schema_loader_mock
        }

    @staticmethod
    def create_rate_limit_config_mock(
        download_tasks: Optional[Dict[str, Dict]] = None
    ) -> Mock:
        """创建一个配置好的速率限制配置 mock
        
        Args:
            download_tasks: 下载任务配置，默认为空字典
            
        Returns:
            配置好的配置 mock 对象
        """
        if download_tasks is None:
            download_tasks = {}
            
        config_mock = Mock()
        config_mock.download_tasks = download_tasks
        
        return config_mock

    @classmethod
    def create_complete_rate_limit_mocks(
        cls,
        download_tasks: Optional[Dict[str, Dict]] = None
    ) -> Dict[str, Any]:
        """为 RateLimitManager 测试创建完整的 mock 集合
        
        Args:
            download_tasks: 下载任务配置
            
        Returns:
            包含所有必需 mock 对象的字典
        """
        config_mock = cls.create_rate_limit_config_mock(download_tasks)
        
        return {
            "config": config_mock
        }

    @staticmethod
    def create_tushare_api_mock(
        token: str = "test_token",
        api_functions: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Mock]:
        """创建一个配置好的 Tushare API mock 集合
        
        Args:
            token: 模拟的 Tushare token
            api_functions: API 函数的返回值配置
            
        Returns:
            包含所有 mock 对象的字典
        """
        import pandas as pd
        
        if api_functions is None:
            api_functions = {"stock_basic": pd.DataFrame({"data": [1, 2, 3]})}
        
        # Mock os.environ.get
        environ_get_mock = Mock(return_value=token)
        
        # Mock tushare
        ts_mock = Mock()
        
        # Mock pro API
        pro_mock = Mock()
        ts_mock.pro_api.return_value = pro_mock
        
        # 为 pro API 添加方法
        for api_name, return_value in api_functions.items():
            api_func_mock = Mock(return_value=return_value)
            setattr(pro_mock, api_name, api_func_mock)
        
        # 为 ts 直接添加方法
        for api_name, return_value in api_functions.items():
            api_func_mock = Mock(return_value=return_value)
            setattr(ts_mock, api_name, api_func_mock)
        
        return {
            "environ_get": environ_get_mock,
            "ts": ts_mock,
            "pro": pro_mock,
            "api_functions": {name: getattr(pro_mock, name) for name in api_functions.keys()}
        }

    @staticmethod
    def create_api_function_mock(return_value=None) -> Mock:
        """创建一个配置好的 API 函数 mock
        
        Args:
            return_value: API 函数的返回值，默认为测试 DataFrame
            
        Returns:
            配置好的 API 函数 mock
        """
        import pandas as pd
        
        if return_value is None:
            return_value = pd.DataFrame({"data": [1, 2, 3]})
        
        api_mock = Mock(return_value=return_value)
        return api_mock

    @staticmethod
    def create_normalize_stock_code_mock(
        normalized_code: str = "600519.SH"
    ) -> Mock:
        """创建一个配置好的 normalize_stock_code mock
        
        Args:
            normalized_code: 标准化后的股票代码
            
        Returns:
            配置好的 normalize_stock_code mock
        """
        normalize_mock = Mock(return_value=normalized_code)
        return normalize_mock

    @classmethod
    def create_complete_fetcher_builder_mocks(
        cls,
        token: str = "test_token",
        api_functions: Optional[Dict[str, Any]] = None,
        normalized_code: str = "600519.SH"
    ) -> Dict[str, Any]:
        """为 FetcherBuilder 测试创建完整的 mock 集合
        
        Args:
            token: Tushare token
            api_functions: API 函数返回值配置
            normalized_code: 标准化后的股票代码
            
        Returns:
            包含所有必需 mock 对象的字典
        """
        tushare_mocks = cls.create_tushare_api_mock(token, api_functions)
        normalize_mock = cls.create_normalize_stock_code_mock(normalized_code)
        
        return {
            **tushare_mocks,
            "normalize_stock_code": normalize_mock
        }
    
    @staticmethod
    def create_mock_api_manager(
        api_function_return_value=None
    ) -> Mock:
        """创建一个模拟的 API 管理器
        
        Args:
            api_function_return_value: get_api_function 方法的返回值
            
        Returns:
            配置好的 API 管理器 mock
        """
        import pandas as pd
        
        if api_function_return_value is None:
            api_function_return_value = Mock(return_value=pd.DataFrame({"data": [1, 2, 3]}))
        
        api_manager_mock = Mock()
        api_manager_mock.get_api_function.return_value = api_function_return_value
        
        return api_manager_mock
    
    @staticmethod  
    def create_environment_context(env_vars: Dict[str, str]):
        """创建一个环境变量上下文管理器
        
        Args:
            env_vars: 环境变量字典
            
        Returns:
            环境变量上下文管理器
        """
        import os
        from contextlib import contextmanager
        
        @contextmanager
        def env_context():
            old_values = {}
            for key, value in env_vars.items():
                old_values[key] = os.environ.get(key)
                os.environ[key] = value
            try:
                yield
            finally:
                for key, old_value in old_values.items():
                    if old_value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = old_value
        
        return env_context()


class CliRunnerWrapper:
    """可测试的 CLI 运行器
    
    封装了 CliRunner 的常用操作，减少重复代码。
    """
    
    def __init__(self, cli_runner):
        self.runner = cli_runner
    
    def invoke_dl_command(
        self, 
        group: Optional[str] = None,
        symbols: Optional[List[str]] = None, 
        debug: bool = False,
        dry_run: bool = False
    ):
        """调用 dl 命令的辅助方法
        
        Args:
            group: 任务组名称
            symbols: 股票代码列表
            debug: 调试模式
            dry_run: 干运行模式
            
        Returns:
            CLI 执行结果
        """
        from neo.main import app
        
        args = ["dl"]
        
        if group is not None:
            args.extend(["--group", group])
            
        if symbols:
            for symbol in symbols:
                args.extend(["--symbols", symbol])
                
        if debug:
            args.append("--debug")
            
        if dry_run:
            args.append("--dry-run")
            
        return self.runner.invoke(app, args)
    
    def invoke_dp_command(
        self,
        queue_name: str,
        debug: bool = False
    ):
        """调用 dp 命令的辅助方法
        
        Args:
            queue_name: 队列名称
            debug: 调试模式
            
        Returns:
            CLI 执行结果
        """
        from neo.main import app
        
        args = ["dp", queue_name]
        
        if debug:
            args.append("--debug")
            
        return self.runner.invoke(app, args)
