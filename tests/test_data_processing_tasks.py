"""数据处理任务模块的专门测试

专门测试 data_processing_tasks.py 模块，提升测试覆盖率至90%以上。
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd


class TestDataProcessor:
    """测试 DataProcessor 类"""

    def setup_method(self):
        """每个测试方法执行前的设置"""
        from neo.tasks.data_processing_tasks import DataProcessor

        self.processor = DataProcessor()

    def test_init(self):
        """测试 DataProcessor 初始化"""
        from neo.tasks.data_processing_tasks import DataProcessor

        processor = DataProcessor()
        assert processor is not None

    def test_validate_data_frame_success(self):
        """测试数据验证成功的情况"""
        data = [{"ts_code": "000001.SZ", "name": "平安银行"}]
        result = self.processor._validate_data_frame(data, "stock_basic", "000001.SZ")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "ts_code" in result.columns
        assert "name" in result.columns

    def test_validate_data_frame_empty_list(self):
        """测试空列表数据验证"""
        with pytest.raises(ValueError, match="数据为空或格式无效"):
            self.processor._validate_data_frame([], "stock_basic", "000001.SZ")

    def test_validate_data_frame_none_data(self):
        """测试None数据验证"""
        with pytest.raises(ValueError, match="数据为空或格式无效"):
            self.processor._validate_data_frame(None, "stock_basic", "000001.SZ")

    def test_validate_data_frame_not_list(self):
        """测试非列表数据验证"""
        with pytest.raises(ValueError, match="数据为空或格式无效"):
            self.processor._validate_data_frame(
                "invalid_data", "stock_basic", "000001.SZ"
            )

    def test_validate_data_frame_invalid_dict_data(self):
        """测试无效字典数据转换失败"""
        # 模拟 pandas.DataFrame 构造函数抛出异常
        with patch("pandas.DataFrame") as mock_df:
            mock_df.side_effect = Exception("DataFrame creation failed")

            invalid_data = [{"key": "value"}]
            with pytest.raises(ValueError, match="数据转换失败"):
                self.processor._validate_data_frame(
                    invalid_data, "stock_basic", "000001.SZ"
                )

    @patch("neo.data_processor.data_processor_factory.DataProcessorFactory")
    @patch("neo.app.container")
    def test_process_with_container_success(self, mock_container, mock_factory_class):
        """测试使用容器处理数据成功"""
        # 设置mock
        mock_data_processor = Mock()
        mock_data_processor.process.return_value = True
        
        mock_factory = Mock()
        mock_factory.create_processor.return_value = mock_data_processor
        mock_factory_class.return_value = mock_factory

        # 创建测试数据
        df_data = pd.DataFrame([{"ts_code": "000001.SZ"}])

        # 调用方法
        result = self.processor._process_with_container("stock_basic", df_data)

        # 验证结果
        assert result is True
        mock_factory_class.assert_called_once_with(mock_container)
        mock_factory.create_processor.assert_called_once_with("stock_basic")
        mock_data_processor.process.assert_called_once_with("stock_basic", df_data)
        mock_data_processor.shutdown.assert_called_once()

    @patch("neo.data_processor.data_processor_factory.DataProcessorFactory")
    @patch("neo.app.container")
    def test_process_with_container_failure(self, mock_container, mock_factory_class):
        """测试使用容器处理数据失败"""
        # 设置mock返回False
        mock_data_processor = Mock()
        mock_data_processor.process.return_value = False
        
        mock_factory = Mock()
        mock_factory.create_processor.return_value = mock_data_processor
        mock_factory_class.return_value = mock_factory

        # 创建测试数据
        df_data = pd.DataFrame([{"ts_code": "000001.SZ"}])

        # 调用方法
        result = self.processor._process_with_container("stock_basic", df_data)

        # 验证结果
        assert result is False
        mock_data_processor.shutdown.assert_called_once()

    @patch("neo.data_processor.data_processor_factory.DataProcessorFactory")
    @patch("neo.app.container")
    def test_process_with_container_exception_with_shutdown(self, mock_container, mock_factory_class):
        """测试容器处理数据时异常，确保shutdown被调用"""
        # 设置mock抛出异常
        mock_data_processor = Mock()
        mock_data_processor.process.side_effect = Exception("Processing error")
        
        mock_factory = Mock()
        mock_factory.create_processor.return_value = mock_data_processor
        mock_factory_class.return_value = mock_factory

        # 创建测试数据
        df_data = pd.DataFrame([{"ts_code": "000001.SZ"}])

        # 调用方法并验证异常
        with pytest.raises(Exception, match="Processing error"):
            self.processor._process_with_container("stock_basic", df_data)

        # 验证shutdown仍然被调用
        mock_data_processor.shutdown.assert_called_once()

    @patch("neo.data_processor.data_processor_factory.DataProcessorFactory")
    @patch("neo.app.container")
    def test_process_data_success(self, mock_container, mock_factory_class):
        """测试处理数据成功的完整流程"""
        # 设置mock
        mock_data_processor = Mock()
        mock_data_processor.process.return_value = True
        
        mock_factory = Mock()
        mock_factory.create_processor.return_value = mock_data_processor
        mock_factory_class.return_value = mock_factory

        # 测试数据
        data = [{"ts_code": "000001.SZ", "name": "平安银行"}]

        # 调用方法
        result = self.processor.process_data("stock_basic", "000001.SZ", data)

        # 验证结果
        assert result is True
        mock_factory_class.assert_called_once_with(mock_container)
        mock_factory.create_processor.assert_called_once_with("stock_basic")
        mock_data_processor.process.assert_called_once_with("stock_basic", mock_data_processor.process.call_args[0][1])
        mock_data_processor.shutdown.assert_called_once()

    def test_process_data_value_error_handling(self):
        """测试处理数据时ValueError异常处理"""
        # 使用空数据触发ValueError
        result = self.processor.process_data("stock_basic", "000001.SZ", [])

        # 验证返回False而不是抛出异常
        assert result is False

    @patch("neo.data_processor.data_processor_factory.DataProcessorFactory")
    @patch("neo.app.container")
    def test_process_data_general_exception_handling(self, mock_container, mock_factory_class):
        """测试处理数据时一般异常处理"""
        # 设置mock抛出非ValueError异常
        mock_data_processor = Mock()
        mock_data_processor.process.side_effect = RuntimeError("Runtime error")
        
        mock_factory = Mock()
        mock_factory.create_processor.return_value = mock_data_processor
        mock_factory_class.return_value = mock_factory

        # 测试数据
        data = [{"ts_code": "000001.SZ"}]

        # 验证抛出RuntimeError
        with pytest.raises(RuntimeError, match="Runtime error"):
            self.processor.process_data("stock_basic", "000001.SZ", data)

        # 验证shutdown仍然被调用
        mock_data_processor.shutdown.assert_called_once()


class TestProcessDataSync:
    """测试 _process_data_sync 函数"""

    def test_process_data_sync_success(self):
        """测试 _process_data_sync 函数成功处理"""
        from neo.tasks.data_processing_tasks import _process_data_sync

        # 创建测试数据
        df_data = pd.DataFrame([{"ts_code": "000001.SZ", "name": "平安银行"}])

        # Mock DataProcessor
        with patch(
            "neo.tasks.data_processing_tasks.DataProcessor"
        ) as mock_processor_class:
            mock_processor = Mock()
            mock_processor.process_data.return_value = True
            mock_processor_class.return_value = mock_processor

            # 调用函数
            result = _process_data_sync("stock_basic", df_data)

            # 验证结果
            assert result is True
            mock_processor_class.assert_called_once()
            # 验证调用参数：task_type, symbol="", data_records
            call_args = mock_processor.process_data.call_args
            assert call_args[0][0] == "stock_basic"  # task_type
            assert call_args[0][1] == ""  # symbol
            assert isinstance(call_args[0][2], list)  # data_records
            assert len(call_args[0][2]) == 1
            assert call_args[0][2][0]["ts_code"] == "000001.SZ"

    def test_process_data_sync_failure(self):
        """测试 _process_data_sync 函数处理失败"""
        from neo.tasks.data_processing_tasks import _process_data_sync

        # 创建测试数据
        df_data = pd.DataFrame([{"ts_code": "000001.SZ"}])

        # Mock DataProcessor返回False
        with patch(
            "neo.tasks.data_processing_tasks.DataProcessor"
        ) as mock_processor_class:
            mock_processor = Mock()
            mock_processor.process_data.return_value = False
            mock_processor_class.return_value = mock_processor

            # 调用函数
            result = _process_data_sync("stock_basic", df_data)

            # 验证结果
            assert result is False


class TestProcessDataTaskIntegration:
    """测试 process_data_task 任务的集成测试"""

    @patch("neo.tasks.data_processing_tasks.logger")
    def test_process_data_task_success_with_logging(self, mock_logger):
        """测试 process_data_task 成功执行并记录日志"""
        from neo.tasks.data_processing_tasks import process_data_task

        # Mock DataProcessor
        with patch(
            "neo.tasks.data_processing_tasks.DataProcessor"
        ) as mock_processor_class:
            mock_processor = Mock()
            mock_processor.process_data.return_value = True
            mock_processor_class.return_value = mock_processor

            # 测试数据
            data = [{"ts_code": "000001.SZ", "name": "平安银行"}]

            # 调用任务函数
            result = process_data_task.func("stock_basic", "000001.SZ", data)

            # 验证结果
            assert result is True
            mock_processor_class.assert_called_once()
            mock_processor.process_data.assert_called_once_with(
                "stock_basic", "000001.SZ", data
            )

            # 验证日志记录
            mock_logger.info.assert_called_once()
            log_call = mock_logger.info.call_args[0][0]
            assert "🏆 [HUEY_SLOW] 最终结果" in log_call
            assert "000001.SZ_stock_basic" in log_call
            assert "成功: True" in log_call

    @patch("neo.tasks.data_processing_tasks.logger")
    def test_process_data_task_exception_with_logging(self, mock_logger):
        """测试 process_data_task 异常处理并记录错误日志"""
        from neo.tasks.data_processing_tasks import process_data_task

        # Mock DataProcessor抛出异常
        with patch(
            "neo.tasks.data_processing_tasks.DataProcessor"
        ) as mock_processor_class:
            mock_processor = Mock()
            mock_processor.process_data.side_effect = RuntimeError("Processing failed")
            mock_processor_class.return_value = mock_processor

            # 测试数据
            data = [{"ts_code": "000001.SZ"}]

            # 调用任务函数并验证异常
            with pytest.raises(RuntimeError, match="Processing failed"):
                process_data_task.func("stock_basic", "000001.SZ", data)

            # 验证错误日志记录
            mock_logger.error.assert_called_once()
            log_call = mock_logger.error.call_args[0][0]
            assert "❌ [HUEY_SLOW] 数据处理任务执行失败" in log_call
            assert "000001.SZ" in log_call
