"""测试 ParquetWriter 的全量替换功能

主要测试场景：
1. 验证 write_full_replace() 方法能正确执行全量替换写入
2. 验证生成的文件结构符合预期（目录/文件格式）
3. 验证文件扩展名为 .parquet
4. 验证已存在文件时的删除和重建行为
5. 使用 stock_basic 类型数据进行测试
"""

import pandas as pd
import tempfile
from pathlib import Path
import os

from neo.writers.parquet_writer import ParquetWriter


class TestParquetWriter:
    """测试 ParquetWriter 的全量替换功能"""

    def create_stock_basic_data(self) -> pd.DataFrame:
        """创建模拟的 stock_basic 数据

        Returns:
            包含股票基础信息的 DataFrame
        """
        return pd.DataFrame(
            {
                "ts_code": [
                    "000001.SZ",
                    "000002.SZ",
                    "600000.SH",
                    "000858.SZ",
                    "002415.SZ",
                ],
                "symbol": ["000001", "000002", "600000", "000858", "002415"],
                "name": ["平安银行", "万科A", "浦发银行", "五粮液", "海康威视"],
                "area": ["深圳", "深圳", "上海", "四川", "浙江"],
                "industry": ["银行", "全国地产", "银行", "白酒", "安防设备"],
                "cnspell": ["payh", "wka", "pfyh", "wly", "hkws"],
                "market": ["主板", "主板", "主板", "主板", "中小板"],
                "list_date": [
                    "19910403",
                    "19910129",
                    "19990810",
                    "19930608",
                    "20100528",
                ],
                "act_name": [
                    "无实际控制人",
                    "深圳市人民政府国有资产监督管理委员会",
                    "上海国际集团有限公司",
                    "宜宾五粮液集团有限公司",
                    "龚虹嘉",
                ],
                "act_ent_type": [
                    "无",
                    "地方国企",
                    "地方国企",
                    "地方国企",
                    "境内自然人",
                ],
            }
        )

    def test_write_full_replace_creates_correct_structure(self):
        """测试：write_full_replace() 创建正确的文件结构"""
        # Arrange
        data = self.create_stock_basic_data()

        with tempfile.TemporaryDirectory() as temp_dir:
            writer = ParquetWriter(base_path=temp_dir)
            task_type = "stock_basic"
            partition_cols = []  # stock_basic 不分区

            # Act
            writer.write_full_replace(data, task_type, partition_cols)

            # Assert
            target_path = Path(temp_dir) / task_type
            assert target_path.exists(), f"目标路径应该存在: {target_path}"
            assert target_path.is_dir(), f"目标路径应该是目录: {target_path}"

            # 检查目录下的文件
            parquet_files = list(target_path.glob("*.parquet"))
            assert len(parquet_files) > 0, "应该至少有一个 parquet 文件"

            # 验证文件扩展名
            for file_path in parquet_files:
                assert file_path.suffix == ".parquet", (
                    f"文件扩展名应该是 .parquet: {file_path}"
                )

            # 验证能正确读取生成的文件
            result_data = pd.read_parquet(target_path)
            assert len(result_data) == len(data), (
                f"数据行数应该一致: {len(result_data)} vs {len(data)}"
            )
            assert set(result_data.columns) == set(data.columns), "列名应该一致"

    def test_write_full_replace_overwrites_existing_file(self):
        """测试：write_full_replace() 覆盖已存在的文件"""
        # Arrange
        self.create_stock_basic_data()
        new_data = pd.DataFrame(
            {
                "ts_code": ["999999.SZ"],
                "symbol": ["999999"],
                "name": ["测试股票"],
                "area": ["测试地区"],
                "industry": ["测试行业"],
                "cnspell": ["test"],
                "market": ["测试市场"],
                "list_date": ["20240101"],
                "act_name": ["测试控制人"],
                "act_ent_type": ["测试类型"],
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            writer = ParquetWriter(base_path=temp_dir)
            task_type = "stock_basic"
            partition_cols = []
            target_path = Path(temp_dir) / task_type

            # 预先创建一个旧文件
            old_file_path = target_path / "old_file.txt"
            os.makedirs(target_path, exist_ok=True)
            with open(old_file_path, "w") as f:
                f.write("这是一个旧文件，应该被删除")

            assert old_file_path.exists(), "旧文件应该存在"

            # Act
            writer.write_full_replace(new_data, task_type, partition_cols)

            # Assert
            assert target_path.exists(), "目标路径应该存在"
            assert target_path.is_dir(), "目标路径应该是目录"
            assert not old_file_path.exists(), "旧文件应该被删除"

            # 验证新文件正确生成
            parquet_files = list(target_path.glob("*.parquet"))
            assert len(parquet_files) > 0, "应该有新的 parquet 文件"

            # 验证新数据正确写入
            result_data = pd.read_parquet(target_path)
            assert len(result_data) == len(new_data), "应该只包含新数据"
            assert result_data.iloc[0]["name"] == "测试股票", "应该包含新数据的内容"

    def test_write_full_replace_with_empty_data(self):
        """测试：write_full_replace() 处理空数据"""
        # Arrange
        empty_data = pd.DataFrame()

        with tempfile.TemporaryDirectory() as temp_dir:
            writer = ParquetWriter(base_path=temp_dir)
            task_type = "stock_basic"
            partition_cols = []
            target_path = Path(temp_dir) / task_type

            # Act
            writer.write_full_replace(empty_data, task_type, partition_cols)

            # Assert
            # 空数据应该跳过处理，不创建任何文件
            assert not target_path.exists(), "空数据不应该创建任何文件"

    def test_write_full_replace_handles_none_data(self):
        """测试：write_full_replace() 处理 None 数据"""
        # Arrange
        none_data = None

        with tempfile.TemporaryDirectory() as temp_dir:
            writer = ParquetWriter(base_path=temp_dir)
            task_type = "stock_basic"
            partition_cols = []
            target_path = Path(temp_dir) / task_type

            # Act
            writer.write_full_replace(none_data, task_type, partition_cols)

            # Assert
            # None 数据应该跳过处理，不创建任何文件
            assert not target_path.exists(), "None 数据不应该创建任何文件"

    def test_write_full_replace_preserves_data_integrity(self):
        """测试：write_full_replace() 保持数据完整性"""
        # Arrange
        original_data = self.create_stock_basic_data()

        with tempfile.TemporaryDirectory() as temp_dir:
            writer = ParquetWriter(base_path=temp_dir)
            task_type = "stock_basic"
            partition_cols = []

            # Act
            writer.write_full_replace(original_data, task_type, partition_cols)

            # Assert
            target_path = Path(temp_dir) / task_type
            result_data = pd.read_parquet(target_path)

            # 验证数据完整性
            pd.testing.assert_frame_equal(
                result_data.sort_values("ts_code").reset_index(drop=True),
                original_data.sort_values("ts_code").reset_index(drop=True),
                check_dtype=False,  # 允许数据类型有轻微差异（parquet 自动推断）
            )

    def test_write_full_replace_overwrites_existing_parquet_file(self):
        """测试：write_full_replace() 覆盖已存在的 parquet 文件（模拟 stock_basic 场景）"""
        # Arrange
        original_data = self.create_stock_basic_data()
        new_data = pd.DataFrame(
            {
                "ts_code": ["888888.SZ", "777777.SH"],
                "symbol": ["888888", "777777"],
                "name": ["新股票1", "新股票2"],
                "area": ["新地区1", "新地区2"],
                "industry": ["新行业1", "新行业2"],
                "cnspell": ["new1", "new2"],
                "market": ["新市场1", "新市场2"],
                "list_date": ["20240201", "20240202"],
                "act_name": ["新控制人1", "新控制人2"],
                "act_ent_type": ["新类型1", "新类型2"],
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            writer = ParquetWriter(base_path=temp_dir)
            task_type = "stock_basic"
            partition_cols = []
            target_path = Path(temp_dir) / task_type

            # 创建一个与目标路径同名的 parquet 文件（模拟问题场景）
            # 这模拟了 stock_basic 作为一个单独 parquet 文件而不是目录的情况
            original_data.to_parquet(target_path, index=False)

            assert target_path.exists(), "目标路径（作为文件）应该存在"
            assert target_path.is_file(), "目标路径应该是一个文件而不是目录"

            # 验证旧文件内容
            old_data = pd.read_parquet(target_path)
            assert len(old_data) == len(original_data), "旧文件应该包含原始数据"

            # Act - 执行全量替换，这时目标路径（不含扩展名）存在一个同名的 parquet 文件
            writer.write_full_replace(new_data, task_type, partition_cols)

            # Assert
            # 旧的单文件应该被删除（尽管路径不完全匹配）
            # 新的目录结构应该被创建
            assert target_path.exists(), "目标路径应该存在"
            assert target_path.is_dir(), "目标路径应该是目录"

            # 检查新文件
            parquet_files = list(target_path.glob("*.parquet"))
            assert len(parquet_files) > 0, "应该有新的 parquet 文件"

            # 验证新数据
            result_data = pd.read_parquet(target_path)
            assert len(result_data) == len(new_data), "应该只包含新数据"

            # 验证旧文件已被替换为目录结构
            # （target_path 现在是目录，不再是原来的单个文件）
