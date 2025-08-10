# -*- coding: utf-8 -*-
"""
进度事件系统的测试用例
"""

import pytest
import time
from unittest.mock import patch, MagicMock

from src.downloader.progress_events import (
    ProgressPhase, ProgressEventType, ProgressEvent, ProgressEventManager,
    start_phase, end_phase, task_started, task_completed, task_failed,
    batch_completed, update_total, send_message
)


class TestProgressEvent:
    """测试进度事件数据类"""
    
    def test_progress_event_creation(self):
        """测试进度事件创建"""
        event = ProgressEvent(
            event_type=ProgressEventType.TASK_START,
            phase=ProgressPhase.DOWNLOADING,
            task_id="test-task",
            symbol="000001.SZ",
            count=1,
            message="测试消息"
        )
        
        assert event.event_type == ProgressEventType.TASK_START
        assert event.phase == ProgressPhase.DOWNLOADING
        assert event.task_id == "test-task"
        assert event.symbol == "000001.SZ"
        assert event.count == 1
        assert event.message == "测试消息"
    
    def test_progress_event_defaults(self):
        """测试进度事件默认值"""
        event = ProgressEvent(event_type=ProgressEventType.MESSAGE)
        
        assert event.event_type == ProgressEventType.MESSAGE
        assert event.phase is None
        assert event.task_id is None
        assert event.symbol is None
        assert event.count == 1
        assert event.total is None
        assert event.message is None
        assert event.metadata is None


class TestProgressEventManager:
    """测试进度事件管理器"""
    
    @pytest.fixture
    def manager(self):
        """创建进度事件管理器实例"""
        return ProgressEventManager()
    
    def test_manager_initialization(self, manager):
        """测试管理器初始化"""
        assert not manager._running
        assert manager._worker_thread is None
        assert manager._current_phase == ProgressPhase.INITIALIZATION
        assert manager._overall_total == 0
        assert manager._overall_completed == 0
        assert manager._overall_failed == 0
    
    def test_start_stop_manager(self, manager):
        """测试启动和停止管理器"""
        # 启动
        manager.start()
        assert manager._running
        assert manager._worker_thread is not None
        assert manager._worker_thread.is_alive()
        
        # 停止
        manager.stop()
        assert not manager._running
        
        # 等待线程结束
        if manager._worker_thread:
            manager._worker_thread.join(timeout=1)
            assert not manager._worker_thread.is_alive()
    
    def test_send_event_when_not_running(self, manager):
        """测试在未运行状态下发送事件"""
        event = ProgressEvent(event_type=ProgressEventType.MESSAGE, message="测试")
        
        # 不应该抛出异常
        manager.send_event(event)
        
        # 队列应该为空（因为管理器未运行）
        assert manager._event_queue.empty()
    
    def test_send_event_when_running(self, manager):
        """测试在运行状态下发送事件"""
        manager.start()
        
        try:
            event = ProgressEvent(event_type=ProgressEventType.MESSAGE, message="测试")
            manager.send_event(event)
            
            # 给一点时间让事件被处理
            time.sleep(0.1)
            
            # 事件应该被处理
            assert manager._event_queue.empty() or manager._event_queue.qsize() == 0
        finally:
            manager.stop()
    
    def test_phase_start_event(self, manager):
        """测试阶段开始事件处理"""
        manager.start()
        
        try:
            # 发送阶段开始事件
            event = ProgressEvent(
                event_type=ProgressEventType.PHASE_START,
                phase=ProgressPhase.DOWNLOADING,
                total=10
            )
            manager.send_event(event)
            
            # 等待事件处理
            time.sleep(0.2)
            
            # 检查状态
            assert manager._current_phase == ProgressPhase.DOWNLOADING
            assert manager._phase_totals[ProgressPhase.DOWNLOADING] == 10
            assert manager._overall_total == 10
        finally:
            manager.stop()
    
    def test_task_complete_event(self, manager):
        """测试任务完成事件处理"""
        manager.start()
        
        try:
            # 先设置阶段
            manager._current_phase = ProgressPhase.DOWNLOADING
            
            # 发送任务完成事件
            event = ProgressEvent(
                event_type=ProgressEventType.TASK_COMPLETE,
                task_id="test-task",
                count=1
            )
            manager.send_event(event)
            
            # 等待事件处理
            time.sleep(0.2)
            
            # 检查统计
            assert manager._overall_completed == 1
            assert manager._phase_completed[ProgressPhase.DOWNLOADING] == 1
        finally:
            manager.stop()
    
    def test_task_failed_event(self, manager):
        """测试任务失败事件处理"""
        manager.start()
        
        try:
            # 先设置阶段
            manager._current_phase = ProgressPhase.DOWNLOADING
            
            # 发送任务失败事件
            event = ProgressEvent(
                event_type=ProgressEventType.TASK_FAILED,
                task_id="test-task",
                count=1
            )
            manager.send_event(event)
            
            # 等待事件处理
            time.sleep(0.2)
            
            # 检查统计
            assert manager._overall_failed == 1
            assert manager._phase_failed[ProgressPhase.DOWNLOADING] == 1
        finally:
            manager.stop()
    
    def test_batch_complete_event(self, manager):
        """测试批次完成事件处理"""
        manager.start()
        
        try:
            # 先设置阶段
            manager._current_phase = ProgressPhase.SAVING
            
            # 发送批次完成事件
            event = ProgressEvent(
                event_type=ProgressEventType.BATCH_COMPLETE,
                count=3
            )
            manager.send_event(event)
            
            # 等待事件处理
            time.sleep(0.2)
            
            # 检查统计 - 批次完成不应该影响总体完成数
            assert manager._overall_completed == 0
            assert manager._phase_completed[ProgressPhase.SAVING] == 3
        finally:
            manager.stop()
    
    def test_update_total_event(self, manager):
        """测试更新总数事件处理"""
        manager.start()
        
        try:
            # 发送更新总数事件
            event = ProgressEvent(
                event_type=ProgressEventType.UPDATE_TOTAL,
                phase=ProgressPhase.DOWNLOADING,
                total=20
            )
            manager.send_event(event)
            
            # 等待事件处理
            time.sleep(0.2)
            
            # 检查统计
            assert manager._phase_totals[ProgressPhase.DOWNLOADING] == 20
            assert manager._overall_total == 20
        finally:
            manager.stop()
    
    def test_get_stats(self, manager):
        """测试获取统计信息"""
        # 设置一些测试数据
        manager._current_phase = ProgressPhase.DOWNLOADING
        manager._overall_total = 10
        manager._overall_completed = 7
        manager._overall_failed = 2
        manager._phase_totals[ProgressPhase.DOWNLOADING] = 10
        manager._phase_completed[ProgressPhase.DOWNLOADING] = 7
        manager._phase_failed[ProgressPhase.DOWNLOADING] = 2
        
        stats = manager.get_stats()
        
        assert stats['current_phase'] == 'downloading'
        assert stats['overall_total'] == 10
        assert stats['overall_completed'] == 7
        assert stats['overall_failed'] == 2
        assert stats['success_rate'] == 70.0
        assert stats['phase_totals']['downloading'] == 10
        assert stats['phase_completed']['downloading'] == 7
        assert stats['phase_failed']['downloading'] == 2
    
    @patch('src.downloader.progress_events.tqdm')
    def test_progress_bar_initialization(self, mock_tqdm, manager):
        """测试进度条初始化"""
        mock_pbar = MagicMock()
        mock_tqdm.return_value = mock_pbar
        
        # 测试下载阶段的初始化
        manager._current_phase = ProgressPhase.DOWNLOADING
        manager._phase_totals[ProgressPhase.DOWNLOADING] = 5
        manager._init_progress_bar()
        
        # 检查进度条是否被正确初始化
        mock_tqdm.assert_called_once()
        assert manager._overall_total == 5  # 下载阶段会设置overall_total
        assert manager._pbar == mock_pbar
        
        # 测试保存阶段的初始化
        mock_tqdm.reset_mock()
        manager._current_phase = ProgressPhase.SAVING
        manager._phase_totals[ProgressPhase.SAVING] = 3
        manager._init_progress_bar()
        
        # 保存阶段不会改变overall_total
        assert manager._overall_total == 5  # 保持之前的值
        mock_tqdm.assert_called_once()


class TestConvenienceFunctions:
    """测试便捷函数"""
    
    @patch('src.downloader.progress_events.progress_event_manager')
    def test_start_phase(self, mock_manager):
        """测试开始阶段便捷函数"""
        start_phase(ProgressPhase.DOWNLOADING, total=10, message="开始下载")
        
        mock_manager.send_event.assert_called_once()
        event = mock_manager.send_event.call_args[0][0]
        assert event.event_type == ProgressEventType.PHASE_START
        assert event.phase == ProgressPhase.DOWNLOADING
        assert event.total == 10
        assert event.message == "开始下载"
    
    @patch('src.downloader.progress_events.progress_event_manager')
    def test_end_phase(self, mock_manager):
        """测试结束阶段便捷函数"""
        end_phase(ProgressPhase.DOWNLOADING, message="下载完成")
        
        mock_manager.send_event.assert_called_once()
        event = mock_manager.send_event.call_args[0][0]
        assert event.event_type == ProgressEventType.PHASE_END
        assert event.phase == ProgressPhase.DOWNLOADING
        assert event.message == "下载完成"
    
    @patch('src.downloader.progress_events.progress_event_manager')
    def test_task_started(self, mock_manager):
        """测试任务开始便捷函数"""
        task_started(task_id="test-task", symbol="000001.SZ", message="开始处理")
        
        mock_manager.send_event.assert_called_once()
        event = mock_manager.send_event.call_args[0][0]
        assert event.event_type == ProgressEventType.TASK_START
        assert event.task_id == "test-task"
        assert event.symbol == "000001.SZ"
        assert event.message == "开始处理"
    
    @patch('src.downloader.progress_events.progress_event_manager')
    def test_task_completed(self, mock_manager):
        """测试任务完成便捷函数"""
        task_completed(task_id="test-task", symbol="000001.SZ", count=2)
        
        mock_manager.send_event.assert_called_once()
        event = mock_manager.send_event.call_args[0][0]
        assert event.event_type == ProgressEventType.TASK_COMPLETE
        assert event.task_id == "test-task"
        assert event.symbol == "000001.SZ"
        assert event.count == 2
    
    @patch('src.downloader.progress_events.progress_event_manager')
    def test_task_failed(self, mock_manager):
        """测试任务失败便捷函数"""
        task_failed(task_id="test-task", symbol="000001.SZ", count=1, message="网络错误")
        
        mock_manager.send_event.assert_called_once()
        event = mock_manager.send_event.call_args[0][0]
        assert event.event_type == ProgressEventType.TASK_FAILED
        assert event.task_id == "test-task"
        assert event.symbol == "000001.SZ"
        assert event.count == 1
        assert event.message == "网络错误"
    
    @patch('src.downloader.progress_events.progress_event_manager')
    def test_batch_completed(self, mock_manager):
        """测试批次完成便捷函数"""
        batch_completed(count=5, message="批次保存完成")
        
        mock_manager.send_event.assert_called_once()
        event = mock_manager.send_event.call_args[0][0]
        assert event.event_type == ProgressEventType.BATCH_COMPLETE
        assert event.count == 5
        assert event.message == "批次保存完成"
    
    @patch('src.downloader.progress_events.progress_event_manager')
    def test_update_total(self, mock_manager):
        """测试更新总数便捷函数"""
        update_total(total=15, phase=ProgressPhase.DOWNLOADING)
        
        mock_manager.send_event.assert_called_once()
        event = mock_manager.send_event.call_args[0][0]
        assert event.event_type == ProgressEventType.UPDATE_TOTAL
        assert event.total == 15
        assert event.phase == ProgressPhase.DOWNLOADING
    
    @patch('src.downloader.progress_events.progress_event_manager')
    def test_send_message(self, mock_manager):
        """测试发送消息便捷函数"""
        send_message("这是一条测试消息")
        
        mock_manager.send_event.assert_called_once()
        event = mock_manager.send_event.call_args[0][0]
        assert event.event_type == ProgressEventType.MESSAGE
        assert event.message == "这是一条测试消息"


class TestIntegration:
    """集成测试"""
    
    def test_complete_workflow(self):
        """测试完整的工作流程"""
        manager = ProgressEventManager()
        manager.start()
        
        try:
            # 1. 初始化阶段
            manager.send_event(ProgressEvent(
                event_type=ProgressEventType.PHASE_START,
                phase=ProgressPhase.INITIALIZATION
            ))
            time.sleep(0.1)
            
            # 2. 准备阶段
            manager.send_event(ProgressEvent(
                event_type=ProgressEventType.PHASE_START,
                phase=ProgressPhase.PREPARATION,
                total=3
            ))
            time.sleep(0.1)
            
            # 3. 下载阶段
            manager.send_event(ProgressEvent(
                event_type=ProgressEventType.UPDATE_TOTAL,
                phase=ProgressPhase.DOWNLOADING,
                total=3
            ))
            manager.send_event(ProgressEvent(
                event_type=ProgressEventType.PHASE_START,
                phase=ProgressPhase.DOWNLOADING,
                total=3
            ))
            time.sleep(0.1)
            
            # 模拟3个任务
            for i in range(3):
                manager.send_event(ProgressEvent(
                    event_type=ProgressEventType.TASK_START,
                    task_id=f"task-{i}",
                    symbol=f"00000{i}.SZ"
                ))
                time.sleep(0.05)
                
                if i == 1:  # 第二个任务失败
                    manager.send_event(ProgressEvent(
                        event_type=ProgressEventType.TASK_FAILED,
                        task_id=f"task-{i}",
                        symbol=f"00000{i}.SZ",
                        message="网络错误"
                    ))
                else:
                    manager.send_event(ProgressEvent(
                        event_type=ProgressEventType.TASK_COMPLETE,
                        task_id=f"task-{i}",
                        symbol=f"00000{i}.SZ"
                    ))
                time.sleep(0.05)
            
            # 4. 保存阶段
            manager.send_event(ProgressEvent(
                event_type=ProgressEventType.PHASE_START,
                phase=ProgressPhase.SAVING
            ))
            manager.send_event(ProgressEvent(
                event_type=ProgressEventType.BATCH_COMPLETE,
                count=2,
                message="保存完成"
            ))
            time.sleep(0.1)
            
            # 5. 完成阶段
            manager.send_event(ProgressEvent(
                event_type=ProgressEventType.PHASE_START,
                phase=ProgressPhase.COMPLETED
            ))
            time.sleep(0.1)
            
            # 检查最终统计
            stats = manager.get_stats()
            assert stats['current_phase'] == 'completed'
            assert stats['overall_total'] == 3
            assert stats['overall_completed'] == 2
            assert stats['overall_failed'] == 1
            assert abs(stats['success_rate'] - 66.67) < 0.1  # 允许小数点误差
            
        finally:
            manager.stop()