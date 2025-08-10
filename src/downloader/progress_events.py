# -*- coding: utf-8 -*-
"""
基于消息队列的进度事件系统
"""

import logging
import threading
import time
from dataclasses import dataclass
from enum import Enum
from queue import Queue, Empty
from typing import Optional, Dict, Any
from tqdm import tqdm

logger = logging.getLogger(__name__)


class ProgressPhase(Enum):
    """进度阶段枚举"""

    INITIALIZATION = "initialization"  # 初始化阶段
    PREPARATION = "preparation"  # 准备阶段
    DOWNLOADING = "downloading"  # 下载阶段
    SAVING = "saving"  # 保存阶段
    COMPLETED = "completed"  # 完成阶段


class ProgressEventType(Enum):
    """进度事件类型"""

    PHASE_START = "phase_start"  # 阶段开始
    PHASE_END = "phase_end"  # 阶段结束
    TASK_START = "task_start"  # 任务开始
    TASK_COMPLETE = "task_complete"  # 任务完成
    TASK_FAILED = "task_failed"  # 任务失败
    BATCH_COMPLETE = "batch_complete"  # 批次完成
    UPDATE_TOTAL = "update_total"  # 更新总数
    MESSAGE = "message"  # 消息通知


@dataclass
class ProgressEvent:
    """进度事件"""

    event_type: ProgressEventType
    phase: Optional[ProgressPhase] = None
    task_id: Optional[str] = None
    symbol: Optional[str] = None
    count: int = 1
    total: Optional[int] = None
    message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class ProgressEventManager:
    """基于消息队列的进度事件管理器"""

    def __init__(self):
        self._event_queue = Queue()
        self._worker_thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()

        # 进度状态
        self._current_phase = ProgressPhase.INITIALIZATION
        self._phase_totals: Dict[ProgressPhase, int] = {}
        self._phase_completed: Dict[ProgressPhase, int] = {}
        self._phase_failed: Dict[ProgressPhase, int] = {}

        # 进度条
        self._pbar: Optional[tqdm] = None
        self._overall_total = 0
        self._overall_completed = 0
        self._overall_failed = 0

        # 动态提示相关
        self._state_count = 0
        self._last_state_update = time.time()
        self._state_timer: Optional[threading.Timer] = None
        self._states = ["🚀", "⚡️", "⏳", "🌪️", "✅"]

    def start(self):
        """启动进度事件处理器"""
        with self._lock:
            if self._running:
                return

            self._running = True
            self._worker_thread = threading.Thread(
                target=self._process_events, name="ProgressEventWorker", daemon=True
            )
            self._worker_thread.start()
            logger.info("进度事件管理器已启动")

    def stop(self):
        """停止进度事件处理器"""
        with self._lock:
            if not self._running:
                return

            self._running = False

            # 停止动画定时器
            self._stop_state_animation()

            # 发送停止信号
            self._event_queue.put(None)

            if self._worker_thread:
                self._worker_thread.join(timeout=3.0)

            if self._pbar:
                self._pbar.close()
                self._pbar = None

            logger.info("进度事件管理器已停止")

    def send_event(self, event: ProgressEvent):
        """发送进度事件"""
        if self._running:
            self._event_queue.put(event)

    def _process_events(self):
        """处理进度事件的工作线程"""
        logger.info("进度事件处理线程已启动")

        while self._running:
            try:
                # 获取事件，超时1秒
                event = self._event_queue.get(timeout=1.0)

                # 停止信号
                if event is None:
                    break

                self._handle_event(event)

            except Empty:
                continue
            except Exception as e:
                logger.error(f"处理进度事件时出错: {e}", exc_info=True)

        logger.info("进度事件处理线程已停止")

    def _handle_event(self, event: ProgressEvent):
        """处理单个进度事件"""
        try:
            if event.event_type == ProgressEventType.PHASE_START:
                self._handle_phase_start(event)
            elif event.event_type == ProgressEventType.PHASE_END:
                self._handle_phase_end(event)
            elif event.event_type == ProgressEventType.TASK_START:
                self._handle_task_start(event)
            elif event.event_type == ProgressEventType.TASK_COMPLETE:
                self._handle_task_complete(event)
            elif event.event_type == ProgressEventType.TASK_FAILED:
                self._handle_task_failed(event)
            elif event.event_type == ProgressEventType.BATCH_COMPLETE:
                self._handle_batch_complete(event)
            elif event.event_type == ProgressEventType.UPDATE_TOTAL:
                self._handle_update_total(event)
            elif event.event_type == ProgressEventType.MESSAGE:
                self._handle_message(event)

        except Exception as e:
            logger.error(f"处理事件 {event.event_type} 时出错: {e}", exc_info=True)

    def _handle_phase_start(self, event: ProgressEvent):
        """处理阶段开始事件"""
        if event.phase:
            # 如果从动态提示阶段切换到其他阶段，先停止动画
            if self._current_phase in [
                ProgressPhase.INITIALIZATION,
                ProgressPhase.PREPARATION,
            ] and event.phase not in [
                ProgressPhase.INITIALIZATION,
                ProgressPhase.PREPARATION,
            ]:
                self._stop_state_animation()

            self._current_phase = event.phase
            self._phase_completed[event.phase] = 0
            self._phase_failed[event.phase] = 0

            if event.total:
                self._phase_totals[event.phase] = event.total
                # 如果是下载阶段，设置总体任务数
                if event.phase == ProgressPhase.DOWNLOADING:
                    self._overall_total = event.total

            # 初始化或更新进度条
            if event.phase == ProgressPhase.INITIALIZATION:
                self._init_progress_bar()
            elif event.phase in [ProgressPhase.PREPARATION]:
                # 准备阶段也使用动态提示，先关闭旧进度条再重新初始化
                if self._pbar:
                    self._pbar.close()
                    self._pbar = None
                self._init_progress_bar()
            elif event.phase in [ProgressPhase.DOWNLOADING, ProgressPhase.SAVING]:
                # 切换到下载或保存阶段，先关闭旧进度条再重新初始化为传统进度条
                if self._pbar:
                    self._pbar.close()
                    self._pbar = None
                self._init_progress_bar()
            else:
                self._update_progress_bar(force_refresh=True)

            logger.info(f"阶段开始: {event.phase.value}")

    def _handle_phase_end(self, event: ProgressEvent):
        """处理阶段结束事件"""
        if event.phase:
            logger.info(f"阶段结束: {event.phase.value}")

            if event.phase == ProgressPhase.COMPLETED:
                self._finish_progress()

    def _handle_task_start(self, event: ProgressEvent):
        """处理任务开始事件"""
        # 在下载阶段，完全不显示当前任务信息，避免频繁刷新
        if self._current_phase == ProgressPhase.DOWNLOADING:
            # 下载阶段不更新描述，减少重绘
            return
        else:
            # 其他阶段正常处理
            current_task = f"{event.symbol or event.task_id or 'Unknown'}"
            if event.message:
                current_task += f" - {event.message}"
            self._update_progress_bar(current_task=current_task, force_refresh=False)

    def _handle_task_complete(self, event: ProgressEvent):
        """处理任务完成事件"""
        if self._current_phase not in self._phase_completed:
            self._phase_completed[self._current_phase] = 0
        self._phase_completed[self._current_phase] += event.count
        
        # 只有下载阶段的完成才计入总体统计
        if self._current_phase == ProgressPhase.DOWNLOADING:
            self._overall_completed += event.count
        
        # 任务完成时需要刷新进度条以更新进度
        self._update_progress_bar(force_refresh=True)

    def _handle_task_failed(self, event: ProgressEvent):
        """处理任务失败事件"""
        if self._current_phase not in self._phase_failed:
            self._phase_failed[self._current_phase] = 0
        self._phase_failed[self._current_phase] += event.count
        
        # 只有下载阶段的失败才计入总体统计
        if self._current_phase == ProgressPhase.DOWNLOADING:
            self._overall_failed += event.count
        
        # 任务失败时需要刷新进度条以更新进度和失败计数
        self._update_progress_bar(force_refresh=True)

    def _handle_batch_complete(self, event: ProgressEvent):
        """处理批次完成事件"""
        # 批次完成不计入总体进度，只在当前阶段内统计
        if self._current_phase not in self._phase_completed:
            self._phase_completed[self._current_phase] = 0
        self._phase_completed[self._current_phase] += event.count
        # 不更新 overall_completed，因为批次完成不是任务完成
        self._update_progress_bar(force_refresh=True)

    def _handle_update_total(self, event: ProgressEvent):
        """处理更新总数事件"""
        if event.total:
            if event.phase:
                self._phase_totals[event.phase] = event.total
                # 如果是下载阶段，更新总体任务数
                if event.phase == ProgressPhase.DOWNLOADING:
                    self._overall_total = event.total
            else:
                self._overall_total = event.total

            self._update_progress_bar(force_refresh=True)

    def _handle_message(self, event: ProgressEvent):
        """处理消息事件"""
        if event.message and self._pbar:
            tqdm.write(f"ℹ️  {event.message}")

    def _init_progress_bar(self):
        """初始化进度条"""
        if self._pbar:
            self._pbar.close()

        # 对于初始化和准备阶段，使用动态提示模式
        if self._current_phase in [
            ProgressPhase.INITIALIZATION,
            ProgressPhase.PREPARATION,
        ]:
            self._pbar = tqdm(
                total=1,  # 使用1作为总数，不显示进度条
                desc=f"[{self._current_phase.value}] 进行中",
                unit="",
                position=0,
                leave=True,
                bar_format="{desc} [{elapsed}]",
                disable=False,
            )
            self._start_state_animation()
        elif self._current_phase == ProgressPhase.COMPLETED:
            # 完成阶段使用总体统计
            total = self._overall_total or (self._overall_completed + self._overall_failed)
            self._pbar = tqdm(
                total=total,
                desc="完成",
                unit="task",
                position=0,
                leave=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
            )
            # 立即设置为完成状态
            self._pbar.n = total
            self._pbar.refresh()
        else:
            # 获取当前阶段的任务总数
            phase_total = self._phase_totals.get(self._current_phase, 0)
            
            # 如果是下载阶段，同时更新overall_total
            if self._current_phase == ProgressPhase.DOWNLOADING:
                self._overall_total = phase_total
            
            self._pbar = tqdm(
                total=phase_total,
                desc=f"[{self._current_phase.value}] 初始化中",
                unit="task",
                position=0,
                leave=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
            )

    def _update_progress_bar(self, current_task: Optional[str] = None, force_refresh: bool = True):
        """更新进度条"""
        if not self._pbar:
            return

        # 对于初始化和准备阶段，使用动态提示
        if self._current_phase in [
            ProgressPhase.INITIALIZATION,
            ProgressPhase.PREPARATION,
        ]:
            # 动态提示由定时器处理，这里只更新描述
            if current_task:
                base_desc = f"[{self._current_phase.value}] {current_task}"
            else:
                base_desc = f"[{self._current_phase.value}] 进行中"

            state_emoji = self._states[
                self._state_count % len(self._states)
            ]  # 轮换emoji状态
            self._pbar.set_description(f"{base_desc} {state_emoji}")
            if force_refresh:
                self._pbar.refresh()
        else:
            # 下载、保存阶段使用传统进度条
            # 使用当前阶段的进度，而不是总体进度
            phase_completed = self._phase_completed.get(self._current_phase, 0)
            phase_failed = self._phase_failed.get(self._current_phase, 0)
            progress = phase_completed + phase_failed
            self._pbar.n = progress

            # 简化描述，不显示中间统计信息，避免多线程环境下的统计不准确
            phase_name = self._current_phase.value
            desc = f"[{phase_name}]"
            
            # 下载和保存阶段不显示当前任务信息，避免频繁刷新
            if self._current_phase not in [ProgressPhase.DOWNLOADING, ProgressPhase.SAVING] and current_task and len(current_task) <= 20:
                desc += f" {current_task[:20]}"

            self._pbar.set_description(desc)
            if force_refresh:
                self._pbar.refresh()

    def _start_state_animation(self):
        """启动动态状态提示动画"""

        def update_state():
            if self._running and self._current_phase in [
                ProgressPhase.INITIALIZATION,
                ProgressPhase.PREPARATION,
            ]:
                self._state_count += 1
                self._update_progress_bar(force_refresh=True)
                # 每1秒更新一次，让状态变化更清晰
                self._state_timer = threading.Timer(1.0, update_state)
                self._state_timer.start()

        self._state_count = 0
        update_state()

    def _stop_state_animation(self):
        """停止动态状态提示动画"""
        if self._state_timer:
            self._state_timer.cancel()
            self._state_timer = None

    def _finish_progress(self):
        """完成进度显示"""
        self._stop_state_animation()

        if self._pbar:
            # 确保进度条显示为100%
            self._pbar.n = self._pbar.total
            
            # 所有阶段完成时都只显示简洁的完成标识，不显示统计信息
            if self._current_phase == ProgressPhase.COMPLETED:
                final_desc = "完成"
            else:
                final_desc = f"[{self._current_phase.value}] 完成"

            self._pbar.set_description(final_desc)
            self._pbar.refresh()
            self._pbar.close()
            self._pbar = None

    def get_stats(self) -> Dict[str, Any]:
        """获取当前统计信息"""
        return {
            "current_phase": self._current_phase.value,
            "overall_total": self._overall_total,
            "overall_completed": self._overall_completed,
            "overall_failed": self._overall_failed,
            "phase_totals": {k.value: v for k, v in self._phase_totals.items()},
            "phase_completed": {k.value: v for k, v in self._phase_completed.items()},
            "phase_failed": {k.value: v for k, v in self._phase_failed.items()},
            "success_rate": (self._overall_completed / max(1, self._overall_total))
            * 100,
        }


# 全局进度事件管理器实例
progress_event_manager = ProgressEventManager()


# 便捷函数
def start_phase(
    phase: ProgressPhase, total: Optional[int] = None, message: Optional[str] = None
):
    """开始新阶段"""
    progress_event_manager.send_event(
        ProgressEvent(
            event_type=ProgressEventType.PHASE_START,
            phase=phase,
            total=total,
            message=message,
        )
    )


def end_phase(phase: ProgressPhase, message: Optional[str] = None):
    """结束阶段"""
    progress_event_manager.send_event(
        ProgressEvent(
            event_type=ProgressEventType.PHASE_END, phase=phase, message=message
        )
    )


def task_started(
    task_id: Optional[str] = None,
    symbol: Optional[str] = None,
    message: Optional[str] = None,
):
    """任务开始"""
    progress_event_manager.send_event(
        ProgressEvent(
            event_type=ProgressEventType.TASK_START,
            task_id=task_id,
            symbol=symbol,
            message=message,
        )
    )


def task_completed(
    task_id: Optional[str] = None, symbol: Optional[str] = None, count: int = 1
):
    """任务完成"""
    progress_event_manager.send_event(
        ProgressEvent(
            event_type=ProgressEventType.TASK_COMPLETE,
            task_id=task_id,
            symbol=symbol,
            count=count,
        )
    )


def task_failed(
    task_id: Optional[str] = None,
    symbol: Optional[str] = None,
    count: int = 1,
    message: Optional[str] = None,
):
    """任务失败"""
    progress_event_manager.send_event(
        ProgressEvent(
            event_type=ProgressEventType.TASK_FAILED,
            task_id=task_id,
            symbol=symbol,
            count=count,
            message=message,
        )
    )


def batch_completed(count: int, message: Optional[str] = None):
    """批次完成"""
    progress_event_manager.send_event(
        ProgressEvent(
            event_type=ProgressEventType.BATCH_COMPLETE, count=count, message=message
        )
    )


def update_total(total: int, phase: Optional[ProgressPhase] = None):
    """更新总数"""
    progress_event_manager.send_event(
        ProgressEvent(
            event_type=ProgressEventType.UPDATE_TOTAL, total=total, phase=phase
        )
    )


def send_message(message: str):
    """发送消息"""
    progress_event_manager.send_event(
        ProgressEvent(event_type=ProgressEventType.MESSAGE, message=message)
    )
