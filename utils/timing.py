#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
耗时记录工具模块

功能说明：
- 提供统一的耗时记录和统计功能
- 支持上下文管理器和装饰器两种使用方式
- 自动记录到日志系统
- 支持嵌套计时和阶段性统计

使用示例：
    # 方式1: 使用上下文管理器
    with Timer("PDF转换") as t:
        convert_pdf()
    
    # 方式2: 使用装饰器
    @timed("模型生成")
    def generate_text():
        pass
    
    # 方式3: 手动计时
    timer = Timer("文件读取")
    timer.start()
    # ... 操作 ...
    timer.stop()
    
    # 方式4: 使用全局计时器记录多个阶段
    timing_logger.start("预处理总流程")
    timing_logger.start("Word转PDF")
    # ... Word转PDF ...
    timing_logger.stop("Word转PDF")
    timing_logger.start("PDF转Markdown")
    # ... PDF转Markdown ...
    timing_logger.stop("PDF转Markdown")
    timing_logger.stop("预处理总流程")
    timing_logger.print_summary()
"""

import time
import logging
import functools
import threading
from typing import Optional, Dict, Any, Callable, List
from dataclasses import dataclass, field
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@dataclass
class TimingRecord:
    """单次计时记录"""
    name: str                           # 操作名称
    start_time: float = 0.0             # 开始时间戳
    end_time: float = 0.0               # 结束时间戳
    duration: float = 0.0               # 耗时（秒）
    parent: Optional[str] = None        # 父级操作名称
    metadata: Dict[str, Any] = field(default_factory=dict)  # 额外元数据
    
    @property
    def duration_ms(self) -> float:
        """耗时（毫秒）"""
        return self.duration * 1000
    
    @property
    def duration_str(self) -> str:
        """格式化的耗时字符串"""
        if self.duration < 1:
            return f"{self.duration_ms:.2f}ms"
        elif self.duration < 60:
            return f"{self.duration:.2f}s"
        else:
            minutes = int(self.duration // 60)
            seconds = self.duration % 60
            return f"{minutes}m {seconds:.2f}s"


class Timer:
    """
    计时器类
    
    支持上下文管理器和手动控制两种方式
    """
    
    def __init__(
        self, 
        name: str, 
        parent: Optional[str] = None,
        log_level: int = logging.INFO,
        metadata: Optional[Dict[str, Any]] = None,
        auto_log: bool = True
    ):
        """
        初始化计时器
        
        Args:
            name: 操作名称（用于日志输出）
            parent: 父级操作名称（用于层级显示）
            log_level: 日志级别
            metadata: 额外的元数据
            auto_log: 是否自动记录到日志
        """
        self.name = name
        self.parent = parent
        self.log_level = log_level
        self.metadata = metadata or {}
        self.auto_log = auto_log
        
        self._start_time: float = 0.0
        self._end_time: float = 0.0
        self._duration: float = 0.0
        self._running: bool = False
    
    @property
    def duration(self) -> float:
        """获取耗时（秒）"""
        if self._running:
            return time.time() - self._start_time
        return self._duration
    
    @property
    def duration_ms(self) -> float:
        """获取耗时（毫秒）"""
        return self.duration * 1000
    
    @property
    def duration_str(self) -> str:
        """获取格式化的耗时字符串"""
        d = self.duration
        if d < 1:
            return f"{d * 1000:.2f}ms"
        elif d < 60:
            return f"{d:.2f}s"
        else:
            minutes = int(d // 60)
            seconds = d % 60
            return f"{minutes}m {seconds:.2f}s"
    
    def start(self) -> 'Timer':
        """开始计时"""
        self._start_time = time.time()
        self._running = True
        if self.auto_log:
            prefix = f"[{self.parent}] " if self.parent else ""
            logger.log(self.log_level, f"⏱️ {prefix}{self.name} - 开始")
        return self
    
    def stop(self) -> float:
        """停止计时并返回耗时（秒）"""
        if not self._running:
            return self._duration
        
        self._end_time = time.time()
        self._duration = self._end_time - self._start_time
        self._running = False
        
        if self.auto_log:
            prefix = f"[{self.parent}] " if self.parent else ""
            logger.log(
                self.log_level, 
                f"✅ {prefix}{self.name} - 完成 [耗时: {self.duration_str}]"
            )
        
        return self._duration
    
    def get_record(self) -> TimingRecord:
        """获取计时记录"""
        return TimingRecord(
            name=self.name,
            start_time=self._start_time,
            end_time=self._end_time,
            duration=self._duration,
            parent=self.parent,
            metadata=self.metadata
        )
    
    def __enter__(self) -> 'Timer':
        """上下文管理器入口"""
        return self.start()
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """上下文管理器出口"""
        self.stop()


class TimingLogger:
    """
    全局计时日志记录器
    
    用于记录多个操作的耗时并生成统计报告
    """
    
    def __init__(self, name: str = "default"):
        """
        初始化计时日志记录器
        
        Args:
            name: 记录器名称
        """
        self.name = name
        self._records: List[TimingRecord] = []
        self._active_timers: Dict[str, Timer] = {}
        self._lock = threading.Lock()
    
    def start(
        self, 
        operation: str, 
        parent: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Timer:
        """
        开始计时一个操作
        
        Args:
            operation: 操作名称
            parent: 父级操作名称
            metadata: 额外元数据
            
        Returns:
            Timer对象
        """
        with self._lock:
            timer = Timer(
                name=operation,
                parent=parent,
                metadata=metadata or {},
                auto_log=True
            )
            timer.start()
            self._active_timers[operation] = timer
            return timer
    
    def stop(self, operation: str) -> Optional[float]:
        """
        停止计时一个操作
        
        Args:
            operation: 操作名称
            
        Returns:
            耗时（秒），如果操作不存在则返回None
        """
        with self._lock:
            timer = self._active_timers.pop(operation, None)
            if timer:
                duration = timer.stop()
                self._records.append(timer.get_record())
                return duration
            return None
    
    def record(
        self, 
        operation: str, 
        duration: float,
        parent: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        直接记录一个操作的耗时
        
        Args:
            operation: 操作名称
            duration: 耗时（秒）
            parent: 父级操作名称
            metadata: 额外元数据
        """
        with self._lock:
            record = TimingRecord(
                name=operation,
                duration=duration,
                parent=parent,
                metadata=metadata or {}
            )
            self._records.append(record)
            
            # 自动记录到日志
            prefix = f"[{parent}] " if parent else ""
            duration_str = record.duration_str
            logger.info(f"⏱️ {prefix}{operation} - 耗时: {duration_str}")
    
    @contextmanager
    def timed(
        self, 
        operation: str, 
        parent: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        上下文管理器方式计时
        
        Args:
            operation: 操作名称
            parent: 父级操作名称
            metadata: 额外元数据
            
        Usage:
            with timing_logger.timed("操作名称"):
                # 执行操作
                pass
        """
        timer = self.start(operation, parent, metadata)
        try:
            yield timer
        finally:
            self.stop(operation)
    
    def get_records(self) -> List[TimingRecord]:
        """获取所有记录"""
        with self._lock:
            return list(self._records)
    
    def get_summary(self) -> Dict[str, Any]:
        """
        获取统计摘要
        
        Returns:
            包含统计信息的字典
        """
        with self._lock:
            if not self._records:
                return {"total_operations": 0, "total_duration": 0, "operations": []}
            
            total_duration = sum(r.duration for r in self._records)
            
            # 按操作名称分组统计
            operation_stats: Dict[str, Dict[str, Any]] = {}
            for record in self._records:
                name = record.name
                if name not in operation_stats:
                    operation_stats[name] = {
                        "name": name,
                        "count": 0,
                        "total_duration": 0.0,
                        "min_duration": float('inf'),
                        "max_duration": 0.0,
                        "durations": []
                    }
                
                stats = operation_stats[name]
                stats["count"] += 1
                stats["total_duration"] += record.duration
                stats["min_duration"] = min(stats["min_duration"], record.duration)
                stats["max_duration"] = max(stats["max_duration"], record.duration)
                stats["durations"].append(record.duration)
            
            # 计算平均值
            for stats in operation_stats.values():
                if stats["count"] > 0:
                    stats["avg_duration"] = stats["total_duration"] / stats["count"]
                else:
                    stats["avg_duration"] = 0.0
                del stats["durations"]  # 移除原始数据
            
            # 按总耗时排序
            sorted_ops = sorted(
                operation_stats.values(), 
                key=lambda x: x["total_duration"], 
                reverse=True
            )
            
            return {
                "total_operations": len(self._records),
                "total_duration": total_duration,
                "total_duration_str": self._format_duration(total_duration),
                "operations": sorted_ops
            }
    
    def _format_duration(self, duration: float) -> str:
        """格式化耗时"""
        if duration < 1:
            return f"{duration * 1000:.2f}ms"
        elif duration < 60:
            return f"{duration:.2f}s"
        else:
            minutes = int(duration // 60)
            seconds = duration % 60
            return f"{minutes}m {seconds:.2f}s"
    
    def print_summary(self) -> None:
        """打印统计摘要到日志"""
        summary = self.get_summary()
        
        logger.info("=" * 60)
        logger.info(f"📊 耗时统计报告 [{self.name}]")
        logger.info("=" * 60)
        logger.info(f"总操作数: {summary['total_operations']}")
        logger.info(f"总耗时: {summary['total_duration_str']}")
        logger.info("-" * 60)
        
        for op in summary["operations"]:
            name = op["name"]
            count = op["count"]
            total = self._format_duration(op["total_duration"])
            avg = self._format_duration(op["avg_duration"])
            
            if count > 1:
                logger.info(f"  {name}: {total} (共{count}次, 平均{avg})")
            else:
                logger.info(f"  {name}: {total}")
        
        logger.info("=" * 60)
    
    def clear(self) -> None:
        """清除所有记录"""
        with self._lock:
            self._records.clear()
            self._active_timers.clear()
    
    def export_json(self) -> Dict[str, Any]:
        """
        导出为JSON格式
        
        Returns:
            可序列化的字典
        """
        with self._lock:
            return {
                "name": self.name,
                "exported_at": datetime.now().isoformat(),
                "summary": self.get_summary(),
                "records": [
                    {
                        "name": r.name,
                        "duration": r.duration,
                        "duration_str": r.duration_str,
                        "parent": r.parent,
                        "metadata": r.metadata
                    }
                    for r in self._records
                ]
            }


def timed(
    operation: str = None, 
    parent: str = None,
    log_level: int = logging.INFO
) -> Callable:
    """
    计时装饰器
    
    Args:
        operation: 操作名称（默认使用函数名）
        parent: 父级操作名称
        log_level: 日志级别
        
    Usage:
        @timed("数据提取")
        def extract_data():
            pass
        
        @timed()  # 使用函数名作为操作名
        def process():
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            op_name = operation or func.__name__
            with Timer(op_name, parent=parent, log_level=log_level):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def timed_async(
    operation: str = None, 
    parent: str = None,
    log_level: int = logging.INFO
) -> Callable:
    """
    异步函数计时装饰器
    
    Args:
        operation: 操作名称（默认使用函数名）
        parent: 父级操作名称
        log_level: 日志级别
        
    Usage:
        @timed_async("异步数据提取")
        async def extract_data_async():
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            op_name = operation or func.__name__
            timer = Timer(op_name, parent=parent, log_level=log_level)
            timer.start()
            try:
                return await func(*args, **kwargs)
            finally:
                timer.stop()
        return wrapper
    return decorator


# ============================================================
# 全局实例
# ============================================================

# 默认的全局计时日志记录器
timing_logger = TimingLogger("global")

# 预处理专用计时器
preprocessing_timer = TimingLogger("preprocessing")

# 生成专用计时器
generation_timer = TimingLogger("generation")

# 模型调用专用计时器
model_timer = TimingLogger("model")


# ============================================================
# 便捷函数
# ============================================================

def log_timing(
    operation: str, 
    duration: float, 
    parent: str = None,
    metadata: Dict[str, Any] = None
) -> None:
    """
    快速记录耗时
    
    Args:
        operation: 操作名称
        duration: 耗时（秒）
        parent: 父级操作名称
        metadata: 额外元数据
    """
    timing_logger.record(operation, duration, parent, metadata)


def get_global_summary() -> Dict[str, Any]:
    """获取全局耗时统计摘要"""
    return timing_logger.get_summary()


def print_global_summary() -> None:
    """打印全局耗时统计摘要"""
    timing_logger.print_summary()


def clear_global_timing() -> None:
    """清除全局计时记录"""
    timing_logger.clear()

