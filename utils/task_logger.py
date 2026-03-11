"""
任务级日志管理器

功能说明：
- 每个任务独立的日志收集器，避免多任务并发时日志混乱
- 内存缓冲，任务结束时统一写入文件
- 错误单独收集，确保不丢失
- 线程安全，支持多线程并发写入

使用示例：
    from utils.task_logger import TaskLogger, get_task_logger, set_task_logger
    
    # 创建任务日志器
    task_logger = TaskLogger(task_id="task_123", output_dir=Path("output/task_123"))
    set_task_logger(task_logger)
    
    # 在任务代码中使用
    logger = get_task_logger()
    logger.info("开始处理")
    logger.debug("详细信息", extra={"key": "value"})
    
    try:
        do_something()
    except Exception as e:
        logger.error("处理失败", exc=e)
    
    # 任务结束时写入文件
    logger.flush()
"""

import json
import threading
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from dataclasses import dataclass, field


# ============================================================
# 日志条目数据结构
# ============================================================

@dataclass
class LogEntry:
    """单条日志记录"""
    timestamp: str
    level: str
    message: str
    logger_name: str = ""
    extra: dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        result = {
            "timestamp": self.timestamp,
            "level": self.level,
            "message": self.message,
        }
        if self.logger_name:
            result["logger"] = self.logger_name
        if self.extra:
            result.update(self.extra)
        return result
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)


@dataclass
class ErrorEntry:
    """错误日志记录（包含完整堆栈）"""
    timestamp: str
    message: str
    error_type: str
    error_message: str
    traceback: str
    logger_name: str = ""
    extra: dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        result = {
            "timestamp": self.timestamp,
            "level": "ERROR",
            "message": self.message,
            "error": {
                "type": self.error_type,
                "message": self.error_message,
                "traceback": self.traceback,
            }
        }
        if self.logger_name:
            result["logger"] = self.logger_name
        if self.extra:
            result.update(self.extra)
        return result
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)


# ============================================================
# 任务日志器
# ============================================================

class TaskLogger:
    """
    任务级日志收集器
    
    特点：
    - 每个任务实例独立，日志不会混乱
    - 内存缓冲，减少 I/O
    - 错误单独收集，确保不丢失
    - 线程安全
    """
    
    def __init__(
        self, 
        task_id: str, 
        output_dir: Optional[Path] = None,
        auto_flush_on_error: bool = True,
        max_buffer_size: int = 10000
    ):
        """
        初始化任务日志器
        
        Args:
            task_id: 任务唯一标识
            output_dir: 日志输出目录，None 则不写文件
            auto_flush_on_error: 遇到错误时是否自动刷新到文件
            max_buffer_size: 最大缓冲条数，超过后自动刷新
        """
        self.task_id = task_id
        self.output_dir = Path(output_dir) if output_dir else None
        self.auto_flush_on_error = auto_flush_on_error
        self.max_buffer_size = max_buffer_size
        
        # 日志缓冲
        self._logs: list[LogEntry] = []
        self._errors: list[ErrorEntry] = []
        self._lock = threading.Lock()
        
        # 统计
        self._counts = {"DEBUG": 0, "INFO": 0, "WARNING": 0, "ERROR": 0}
        
        # 确保输出目录存在
        if self.output_dir:
            self.output_dir.mkdir(parents=True, exist_ok=True)
    
    # ========== 日志方法 ==========
    
    def debug(self, message: str, logger_name: str = "", **extra):
        """记录 DEBUG 级别日志"""
        self._append_log("DEBUG", message, logger_name, extra)
    
    def info(self, message: str, logger_name: str = "", **extra):
        """记录 INFO 级别日志"""
        self._append_log("INFO", message, logger_name, extra)
    
    def warning(self, message: str, logger_name: str = "", **extra):
        """记录 WARNING 级别日志"""
        self._append_log("WARNING", message, logger_name, extra)
    
    def error(
        self, 
        message: str, 
        exc: Optional[Exception] = None,
        logger_name: str = "",
        **extra
    ):
        """
        记录 ERROR 级别日志
        
        Args:
            message: 错误描述
            exc: 异常对象（可选），会自动提取类型、消息和堆栈
            logger_name: 日志来源
            **extra: 额外上下文信息
        """
        timestamp = datetime.now().isoformat()
        
        # 提取异常信息
        if exc:
            error_type = type(exc).__name__
            error_message = str(exc)
            tb = traceback.format_exc()
            # 如果 format_exc 返回 "NoneType: None"，说明不在异常上下文中
            if tb.strip() == "NoneType: None":
                tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        else:
            error_type = "Unknown"
            error_message = message
            tb = ""
        
        error_entry = ErrorEntry(
            timestamp=timestamp,
            message=message,
            error_type=error_type,
            error_message=error_message,
            traceback=tb,
            logger_name=logger_name,
            extra=extra
        )
        
        with self._lock:
            # 同时记录到普通日志和错误日志
            self._logs.append(LogEntry(
                timestamp=timestamp,
                level="ERROR",
                message=f"{message} | {error_type}: {error_message}",
                logger_name=logger_name,
                extra=extra
            ))
            self._errors.append(error_entry)
            self._counts["ERROR"] += 1
        
        # 错误时自动刷新
        if self.auto_flush_on_error and self.output_dir:
            self._flush_errors()
    
    def exception(self, message: str, logger_name: str = "", **extra):
        """
        记录异常（在 except 块中使用，自动捕获当前异常）
        """
        import sys
        exc_info = sys.exc_info()
        if exc_info[1]:
            self.error(message, exc=exc_info[1], logger_name=logger_name, **extra)
        else:
            self.error(message, logger_name=logger_name, **extra)
    
    # ========== 内部方法 ==========
    
    def _append_log(self, level: str, message: str, logger_name: str, extra: dict):
        """添加日志条目"""
        entry = LogEntry(
            timestamp=datetime.now().isoformat(),
            level=level,
            message=message,
            logger_name=logger_name,
            extra=extra
        )
        
        with self._lock:
            self._logs.append(entry)
            self._counts[level] += 1
            
            # 超过缓冲上限时自动刷新
            if len(self._logs) >= self.max_buffer_size:
                self._flush_logs_unsafe()
    
    def _flush_logs_unsafe(self):
        """刷新日志到文件（不加锁，内部使用）"""
        if not self.output_dir or not self._logs:
            return
        
        log_file = self.output_dir / "task.log"
        with open(log_file, "a", encoding="utf-8") as f:
            for entry in self._logs:
                f.write(entry.to_json() + "\n")
        self._logs.clear()
    
    def _flush_errors(self):
        """刷新错误日志到文件"""
        if not self.output_dir:
            return
        
        with self._lock:
            if not self._errors:
                return
            
            error_file = self.output_dir / "error.log"
            with open(error_file, "a", encoding="utf-8") as f:
                for entry in self._errors:
                    f.write(entry.to_json() + "\n")
            self._errors.clear()
    
    # ========== 公开方法 ==========
    
    def flush(self):
        """
        刷新所有缓冲日志到文件
        
        应在任务结束时调用
        """
        if not self.output_dir:
            return
        
        with self._lock:
            # 写入普通日志
            if self._logs:
                log_file = self.output_dir / "task.log"
                with open(log_file, "a", encoding="utf-8") as f:
                    for entry in self._logs:
                        f.write(entry.to_json() + "\n")
                self._logs.clear()
            
            # 写入错误日志
            if self._errors:
                error_file = self.output_dir / "error.log"
                with open(error_file, "a", encoding="utf-8") as f:
                    for entry in self._errors:
                        f.write(entry.to_json() + "\n")
                self._errors.clear()
    
    def get_logs(self) -> list[dict]:
        """获取所有日志（用于调试或返回给调用方）"""
        with self._lock:
            return [entry.to_dict() for entry in self._logs]
    
    def get_errors(self) -> list[dict]:
        """获取所有错误日志"""
        with self._lock:
            return [entry.to_dict() for entry in self._errors]
    
    def get_summary(self) -> dict:
        """获取日志统计摘要"""
        with self._lock:
            return {
                "task_id": self.task_id,
                "total_logs": sum(self._counts.values()),
                "counts": self._counts.copy(),
                "has_errors": self._counts["ERROR"] > 0,
                "buffered_logs": len(self._logs),
                "buffered_errors": len(self._errors),
            }
    
    def __enter__(self):
        """支持 with 语句"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出时自动刷新"""
        if exc_val:
            self.error("任务异常退出", exc=exc_val)
        self.flush()
        return False  # 不抑制异常


# ============================================================
# 线程本地存储 - 当前任务的日志器
# ============================================================

_thread_local = threading.local()


def set_task_logger(logger: TaskLogger):
    """设置当前线程的任务日志器"""
    _thread_local.task_logger = logger


def get_task_logger() -> Optional[TaskLogger]:
    """获取当前线程的任务日志器"""
    return getattr(_thread_local, "task_logger", None)


def clear_task_logger():
    """清除当前线程的任务日志器"""
    if hasattr(_thread_local, "task_logger"):
        delattr(_thread_local, "task_logger")


# ============================================================
# 便捷函数 - 直接使用当前任务的日志器
# ============================================================

def task_debug(message: str, logger_name: str = "", **extra):
    """记录 DEBUG 日志到当前任务"""
    logger = get_task_logger()
    if logger:
        logger.debug(message, logger_name, **extra)


def task_info(message: str, logger_name: str = "", **extra):
    """记录 INFO 日志到当前任务"""
    logger = get_task_logger()
    if logger:
        logger.info(message, logger_name, **extra)


def task_warning(message: str, logger_name: str = "", **extra):
    """记录 WARNING 日志到当前任务"""
    logger = get_task_logger()
    if logger:
        logger.warning(message, logger_name, **extra)


def task_error(message: str, exc: Optional[Exception] = None, logger_name: str = "", **extra):
    """记录 ERROR 日志到当前任务"""
    logger = get_task_logger()
    if logger:
        logger.error(message, exc, logger_name, **extra)


def task_exception(message: str, logger_name: str = "", **extra):
    """记录异常到当前任务（在 except 块中使用）"""
    logger = get_task_logger()
    if logger:
        logger.exception(message, logger_name, **extra)
