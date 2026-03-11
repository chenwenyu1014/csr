"""
标签错误管理器

功能说明：
- 统一管理CSR标签（段落）的错误状态
- 当一个标签遇到错误时，记录错误并标记该标签为失败
- 不影响其他标签的正常处理

设计原则：
- 标签级别隔离：每个标签是独立的处理单元
- 遇到错误时：记录错误 → 停止该标签的后续处理 → 标记为失败
- 不影响其他标签：其他标签继续正常运作

使用方式：
    from utils.tag_error_manager import TagErrorManager, TagError
    
    # 创建管理器
    error_manager = TagErrorManager()
    
    # 记录错误并标记标签失败
    error_manager.record_error("tag_001", TagError(
        stage="extraction",
        error_type="LLM_ERROR",
        message="API调用失败",
        exception=e
    ))
    
    # 检查标签是否已失败
    if error_manager.is_failed("tag_001"):
        # 跳过后续处理
        pass
    
    # 获取标签的所有错误
    errors = error_manager.get_errors("tag_001")
    
    # 获取所有失败的标签
    failed_tags = error_manager.get_failed_tags()
"""

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class ErrorStage(Enum):
    """错误发生的阶段"""
    PREPROCESSING = "preprocessing"  # 预处理阶段
    EXTRACTION = "extraction"        # 数据提取阶段
    GENERATION = "generation"        # 内容生成阶段
    INSERTION = "insertion"          # 文档插入阶段
    VALIDATION = "validation"        # 校验阶段
    UNKNOWN = "unknown"              # 未知阶段


class ErrorSeverity(Enum):
    """错误严重程度"""
    WARNING = "warning"    # 警告：可以继续处理
    ERROR = "error"        # 错误：必须停止该标签的处理
    CRITICAL = "critical"  # 严重错误：可能影响整个任务


@dataclass
class TagError:
    """
    标签错误信息
    
    记录单个错误的完整信息，包括：
    - 错误发生的阶段
    - 错误类型和消息
    - 原始异常（如果有）
    - 时间戳
    - 上下文信息
    """
    stage: str                                    # 错误阶段（extraction/generation/insertion）
    error_type: str                               # 错误类型（LLM_ERROR/PARSE_ERROR等）
    message: str                                  # 错误消息
    exception: Optional[Exception] = None         # 原始异常对象
    timestamp: datetime = field(default_factory=datetime.now)  # 错误发生时间
    context: Dict[str, Any] = field(default_factory=dict)     # 上下文信息
    severity: ErrorSeverity = ErrorSeverity.ERROR  # 错误严重程度
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式（用于序列化）"""
        return {
            "stage": self.stage,
            "error_type": self.error_type,
            "message": self.message,
            "exception_type": type(self.exception).__name__ if self.exception else None,
            "exception_str": str(self.exception) if self.exception else None,
            "timestamp": self.timestamp.isoformat(),
            "context": self.context,
            "severity": self.severity.value
        }


@dataclass
class TagStatus:
    """
    标签状态
    
    跟踪单个标签的完整处理状态
    """
    tag_id: str                                   # 标签ID
    status: str = "pending"                       # pending/processing/success/failed
    errors: List[TagError] = field(default_factory=list)  # 错误列表
    started_at: Optional[datetime] = None         # 开始处理时间
    finished_at: Optional[datetime] = None        # 结束处理时间
    current_stage: str = ""                       # 当前处理阶段
    
    @property
    def is_failed(self) -> bool:
        """检查标签是否失败"""
        return self.status == "failed"
    
    @property
    def has_errors(self) -> bool:
        """检查是否有错误"""
        return len(self.errors) > 0
    
    @property
    def first_error(self) -> Optional[TagError]:
        """获取第一个错误"""
        return self.errors[0] if self.errors else None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "tag_id": self.tag_id,
            "status": self.status,
            "errors": [e.to_dict() for e in self.errors],
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "current_stage": self.current_stage
        }


class TagErrorManager:
    """
    标签错误管理器
    
    线程安全的错误管理器，用于跟踪和管理所有标签的处理状态和错误。
    
    核心原则：
    1. 标签级别隔离：每个标签独立管理
    2. 遇错即停：标签遇到错误后立即标记为失败
    3. 不影响其他：其他标签继续正常处理
    
    使用示例：
        manager = TagErrorManager()
        
        # 开始处理标签
        manager.start_tag("para_001")
        
        try:
            # 提取阶段
            manager.set_stage("para_001", "extraction")
            result = extract_data(...)
        except Exception as e:
            # 记录错误并标记失败
            manager.record_error("para_001", TagError(
                stage="extraction",
                error_type="EXTRACTION_ERROR",
                message=str(e),
                exception=e
            ))
            # 标签已失败，跳过后续处理
            return
        
        # 检查是否可以继续
        if manager.is_failed("para_001"):
            return
        
        # 继续生成阶段...
    """
    
    def __init__(self):
        """初始化错误管理器"""
        self._tags: Dict[str, TagStatus] = {}
        self._lock = threading.RLock()  # 可重入锁，支持同一线程多次获取
        logger.info("TagErrorManager 已初始化")
    
    def start_tag(self, tag_id: str) -> None:
        """
        标记标签开始处理
        
        Args:
            tag_id: 标签ID
        """
        with self._lock:
            if tag_id not in self._tags:
                self._tags[tag_id] = TagStatus(tag_id=tag_id)
            
            self._tags[tag_id].status = "processing"
            self._tags[tag_id].started_at = datetime.now()
            logger.debug(f"标签开始处理: {tag_id}")
    
    def set_stage(self, tag_id: str, stage: str) -> None:
        """
        设置标签当前处理阶段
        
        Args:
            tag_id: 标签ID
            stage: 阶段名称（extraction/generation/insertion）
        """
        with self._lock:
            if tag_id not in self._tags:
                self._tags[tag_id] = TagStatus(tag_id=tag_id)
            
            self._tags[tag_id].current_stage = stage
            logger.debug(f"标签 {tag_id} 进入阶段: {stage}")
    
    def record_error(self, tag_id: str, error: TagError) -> None:
        """
        记录标签错误并标记为失败
        
        这是核心方法：记录错误的同时自动将标签标记为失败状态。
        后续对该标签的处理应该被跳过。
        
        Args:
            tag_id: 标签ID
            error: 错误信息
        """
        with self._lock:
            if tag_id not in self._tags:
                self._tags[tag_id] = TagStatus(tag_id=tag_id)
            
            tag_status = self._tags[tag_id]
            
            # 记录错误
            tag_status.errors.append(error)
            
            # 标记为失败
            tag_status.status = "failed"
            tag_status.finished_at = datetime.now()
            
            # 记录日志
            logger.error(
                f"❌ 标签 {tag_id} 处理失败\n"
                f"   阶段: {error.stage}\n"
                f"   类型: {error.error_type}\n"
                f"   消息: {error.message}\n"
                f"   堆栈信息: {error.exception}\n"
            )
            
            # 如果有异常，记录详细信息
            if error.exception:
                logger.error(f"   异常: {type(error.exception).__name__}: {error.exception}")

    def mark_success(self, tag_id: str) -> None:
        """
        标记标签处理成功
        
        Args:
            tag_id: 标签ID
        """
        with self._lock:
            if tag_id not in self._tags:
                self._tags[tag_id] = TagStatus(tag_id=tag_id)
            
            tag_status = self._tags[tag_id]
            
            # 只有非失败状态才能标记为成功
            if tag_status.status != "failed":
                tag_status.status = "success"
                tag_status.finished_at = datetime.now()
                logger.debug(f"✓ 标签处理成功: {tag_id}")
    
    def is_failed(self, tag_id: str) -> bool:
        """
        检查标签是否已失败
        
        Args:
            tag_id: 标签ID
            
        Returns:
            bool: True表示已失败，后续处理应跳过
        """
        with self._lock:
            if tag_id not in self._tags:
                return False
            return self._tags[tag_id].is_failed
    
    def can_continue(self, tag_id: str) -> bool:
        """
        检查标签是否可以继续处理
        
        与 is_failed 相反，用于更清晰的代码逻辑。
        
        Args:
            tag_id: 标签ID
            
        Returns:
            bool: True表示可以继续，False表示应该停止
        """
        return not self.is_failed(tag_id)
    
    def get_errors(self, tag_id: str) -> List[TagError]:
        """
        获取标签的所有错误
        
        Args:
            tag_id: 标签ID
            
        Returns:
            List[TagError]: 错误列表
        """
        with self._lock:
            if tag_id not in self._tags:
                return []
            return self._tags[tag_id].errors.copy()
    
    def get_status(self, tag_id: str) -> Optional[TagStatus]:
        """
        获取标签的完整状态
        
        Args:
            tag_id: 标签ID
            
        Returns:
            TagStatus: 标签状态，如果不存在返回None
        """
        with self._lock:
            return self._tags.get(tag_id)
    
    def get_failed_tags(self) -> List[str]:
        """
        获取所有失败的标签ID列表
        
        Returns:
            List[str]: 失败的标签ID列表
        """
        with self._lock:
            return [
                tag_id for tag_id, status in self._tags.items()
                if status.is_failed
            ]
    
    def get_successful_tags(self) -> List[str]:
        """
        获取所有成功的标签ID列表
        
        Returns:
            List[str]: 成功的标签ID列表
        """
        with self._lock:
            return [
                tag_id for tag_id, status in self._tags.items()
                if status.status == "success"
            ]
    
    def get_summary(self) -> Dict[str, Any]:
        """
        获取处理摘要
        
        Returns:
            Dict: 包含总数、成功数、失败数等统计信息
        """
        with self._lock:
            total = len(self._tags)
            failed = len([s for s in self._tags.values() if s.is_failed])
            success = len([s for s in self._tags.values() if s.status == "success"])
            processing = len([s for s in self._tags.values() if s.status == "processing"])
            pending = len([s for s in self._tags.values() if s.status == "pending"])
            
            return {
                "total": total,
                "success": success,
                "failed": failed,
                "processing": processing,
                "pending": pending,
                "failed_tags": self.get_failed_tags(),
                "all_statuses": {
                    tag_id: status.to_dict() 
                    for tag_id, status in self._tags.items()
                }
            }
    
    def clear(self) -> None:
        """清除所有状态（用于新任务）"""
        with self._lock:
            self._tags.clear()
            logger.debug("TagErrorManager 已清除所有状态")
    
    def reset_tag(self, tag_id: str) -> None:
        """
        重置单个标签的状态（用于重试）
        
        Args:
            tag_id: 标签ID
        """
        with self._lock:
            if tag_id in self._tags:
                del self._tags[tag_id]
                logger.debug(f"标签状态已重置: {tag_id}")


# 全局单例实例
_global_error_manager: Optional[TagErrorManager] = None
_global_lock = threading.Lock()


def get_error_manager() -> TagErrorManager:
    """
    获取全局错误管理器实例
    
    使用单例模式确保整个应用共享同一个错误管理器。
    
    Returns:
        TagErrorManager: 全局错误管理器实例
    """
    global _global_error_manager
    
    if _global_error_manager is None:
        with _global_lock:
            if _global_error_manager is None:
                _global_error_manager = TagErrorManager()
    
    return _global_error_manager


def reset_error_manager() -> TagErrorManager:
    """
    重置全局错误管理器（用于新任务开始时）
    
    Returns:
        TagErrorManager: 新的错误管理器实例
    """
    global _global_error_manager
    
    with _global_lock:
        _global_error_manager = TagErrorManager()
    
    return _global_error_manager


# 便捷函数
def record_tag_error(
    tag_id: str,
    stage: str,
    error_type: str,
    message: str,
    exception: Optional[Exception] = None,
    context: Optional[Dict[str, Any]] = None
) -> None:
    """
    便捷函数：记录标签错误
    
    Args:
        tag_id: 标签ID
        stage: 错误阶段
        error_type: 错误类型
        message: 错误消息
        exception: 原始异常
        context: 上下文信息
    """
    manager = get_error_manager()
    error = TagError(
        stage=stage,
        error_type=error_type,
        message=message,
        exception=exception,
        context=context or {}
    )
    manager.record_error(tag_id, error)


def is_tag_failed(tag_id: str) -> bool:
    """
    便捷函数：检查标签是否已失败
    
    Args:
        tag_id: 标签ID
        
    Returns:
        bool: 是否已失败
    """
    return get_error_manager().is_failed(tag_id)


def can_tag_continue(tag_id: str) -> bool:
    """
    便捷函数：检查标签是否可以继续处理
    
    Args:
        tag_id: 标签ID
        
    Returns:
        bool: 是否可以继续
    """
    return get_error_manager().can_continue(tag_id)


__all__ = [
    "TagError",
    "TagStatus",
    "TagErrorManager",
    "ErrorStage",
    "ErrorSeverity",
    "get_error_manager",
    "reset_error_manager",
    "record_tag_error",
    "is_tag_failed",
    "can_tag_continue"
]



