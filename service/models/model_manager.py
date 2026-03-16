"""
模型服务管理器

功能说明：
- 统一管理所有模型实例（单例模式）
- 支持不同任务使用不同模型配置
- 避免重复实例化，节省资源
- 提供统一的获取接口

使用示例：
    from service.models.model_manager import get_llm_service, get_vision_service
    
    # 获取提取任务专用的LLM
    llm = get_llm_service("extraction")
    
    # 获取验证任务专用的LLM
    llm = get_llm_service("validation")
    
    # 获取视觉模型
    vision = get_vision_service()
"""

from __future__ import annotations

# ========== 标准库导入 ==========
import logging
import threading
from typing import TYPE_CHECKING, Any, Dict, Optional

# 类型检查时导入（运行时不导入，避免循环依赖）
if TYPE_CHECKING:
    from service.models.llm_service import LLMService
    from service.models.vision_model_service import VisionModelService

# ========== 日志配置 ==========
logger = logging.getLogger(__name__)


class ModelManager:
    """
    模型服务管理器（单例）
    
    负责管理所有模型实例的生命周期，确保：
    - 同一任务类型只创建一个模型实例
    - 线程安全的实例获取
    - 统一的配置管理
    """
    
    _instance: Optional['ModelManager'] = None
    _lock = threading.Lock()
    
    def __init__(self):
        """初始化模型管理器（私有，通过 get_instance 获取）"""
        self._llm_instances: Dict[str, 'LLMService'] = {}
        self._vision_instances: Dict[str, 'VisionModelService'] = {}
        logger.info("模型管理器已初始化")
    
    @classmethod
    def get_instance(cls) -> 'ModelManager':
        """
        获取模型管理器单例实例（线程安全）
        
        Returns:
            ModelManager: 全局唯一的模型管理器实例
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    def get_llm(
        self, 
        task_type: str = "default", 
        model_name: Optional[str] = None
    ) -> 'LLMService':
        """
        获取LLM服务实例（缓存复用）
        
        Args:
            task_type: 任务类型，支持：
                - "extraction": 数据提取任务（默认 deepseek-v3.2）
                - "validation": 数据验证任务（默认 qwen3-max）
                - "generation": 内容生成任务（默认 qwen3-max）
                - "default": 默认任务（使用全局配置）
            model_name: 指定模型名称（可选，会覆盖任务类型的默认值）
        
        Returns:
            LLMService: LLM服务实例
        """
        # 构建缓存键
        cache_key = f"{task_type}:{model_name or 'auto'}"
        
        # 如果已缓存，直接返回
        if cache_key in self._llm_instances:
            return self._llm_instances[cache_key]
        
        # 创建新实例
        from config import get_settings
        from service.models.llm_service import LLMService
        
        config = get_settings()
        
        # 根据任务类型选择模型（使用 settings.py 中的属性方法）
        if model_name is None:
            model_map = {
                "extraction": config.extraction_model_name,  # 默认 deepseek-v3.2
                "validation": config.validation_model_name,  # 默认使用 extraction_model
                "generation": config.generation_model_name,  # 默认使用 llm_model
                "default": config.llm_model_name
            }
            model_name = model_map.get(task_type, config.llm_model_name)
        
        logger.info(f"创建LLM实例: task_type={task_type}, model={model_name}")
        
        # 创建并缓存实例
        self._llm_instances[cache_key] = LLMService(
            api_key=config.dashscope_api_key,
            model_name=model_name
        )
        
        return self._llm_instances[cache_key]
    
    def get_vision(self, timeout: int = 600) -> 'VisionModelService':
        """
        获取视觉模型服务实例（缓存复用）
        
        Args:
            timeout: 请求超时时间（秒）
        
        Returns:
            VisionModelService: 视觉模型服务实例
        """
        cache_key = f"vision:{timeout}"
        
        if cache_key in self._vision_instances:
            return self._vision_instances[cache_key]
        
        from service.models.vision_model_service import VisionModelService
        
        logger.info(f"创建视觉模型实例: timeout={timeout}")
        
        self._vision_instances[cache_key] = VisionModelService(timeout=timeout)
        
        return self._vision_instances[cache_key]
    
    def clear_cache(self):
        """清除所有缓存的模型实例（用于测试或重新初始化）"""
        logger.info("清除所有模型实例缓存")
        self._llm_instances.clear()
        self._vision_instances.clear()


# ============================================================
# 便捷函数（推荐使用）
# ============================================================

def get_llm_service(
    task_type: str = "default", 
    model_name: Optional[str] = None
) -> 'LLMService':
    """
    获取LLM服务（全局单例）
    
    这是推荐的获取LLM服务的方式，会自动管理实例生命周期。
    
    Args:
        task_type: 任务类型（extraction/validation/generation/default）
        model_name: 指定模型名称（可选）
    
    Returns:
        LLMService: LLM服务实例
    
    Example:
        >>> llm = get_llm_service("extraction")
        >>> result = llm.extract_data(prompt, content)
    """
    return ModelManager.get_instance().get_llm(task_type, model_name)


def get_vision_service(timeout: int = 600) -> 'VisionModelService':
    """
    获取视觉模型服务（全局单例）
    
    Args:
        timeout: 请求超时时间（秒）
    
    Returns:
        VisionModelService: 视觉模型服务实例
    
    Example:
        >>> vision = get_vision_service()
        >>> result = vision.ocr_image(image_path)
    """
    return ModelManager.get_instance().get_vision(timeout)


def clear_model_cache():
    """清除所有模型缓存（用于测试或重新初始化）"""
    ModelManager.get_instance().clear_cache()

