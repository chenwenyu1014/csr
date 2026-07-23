"""
模型服务模块
============

提供统一的AI模型服务接口。

主要组件：
- LLMService: 大语言模型服务
- VisionModelService: 视觉模型服务
- ModelManager: 模型服务管理器（单例，推荐使用）

推荐使用方式：
    from service.models import get_llm_service, get_vision_service
    
    # 获取提取任务专用的LLM
    llm = get_llm_service("extraction")
    
    # 获取生成任务专用的LLM
    llm = get_llm_service("generation")
    
    # 获取视觉模型
    vision = get_vision_service()
"""

# ========== 新版推荐接口（统一管理）==========
from service.models.model_manager import (
    get_llm_service,
    get_vision_service,
    clear_model_cache,
    ModelManager
)


__all__ = [
    # 推荐使用（新版）
    'get_llm_service',          # 获取LLM服务（单例管理）
    'get_vision_service',       # 获取视觉模型服务（单例管理）
    'clear_model_cache',        # 清除模型缓存
    'ModelManager',             # 模型管理器类
]
