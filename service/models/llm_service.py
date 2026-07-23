#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
大模型服务

用于根据提取提示词从视觉内容中提取数据，以及进行内容总结

注意：
- LLMService 是底层服务类，提供实际的LLM调用能力
- 推荐使用 get_llm_service() 获取实例（自动管理单例和缓存）
"""

import logging
from typing import Any, Dict, List, Optional
import json
import os

logger = logging.getLogger(__name__)

from utils.task_logger import get_task_logger


def _task_log_error(message: str, exc: Exception = None, **extra):
    """记录错误到任务日志"""
    task_logger = get_task_logger()
    if task_logger:
        task_logger.error(message, exc=exc, logger_name="llm_service", **extra)


class LLMService:
    """
    大模型服务（底层实现）
    
    这是底层服务类，直接调用模型API。
    推荐通过 get_llm_service() 获取实例，而不是直接实例化。
    """
    
    def __init__(self, api_key: Optional[str] = None, model_name: Optional[str] = None):
        """
        初始化大模型服务
        
        Args:
            api_key: API密钥
            model_name: 模型名称
            
        Note:
            推荐使用 get_llm_service("task_type") 替代直接实例化
        """
        try:
            from config import get_settings as _get_settings
            _cfg = _get_settings()
        except Exception:
            _cfg = None
        self.api_key = api_key or os.getenv("DASHSCOPE_API_KEY") or (_cfg.llm_api_key if _cfg else None)
        self.model_name = (
            model_name
            or (_cfg.llm_model if _cfg else None)
            or "qwen3.6-flash"
        )
        try:
            logger.info("LLMService init: model=%s, api_key=%s", self.model_name, ("***" if self.api_key else "(missing)"))
        except Exception:
            pass
        self._setup_client()
    
    def _setup_client(self):
        """设置客户端连接"""
        pass

    def _get_max_tokens(self) -> Optional[int]:
        """获取最大token数，返回None表示不限制（使用模型默认值）"""
        # 不设置最大上下文限制，让模型使用默认的最大值
        return None
    
    def _get_model_extra_params(self) -> Dict[str, Any]:
        """获取模型额外参数（从统一配置读取，回退到硬编码默认）"""
        params = {}
        try:
            from service.models.model_service import _model_config
            cfg = _model_config(self.model_name)
            if cfg and "enable_thinking" in cfg:
                params["enable_thinking"] = cfg["enable_thinking"]
                return params
        except Exception:
            pass
        # 回退：未配置时保持原有硬编码逻辑（deepseek开启思考模式）
        params["enable_thinking"] = False
        if "deepseek" in self.model_name.lower():
            params["enable_thinking"] = True
        return params
    
    def generate(self, prompt: str, timeout: Optional[int] = None, system: Optional[str] = None) -> str:
        """
        生成单个响应（简化接口）
        
        Args:
            prompt: 用户提示词（动态数据部分）
            timeout: HTTP请求超时(秒)，None时使用默认值
            system: 系统提示词（静态指令部分，可被模型端缓存以降低费用与延迟）
            
        Returns:
            生成的文本
        """
        try:
            from service.models.model_service import generate
        except Exception:
            return "[无法导入generate]"
        
        try:
            return generate(
                prompt=prompt,
                system=system,
                model=self.model_name,
                temperature=0.3,
                max_tokens=self._get_max_tokens(),
                extra=self._get_model_extra_params(),
                timeout=timeout
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"生成单个响应失败: {e}")
            _task_log_error("LLM生成失败", exc=e)
            return f"[生成失败] {str(e)}"
    
    def generate_single(self, prompt: str, system: Optional[str] = None) -> str:
        """
        生成单个响应（兼容旧接口）
        
        Args:
            prompt: 用户提示词
            system: 系统提示词（静态指令，可选）
        
        Returns:
            生成的文本
        """
        return self.generate(prompt, system=system)
    
    async def generate_single_async(self, prompt: str, system: Optional[str] = None) -> str:
        """
        异步生成单个响应（兼容旧接口）
        
        Args:
            prompt: 用户提示词
            system: 系统提示词（静态指令，可选）
            
        Returns:
            生成的文本
        """
        try:
            from service.models.model_service import generate_async
        except Exception:
            logger.error("无法导入 model_service.generate_async")
            return "[无法导入generate_async]"
        
        try:
            result = await generate_async(
                prompt=prompt,
                system=system,
                model=self.model_name,
                temperature=0.3,
                max_tokens=self._get_max_tokens(),
                extra=self._get_model_extra_params()
            )
            return result if isinstance(result, str) else str(result)
        except Exception as e:
            logger.error(f"异步生成单个响应失败: {e}")
            return f"[生成失败] {str(e)}"

