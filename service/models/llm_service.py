#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
大模型服务

用于根据提取提示词从视觉内容中提取数据，以及进行内容总结

注意：
- LLMService 是底层服务类，提供实际的LLM调用能力
- 推荐使用 get_llm_service() 获取实例（自动管理单例和缓存）
- create_llm_service() 已过时，仅用于兼容旧代码
"""

import logging
from typing import Any, Dict, List, Optional
import json
import os

logger = logging.getLogger(__name__)

# 导入系统提示词管理器
from service.prompts.system_prompt_manager import system_prompt_manager
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
        self.api_key = api_key or os.getenv("DASHSCOPE_API_KEY") or (_cfg.dashscope_api_key if _cfg else None)
        self.model_name = (
            model_name
            or (_cfg.llm_model_name if _cfg else None)
            or os.getenv("QWEN_MODEL")
            or "qwen3-max"
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
        """获取模型额外参数"""
        params = {}
        model_lower = self.model_name.lower()
        
        if "deepseek" in model_lower:
            params["enable_thinking"] = True
        
        return params
    
    # def extract_data(
    #     self,
    #     extraction_prompt: str,
    #     content: str,
    #     return_format: str = "json",
    #     system_prompt: Optional[str] = None,
    #     extra_params: Optional[Dict[str, Any]] = None
    # ) -> Optional[Dict[str, Any]]:
    #     """
    #     提取数据（使用底层 model_service）
    #
    #     Args:
    #         extraction_prompt: 提取提示词
    #         content: 待提取内容
    #         return_format: 返回格式（json/text）
    #         system_prompt: 系统提示词
    #         extra_params: 额外参数
    #
    #     Returns:
    #         提取结果字典，失败返回None
    #     """
    #     try:
    #         from service.models.model_service import generate
    #     except Exception:
    #         logger.error("无法导入 model_service.generate")
    #         return None
    #
    #     if system_prompt is None:
    #         system_prompt = system_prompt_manager.get_system_prompt("extraction_default")
    #
    #     full_prompt = f"{extraction_prompt}\n\n{content}"
    #
    #     model_extra = self._get_model_extra_params()
    #     if extra_params:
    #         model_extra.update(extra_params)
    #
    #     try:
    #         result = generate(
    #             prompt=full_prompt,
    #             model=self.model_name,
    #             temperature=0.3,
    #             max_tokens=self._get_max_tokens(),
    #             system=system_prompt,
    #             extra=model_extra
    #         )
    #
    #         if return_format == "json" and isinstance(result, str):
    #             try:
    #                 return json.loads(result)
    #             except json.JSONDecodeError:
    #                 logger.warning("返回结果不是有效的JSON")
    #                 return {"raw_text": result}
    #
    #         return result if isinstance(result, dict) else {"content": result}
    #
    #     except Exception as e:
    #         logger.error(f"数据提取失败: {e}", exc_info=True)
    #         _task_log_error("LLM数据提取失败", exc=e)
    #         return None
    #
    # async def extract_data_async(
    #     self,
    #     extraction_prompt: str,
    #     content: str,
    #     return_format: str = "json",
    #     system_prompt: Optional[str] = None,
    #     extra_params: Optional[Dict[str, Any]] = None
    # ) -> Optional[Dict[str, Any]]:
    #     """
    #     异步提取数据
    #
    #     Args:
    #         extraction_prompt: 提取提示词
    #         content: 待提取内容
    #         return_format: 返回格式
    #         system_prompt: 系统提示词
    #         extra_params: 额外参数
    #
    #     Returns:
    #         提取结果字典
    #     """
    #     try:
    #         from service.models.model_service import generate_async
    #     except Exception:
    #         logger.error("无法导入 model_service.generate_async")
    #         return None
    #
    #     if system_prompt is None:
    #         system_prompt = system_prompt_manager.get_system_prompt("extraction_default")
    #
    #     full_prompt = f"{extraction_prompt}\n\n{content}"
    #
    #     model_extra = self._get_model_extra_params()
    #     if extra_params:
    #         model_extra.update(extra_params)
    #
    #     try:
    #         result = await generate_async(
    #             prompt=full_prompt,
    #             model=self.model_name,
    #             temperature=0.3,
    #             max_tokens=self._get_max_tokens(),
    #             system=system_prompt,
    #             extra=model_extra
    #         )
    #
    #         if return_format == "json" and isinstance(result, str):
    #             try:
    #                 return json.loads(result)
    #             except json.JSONDecodeError:
    #                 logger.warning("返回结果不是有效的JSON")
    #                 return {"raw_text": result}
    #
    #         return result if isinstance(result, dict) else {"content": result}
    #
    #     except Exception as e:
    #         logger.error(f"异步数据提取失败: {e}", exc_info=True)
    #         return None
    #
    # def summarize(
    #     self,
    #     content: str,
    #     summary_prompt: Optional[str] = None,
    #     system_prompt: Optional[str] = None,
    #     max_length: Optional[int] = None
    # ) -> Optional[str]:
    #     """
    #     总结内容
    #
    #     Args:
    #         content: 待总结内容
    #         summary_prompt: 总结提示词
    #         system_prompt: 系统提示词
    #         max_length: 最大长度
    #
    #     Returns:
    #         总结结果
    #     """
    #     try:
    #         from service.models.model_service import generate
    #     except Exception:
    #         logger.error("无法导入 model_service.generate")
    #         return None
    #
    #     if system_prompt is None:
    #         system_prompt = system_prompt_manager.get_system_prompt("summary_default")
    #
    #     if summary_prompt is None:
    #         summary_prompt = "请对以下内容进行总结："
    #
    #     full_prompt = f"{summary_prompt}\n\n{content}"
    #
    #     try:
    #         result = generate(
    #             prompt=full_prompt,
    #             model=self.model_name,
    #             temperature=0.3,
    #             max_tokens=max_length or self._get_max_tokens(),
    #             system=system_prompt
    #         )
    #
    #         return result if isinstance(result, str) else str(result)
    #
    #     except Exception as e:
    #         logger.error(f"内容总结失败: {e}", exc_info=True)
    #         return None
    #
    # def validate(
    #     self,
    #     content: str,
    #     validation_prompt: str,
    #     system_prompt: Optional[str] = None
    # ) -> Optional[Dict[str, Any]]:
    #     """
    #     验证内容
    #
    #     Args:
    #         content: 待验证内容
    #         validation_prompt: 验证提示词
    #         system_prompt: 系统提示词
    #
    #     Returns:
    #         验证结果
    #     """
    #     try:
    #         from service.models.model_service import generate
    #     except Exception:
    #         logger.error("无法导入 model_service.generate")
    #         return None
    #
    #     if system_prompt is None:
    #         system_prompt = system_prompt_manager.get_system_prompt("validation_default")
    #
    #     full_prompt = f"{validation_prompt}\n\n{content}"
    #
    #     try:
    #         result = generate(
    #             prompt=full_prompt,
    #             model=self.model_name,
    #             temperature=0.3,
    #             max_tokens=self._get_max_tokens(),
    #             system=system_prompt
    #         )
    #
    #         if isinstance(result, str):
    #             try:
    #                 return json.loads(result)
    #             except json.JSONDecodeError:
    #                 return {"raw_response": result}
    #
    #         return result if isinstance(result, dict) else {"response": result}
    #
    #     except Exception as e:
    #         logger.error(f"内容验证失败: {e}", exc_info=True)
    #         return None
    #
    # def generate_text(
    #     self,
    #     prompt: str,
    #     system_prompt: Optional[str] = None,
    #     temperature: float = 0.7,
    #     max_tokens: Optional[int] = None,
    #     extra_params: Optional[Dict[str, Any]] = None
    # ) -> Optional[str]:
    #     """
    #     生成文本
    #
    #     Args:
    #         prompt: 提示词
    #         system_prompt: 系统提示词
    #         temperature: 温度参数
    #         max_tokens: 最大token数
    #         extra_params: 额外参数
    #
    #     Returns:
    #         生成的文本
    #     """
    #     try:
    #         from service.models.model_service import generate
    #     except Exception:
    #         logger.error("无法导入 model_service.generate")
    #         return None
    #
    #     if system_prompt is None:
    #         system_prompt = system_prompt_manager.get_system_prompt("generation_default")
    #
    #     model_extra = self._get_model_extra_params()
    #     if extra_params:
    #         model_extra.update(extra_params)
    #
    #     try:
    #         result = generate(
    #             prompt=prompt,
    #             model=self.model_name,
    #             temperature=temperature,
    #             max_tokens=max_tokens or self._get_max_tokens(),
    #             system=system_prompt,
    #             extra=model_extra
    #         )
    #
    #         return result if isinstance(result, str) else str(result)
    #
    #     except Exception as e:
    #         logger.error(f"文本生成失败: {e}", exc_info=True)
    #         return None
    #
    # async def generate_text_async(
    #     self,
    #     prompt: str,
    #     system_prompt: Optional[str] = None,
    #     temperature: float = 0.7,
    #     max_tokens: Optional[int] = None,
    #     extra_params: Optional[Dict[str, Any]] = None
    # ) -> Optional[str]:
    #     """
    #     异步生成文本
    #
    #     Args:
    #         prompt: 提示词
    #         system_prompt: 系统提示词
    #         temperature: 温度参数
    #         max_tokens: 最大token数
    #         extra_params: 额外参数
    #
    #     Returns:
    #         生成的文本
    #     """
    #     try:
    #         from service.models.model_service import generate_async
    #     except Exception:
    #         logger.error("无法导入 model_service.generate_async")
    #         return None
    #
    #     if system_prompt is None:
    #         system_prompt = system_prompt_manager.get_system_prompt("generation_default")
    #
    #     model_extra = self._get_model_extra_params()
    #     if extra_params:
    #         model_extra.update(extra_params)
    #
    #     try:
    #         result = await generate_async(
    #             prompt=prompt,
    #             model=self.model_name,
    #             temperature=temperature,
    #             max_tokens=max_tokens or self._get_max_tokens(),
    #             system=system_prompt,
    #             extra=model_extra
    #         )
    #
    #         return result if isinstance(result, str) else str(result)
    #
    #     except Exception as e:
    #         logger.error(f"异步文本生成失败: {e}", exc_info=True)
    #         return None
    #
    def generate(self, prompt: str) -> str:
        """
        生成单个响应（简化接口）
        
        Args:
            prompt: 提示词
            
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
                model=self.model_name,
                temperature=0.3,
                max_tokens=self._get_max_tokens(),
                extra=self._get_model_extra_params()
            )
        except Exception as e:
            logger.error(f"生成单个响应失败: {e}")
            _task_log_error("LLM生成失败", exc=e)
            return f"[生成失败] {str(e)}"
    
    def generate_single(self, prompt: str) -> str:
        """
        生成单个响应（兼容旧接口）
        
        Args:
            prompt: 提示词
        
        Returns:
            生成的文本
        """
        return self.generate(prompt)
    
    async def generate_single_async(self, prompt: str) -> str:
        """
        异步生成单个响应（兼容旧接口）
        
        Args:
            prompt: 提示词
            
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
                model=self.model_name,
                temperature=0.3,
                max_tokens=self._get_max_tokens(),
                extra=self._get_model_extra_params()
            )
            return result if isinstance(result, str) else str(result)
        except Exception as e:
            logger.error(f"异步生成单个响应失败: {e}")
            return f"[生成失败] {str(e)}"

    # def stream_generate(self, prompt: str):
    #     """
    #     流式生成（生成器）
    #
    #     Args:
    #         prompt: 提示词
    #
    #     Yields:
    #         文本片段
    #     """
    #     try:
    #         from service.models.model_service import stream_generate as _stream_generate
    #     except Exception:
    #         _stream_generate = None
    #
    #     if _stream_generate is None:
    #         return iter(())
    #
    #     return _stream_generate(
    #         prompt=prompt,
    #         model=self.model_name,
    #         temperature=0.3,
    #         max_tokens=self._get_max_tokens(),
    #         extra=self._get_model_extra_params()
    #     )


# ============================================================
# 工厂函数（兼容旧代码，不推荐使用）
# ============================================================

def create_llm_service(**kwargs) -> LLMService:
    """
    创建LLM服务实例（旧版工厂函数）
    
    ⚠️ 已过时：推荐使用 get_llm_service() 替代
    
    新方式：
        from service.models import get_llm_service
        llm = get_llm_service("extraction")  # 自动管理单例和缓存
    
    Args:
        **kwargs: LLMService 构造参数
    
    Returns:
        LLMService实例（每次调用都创建新实例）
    """
    from config import get_settings
    config = get_settings()
    kwargs.setdefault('api_key', config.dashscope_api_key)
    kwargs.setdefault('model_name', config.llm_model_name)
    return LLMService(**kwargs)
