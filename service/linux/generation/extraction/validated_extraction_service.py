#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
带校验的提取服务
根据文档类型智能决定是否启用校验
"""

import logging
from typing import Dict, Any, Optional, Callable

from service.linux.generation.extraction.extraction_validation_service import extraction_validation_service
from utils.task_logger import get_task_logger

logger = logging.getLogger(__name__)


def _task_log_error(message: str, exc: Exception = None, **extra):
    """记录错误到任务日志"""
    task_logger = get_task_logger()
    if task_logger:
        task_logger.error(message, exc=exc, logger_name="validated_extraction", **extra)


class ValidatedExtractionService:
    """
    带校验的提取服务（根据文档类型智能决策）
    
    功能：
    - Word/PDF文档：默认不校验，快速提取
    - Excel/RTF文档：默认校验，确保数据准确
    - 支持覆盖默认行为
    """
    
    # 文档类型的默认校验策略
    # ⚠️ 默认关闭校验，加快处理速度；需要校验时外部显式传 enable_validation=True
    VALIDATION_STRATEGY = {
        "word": {
            "enable_validation": False,
            "enable_retry": False,
            "max_retries": 0
        },
        "pdf": {
            "enable_validation": False,
            "enable_retry": False,
            "max_retries": 0
        },
        "excel": {
            "enable_validation": False,
            "enable_retry": False,
            "max_retries": 0
        },
        "rtf": {
            "enable_validation": False,
            "enable_retry": False,
            "max_retries": 0
        }
    }
    
    def __init__(self, max_retries: int = None):
        """
        初始化服务
        
        Args:
            max_retries: 最大重试次数（如果为None，根据文档类型决定）
        """
        self.default_max_retries = max_retries
        self.validation_service = extraction_validation_service
        logger.info("带校验的提取服务V2已初始化")
    
    def extract_with_validation(self,
                                extraction_func: Callable,
                                extraction_kwargs: Dict[str, Any],
                                source_content: str,
                                doc_type: str = "word",
                                enable_validation: Optional[bool] = None,
                                enable_retry: Optional[bool] = None,
                                max_retries: Optional[int] = None) -> Dict[str, Any]:
        """
        执行提取并根据文档类型智能决定是否校验
        
        Args:
            extraction_func: 提取函数
            extraction_kwargs: 提取函数的参数
            source_content: 原始文档内容
            doc_type: 文档类型 ("word", "excel", "pdf", "rtf")
            enable_validation: 是否启用校验（None时根据doc_type自动决定）
            enable_retry: 是否启用重试（None时根据doc_type自动决定）
            max_retries: 最大重试次数（None时根据doc_type自动决定）
        
        Returns:
            提取结果，包含是否校验的信息
        """
        try:
            # 获取文档类型的默认策略
            strategy = self.VALIDATION_STRATEGY.get(
                doc_type.lower(), 
                self.VALIDATION_STRATEGY["word"]  # 默认使用Word策略
            )
            
            # 根据策略决定参数
            if enable_validation is None:
                enable_validation = strategy["enable_validation"]
            
            if enable_retry is None:
                enable_retry = strategy["enable_retry"]
            
            if max_retries is None:
                max_retries = self.default_max_retries or strategy["max_retries"]
            
            logger.info(
                f"文档类型: {doc_type}, "
                f"启用校验: {enable_validation}, "
                f"启用重试: {enable_retry}, "
                f"最大重试: {max_retries}"
            )
            
            # 执行提取
            extraction_query = extraction_kwargs.get("extraction_query", "")
            retry_history = []
            current_attempt = 0
            improvement_suggestions = None
            
            while current_attempt <= (max_retries if enable_retry else 0):
                current_attempt += 1
                logger.info(f"第 {current_attempt} 次提取尝试")
                
                # 如果有改进建议，附加到提取提示词
                if improvement_suggestions and enable_retry:
                    original_query = extraction_query
                    enhanced_query = f"""{original_query}

【上次提取存在以下问题，请改进】
{improvement_suggestions}

请根据上述改进建议，重新提取数据。"""
                    extraction_kwargs["extraction_query"] = enhanced_query
                    logger.info(f"附加改进建议进行重试")
                
                # 执行提取
                extraction_result = extraction_func(**extraction_kwargs)
                
                if not extraction_result.get("success"):
                    error_msg = extraction_result.get('error', '未知错误')
                    logger.error(f"提取失败: {error_msg}")
                    # ✅ 记录更详细的错误信息
                    if extraction_result.get("stage1_result"):
                        s1 = extraction_result.get("stage1_result", {})
                        if not s1.get("success"):
                            logger.error(f"  筛选阶段失败: {s1.get('error', '未知')}")
                    if extraction_result.get("stage2_result"):
                        s2 = extraction_result.get("stage2_result", {})
                        if not s2.get("success"):
                            logger.error(f"  提取阶段失败: {s2.get('error', '未知')}")
                    return {
                        "success": False,
                        "error": f"提取失败: {error_msg}",
                        "doc_type": doc_type,
                        "retry_history": retry_history,
                        "final_attempt": current_attempt,
                        "extraction_result": extraction_result  # ✅ 包含完整的提取结果以便排查
                    }
                
                extracted_content = (
                    extraction_result.get("extracted_content") or
                    extraction_result.get("combined_content") or
                    extraction_result.get("content", "")
                )
                
                # 获取源内容（优先使用提取结果中的source_content，其次使用两阶段提取的selected_chunks_content）
                actual_source_content = (
                    extraction_result.get("source_content")
                    or extraction_result.get("selected_chunks_content")
                    or source_content
                )
                
                # 是否启用校验
                if not enable_validation:
                    return {
                        "success": True,
                        "extracted_content": extracted_content,
                        "is_validated": False,
                        "is_valid": True,  # 未校验视为有效
                        "doc_type": doc_type,
                        "extraction_result": extraction_result,
                        "final_attempt": current_attempt,
                        "message": f"{doc_type.upper()}文档已提取（快速模式，未校验）"
                    }
                
                # 阶段提示：开始校验
                logger.info("正在校验数据...")
                
                # 执行校验
                validation_result = self.validation_service.validate_extraction(
                    extraction_query=extraction_query,
                    source_content=actual_source_content,  # 使用实际的源内容
                    extracted_content=extracted_content
                )
                
                # 检查校验服务是否因 LLM 错误而失败
                if not validation_result.get("success") and "LLM" in str(validation_result.get("error", "")):
                    logger.warning(f"校验服务 LLM 调用失败，跳过校验返回提取结果: {validation_result.get('error')}")
                    return {
                        "success": True,  # 提取本身是成功的
                        "extracted_content": extracted_content,
                        "is_validated": False,
                        "is_valid": None,  # 未知，因为校验失败
                        "doc_type": doc_type,
                        "extraction_result": extraction_result,
                        "validation_error": validation_result.get("error"),
                        "final_attempt": current_attempt,
                        "message": f"{doc_type.upper()}文档已提取（校验服务暂时不可用，跳过校验）",
                        "warning": "校验服务调用失败，建议人工复核提取结果"
                    }
                
                # 记录历史
                retry_history.append({
                    "attempt": current_attempt,
                    "extraction_result": extraction_result,
                    "validation_result": validation_result,
                    "is_valid": validation_result.get("is_valid", False)
                })
                
                # 判断是否通过
                if validation_result.get("is_valid"):
                    # 阶段提示：校验完成
                    logger.info(f"✓ 校验完成: 得分{validation_result.get('overall_score')}")
                    return {
                        "success": True,
                        "extracted_content": extracted_content,
                        "is_validated": True,
                        "is_valid": True,
                        "doc_type": doc_type,
                        "validation_result": validation_result,
                        "extraction_result": extraction_result,
                        "retry_history": retry_history,
                        "final_attempt": current_attempt,
                        "message": f"{doc_type.upper()}文档已提取并校验通过"
                    }
                
                # 如果不通过且还能重试
                if enable_retry and current_attempt <= max_retries:
                    improvement_suggestions = validation_result.get(
                        "improvement_suggestions",
                        "请重新检查提取内容的准确性和完整性"
                    )
                    logger.info(f"校验未通过，正在重试...")
                    # 恢复原始查询以备下次修改
                    extraction_kwargs["extraction_query"] = extraction_query
                else:
                    # 达到重试上限或不允许重试
                    logger.warning(
                        f"✗ 校验未通过: 得分{validation_result.get('overall_score')}"
                    )
                    break
            
            # 返回最后一次的结果
            best_attempt = max(
                retry_history,
                key=lambda x: x["validation_result"].get("overall_score", 0)
            ) if retry_history else None
            
            return {
                "success": False,
                "extracted_content": extracted_content,
                "is_validated": True,
                "is_valid": False,
                "doc_type": doc_type,
                "validation_result": best_attempt["validation_result"] if best_attempt else None,
                "extraction_result": best_attempt["extraction_result"] if best_attempt else extraction_result,
                "retry_history": retry_history,
                "final_attempt": current_attempt,
                "message": f"{doc_type.upper()}文档提取完成但校验未通过",
                "warning": "建议人工复核提取结果"
            }
            
        except Exception as e:
            logger.error(f"提取服务错误: {e}", exc_info=True)
            _task_log_error("提取服务错误", exc=e, doc_type=doc_type)
            return {
                "success": False,
                "error": str(e),
                "doc_type": doc_type,
                "is_validated": False
            }
    
    def get_strategy(self, doc_type: str) -> Dict[str, Any]:
        """获取文档类型的验证策略"""
        return self.VALIDATION_STRATEGY.get(
            doc_type.lower(),
            self.VALIDATION_STRATEGY["word"]
        )
    
    def update_strategy(self, doc_type: str, strategy: Dict[str, Any]) -> None:
        """更新文档类型的验证策略"""
        self.VALIDATION_STRATEGY[doc_type.lower()] = strategy
        logger.info(f"更新{doc_type}的验证策略: {strategy}")


# 创建全局实例
validated_extraction_service = ValidatedExtractionService()
