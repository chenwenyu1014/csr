#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
提取结果校验服务
负责校验提取结果的准确性、完整性、格式规范和逻辑一致性
"""

import json
import logging
import os
import re
import uuid
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

from service.models import get_llm_service
from service.prompts.system_prompt_manager import system_prompt_manager
from utils.context_manager import get_current_output_dir

logger = logging.getLogger(__name__)


class ExtractionValidationService:
    """
    提取结果校验服务
    
    功能：
    - 校验提取结果的准确性、完整性、格式规范和逻辑一致性
    - 判断提取结果是否合格
    - 提供改进建议用于重新提取
    
    校验标准：
    - 综合评分 ≥ 80分
    - 所有维度评分 ≥ 70分
    - 无重大错误
    """
    
    def __init__(self, model_name: Optional[str] = None):
        """
        初始化校验服务
        
        Args:
            model_name: 指定使用的模型名称（可选）
                       如果为None，则使用验证任务的默认模型
        """
        # 使用统一的模型管理器获取LLM实例
        self.llm = get_llm_service("validation", model_name)
        logger.info(f"提取校验服务已初始化，使用统一模型管理器")
    
    def _get_prompts_dir(self) -> Path:
        """获取提示词保存目录（使用线程安全的方式）"""
        session_dir = get_current_output_dir(default="output")
        prompts_dir = Path(session_dir) / "prompts" / "validation"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        return prompts_dir
    
    def _save_prompt_and_output(self, prompt: str, output: str, extraction_query: str) -> dict:
        """
        保存校验的提示词和输出
        
        Args:
            prompt: 完整提示词
            output: 模型输出
            extraction_query: 提取需求（用于标识）
            
        Returns:
            保存的文件路径信息
        """
        try:
            prompts_dir = self._get_prompts_dir()
            
            # 生成唯一文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            rand6 = uuid.uuid4().hex[:6]
            
            # 保存提示词
            prompt_file = prompts_dir / f"validation_prompt_{timestamp}_{rand6}.txt"
            prompt_file.write_text(prompt, encoding='utf-8')
            
            # 保存输出
            output_file = prompts_dir / f"validation_output_{timestamp}_{rand6}.txt"
            output_file.write_text(output or "", encoding='utf-8')
            
            # 保存溯源JSON
            provenance_data = {
                "type": "extraction_validation",
                "extraction_query": extraction_query[:200] if extraction_query else "",  # 截断
                "timestamp": timestamp,
                "prompt_length": len(prompt),
                "output_length": len(output) if output else 0,
                "prompt_file": str(prompt_file),
                "output_file": str(output_file)
            }
            provenance_file = prompts_dir / f"validation_provenance_{timestamp}_{rand6}.json"
            provenance_file.write_text(json.dumps(provenance_data, ensure_ascii=False, indent=2), encoding='utf-8')
            
            logger.info(f"✅ 校验提示词已保存: {prompt_file.name}")
            
            return {
                "prompt_file": str(prompt_file),
                "output_file": str(output_file),
                "provenance_file": str(provenance_file)
            }
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.warning(f"保存校验提示词失败: {e}")
            return {}
    
    def validate_extraction(self,
                          extraction_query: str,
                          source_content: str,
                          extracted_content: str) -> Dict[str, Any]:
        """
        校验提取结果
        
        Args:
            extraction_query: 用户的提取需求
            source_content: 原始文档内容
            extracted_content: 提取结果
        
        Returns:
            Dict: 校验结果
            {
                "success": bool,
                "is_valid": bool,  # 是否合格
                "overall_score": int,  # 综合评分
                "validation_details": {...},  # 详细评分
                "failed_reasons": [...],  # 不合格原因
                "improvement_suggestions": str,  # 改进建议
                "raw_output": str  # 模型原始输出
            }
        """
        try:
            logger.info("开始校验提取结果")
            
            # 1. 构建校验提示词
            variables = {
                "extraction_query": extraction_query,
                "source_content": source_content,
                "extracted_content": extracted_content
            }
            prompt = system_prompt_manager.build_prompt("extraction_validation", variables)
            
            # 记录提示词大小，便于调试
            logger.info(f"📝 校验提示词大小: {len(prompt)} 字符")
            
            # 2. 调用模型校验
            model_output = None
            try:
                model_output = self.llm.generate_single(prompt)
            except Exception as llm_error:
                import traceback
                traceback.print_exc()
                logger.error(f"LLM 调用异常: {llm_error}")
                # 保存失败的提示词（用于排查）
                self._save_prompt_and_output(prompt, str(llm_error), extraction_query)
                return {
                    "success": False,
                    "error": f"LLM调用失败: {str(llm_error)}",
                    "is_valid": False,
                    "overall_score": 0
                }
            
            # 3. 保存提示词和输出（用于排查）
            saved_files = self._save_prompt_and_output(prompt, model_output, extraction_query)
            
            # 检查是否是 LLM 错误响应
            if not model_output or model_output.startswith("[生成失败]") or model_output.startswith("[API调用失败]"):
                logger.error(f"LLM 返回错误: {model_output[:200] if model_output else '(空)'}")
                return {
                    "success": False,
                    "error": f"LLM返回错误响应: {model_output[:100] if model_output else '(空)'}",
                    "is_valid": False,
                    "overall_score": 0,
                    "raw_output": model_output,
                    "saved_files": saved_files
                }
            
            # 3. 解析校验结果
            validation_result = self._parse_validation_result(model_output)
            
            if not validation_result:
                logger.error("无法解析校验结果")
                return {
                    "success": False,
                    "error": "无法解析校验结果（模型输出格式不正确）",
                    "is_valid": False,
                    "overall_score": 0,
                    "raw_output": model_output
                }
            
            # 4. 记录校验结果
            is_valid = validation_result.get("is_valid", False)
            overall_score = validation_result.get("overall_score", 0)
            
            if is_valid:
                logger.info(f"✅ 提取结果合格，综合评分: {overall_score}")
            else:
                failed_reasons = validation_result.get("failed_reasons", [])
                logger.warning(f"❌ 提取结果不合格，综合评分: {overall_score}")
                logger.warning(f"不合格原因: {failed_reasons}")
            
            return {
                "success": True,
                "is_valid": is_valid,
                "overall_score": overall_score,
                "validation_details": validation_result.get("validation_details", {}),
                "failed_reasons": validation_result.get("failed_reasons", []),
                "improvement_suggestions": validation_result.get("improvement_suggestions", ""),
                "raw_output": model_output
            }
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"校验提取结果失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _parse_validation_result(self, model_output: str) -> Optional[Dict[str, Any]]:
        """
        解析模型输出的校验结果
        
        Args:
            model_output: 模型输出
        
        Returns:
            解析后的校验结果，如果解析失败返回None
        """
        try:
            # 首先检查是否是 LLM 服务返回的错误信息
            if not model_output:
                logger.error("模型输出为空")
                return None
            
            # 检测常见的 LLM 错误标记
            error_markers = ["[生成失败]", "[API调用失败]", "[LLM未配置", "400 Client Error", "500 Server Error"]
            for marker in error_markers:
                if marker in model_output:
                    logger.error(f"检测到 LLM 服务错误: {model_output[:200]}")
                    return None
            
            # 方法1：直接解析JSON
            cleaned_output = model_output.strip()
            
            # 移除可能的markdown代码块标记
            if cleaned_output.startswith("```json"):
                cleaned_output = cleaned_output[7:]
            elif cleaned_output.startswith("```"):
                cleaned_output = cleaned_output[3:]
            
            if cleaned_output.endswith("```"):
                cleaned_output = cleaned_output[:-3]
            
            cleaned_output = cleaned_output.strip()
            
            # 尝试解析JSON
            result = json.loads(cleaned_output)
            
            # 验证必需字段
            required_fields = ["is_valid", "overall_score", "validation_details", 
                             "failed_reasons", "improvement_suggestions"]
            
            if all(field in result for field in required_fields):
                return result
            else:
                logger.warning("校验结果缺少必需字段")
                return None
            
        except json.JSONDecodeError as e:
            import traceback
            traceback.print_exc()
            logger.warning(f"JSON解析失败: {e}")
            
            # 方法2：正则提取JSON
            try:
                json_match = re.search(r'\{[\s\S]*\}', model_output)
                if json_match:
                    result = json.loads(json_match.group())
                    return result
            except Exception as e2:
                import traceback
                traceback.print_exc()
                logger.error(f"正则提取JSON失败: {e2}")
            
            return None
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"解析校验结果失败: {e}")
            return None


# 创建全局服务实例
extraction_validation_service = ExtractionValidationService()
