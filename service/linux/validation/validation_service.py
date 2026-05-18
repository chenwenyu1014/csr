#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据源校验服务
提供数据源校验和匹配的完整服务封装
"""

from typing import Dict, Any, Optional
import copy
import logging
from pathlib import Path
import json
from datetime import datetime

from service.linux.allocation.data_source_validator import DataSourceValidator, ValidationResult

logger = logging.getLogger(__name__)


class DataSourceValidationService:
    """
    数据源校验服务
    用于全面检查数据源的命名、分类、数量等问题
    """
    
    def __init__(self, model_name: Optional[str] = None):
        """
        初始化校验服务
        
        Args:
            model_name: 指定使用的模型名称（可选）
        """
        self.validator = DataSourceValidator(model_name=model_name)
        logger.info(f"数据源校验服务已初始化，使用模型: {model_name or '默认'}")

    async def validate_pure_async(self, 
                                  spec: Dict[str, Any], 
                                  task_name: Optional[str] = None) -> Dict[str, Any]:
        """
        异步版本的纯LLM校验
        
        使用异步 LLM 调用，不阻塞事件循环，提高并发性能。
        建议在 FastAPI 接口中使用此方法。
        
        Args:
            spec: 数据源校验规范
            task_name: 任务名称（可选）
            
        Returns:
            Dict: 校验结果
        """
        try:
            result: ValidationResult = await self.validator.validate_pure_async(spec, task_name)
            if not result.success:
                prov_path = _save_validation_provenance(
                    stage="validation",
                    model_name=self.validator.llm.model_name,
                    prompt_path=result.prompt_path,
                    output_path=result.model_output_path,
                    task_name=task_name,
                )
                return {
                    "success": False,
                    "error": "校验执行失败",
                    "details": result.model_result,
                    "prompt_path": result.prompt_path,
                    "model_output_path": result.model_output_path,
                    "model_name": self.validator.llm.model_name,
                    "provenance_path": str(prov_path) if prov_path else None,
                }
            prov_path = _save_validation_provenance(
                stage="validation",
                model_name=self.validator.llm.model_name,
                prompt_path=result.prompt_path,
                output_path=result.model_output_path,
                task_name=task_name,
            )
            return {
                "success": True,
                "data": result.model_result,
                "prompt_path": result.prompt_path,
                "model_output_path": result.model_output_path,
                "model_name": self.validator.llm.model_name,
                "provenance_path": str(prov_path) if prov_path else None,
            }
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"[异步] 数据源纯校验执行失败: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    


# 创建全局单例
validation_service = DataSourceValidationService()
# matching_service = DataSourceMatchingService()


__all__ = [
    "DataSourceValidationService",
    # "DataSourceMatchingService",
    "validation_service",
    # "matching_service"
]


def _save_validation_provenance(stage: str,
                                model_name: Optional[str],
                                prompt_path: Optional[str],
                                output_path: Optional[str],
                                task_name: Optional[str]) -> Optional[Path]:
    try:
        from config import get_settings
        settings = get_settings()
        base = Path(settings.output_dir)
        tn = (task_name or "default").strip() or "default"
        safe_tn = (tn.replace("\\", "_")
                     .replace("/", "_")
                     .replace(":", "_")
                     .replace("*", "_")
                     .replace("?", "_")
                     .replace("\"", "_")
                     .replace("<", "_")
                     .replace(">", "_")
                     .replace("|", "_"))
        out_dir = base / "validation" / safe_tn / "provenance"
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        fp = out_dir / f"{stage}_provenance_{ts}.json"
        payload = {
            "stage": stage,
            "model": model_name,
            "prompt_path": prompt_path,
            "output_path": output_path,
            "timestamp": ts,
        }
        fp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return fp
    except Exception:
        return None
