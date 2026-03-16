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
    
    # def validate(self,
    #              spec: Dict[str, Any],
    #              task_name: Optional[str] = None) -> Dict[str, Any]:
    #     """统一走纯LLM路径并返回 canonical categories（三字段）结果，不再合并原始结构。"""
    #     try:
    #         result: ValidationResult = self.validator.validate_pure(spec, task_name)
    #         if not result.success:
    #             prov_path = _save_validation_provenance(
    #                 stage="validation",
    #                 model_name=self.validator.llm.model_name,
    #                 prompt_path=result.prompt_path,
    #                 output_path=result.model_output_path,
    #                 task_name=task_name,
    #             )
    #             return {
    #                 "success": False,
    #                 "error": "校验执行失败",
    #                 "details": result.model_result,
    #                 "prompt_path": result.prompt_path,
    #                 "model_output_path": result.model_output_path,
    #                 "model_name": self.validator.llm.model_name,
    #                 "provenance_path": str(prov_path) if prov_path else None,
    #             }
    #         prov_path = _save_validation_provenance(
    #             stage="validation",
    #             model_name=self.validator.llm.model_name,
    #             prompt_path=result.prompt_path,
    #             output_path=result.model_output_path,
    #             task_name=task_name,
    #         )
    #         return {
    #             "success": True,
    #             "data": result.model_result,
    #             "prompt_path": result.prompt_path,
    #             "model_output_path": result.model_output_path,
    #             "model_name": self.validator.llm.model_name,
    #             "provenance_path": str(prov_path) if prov_path else None,
    #         }
    #     except Exception as e:
    #         logger.error(f"数据源校验服务执行失败: {e}", exc_info=True)
    #         return {
    #             "success": False,
    #             "error": str(e)
    #         }

    # def validate_pure(self,
    #                   spec: Dict[str, Any],
    #                   task_name: Optional[str] = None) -> Dict[str, Any]:
    #     try:
    #         result: ValidationResult = self.validator.validate_pure(spec, task_name)
    #         if not result.success:
    #             prov_path = _save_validation_provenance(
    #                 stage="validation",
    #                 model_name=self.validator.llm.model_name,
    #                 prompt_path=result.prompt_path,
    #                 output_path=result.model_output_path,
    #                 task_name=task_name,
    #             )
    #             return {
    #                 "success": False,
    #                 "error": "校验执行失败",
    #                 "details": result.model_result,
    #                 "prompt_path": result.prompt_path,
    #                 "model_output_path": result.model_output_path,
    #                 "model_name": self.validator.llm.model_name,
    #                 "provenance_path": str(prov_path) if prov_path else None,
    #             }
    #         prov_path = _save_validation_provenance(
    #             stage="validation",
    #             model_name=self.validator.llm.model_name,
    #             prompt_path=result.prompt_path,
    #             output_path=result.model_output_path,
    #             task_name=task_name,
    #         )
    #         return {
    #             "success": True,
    #             "data": result.model_result,
    #             "prompt_path": result.prompt_path,
    #             "model_output_path": result.model_output_path,
    #             "model_name": self.validator.llm.model_name,
    #             "provenance_path": str(prov_path) if prov_path else None,
    #         }
    #     except Exception as e:
    #         logger.error(f"数据源纯校验执行失败: {e}", exc_info=True)
    #         return {
    #             "success": False,
    #             "error": str(e)
    #         }

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
    
    # def _generate_summary(self, validated_data: Dict[str, Any]) -> Dict[str, Any]:
    #     """
    #     生成校验摘要统计
    #
    #     Args:
    #         validated_data: 校验后的数据
    #
    #     Returns:
    #         Dict: 统计摘要
    #     """
    #     # 获取分类字典
    #     if "name" in validated_data:
    #         categories_dict = validated_data.get("name", {})
    #     else:
    #         categories_dict = validated_data
    #
    #     total_categories = 0
    #     passed_categories = 0
    #     total_files = 0
    #     compliant_files = 0
    #     check_files = 0
    #
    #     for cat_name, cat_data in categories_dict.items():
    #         if not isinstance(cat_data, dict):
    #             continue
    #
    #         total_categories += 1
    #
    #         files = cat_data.get("file", [])
    #         total_files += len(files)
    #
    #         compliant = cat_data.get("Compliant", [])
    #         compliant_files += len(compliant)
    #
    #         check = cat_data.get("Check", [])
    #         check_files += len(check)
    #
    #         # 判断该分类是否通过（没有需要检查的文件）
    #         if len(check) == 0:
    #             passed_categories += 1
    #
    #     return {
    #         "total_categories": total_categories,
    #         "passed_categories": passed_categories,
    #         "failed_categories": total_categories - passed_categories,
    #         "total_files": total_files,
    #         "compliant_files": compliant_files,
    #         "check_files": check_files,
    #         "pass_rate": f"{(compliant_files / total_files * 100):.2f}%" if total_files > 0 else "0%"
    #     }
    #
    # def _merge_pure(self, original: Dict[str, Any], validated: Dict[str, Any]) -> Dict[str, Any]:
    #     merged = copy.deepcopy(original)
    #
    #     def _files(cat: Dict[str, Any]):
    #         fs = cat.get("file") or cat.get("files") or []
    #         if isinstance(fs, list):
    #             return [str(x) for x in fs]
    #         if isinstance(fs, str):
    #             return [fs]
    #         return []
    #
    #     def _merge_cat(orig_cat: Dict[str, Any], val_cat: Dict[str, Any]):
    #         base = _files(orig_cat)
    #         comp = val_cat.get("Compliant") or []
    #         chk = val_cat.get("Check") or []
    #         if not isinstance(comp, list):
    #             comp = [comp] if comp else []
    #         if not isinstance(chk, list):
    #             chk = [chk] if chk else []
    #         comp = [x for x in comp if x in base]
    #         chk = [x for x in chk if x in base and x not in comp]
    #         reasons = val_cat.get("CheckReasons") or {}
    #         if not isinstance(reasons, dict):
    #             reasons = {}
    #         reasons = {str(k): ("" if v is None else str(v)) for k, v in reasons.items() if k in chk}
    #         orig_cat["Compliant"] = comp
    #         orig_cat["Check"] = chk
    #         orig_cat["CheckReasons"] = reasons
    #
    #     # 扁平结构支持：顶层只有 files/file 时，允许模型仅输出三字段对象或 categories[0]
    #     try:
    #         top_files = merged.get("file") or merged.get("files")
    #         if isinstance(top_files, (list, str)):
    #             val_cat = None
    #             if isinstance(validated, dict):
    #                 cats = validated.get("categories")
    #                 if isinstance(cats, list) and cats and isinstance(cats[0], dict):
    #                     val_cat = cats[0]
    #                 else:
    #                     val_cat = validated
    #             if isinstance(val_cat, dict):
    #                 _merge_cat(merged, val_cat)
    #             else:
    #                 merged.setdefault("Compliant", [])
    #                 merged.setdefault("Check", [])
    #                 merged.setdefault("CheckReasons", {})
    #             return merged
    #     except Exception:
    #         pass
    #
    #     if isinstance(merged.get("name"), dict):
    #         orig_cats = merged.get("name", {})
    #         val_cats = validated.get("name", validated if isinstance(validated, dict) else {})
    #         if not isinstance(val_cats, dict):
    #             val_cats = {}
    #         vlist = validated.get("categories", []) if isinstance(validated, dict) else []
    #         for idx, (k, orig_cat) in enumerate(list(orig_cats.items())):
    #             if isinstance(orig_cat, dict):
    #                 v = val_cats.get(k) if isinstance(val_cats, dict) else None
    #                 if not isinstance(v, dict) and isinstance(vlist, list) and idx < len(vlist) and isinstance(vlist[idx], dict):
    #                     v = vlist[idx]
    #                 if isinstance(v, dict):
    #                     _merge_cat(orig_cat, v)
    #                 else:
    #                     orig_cat.setdefault("Compliant", [])
    #                     orig_cat.setdefault("Check", [])
    #                     orig_cat.setdefault("CheckReasons", {})
    #         return merged
    #
    #     if isinstance(merged.get("categories"), list):
    #         olist = merged.get("categories", [])
    #         vlist = validated.get("categories", [])
    #         if not isinstance(vlist, list):
    #             vlist = []
    #         for i, oc in enumerate(olist):
    #             if isinstance(oc, dict):
    #                 vc = vlist[i] if i < len(vlist) and isinstance(vlist[i], dict) else None
    #                 if isinstance(vc, dict):
    #                     _merge_cat(oc, vc)
    #                 else:
    #                     oc.setdefault("Compliant", [])
    #                     oc.setdefault("Check", [])
    #                     oc.setdefault("CheckReasons", {})
    #         return merged
    #
    #     return merged
    #
    #     if isinstance(merged.get("categories"), list):
    #         olist = merged.get("categories", [])
    #         vlist = validated.get("categories", [])
    #         if not isinstance(vlist, list):
    #             vlist = []
    #         for i, oc in enumerate(olist):
    #             if isinstance(oc, dict):
    #                 vc = vlist[i] if i < len(vlist) and isinstance(vlist[i], dict) else None
    #                 if isinstance(vc, dict):
    #                     _merge_cat(oc, vc)
    #                 else:
    #                     oc.setdefault("Compliant", [])
    #                     oc.setdefault("Check", [])
    #                     oc.setdefault("CheckReasons", {})
    #         return merged
#
# class DataSourceMatchingService:
#     """
#     数据源匹配服务
#     用于快速筛选出符合要求的文件
#     """
#
#     def __init__(self, model_name: Optional[str] = None):
#         """
#         初始化匹配服务
#
#         Args:
#             model_name: 指定使用的模型名称（可选）
#         """
#         self.validator = DataSourceValidator(model_name=model_name)
#         logger.info(f"数据源匹配服务已初始化，使用模型: {model_name or '默认'}")
#
#     def match(self,
#               spec: Dict[str, Any],
#               task_name: Optional[str] = None) -> Dict[str, Any]:
#         """
#         执行数据源匹配
#
#         Args:
#             spec: 数据源配置JSON
#             task_name: 任务名称（可选）
#
#         Returns:
#             Dict: 匹配结果
#             {
#                 "success": bool,
#                 "data": {
#                     "name": {
#                         "分类名": {
#                             ...原始字段,
#                             "Compliant": []  # 只有这个新字段
#                         }
#                     }
#                 },
#                 "prompt_path": str,
#                 "summary": {
#                     "total_categories": int,
#                     "total_files": int,
#                     "matched_files": int,
#                     "match_rate": str
#                 }
#             }
#         """
#         try:
#             # 执行匹配
#             result = self.validator.match(spec, task_name)
#
#             if not result.get("success"):
#                 return {
#                     "success": False,
#                     "error": result.get("error", "匹配执行失败"),
#                     "model_name": self.validator.llm.model_name
#                 }
#
#             # 解析结果
#             matched_data = result.get("result")
#
#             # 统计摘要
#             summary = self._generate_summary(matched_data)
#             prov_path = _save_validation_provenance(
#                 stage="matching",
#                 model_name=self.validator.llm.model_name,
#                 prompt_path=result.get("prompt_path"),
#                 output_path=result.get("raw_output_path"),
#                 task_name=task_name,
#             )
#
#             return {
#                 "success": True,
#                 "data": matched_data,
#                 "prompt_path": result.get("prompt_path"),
#                 "raw_output_path": result.get("raw_output_path"),
#                 "summary": summary,
#                 "model_name": self.validator.llm.model_name,
#                 "provenance_path": str(prov_path) if prov_path else None,
#             }
#
#         except Exception as e:
#             logger.error(f"数据源匹配服务执行失败: {e}", exc_info=True)
#             return {
#                 "success": False,
#                 "error": str(e)
#             }
#
#     def _generate_summary(self, matched_data: Dict[str, Any]) -> Dict[str, Any]:
#         """
#         生成匹配摘要统计
#
#         Args:
#             matched_data: 匹配后的数据
#
#         Returns:
#             Dict: 统计摘要
#         """
#         # 扁平结构（顶层包含 Compliant/Check）
#         if isinstance(matched_data, dict) and ("Compliant" in matched_data or "Check" in matched_data):
#             comp = matched_data.get("Compliant") or []
#             chk = matched_data.get("Check") or []
#             if not isinstance(comp, list):
#                 comp = [comp] if comp else []
#             if not isinstance(chk, list):
#                 chk = [chk] if chk else []
#             # 取顶层 files 作为总数；若不存在，则用 comp+chk 估算
#             files = matched_data.get("files") or matched_data.get("file") or []
#             if isinstance(files, list):
#                 total_files = len(files)
#             elif isinstance(files, str):
#                 total_files = 1
#             else:
#                 total_files = len(comp) + len(chk)
#             matched_files = len(comp)
#             return {
#                 "total_categories": 1,
#                 "total_files": total_files,
#                 "matched_files": matched_files,
#                 "unmatched_files": max(total_files - matched_files, 0),
#                 "match_rate": f"{(matched_files / total_files * 100):.2f}%" if total_files > 0 else "0%",
#                 "category_stats": [{
#                     "name": "flat",
#                     "total": total_files,
#                     "matched": matched_files,
#                     "match_rate": f"{(matched_files / total_files * 100):.2f}%" if total_files > 0 else "0%"
#                 }]
#             }
#
#         # 分类结构：name 字典或直接以分类名为键
#         categories_dict = matched_data.get("name", matched_data if isinstance(matched_data, dict) else {})
#         total_categories = 0
#         total_files = 0
#         matched_files = 0
#         category_stats = []
#         for cat_name, cat_data in categories_dict.items():
#             if not isinstance(cat_data, dict):
#                 continue
#             total_categories += 1
#             files = cat_data.get("file") or cat_data.get("files") or []
#             cat_total = len(files) if isinstance(files, list) else (1 if isinstance(files, str) else 0)
#             total_files += cat_total
#             compliant = cat_data.get("Compliant", [])
#             cat_matched = len(compliant) if isinstance(compliant, list) else (1 if compliant else 0)
#             matched_files += cat_matched
#             category_stats.append({
#                 "name": cat_name,
#                 "total": cat_total,
#                 "matched": cat_matched,
#                 "match_rate": f"{(cat_matched / cat_total * 100):.2f}%" if cat_total > 0 else "0%"
#             })
#         return {
#             "total_categories": total_categories,
#             "total_files": total_files,
#             "matched_files": matched_files,
#             "unmatched_files": max(total_files - matched_files, 0),
#             "match_rate": f"{(matched_files / total_files * 100):.2f}%" if total_files > 0 else "0%",
#             "category_stats": category_stats
#         }


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
