#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据源校验模块

根据用户提供的JSON（包含各分类的Rules与文件名称），构建提示词交给模型进行校验，
并返回结构化校验报告。提示词渲染遵循系统提示词管理模块（prompts/system/system_prompts.json）。
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from service.prompts.system_prompt_manager import system_prompt_manager
from service.models import get_llm_service

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    success: bool
    prompt_path: Optional[str]
    model_output_raw: Optional[str]
    model_output_path: Optional[str]
    model_result: Optional[Dict[str, Any]]
    pre_checks: Dict[str, Any]


class DataSourceValidator:
    """数据源校验入口"""

    def __init__(self, model_name: Optional[str] = None):
        """
        初始化数据源验证器
        
        Args:
            model_name: 指定使用的模型名称（可选）
                       如果为None，则使用验证任务的默认模型
        """
        # 使用统一的模型管理器获取LLM实例
        self.llm = get_llm_service("validation", model_name)

    # def validate(self, spec: Dict[str, Any], task_name: Optional[str] = None) -> ValidationResult:
    #     """
    #     执行数据源校验。
    #
    #     Args:
    #         spec: 用户JSON配置（包含各分类的Rules与文件名列表）
    #         task_name: 任务名称（用于提示词标题）
    #
    #     Returns:
    #         ValidationResult: 校验结果
    #     """
    #     # 1) 构建提示词（仅 rules 与 files）
    #     prompt_text = self.build_prompt_from_spec_raw(spec, task_name=task_name, prompt_id="data_source_validation")
    #
    #     # 2) 保存提示词到 output/prompts 以便调试复盘
    #     prompt_path = self._save_prompt(prompt_text, prefix="data_source_validation", task_name=task_name)
    #
    #     # 3) 模型校验
    #     model_output_raw = None
    #     model_output_path: Optional[Path] = None
    #     parsed: Optional[Dict[str, Any]] = None
    #     try:
    #         model_output_raw = self.llm.generate_single(prompt_text)
    #         # 保存模型原始输出，便于排查
    #         model_output_path = self._save_model_output(model_output_raw, prefix="data_source_validation_output", task_name=task_name)
    #         parsed = self._parse_json_response(model_output_raw)
    #         return ValidationResult(
    #             success=(parsed is not None),
    #             prompt_path=str(prompt_path) if prompt_path else None,
    #             model_output_raw=model_output_raw,
    #             model_output_path=str(model_output_path) if model_output_path else None,
    #             model_result=parsed,
    #             pre_checks={"skipped": True},
    #         )
    #     except Exception as e:
    #         # 失败兜底
    #         fallback = {
    #             "overall_pass": False,
    #             "summary": f"模型校验失败: {e}",
    #             "categories": [],
    #             "files": [],
    #             "suggestions": ["请检查API配置或稍后重试"]
    #         }
    #         return ValidationResult(
    #             success=False,
    #             prompt_path=str(prompt_path) if prompt_path else None,
    #             model_output_raw=model_output_raw,
    #             model_output_path=str(model_output_path) if model_output_path else None,
    #             model_result=fallback,
    #             pre_checks=pre_checks,
    #         )

    def match(self, spec: Dict[str, Any], task_name: Optional[str] = None) -> Dict[str, Any]:
        """
        执行数据源匹配（只返回符合要求的文件）。
        
        Args:
            spec: 用户JSON配置（包含各分类的Rules与文件名列表）
            task_name: 任务名称（用于提示词标题）
        
        Returns:
            Dict: 在原始JSON基础上添加Compliant字段的结果
        """
        # 1) 构建提示词（扁平结构仅传 {rules, files}，否则传原始结构）
        try:
            is_name_dict = ("name" in spec) and isinstance(spec.get("name"), dict)
            has_categories = isinstance(spec.get("categories"), list)
            top_files = spec.get("files") or spec.get("file")
            if (not has_categories) and (not is_name_dict) and isinstance(top_files, (list, str)):
                files = list(top_files) if isinstance(top_files, list) else ([top_files] if isinstance(top_files, str) else [])
                payload = {
                    "rules": spec.get("rules") or spec.get("Rules") or "",
                    "files": files,
                }
            else:
                payload = spec
        except Exception:
            payload = spec
        category_list_text = json.dumps(payload, ensure_ascii=False, indent=2)
        variables = {
            "category_list": category_list_text,
            "project_desc": os.getenv("CURRENT_PROJECT_DESC", ""),
        }
        prompt_text = system_prompt_manager.build_prompt("data_source_matching", variables)
        
        # 2) 保存提示词到 output/prompts 以便调试复盘
        prompt_path = self._save_prompt(prompt_text, prefix="data_source_matching", task_name=task_name)
        
        # 3) 模型匹配
        try:
            model_output_raw = self.llm.generate_single(prompt_text)
            model_output_path = self._save_model_output(model_output_raw, prefix="data_source_matching_output", task_name=task_name)
            parsed = self._parse_json_response(model_output_raw)
            
            if parsed:
                return {
                    "success": True,
                    "result": parsed,
                    "prompt_path": str(prompt_path) if prompt_path else None,
                    "raw_output": model_output_raw,
                    "raw_output_path": str(model_output_path) if model_output_path else None
                }
            else:
                return {
                    "success": False,
                    "error": "无法解析模型输出",
                    "raw_output": model_output_raw,
                    "raw_output_path": str(model_output_path) if model_output_path else None
                }
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": str(e)
            }

    # # -------------------- 内部方法 --------------------
    #
    # def _parse_categories(self, spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    #     """已弃用：返回空列表。"""
    #     return []
    #
    # def _run_pre_checks(self, categories: List[Dict[str, Any]]) -> Dict[str, Any]:
    #     """已弃用：返回空检查结果。"""
    #     return {"categories": [], "global_issues": []}
    #
    # def _build_category_list_text(self, categories: List[Dict[str, Any]]) -> str:
    #     """已弃用：返回空字符串。"""
    #     return ""
    #
    # def _canonicalize_to_categories(self, spec: Dict[str, Any]) -> Dict[str, Any]:
    #     """已弃用：基于当前规则/文件返回单分类包装。"""
    #     rules, files_list = self._extract_rules_files(spec)
    #     return {"categories": [{"rules": rules, "files": files_list}]}
    #
    ## ============== 极简输出：仅返回模型文本 ==============
    # def build_prompt_from_spec(self, spec: Dict[str, Any], task_name: Optional[str] = None,
    #                            prompt_id: str = "data_source_validation") -> str:
    #     rules, files_list = self._extract_rules_files(spec)
    #     variables = {
    #         "rules": rules,
    #         "files": json.dumps(files_list, ensure_ascii=False, indent=2),
    #     }
    #     return system_prompt_manager.build_prompt(prompt_id, variables)
    #
    # def validate_to_text(self, spec: Dict[str, Any], task_name: Optional[str] = None,
    #                      prompt_id: str = "data_source_validation") -> str:
    #     """执行校验并仅返回模型输出文本，不进行本地解析或预检查。"""
    #     prompt = self.build_prompt_from_spec(spec, task_name=task_name, prompt_id=prompt_id)
    #     return self.llm.generate_single(prompt)

    def build_prompt_from_spec_raw(self, spec: Dict[str, Any], task_name: Optional[str] = None,
                                   prompt_id: str = "data_source_validation") -> str:
        # 从规范JSON中提取rules和files，渲染提示词模板
        rules, files_list = self._extract_rules_files(spec)
        variables = {
            "rules": rules,
            "files": json.dumps(files_list, ensure_ascii=False, indent=2),
        }
        return system_prompt_manager.build_prompt(prompt_id, variables)

    def _extract_rules_files(self, spec: Dict[str, Any]) -> tuple[str, List[str]]:
        """从任意输入结构中提取 rules 与 files（数组）。"""
        try:
            rules = spec.get("rules") or spec.get("Rules") #or ""
            files = spec.get("files") or spec.get("file")# or []
            if isinstance(files, str):
                files_list = [files]
            elif isinstance(files, list):
                files_list = [str(x) for x in files]
            else:
                # 如果传入了旧的 categories/name 结构，取第一类的 files
                files_list = []
                if isinstance(spec.get("categories"), list) and spec.get("categories"):
                    c0 = spec.get("categories")[0]
                    if isinstance(c0, dict):
                        f0 = c0.get("files") or c0.get("file") or []
                        if isinstance(f0, str):
                            files_list = [f0]
                        elif isinstance(f0, list):
                            files_list = [str(x) for x in f0]
                        rules = c0.get("rules") or c0.get("Rules") or rules
                elif isinstance(spec.get("name"), dict):
                    d = spec.get("name")
                    if isinstance(d, dict):
                        for _, cfg in d.items():
                            if isinstance(cfg, dict):
                                f = cfg.get("files") or cfg.get("file") or []
                                if isinstance(f, str):
                                    files_list = [f]
                                elif isinstance(f, list):
                                    files_list = [str(x) for x in f]
                                rules = cfg.get("rules") or cfg.get("Rules") or rules
                                break
        except Exception:
            rules = ""
            files_list = []
        return str(rules), files_list

    # def validate_pure(self, spec: Dict[str, Any], task_name: Optional[str] = None,
    #                   prompt_id: str = "data_source_validation") -> ValidationResult:
    #     logger.info(f"开始纯LLM校验 - task_name={task_name}, prompt_id={prompt_id}")
    #
    #     prompt_text = self.build_prompt_from_spec_raw(spec, task_name=task_name, prompt_id=prompt_id)
    #     logger.info(f"提示词构建完成，长度={len(prompt_text)}")
    #
    #     prompt_path = self._save_prompt(prompt_text, prefix="data_source_validation", task_name=task_name)
    #     logger.info(f"提示词已保存: {prompt_path}")
    #
    #     model_output_raw = None
    #     model_output_path: Optional[Path] = None
    #     parsed: Optional[Dict[str, Any]] = None
    #     try:
    #         logger.info("调用模型生成...")
    #         model_output_raw = self.llm.generate_single(prompt_text)
    #         logger.info(f"模型返回完成，输出长度={len(model_output_raw) if model_output_raw else 0}")
    #         # 保存模型原始输出
    #         model_output_path = self._save_model_output(model_output_raw, prefix="data_source_validation_output", task_name=task_name)
    #         if model_output_path:
    #             logger.info(f"模型原始输出已保存: {model_output_path}")
    #
    #         parsed = self._parse_json_response(model_output_raw)
    #
    #         # 如果解析失败，记录详细错误信息
    #         if parsed is None:
    #             logger.error(
    #                 f"❌ JSON解析失败 - 模型输出前500字符: {model_output_raw[:500] if model_output_raw else 'None'}"
    #             )
    #             logger.error(f"提示词路径: {prompt_path}")
    #             if model_output_path:
    #                 logger.error(f"模型输出路径: {model_output_path}")
    #         else:
    #             logger.info(f"✓ JSON解析成功，包含 {len(parsed)} 个顶层键")
    #
    #         return ValidationResult(
    #             success=(parsed is not None),
    #             prompt_path=str(prompt_path) if prompt_path else None,
    #             model_output_raw=model_output_raw,
    #             model_output_path=str(model_output_path) if model_output_path else None,
    #             model_result=parsed,
    #             pre_checks={"skipped": True},
    #         )
    #     except Exception as e:
    #         logger.error(f"❌ 模型调用异常: {type(e).__name__}: {e}", exc_info=True)
    #         logger.error(f"模型输出（如有）: {model_output_raw[:500] if model_output_raw else 'None'}")
    #         fallback = {
    #             "overall_pass": False,
    #             "summary": f"模型校验失败: {e}",
    #             "categories": [],
    #             "files": [],
    #             "suggestions": ["请检查API配置或稍后重试"]
    #         }
    #         return ValidationResult(
    #             success=False,
    #             prompt_path=str(prompt_path) if prompt_path else None,
    #             model_output_raw=model_output_raw,
    #             model_output_path=str(model_output_path) if model_output_path else None,
    #             model_result=fallback,
    #             pre_checks={"skipped": True},
    #         )

    async def validate_pure_async(self, spec: Dict[str, Any], task_name: Optional[str] = None,
                                  prompt_id: str = "data_source_validation") -> ValidationResult:
        """
        异步版本的纯LLM校验
        
        使用 LLM 的异步接口，不阻塞事件循环，提高并发性能。
        
        Args:
            spec: 用户JSON配置
            task_name: 任务名称
            prompt_id: 提示词模板ID
            
        Returns:
            ValidationResult: 校验结果
        """
        logger.info(f"[异步] 开始纯LLM校验 - task_name={task_name}, prompt_id={prompt_id}")
        
        prompt_text = self.build_prompt_from_spec_raw(spec, task_name=task_name, prompt_id=prompt_id)
        logger.info(f"[异步] 提示词构建完成，长度={len(prompt_text)}")
        
        prompt_path = self._save_prompt(prompt_text, prefix="data_source_validation", task_name=task_name)
        logger.info(f"[异步] 提示词已保存: {prompt_path}")
        
        model_output_raw = None
        model_output_path: Optional[Path] = None
        parsed: Optional[Dict[str, Any]] = None
        try:
            logger.info("[异步] 调用模型生成...")
            # 使用异步版本的 LLM 调用
            model_output_raw = await self.llm.generate_single_async(prompt_text)
            logger.info(f"[异步] 模型返回完成，输出长度={len(model_output_raw) if model_output_raw else 0}")
            
            # 保存模型原始输出
            model_output_path = self._save_model_output(model_output_raw, prefix="data_source_validation_output", task_name=task_name)
            if model_output_path:
                logger.info(f"[异步] 模型原始输出已保存: {model_output_path}")
            
            parsed = self._parse_json_response(model_output_raw)
            
            # 如果解析失败，记录详细错误信息
            if parsed is None:
                logger.error(
                    f"[异步] ❌ JSON解析失败 - 模型输出前500字符: {model_output_raw[:500] if model_output_raw else 'None'}"
                )
                logger.error(f"[异步] 提示词路径: {prompt_path}")
                if model_output_path:
                    logger.error(f"[异步] 模型输出路径: {model_output_path}")
            else:
                logger.info(f"[异步] ✓ JSON解析成功，包含 {len(parsed)} 个顶层键")
            
            return ValidationResult(
                success=(parsed is not None),
                prompt_path=str(prompt_path) if prompt_path else None,
                model_output_raw=model_output_raw,
                model_output_path=str(model_output_path) if model_output_path else None,
                model_result=parsed,
                pre_checks={"skipped": True},
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"[异步] ❌ 模型调用异常: {type(e).__name__}: {e}", exc_info=True)
            logger.error(f"[异步] 模型输出（如有）: {model_output_raw[:500] if model_output_raw else 'None'}")
            fallback = {
                "overall_pass": False,
                "summary": f"模型校验失败: {e}",
                "categories": [],
                "files": [],
                "suggestions": ["请检查API配置或稍后重试"]
            }
            return ValidationResult(
                success=False,
                prompt_path=str(prompt_path) if prompt_path else None,
                model_output_raw=model_output_raw,
                model_output_path=str(model_output_path) if model_output_path else None,
                model_result=fallback,
                pre_checks={"skipped": True},
            )

    def _save_prompt(self, prompt_text: str, prefix: str, task_name: Optional[str] = None) -> Optional[Path]:
        """将渲染后的提示词落盘保存到AAA共享文件夹。"""
        try:
            from config import get_settings
            config = get_settings()
            output_dir = Path(config.output_dir)
            tn = (task_name or "default").strip() or "default"
            safe_tn = (
                tn.replace("\\", "_")
                  .replace("/", "_")
                  .replace(":", "_")
                  .replace("*", "_")
                  .replace("?", "_")
                  .replace("\"", "_")
                  .replace("<", "_")
                  .replace(">", "_")
                  .replace("|", "_")
            )
            prompts_dir = output_dir / "validation" / safe_tn / "prompts"
            prompts_dir.mkdir(parents=True, exist_ok=True)
            from datetime import datetime
            ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            import uuid as _uuid
            rand6 = _uuid.uuid4().hex[:6]
            fp = prompts_dir / f"{prefix}_{ts}_{rand6}.md"
            fp.write_text(prompt_text, encoding="utf-8")
            logger.info(f"提示词已保存到共享文件夹: {fp}")
            return fp
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.warning(f"提示词保存失败: {e}")
            return None

    def _save_model_output(self, content: Optional[str], prefix: str, task_name: Optional[str] = None) -> Optional[Path]:
        """将模型原始输出原样保存到AAA共享文件夹。"""
        if content is None:
            return None
        try:
            from config import get_settings
            config = get_settings()
            output_dir = Path(config.output_dir)
            tn = (task_name or "default").strip() or "default"
            safe_tn = (
                tn.replace("\\", "_")
                  .replace("/", "_")
                  .replace(":", "_")
                  .replace("*", "_")
                  .replace("?", "_")
                  .replace("\"", "_")
                  .replace("<", "_")
                  .replace(">", "_")
                  .replace("|", "_")
            )
            out_dir = output_dir / "validation" / safe_tn / "outputs"
            out_dir.mkdir(parents=True, exist_ok=True)
            from datetime import datetime
            ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            import uuid as _uuid
            rand6 = _uuid.uuid4().hex[:6]
            fp = out_dir / f"{prefix}_{ts}_{rand6}.txt"
            fp.write_text(content, encoding="utf-8")
            return fp
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.warning(f"模型原始输出保存失败: {e}")
            return None

    def _parse_json_response(self, text: Optional[str]) -> Optional[Dict[str, Any]]:
        """尽力从模型输出中解析严格JSON，提供尽可能多的容错处理。

        策略：
        1) 去除 Markdown 代码块包裹、BOM、智能引号
        2) 移除注释 // ... 和 /* ... */
        3) 直接 json.loads 尝试
        4) 括号配对提取顶层 JSON（忽略字符串内部的括号）
        5) 去除尾随逗号再尝试
        6) 移除非法控制字符再尝试
        7) 最后兜底：将 true/false/null 转换为 Python 等价并用 ast.literal_eval 尝试
        """
        if not text:
            return None

        import re as _re
        import ast as _ast

        def _strip_code_fences(s: str) -> str:
            s = s.lstrip('\ufeff').strip()
            s = _re.sub(r'^```(?:json)?\s*\n?', '', s)
            s = _re.sub(r'\n?```\s*$', '', s)
            return s.strip()

        def _strip_comments(s: str) -> str:
            # 移除 // 行注释 与 /* */ 块注释
            s = _re.sub(r'(^|\s)//.*?$', r'\1', s, flags=_re.MULTILINE)
            s = _re.sub(r'/\*[\s\S]*?\*/', '', s)
            return s

        def _normalize_quotes(s: str) -> str:
            # 智能引号替换为普通双引号
            return s.replace('“', '"').replace('”', '"').replace('‘', "'").replace('’', "'")

        def _remove_trailing_commas(s: str) -> str:
            # 移除对象或数组末尾的尾随逗号: {...,} 或 [...,]
            return _re.sub(r',\s*([}\]])', r'\1', s)

        def _remove_illegal_ctrl(s: str) -> str:
            # 移除除 \t\r\n 外的控制字符
            return _re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', s)

        def _extract_top_level_json(s: str) -> Optional[str]:
            # 提取从第一个 { 或 [ 开始的顶层 JSON 片段，忽略字符串中的括号
            start_obj = s.find('{')
            start_arr = s.find('[')
            if start_obj == -1 and start_arr == -1:
                return None
            if start_obj == -1 or (start_arr != -1 and start_arr < start_obj):
                start = start_arr
                open_ch, close_ch = '[', ']'
            else:
                start = start_obj
                open_ch, close_ch = '{', '}'
            depth = 0
            in_str = False
            esc = False
            for i in range(start, len(s)):
                ch = s[i]
                if in_str:
                    if esc:
                        esc = False
                    elif ch == '\\':
                        esc = True
                    elif ch == '"':
                        in_str = False
                else:
                    if ch == '"':
                        in_str = True
                    elif ch == open_ch:
                        depth += 1
                    elif ch == close_ch:
                        depth -= 1
                        if depth == 0:
                            return s[start:i+1]
            return None

        cleaned = _remove_illegal_ctrl(_normalize_quotes(_strip_comments(_strip_code_fences(text))))

        # 1) 直接解析
        try:
            return json.loads(cleaned)
        except Exception:
            pass

        # 2) 提取顶层 JSON 片段再解析
        candidate = _extract_top_level_json(cleaned)
        if candidate:
            try:
                return json.loads(candidate)
            except Exception:
                # 2.1 移除尾随逗号再试
                try:
                    fixed = _remove_trailing_commas(candidate)
                    return json.loads(fixed)
                except Exception:
                    # 2.2 移除非法控制字符再试
                    try:
                        fixed2 = _remove_illegal_ctrl(fixed)
                        return json.loads(fixed2)
                    except Exception:
                        pass

        # 3) 进一步在全文上移除尾随逗号并重试
        try:
            fixed_all = _remove_trailing_commas(cleaned)
            return json.loads(fixed_all)
        except Exception:
            pass

        # 4) 尝试修复：补全未闭合的字符串与括号，然后再解析
        def _find_json_start(s: str) -> int:
            a, b = s.find('{'), s.find('[')
            if a == -1 and b == -1:
                return -1
            if a == -1:
                return b
            if b == -1:
                return a
            return min(a, b)

        def _repair_by_closing(s: str) -> str:
            # 仅对可疑的JSON片段进行修复：补齐未闭合的引号与括号
            in_str = False
            esc = False
            stack = []  # 记录未闭合的括号
            for ch in s:
                if in_str:
                    if esc:
                        esc = False
                    elif ch == '\\':
                        esc = True
                    elif ch == '"':
                        in_str = False
                else:
                    if ch == '"':
                        in_str = True
                    elif ch in ('{', '['):
                        stack.append(ch)
                    elif ch in ('}', ']') and stack:
                        top = stack[-1]
                        if (top == '{' and ch == '}') or (top == '[' and ch == ']'):
                            stack.pop()
                        else:
                            # 括号类型不匹配，忽略该关闭符
                            pass
            # 如果字符串在结尾仍未闭合，补一个引号
            repaired = s + ('"' if in_str else '')
            # 依次补齐未闭合的括号
            closing = []
            for ch in reversed(stack):
                closing.append('}' if ch == '{' else ']')
            repaired += ''.join(closing)
            # 再次移除可能在闭合前的尾随逗号
            repaired = _remove_trailing_commas(repaired)
            return repaired

        try:
            start_idx = _find_json_start(cleaned)
            if start_idx != -1:
                frag = cleaned[start_idx:]
                repaired = _repair_by_closing(frag)
                return json.loads(repaired)
        except Exception:
            pass

        # 5) 兜底：将 true/false/null 转为 Python 并 literal_eval，再转回 dict
        try:
            py_like = _re.sub(r'\btrue\b', 'True', cleaned)
            py_like = _re.sub(r'\bfalse\b', 'False', py_like)
            py_like = _re.sub(r'\bnull\b', 'None', py_like)
            obj = _ast.literal_eval(py_like)
            # 仅接受字典/列表
            if isinstance(obj, (dict, list)):
                # 转回 JSON 再加载一次，保证是标准 JSON
                return json.loads(json.dumps(obj, ensure_ascii=False))
        except Exception:
            pass

        return None


__all__ = [
    "DataSourceValidator",
    "ValidationResult",
]