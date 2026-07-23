#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Excel提取服务
处理Excel预处理后的多个Sheet的Markdown文件

技术特点：
- 支持多Sheet并发提取（受限并发，避免API限流）
- 支持请求间隔控制
"""

import logging
import os
import json
import ast
import time
import re
import threading
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import get_settings
from service.models import get_llm_service
from service.prompts.system_prompt_manager import system_prompt_manager
from utils.context_manager import get_current_output_dir, get_project_desc, inherit_context
from utils.output_manager import save_json, save_text

logger = logging.getLogger(__name__)


class ExcelExtractionService:
    """
    Excel提取服务
    
    功能：
    - 处理Excel预处理后的多个Sheet（每个Sheet一个md文件）
    - 使用相同的提取提示词循环处理每个Sheet
    - 汇总所有Sheet的提取结果
    
    使用场景：
    - 单个Excel文件：一个文件夹，多个md文件（每个Sheet一个）
    - 多个Excel文件：多个文件夹，每个文件夹包含多个md文件
    """
    
    def __init__(self, model_name: Optional[str] = None):
        """
        初始化Excel提取服务
        
        Args:
            model_name: 指定使用的模型名称（可选）
                       如果为None，则使用提取任务的默认模型
        """
        # 使用统一的模型管理器获取LLM实例
        self.llm = get_llm_service("extraction", model_name)
        logger.info(f"Excel提取服务已初始化，使用统一模型管理器")
    
    def _get_paragraph_prompts_dir(self) -> tuple[Path, str]:
        """获取当前段落的提取目录：output/extraction/<paragraph_id>/
        返回基目录，后续会在该目录下创建 prompts/ outputs/ provenance/ 子目录。
        """
        from utils.context_manager import get_paragraph_id

        output_dir = get_current_output_dir(default="output")
        paragraph_id = get_paragraph_id("") or "unknown"

        # 每个段落ID一个独立目录（与Word提取一致）
        base_dir = Path(output_dir) / "extraction" / paragraph_id
        base_dir.mkdir(parents=True, exist_ok=True)

        return base_dir, paragraph_id

    def _extract_title_from_content(self, content: str, fallback: str) -> str:
        """
        从Markdown内容中抽取标题：
        - 优先第一个一级/二级标题（# 或 ##）
        - 次选首个非空行
        - 兜底使用 fallback
        """
        try:
            for ln in content.splitlines():
                t = ln.strip()
                if not t:
                    continue
                if t.startswith("#"):
                    return t.lstrip("#").strip()
                if t.startswith("表"):
                    return t
                return t
        except Exception:
            pass
        return fallback
    def _parse_json_response(self, text: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        尽力从模型输出中解析JSON，提供容错处理

        策略：
        1) 去除 Markdown 代码块包裹、BOM、智能引号
        2) 移除注释 // ... 和 /* ... */
        3) 直接 json.loads 尝试
        4) 括号配对提取顶层 JSON
        5) 去除尾随逗号再尝试
        6) 移除非法控制字符再尝试
        7) 最后兜底：将 true/false/null 转换为 Python 等价并用 ast.literal_eval 尝试
        """
        if not text:
            return None

        def _strip_code_fences(s: str) -> str:
            s = s.lstrip('﻿').strip()
            s = re.sub(r'^```(?:json)?\s*\n?', '', s)
            s = re.sub(r'\n?```\s*$', '', s)
            return s.strip()

        def _strip_comments(s: str) -> str:
            # 移除 // 行注释 与 /* */ 块注释
            s = re.sub(r'(^|\s)//.*?$', r'\1', s, flags=re.MULTILINE)
            s = re.sub(r'/\*[\s\S]*?\*/', '', s)
            return s

        def _normalize_quotes(s: str) -> str:
            # 智能引号替换为普通双引号
            return s.replace('"', '"').replace('"', '"').replace("'", "'").replace("'", "'")

        def _remove_trailing_commas(s: str) -> str:
            # 移除对象或数组末尾的尾随逗号: {...,} 或 [...,]
            return re.sub(r',\s*([}\]])', r'\1', s)

        def _remove_illegal_ctrl(s: str) -> str:
            # 移除除 \t\r\n 外的控制字符
            return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', s)

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
                try:
                    fixed = _remove_trailing_commas(candidate)
                    return json.loads(fixed)
                except Exception:
                    try:
                        fixed2 = _remove_illegal_ctrl(fixed)
                        return json.loads(fixed2)
                    except Exception:
                        pass

        # 3) 移除尾随逗号并重试
        try:
            fixed_all = _remove_trailing_commas(cleaned)
            return json.loads(fixed_all)
        except Exception:
            pass

        # 4) 兜底：将 true/false/null 转为 Python 并 literal_eval
        try:
            py_like = re.sub(r'\btrue\b', 'True', cleaned)
            py_like = re.sub(r'\bfalse\b', 'False', py_like)
            py_like = re.sub(r'\bnull\b', 'None', py_like)
            obj = ast.literal_eval(py_like)
            if isinstance(obj, (dict, list)):
                return json.loads(json.dumps(obj, ensure_ascii=False))
        except Exception:
            pass

        return None
    def _call_model_with_json_retry(self, prompt: str, sheet_name: str, max_retries: int = 2,
                                    system: str = "") -> Dict[str, Any]:
        """
        调用模型并解析 JSON，支持重试机制

        Args:
            prompt: 用户段提示词
            sheet_name: Sheet 名称（用于日志和兜底）
            max_retries: 最大重试次数（默认2次，共3次尝试）
            system: 系统提示词（可选，作为可缓存前缀单独传入）

        Returns:
            Dict: 包含解析结果和重试信息
                - parse_success: 是否解析成功
                - parsed_json: 解析后的 JSON 对象（成功时）
                - raw_output: 最后一次模型输出
                - retry_attempts: 重试次数
        """
        all_attempts_outputs = []
        current_output = self.llm.generate_single(prompt, system=system)
        all_attempts_outputs.append(current_output)

        parsed = None

        for attempt in range(max_retries + 1):
            parsed = self._parse_json_response(current_output)

            if parsed:
                logger.info(f"✅ Excel提取JSON解析成功 (第 {attempt + 1} 次尝试)")
                return {
                    "parse_success": True,
                    "parsed_json": parsed,
                    "raw_output": current_output,
                    "retry_attempts": attempt + 1
                }

            # 解析失败
            logger.warning(f"⚠️ Excel提取JSON解析失败 (第 {attempt + 1} 次尝试), sheet={sheet_name}")

            if attempt < max_retries:
                logger.info(f"🔄 发起第 {attempt + 2} 次模型调用...")
                current_output = self.llm.generate_single(prompt, system=system)
                all_attempts_outputs.append(current_output)
            else:
                # 达到最大重试次数
                logger.error(f"❌ Excel提取JSON解析最终失败 (已重试{max_retries}次), sheet={sheet_name}")
                logger.error(f"最后一次模型输出（前500字符）:\n{current_output[:500] if current_output else 'None'}")
                break

        # 返回失败结果
        return {
            "parse_success": False,
            "parsed_json": None,
            "raw_output": current_output,
            "retry_attempts": len(all_attempts_outputs),
            "all_attempts_outputs": all_attempts_outputs  # 所有尝试的输出，用于排查
        }

    def _build_parsed_result(self, parse_result: Dict[str, Any], sheet_name: str) -> Dict[str, Any]:
        """
        从解析结果构建返回字段

        Args:
            parse_result: _call_model_with_json_retry 的返回结果
            sheet_name: 传入的 sheet 名称（用于兜底）

        Returns:
            Dict: 包含 sheet_name, content, available, reason 等字段
        """
        result = {
            "sheet_name": sheet_name,
            "content": "",
            "available": False,
            "relevance_score":0,
            "reason": "",
            "parse_success": parse_result["parse_success"],
            "retry_attempts": parse_result["retry_attempts"]
        }

        if parse_result["parse_success"] and parse_result["parsed_json"]:
            parsed_json = parse_result["parsed_json"]

            # 提取各个字段，使用传入的 sheet_name 作为兜底
            result["sheet_name"] = parsed_json.get("sheet_name", sheet_name)
            result["content"] = parsed_json.get("content", "")
            result["relevance_score"] = parsed_json.get("relevance_score", 0)

            # available 字段可能是字符串 "true"/"false" 或布尔值
            available_val = parsed_json.get("available", "false")
            if isinstance(available_val, str):
                result["available"] = available_val.lower() == "true"
            elif isinstance(available_val, bool):
                result["available"] = available_val
            else:
                result["available"] = False

            result["reason"] = parsed_json.get("reason", "")
        else:
            # JSON 解析失败，记录失败原因
            result["reason"] = f"JSON解析失败（已重试{parse_result['retry_attempts']}次）"

        return result

    def _save_prompt_and_output(self, sheet_name: str, prompt: str, output: str, source_file: str) -> dict:
        """
        保存Excel提取的提示词和输出（按段落ID组织目录）

        Args:
            sheet_name: Sheet名称
            prompt: 完整提示词
            output: 模型输出
            source_file: 源文件名

        Returns:
            保存的文件路径信息
        """
        try:
            # 使用段落ID目录（与Word提取一致）
            base_dir, paragraph_id = self._get_paragraph_prompts_dir()

            # 创建子目录
            prompts_dir = base_dir / "prompts"
            outputs_dir = base_dir / "outputs"
            provenance_dir = base_dir / "provenance"
            prompts_dir.mkdir(parents=True, exist_ok=True)
            outputs_dir.mkdir(parents=True, exist_ok=True)
            provenance_dir.mkdir(parents=True, exist_ok=True)

            # 生成时间戳
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            safe_sheet = sheet_name.replace("/", "_").replace("\\", "_").replace(":", "_")

            # 保存提示词
            prompt_file = prompts_dir / f"excel_prompt_{paragraph_id}_{safe_sheet}_{timestamp}.txt"
            save_text(prompt_file, prompt)
            
            # 保存输出
            output_file = outputs_dir / f"excel_output_{paragraph_id}_{safe_sheet}_{timestamp}.txt"
            save_text(output_file, output)

            # 保存溯源JSON
            provenance_data = {
                "type": "excel_extraction",
                "paragraph_id": paragraph_id,
                "sheet_name": sheet_name,
                "source_file": source_file,
                "timestamp": timestamp,
                "prompt_length": len(prompt),
                "output_length": len(output),
                "prompt_file": str(prompt_file),
                "output_file": str(output_file)
            }
            provenance_file = provenance_dir / f"excel_provenance_{paragraph_id}_{safe_sheet}_{timestamp}.json"
            save_json(provenance_file, provenance_data)

            logger.info(f"✅ Excel提取提示词已保存: {prompt_file.name}")

            return {
                "prompt_file": str(prompt_file),
                "output_file": str(output_file),
                "provenance_file": str(provenance_file)
            }
        except Exception as e:
            logger.warning(f"保存Excel提取提示词失败: {e}")
            return {}
    
    def extract_from_excel(self,
                          excel_dir: str,
                          extraction_query: str,
                          source_file: Optional[str] = None,
                          paragraph_id: Optional[str] = None) -> Dict[str, Any]:
        """
        从Excel的多个Sheet中提取数据

        Args:
            excel_dir: Excel预处理后的目录，包含多个md文件（每个sheet一个）
            extraction_query: 用户的提取需求
            source_file: 源Excel文件名（可选，用于标注）
            paragraph_id: 段落ID（用于组织输出文件目录）

        Returns:
            Dict: 提取结果
            {
                "success": bool,
                "sheets_results": [...],  # 每个Sheet的提取结果
                "combined_content": str,  # 合并后的提取内容
                "summary": {...}  # 处理摘要
            }
        """
        try:
            # 1. 扫描目录，获取所有md文件
            excel_path = Path(excel_dir)
            if not excel_path.exists():
                return {
                    "success": False,
                    "error": f"目录不存在: {excel_dir}"
                }
            
            md_files = list(excel_path.glob("*.md"))
            if not md_files:
                return {
                    "success": False,
                    "error": f"目录中没有找到md文件: {excel_dir}"
                }
            
            # 按文件名排序，确保处理顺序一致
            md_files.sort()
            
            logger.info(f"找到{len(md_files)}个Sheet文件")
            
            # 获取并发配置
            settings = get_settings()
            max_sheet_workers = settings.max_file_extraction_workers
            llm_request_interval = settings.llm_request_interval
            
            # 请求间隔控制
            _sheet_request_lock = threading.Lock()
            _sheet_last_request_time = [0.0]
            
            def _extract_sheet_task(md_file: Path) -> Dict[str, Any]:
                """单个Sheet的提取任务"""
                # 在工作线程中设置 paragraph_id，确保文件保存到正确的目录
                if paragraph_id:
                    from utils.context_manager import set_paragraph_id
                    set_paragraph_id(paragraph_id)

                sheet_name = md_file.stem  # 默认使用文件名

                # 读取Sheet内容以提取标题
                try:
                    raw_content = md_file.read_text(encoding="utf-8")
                except Exception:
                    raw_content = ""
                sheet_title = self._extract_title_from_content(raw_content, sheet_name)
                sheet_info = {"sheet_name": sheet_name, "sheet_title": sheet_title}
                
                # 请求间隔控制：避免瞬时高并发
                with _sheet_request_lock:
                    elapsed = time.time() - _sheet_last_request_time[0]
                    if elapsed < llm_request_interval:
                        time.sleep(llm_request_interval - elapsed)
                    _sheet_last_request_time[0] = time.time()

                logger.info(f"处理Sheet: {sheet_info['sheet_name']}_{sheet_info['sheet_title']}")
                
                # 提取单个Sheet（传入标题作为sheet_name）
                return self._extract_from_sheet(
                    md_file,
                    sheet_info,
                    extraction_query,
                    source_file or excel_path.name
                )
            
            # 2. 使用受限并发处理多个Sheet
            sheets_results = []
            if len(md_files) == 1:
                # 单Sheet直接处理
                sheets_results = [_extract_sheet_task(md_files[0])]
            else:
                # 多Sheet使用受限并发
                actual_workers = min(max_sheet_workers, len(md_files))
                logger.info(f"📊 多Sheet并发提取: {len(md_files)}个Sheet, 并发数: {actual_workers}")
                
                with ThreadPoolExecutor(max_workers=actual_workers) as executor:
                    future_to_md = {executor.submit(inherit_context(_extract_sheet_task), md): md for md in md_files}
                    for future in as_completed(future_to_md):
                        md_file = future_to_md[future]
                        try:
                            result = future.result()
                            sheets_results.append(result)
                        except Exception as e:
                            logger.error(f"❌ Sheet提取异常: {md_file.name} - {e}")
                            sheets_results.append({
                                "success": False,
                                "sheet_name": md_file.stem,
                                "error": str(e)
                            })
            
            # 3. 汇总结果
            successful_sheets = [r for r in sheets_results if r.get("success")]
            failed_sheets = [r for r in sheets_results if not r.get("success")]

            # 合并所有成功的提取内容（只合并 parse_success=True 且 available=True 的内容）
            combined_parts = []
            for result in successful_sheets:
                sheet_name = result.get("sheet_name")
                content = result.get("content", "")
                available = result.get("available", False)
                parse_success = result.get("parse_success", False)

                # 只合并 JSON 解析成功且 available=True 的内容
                if parse_success and available and content:
                    combined_parts.append(content)

            combined_content = "\n\n".join(combined_parts)
            
            # 构建完整提示词（合并所有sheet的提示词）
            full_prompt_parts = []
            for i, result in enumerate(sheets_results, 1):
                if result.get("success") and result.get("full_prompt"):
                    sheet_name = result.get("sheet_name", f"Sheet{i}")
                    full_prompt_parts.append(f"=== Sheet: {sheet_name} ===\n{result.get('full_prompt')}")
            
            full_prompt = "\n\n".join(full_prompt_parts) if full_prompt_parts else ""
            
            # 4. 组合源内容（用于校验）
            combined_source_content = "\n\n".join([
                f"## {result['sheet_name']}\n{result.get('source_content', '')}"
                for result in sheets_results
                if result.get('success') and result.get('source_content')
            ])
            
            # 5. 返回结果
            return {
                "success": True,
                "sheets_results": sheets_results,
                "combined_content": combined_content,
                "source_content": combined_source_content,  # 添加组合的源内容
                "full_prompt": full_prompt,
                "summary": {
                    "total_sheets": len(md_files),
                    "successful_sheets": len(successful_sheets),
                    "failed_sheets": len(failed_sheets),
                    "source_file": source_file or excel_path.name,
                    "extraction_query": extraction_query
                }
            }
            
        except Exception as e:
            logger.error(f"Excel提取失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def extract_from_multiple_excels(self,
                                    excel_dirs: List[str],
                                    extraction_query: str,
                                    paragraph_id: Optional[str] = None) -> Dict[str, Any]:
        """
        从多个Excel文件中提取数据

        Args:
            excel_dirs: 多个Excel预处理后的目录列表
            extraction_query: 用户的提取需求
            paragraph_id: 段落ID（用于组织输出文件目录）

        Returns:
            Dict: 提取结果
            {
                "success": bool,
                "excel_results": [...],  # 每个Excel的提取结果
                "combined_content": str,  # 合并后的提取内容
                "summary": {...}  # 处理摘要
            }
        """
        try:
            logger.info(f"开始处理{len(excel_dirs)}个Excel文件")

            excel_results = []
            for excel_dir in excel_dirs:
                logger.info(f"处理Excel目录: {excel_dir}")

                # 提取单个Excel
                excel_result = self.extract_from_excel(
                    excel_dir=excel_dir,
                    extraction_query=extraction_query,
                    paragraph_id=paragraph_id
                )
                
                excel_results.append(excel_result)
            
            # 汇总结果
            successful_excels = [r for r in excel_results if r.get("success")]
            failed_excels = [r for r in excel_results if not r.get("success")]
            
            # 合并所有Excel的提取内容
            combined_parts = []
            for result in successful_excels:
                source_file = result.get("summary", {}).get("source_file", "未知")
                content = result.get("combined_content", "")
                
                combined_parts.append(f"## Excel文件: {source_file}\n\n{content}\n")
            
            combined_content = "\n\n" + "="*70 + "\n\n".join(combined_parts)
            
            return {
                "success": True,
                "excel_results": excel_results,
                "combined_content": combined_content,
                "summary": {
                    "total_excels": len(excel_dirs),
                    "successful_excels": len(successful_excels),
                    "failed_excels": len(failed_excels),
                    "extraction_query": extraction_query
                }
            }
            
        except Exception as e:
            logger.error(f"批量Excel提取失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _extract_from_sheet(self,
                           md_file: Path,
                           sheet_info: dict,
                           extraction_query: str,
                           source_file: str) -> Dict[str, Any]:
        """
        从单个Sheet提取数据

        Args:
            md_file: Sheet的md文件路径
            sheet_name: Sheet名称
            extraction_query: 提取需求
            source_file: 源文件名

        Returns:
            Dict: 提取结果
        """
        sheet_name = sheet_info["sheet_name"]
        sheet_title = sheet_info["sheet_title"]
        try:
            # 1. 读取Sheet内容
            with open(md_file, 'r', encoding='utf-8') as f:
                sheet_content = f.read()

            if not sheet_content.strip():
                return {
                    "success": False,
                    "sheet_name": sheet_name,
                    "error": "Sheet内容为空"
                }

            # 2. 构建提示词（仅传递提取需求和Sheet内容）
            # 将标题前置到内容，确保模型看到标题
            sheet_content_with_title = f"【sheet_name】\n{sheet_name}\n【sheet_content】:\n{sheet_content}"

            # 提示词为空，直接返回原文内容
            if not extraction_query:
                return {
                    "success": True,
                    "sheet_name": sheet_name,
                    "content": sheet_content,
                    "sheet_title":sheet_title,
                    "reason": "提示词为空，默认返回原文内容",
                    "available": True,
                    "relevance_score":1,
                    "source_file": source_file,
                    "md_file": str(md_file),
                    "source_content": sheet_content_with_title,  # 添加标题后的源内容供校验使用
                    "full_prompt": '',  # 添加完整提示词
                    "saved_files": ''  # 保存的文件路径
                }

            variables = {
                "extraction_query": extraction_query,
                "sheet_content": sheet_content_with_title,
                "project_desc": get_project_desc()
            }
            messages = system_prompt_manager.build_messages("excel_extraction", variables)
            system_prompt = messages.get("system", "")
            user_prompt = messages.get("user", "")
            # 留痕用完整文本（system + user）；调用模型时只传 user，system 走独立参数以命中缓存
            prompt = (system_prompt + "\n\n" + user_prompt) if system_prompt else user_prompt

            # 3. 调用模型并解析 JSON（带重试机制）
            logger.info(f"调用模型提取Sheet: {sheet_name}")
            logger.info(f"📝 Excel提取提示词长度: {len(prompt)}字符")

            parse_result = self._call_model_with_json_retry(user_prompt, sheet_name, max_retries=2, system=system_prompt)

            # 4. 构建解析后的字段
            parsed_fields = self._build_parsed_result(parse_result, sheet_name)

            # 5. 保存提示词和输出（用于排查）
            saved_files = self._save_prompt_and_output(
                sheet_name=sheet_name,
                prompt=prompt,
                output=parse_result["raw_output"],
                source_file=source_file
            )

            # 6. 构建返回结果
            # 如果 JSON 解析失败，success=False，但仍然返回原始输出供排查
            return {
                "success": parsed_fields["parse_success"] or parsed_fields["available"],  # 解析成功或内容可用都算成功
                "sheet_name": parsed_fields["sheet_name"],
                "sheet_title": sheet_title,
                "content": parsed_fields["content"],
                "available": parsed_fields["available"],
                "relevance_score":parsed_fields["relevance_score"],
                "reason": parsed_fields["reason"],
                "source_file": source_file,
                "md_file": str(md_file),
                "source_content": sheet_content_with_title,
                "full_prompt": prompt,
                "saved_files": saved_files,
                "raw_output": parse_result.get("raw_output"),
                "parse_success": parsed_fields["parse_success"],
                "retry_attempts": parsed_fields["retry_attempts"]
            }

        except Exception as e:
            logger.error(f"提取Sheet失败 {sheet_name}: {e}")
            return {
                "success": False,
                "sheet_name": sheet_name,
                "error": str(e)
            }


# 创建全局服务实例
excel_extraction_service = ExcelExtractionService()
