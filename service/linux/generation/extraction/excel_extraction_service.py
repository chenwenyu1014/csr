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
import uuid
import time
import threading
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import get_settings
from service.models import get_llm_service
from service.prompts.system_prompt_manager import system_prompt_manager
from utils.context_manager import get_current_output_dir

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
    
    def _get_prompts_dir(self) -> Path:
        """获取提示词保存目录（使用线程安全的方式）"""
        session_dir = get_current_output_dir(default="output")
        prompts_dir = Path(session_dir) / "prompts" / "excel_extraction"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        return prompts_dir

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
    
    def _save_prompt_and_output(self, sheet_name: str, prompt: str, output: str, source_file: str) -> dict:
        """
        保存Excel提取的提示词和输出
        
        Args:
            sheet_name: Sheet名称
            prompt: 完整提示词
            output: 模型输出
            source_file: 源文件名
            
        Returns:
            保存的文件路径信息
        """
        try:
            prompts_dir = self._get_prompts_dir()
            
            # 生成唯一文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            rand6 = uuid.uuid4().hex[:6]
            safe_sheet = sheet_name.replace("/", "_").replace("\\", "_").replace(":", "_")
            
            # 保存提示词
            prompt_file = prompts_dir / f"excel_prompt_{safe_sheet}_{timestamp}_{rand6}.txt"
            prompt_file.write_text(prompt, encoding='utf-8')
            
            # 保存输出
            output_file = prompts_dir / f"excel_output_{safe_sheet}_{timestamp}_{rand6}.txt"
            output_file.write_text(output, encoding='utf-8')
            
            # 保存溯源JSON
            provenance_data = {
                "type": "excel_extraction",
                "sheet_name": sheet_name,
                "source_file": source_file,
                "timestamp": timestamp,
                "prompt_length": len(prompt),
                "output_length": len(output),
                "prompt_file": str(prompt_file),
                "output_file": str(output_file)
            }
            provenance_file = prompts_dir / f"excel_provenance_{safe_sheet}_{timestamp}_{rand6}.json"
            provenance_file.write_text(json.dumps(provenance_data, ensure_ascii=False, indent=2), encoding='utf-8')
            
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
                          source_file: Optional[str] = None) -> Dict[str, Any]:
        """
        从Excel的多个Sheet中提取数据
        
        Args:
            excel_dir: Excel预处理后的目录，包含多个md文件（每个sheet一个）
            extraction_query: 用户的提取需求
            source_file: 源Excel文件名（可选，用于标注）
        
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
                sheet_name = md_file.stem  # 默认使用文件名
                
                # 读取Sheet内容以提取标题
                try:
                    raw_content = md_file.read_text(encoding="utf-8")
                except Exception:
                    raw_content = ""
                sheet_title = self._extract_title_from_content(raw_content, fallback=sheet_name)
                
                # 请求间隔控制：避免瞬时高并发
                with _sheet_request_lock:
                    elapsed = time.time() - _sheet_last_request_time[0]
                    if elapsed < llm_request_interval:
                        time.sleep(llm_request_interval - elapsed)
                    _sheet_last_request_time[0] = time.time()
                
                logger.info(f"处理Sheet: {sheet_title}")
                
                # 提取单个Sheet（传入标题作为sheet_name）
                return self._extract_from_sheet(
                    md_file,
                    sheet_title,
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
                    future_to_md = {executor.submit(_extract_sheet_task, md): md for md in md_files}
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
            
            # 合并所有成功的提取内容
            combined_parts = []
            for result in successful_sheets:
                sheet_name = result.get("sheet_name")
                content = result.get("content", "")
                if content:
                    combined_parts.append(f"### Sheet: {sheet_name}\n\n{content}\n")
            
            combined_content = "\n---\n\n".join(combined_parts)
            
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
                                    extraction_query: str) -> Dict[str, Any]:
        """
        从多个Excel文件中提取数据
        
        Args:
            excel_dirs: 多个Excel预处理后的目录列表
            extraction_query: 用户的提取需求
        
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
                    extraction_query=extraction_query
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
                           sheet_name: str,
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
            sheet_content_with_title = f"# {sheet_name}\n\n{sheet_content}"

            # 提示词为空，直接返回原文内容
            if not extraction_query:
                return {
                    "success": True,
                    "sheet_name": sheet_name,
                    "content": sheet_content,
                    "source_file": source_file,
                    "md_file": str(md_file),
                    "source_content": sheet_content_with_title,  # 添加标题后的源内容供校验使用
                    "full_prompt": '',  # 添加完整提示词
                    "saved_files": ''  # 保存的文件路径
                }

            variables = {
                "extraction_query": extraction_query,
                "sheet_content": sheet_content_with_title,
                "project_desc": os.getenv("CURRENT_PROJECT_DESC", "")
            }
            prompt = system_prompt_manager.build_prompt("excel_extraction", variables)
            
            # 3. 调用模型提取
            logger.info(f"调用模型提取Sheet: {sheet_name}")
            logger.info(f"📝 Excel提取提示词长度: {len(prompt)}字符")
            model_output = self.llm.generate_single(prompt)
            
            # 4. 保存提示词和输出（用于排查）
            saved_files = self._save_prompt_and_output(
                sheet_name=sheet_name,
                prompt=prompt,
                output=model_output,
                source_file=source_file
            )
            
            return {
                "success": True,
                "sheet_name": sheet_name,
                "content": model_output,
                "source_file": source_file,
                "md_file": str(md_file),
                "source_content": sheet_content_with_title,  # 添加标题后的源内容供校验使用
                "full_prompt": prompt,  # 添加完整提示词
                "saved_files": saved_files  # 保存的文件路径
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
