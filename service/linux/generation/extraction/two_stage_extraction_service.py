#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
两阶段智能提取服务
第一阶段：根据提取需求筛选相关分块
第二阶段：从筛选的分块中提取内容
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, Any, List, Optional

from service.models import get_llm_service
from service.prompts.system_prompt_manager import system_prompt_manager
from utils.context_manager import get_current_output_dir
from utils.task_logger import get_task_logger

logger = logging.getLogger(__name__)


def _task_log_error(message: str, exc: Exception = None, **extra):
    """记录错误到任务日志"""
    task_logger = get_task_logger()
    if task_logger:
        task_logger.error(message, exc=exc, logger_name="two_stage_extraction", **extra)


class TwoStageExtractionService:
    """
    两阶段智能提取服务
    
    功能：
    1. 阶段1：分块筛选 - 根据提取需求和分块摘要筛选相关分块
    2. 阶段2：内容提取 - 只对筛选出的分块进行详细提取
    
    优势：
    - 减少无关内容的处理
    - 提高提取准确性
    - 降低Token消耗
    - 加快处理速度
    """
    
    def __init__(self, model_name: Optional[str] = None):
        """
        初始化两阶段提取服务
        
        Args:
            model_name: 指定使用的模型名称（可选）
                       如果为None，则使用提取任务的默认模型
        """
        # 使用统一的模型管理器获取LLM实例
        self.llm = get_llm_service("extraction", model_name)
        logger.info(f"两阶段提取服务已初始化，使用统一模型管理器")
    
    def extract_from_chunks(self,
                           chunks_index_path: str,
                           chunks_dir: str,
                           extraction_query: str,
                           task_name: Optional[str] = None,
                           doc_type: str = "word") -> Dict[str, Any]:
        """
        从文档分块中进行两阶段提取
        
        Args:
            chunks_index_path: 分块索引文件路径（chunks_index.json）
            chunks_dir: 分块文件目录
            extraction_query: 用户的提取需求
            task_name: 任务名称（可选）
            doc_type: 文档类型，"word" 或 "pdf"（默认"word"）
        
        Returns:
            Dict: 提取结果
            {
                "success": bool,
                "stage1_result": {...},  # 分块筛选结果
                "stage2_result": {...},  # 内容提取结果
                "extracted_content": str,  # 最终提取的内容
                "chunks_used": [...],  # 使用的分块列表
                "summary": {...}  # 处理摘要
            }
        """
        try:
            logger.info(f"[Two Stage Extraction] 开始执行两阶段提取模式: {chunks_index_path}")
            # 1. 加载分块索引
            chunks_index = self._load_chunks_index(chunks_index_path)
            if not chunks_index:
                return {
                    "success": False,
                    "error": "无法加载分块索引文件"
                }
            
            # ✅ 保存读取的chunks数据
            self._save_loaded_chunks_data(chunks_index, chunks_index_path)
            
            # 2. 阶段1：筛选相关分块
            logger.info(f"阶段1：开始筛选相关分块（文档类型：{doc_type}）")
            filtering_result = self._filter_relevant_chunks(
                chunks_index,
                extraction_query,
                task_name,
                doc_type
            )
            
            if not filtering_result.get("success"):
                error_detail = filtering_result.get("error", "未知错误")
                logger.error(f"❌ 分块筛选失败: {error_detail}")
                # 如果有traceback，也记录
                if filtering_result.get("traceback"):
                    logger.error(f"❌ 异常堆栈:\n{filtering_result.get('traceback')}")
                return {
                    "success": False,
                    "error": f"分块筛选失败: {error_detail}",
                    "stage1_result": filtering_result
                }
            
            # 从筛选结果的parsed_result中获取relevant_sections（兼容旧字段名relevant_chunks）
            parsed = filtering_result.get("parsed_result", {})
            relevant_chunks = parsed.get("relevant_sections", []) or parsed.get("relevant_chunks", [])
            
            # 获取总分块数（支持新旧两种结构）
            total_chunks = len(chunks_index.get("sections", chunks_index.get("chunks", [])))
            
            if not relevant_chunks:
                return {
                    "success": True,
                    "extracted_content": "",
                    "stage1_result": filtering_result,
                    "stage2_result": None,
                    "chunks_used": [],
                    "summary": {
                        "total_chunks": total_chunks,
                        "selected_chunks": 0,
                        "message": "未找到相关分块"
                    }
                }
            
            # 3. 加载筛选出的分块内容
            logger.info(f"加载{len(relevant_chunks)}个相关分块的内容")
            chunks_content = self._load_chunks_content(
                chunks_index,
                chunks_dir,
                relevant_chunks
            )
            
            # 4. 阶段2：从筛选的分块中提取内容
            logger.info(f"阶段2：开始内容提取（文档类型：{doc_type}）")
            extraction_result = self._extract_from_selected_chunks(
                chunks_content,
                extraction_query,
                task_name,
                doc_type
            )
            
            if not extraction_result.get("success"):
                return {
                    "success": False,
                    "error": "内容提取失败",
                    "stage1_result": filtering_result,
                    "stage2_result": extraction_result
                }
            
            # 5. 构建完整提示词（包含两个阶段）
            full_prompt_parts = []
            stage1_prompt = filtering_result.get("full_prompt", "")
            stage2_prompt = extraction_result.get("full_prompt", "")
            
            if stage1_prompt:
                full_prompt_parts.append("=== 第一阶段：分块筛选 ===\n" + stage1_prompt)
            if stage2_prompt:
                full_prompt_parts.append("=== 第二阶段：内容提取 ===\n" + stage2_prompt)
            
            full_prompt = "\n\n".join(full_prompt_parts) if full_prompt_parts else ""
            
            # 6. 获取筛选的分块内容文本（用于校验）
            selected_chunks_text = ""
            if extraction_result.get("success"):
                # 从提取阶段获取已构建的分块文本
                selected_chunks_text = self._build_selected_chunks_text(chunks_content)
            
            # 7. 返回完整结果
            return {
                "success": True,
                "stage1_result": filtering_result,
                "stage2_result": extraction_result,
                "extracted_content": extraction_result.get("content", extraction_result.get("model_output", "")),
                "chunks_used": relevant_chunks,
                "selected_chunks_content": selected_chunks_text,  # 添加筛选的分块内容
                "full_prompt": full_prompt,  # 添加合并的完整提示词
                "summary": {
                    "total_chunks": total_chunks,
                    "selected_chunks": len(relevant_chunks),
                    "selection_rate": f"{(len(relevant_chunks) / max(total_chunks, 1) * 100):.2f}%",
                    "avg_relevance_score": self._calculate_avg_score(relevant_chunks)
                }
            }
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"两阶段提取失败: {e}", exc_info=True)
            _task_log_error("两阶段提取失败", exc=e)
            return {
                "success": False,
                "error": str(e)
            }

    def extract_filter_only(self,
                            chunks_index_path: str,
                            chunks_dir: str,
                            extraction_query: str,
                            task_name: Optional[str] = None,
                            doc_type: str = "word") -> Dict[str, Any]:
        """
        【新增方法】仅执行筛选阶段，不执行生成阶段。
        适用于：提取逻辑非空 + 原文模式

        流程：
        1. 加载分块索引。
        2. 调用 LLM 进行相关性筛选 (阶段1)。
        3. 根据筛选结果，直接读取原始分块内容。
        4. 拼接原文返回，跳过内容提取/总结 (阶段2)。

        Returns:
            Dict: 包含 success, content (拼接后的原文), parsed_result (筛选详情)
        """
        try:
            logger.info(f"[Filter Only] 开始执行纯筛选模式: {chunks_index_path}")
            # 1. 加载分块索引
            chunks_index = self._load_chunks_index(chunks_index_path)
            if not chunks_index:
                return {
                    "success": False,
                    "error": "无法加载分块索引文件"
                }
            # ✅ 保存读取的chunks数据
            self._save_loaded_chunks_data(chunks_index, chunks_index_path)

            # 2. 阶段1：筛选相关分块
            logger.info(f"阶段1：开始筛选相关分块（文档类型：{doc_type}）")
            filtering_result = self._filter_relevant_chunks(
                chunks_index,
                extraction_query,
                task_name,
                doc_type
            )

            if not filtering_result.get("success"):
                error_detail = filtering_result.get("error", "未知错误")
                logger.error(f"❌ 分块筛选失败: {error_detail}")
                # 如果有traceback，也记录
                if filtering_result.get("traceback"):
                    logger.error(f"❌ 异常堆栈:\n{filtering_result.get('traceback')}")
                return {
                    "success": False,
                    "error": f"分块筛选失败: {error_detail}",
                    "stage1_result": filtering_result
                }

            # 从筛选结果的parsed_result中获取relevant_sections（兼容旧字段名relevant_chunks）
            parsed = filtering_result.get("parsed_result", {})
            relevant_chunks = parsed.get("relevant_sections", []) or parsed.get("relevant_chunks", [])

            # 获取总分块数（支持新旧两种结构）
            total_chunks = len(chunks_index.get("sections", chunks_index.get("chunks", [])))

            if not relevant_chunks:
                return {
                    "success": True,
                    "extracted_content": "",
                    "stage1_result": filtering_result,
                    "stage2_result": None,
                    "chunks_used": [],
                    "summary": {
                        "total_chunks": total_chunks,
                        "selected_chunks": 0,
                        "message": "未找到相关分块"
                    }
                }

            # 3. 加载筛选出的分块内容
            logger.info(f"加载{len(relevant_chunks)}个相关分块的内容")
            chunks_content = self._load_chunks_content(
                chunks_index,
                chunks_dir,
                relevant_chunks
            )

            # 4. 拼接原文 (不进行任何LLM生成)
            final_content = self._build_selected_chunks_text(chunks_content)

            # 防御性检查：如果内置方法返回空且应该有内容
            if not final_content and relevant_chunks:
                logger.warning("_build_selected_chunks_text 返回为空，尝试 fallback 到简单拼接")
                # Fallback 逻辑（以防内置方法对输入格式有特殊要求导致失效）
                parts = []
                for chunk in relevant_chunks:
                    cid = chunk.get("id") or chunk.get("section_id")
                    if cid and cid in chunks_content:
                        parts.append(chunks_content[cid])
                final_content = "\n\n".join(parts)

            return {
                "success": True,
                "content": final_content,
                "extracted_content": final_content,
                "stage1_result": filtering_result,
                "method": "filter_only",
                "parsed_result": parsed,
                "chunks_used": relevant_chunks,
                "selected_chunks_content": final_content,
                "full_prompt": extraction_query,
                "summary": {
                    "total_chunks": total_chunks,
                    "selected_chunks": len(relevant_chunks),
                    "selection_rate": f"{(len(relevant_chunks) / max(total_chunks, 1) * 100):.2f}%",
                    "avg_relevance_score": self._calculate_avg_score(relevant_chunks)
                }
            }

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"仅执行筛选阶段: {e}", exc_info=True)
            _task_log_error("仅执行筛选阶段", exc=e)
            return {
                "success": False,
                "error": str(e)
            }

    def _load_chunks_index(self, index_path: str) -> Optional[Dict[str, Any]]:
        """加载分块索引文件"""
        try:
            index_file = Path(index_path)
            if not index_file.exists():
                logger.error(f"分块索引文件不存在: {index_path}")
                _task_log_error(f"分块索引文件不存在: {index_path}")
                return None
            
            with open(index_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"加载分块索引失败: {e}")
            return None
    
    def _filter_relevant_chunks(self,
                               chunks_index: Dict[str, Any],
                               extraction_query: str,
                               task_name: Optional[str] = None,
                               doc_type: str = "word") -> Dict[str, Any]:
        """
        阶段1：筛选相关分块
        
        Args:
            chunks_index: 分块索引数据
            extraction_query: 提取需求
            task_name: 任务名称
            doc_type: 文档类型，"word" 或 "pdf"
        
        Returns:
            Dict: 筛选结果
        """
        try:
            # 构建分块列表文本
            chunks_list = self._build_chunks_list_text(chunks_index)
            
            # 根据文档类型选择提示词模板
            template_name = "chunk_filtering_pdf" if doc_type == "pdf" else "chunk_filtering"
            
            # 构建提示词（不包含task_name）
            variables = {
                "extraction_query": extraction_query,
                "chunks_list": chunks_list,
                "project_desc": os.getenv("CURRENT_PROJECT_DESC", "")
            }
            
            logger.info(f"🔧 使用提示词模板: {template_name}")
            logger.info(f"🔧 提取查询: {extraction_query}")
            logger.info(f"🔧 Chunks列表长度: {len(chunks_list)}字符")
            
            prompt = system_prompt_manager.build_prompt(template_name, variables)
            
            logger.info(f"📝 筛选提示词已构建，长度: {len(prompt)}字符")
            logger.info(f"📝 筛选提示词（前500字符）:\n{prompt[:500]}")
            
            # 调用模型
            logger.info(f"🤖 正在调用LLM进行分块筛选...")
            model_output = self.llm.generate_single(prompt)
            
            # ✅ 调试：输出LLM原始响应
            logger.info(f"🔍 LLM原始输出长度: {len(model_output)}字符")
            logger.info(f"🔍 LLM原始输出（前1000字符）:\n{model_output[:1000]}")
            current_output = model_output
            # 解析结果 带重试
            max_retries = 2
            parsed = None
            all_attempts_outputs = [model_output]
            for attempt in range(max_retries + 1):
                parsed = self._parse_json_response(current_output)
                if parsed:
                    logger.info(f"✅ JSON 解析成功 (第 {attempt + 1} 次尝试)")
                    model_output = current_output
                    break
                # 2. 解析失败处理
                logger.warning(f"⚠️ 第 {attempt + 1} 次解析失败。")

                if attempt < max_retries:
                    logger.info(f"🔄 发起第 {attempt + 1} 次重试 ...")
                    current_output = self.llm.generate_single(prompt)
                    logger.info(f"🔍 重试后 LLM 输出长度: {len(current_output)}")
                    all_attempts_outputs.append(current_output)

                else:
                    # 达到最大重试次数，不再调用模型，跳出循环去报错
                    logger.error(f"❌ 已达最大重试次数 ({max_retries})，所有尝试均失败。")
                    break
            if not parsed:
                logger.error(f"❌ JSON 解析最终失败。最后一次输出:\n{current_output[:500]}...")
                from datetime import datetime
                error_result = {
                    "success": False,
                    "stage": "chunk_filtering",
                    "timestamp": datetime.now().isoformat(),
                    "error": f"无法解析模型输出为JSON (已重试{len(all_attempts_outputs)}次)",
                    "input_data": {
                        "extraction_query": extraction_query,
                        "task_name": task_name or "分块筛选",
                        "doc_type": doc_type,
                        "total_chunks": len(chunks_index.get("sections", chunks_index.get("chunks", [])))
                    },
                    "variables": variables,
                    "full_prompt": prompt,
                    "prompt_length": len(prompt),
                    "model_output": current_output,  # 保存最后一次尝试的输出
                    "output_length": len(current_output),
                    "parse_error": "JSON解析失败",
                    "retry_attempts": len(all_attempts_outputs),
                }
                self._save_filtering_result(error_result)
                return error_result
            
            # ✅ 保存筛选阶段完整溯源数据
            from datetime import datetime
            filtering_result = {
                "success": True,
                "stage": "chunk_filtering",
                "timestamp": datetime.now().isoformat(),
                "input_data": {
                    "extraction_query": extraction_query,
                    "task_name": task_name or "分块筛选",
                    "doc_type": doc_type,
                    "total_chunks": len(chunks_index.get("sections", chunks_index.get("chunks", [])))
                },
                "variables": variables,
                "full_prompt": prompt,
                "prompt_length": len(prompt),
                "model_output": model_output,
                "output_length": len(model_output),
                "parsed_result": {
                    # 优先使用新字段名 relevant_sections，兼容旧字段名 relevant_chunks
                    "relevant_sections": parsed.get("relevant_sections", []) or parsed.get("relevant_chunks", []),
                    "relevant_chunks": parsed.get("relevant_sections", []) or parsed.get("relevant_chunks", []),  # 保持兼容
                    "total_selected": parsed.get("total_selected", 0),
                    "selection_summary": parsed.get("selection_summary", "")
                }
            }
            
            # 保存筛选结果到文件
            self._save_filtering_result(filtering_result)
            
            return filtering_result
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"筛选相关分块失败: {e}", exc_info=True)
            # ✅ 异常时也保存溯源数据
            from datetime import datetime
            import traceback
            error_result = {
                "success": False,
                "stage": "chunk_filtering",
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "error_type": type(e).__name__,
                "traceback": traceback.format_exc(),
                "input_data": {
                    "extraction_query": extraction_query,
                    "task_name": task_name or "分块筛选",
                    "doc_type": doc_type
                }
            }
            self._save_filtering_result(error_result)
            return error_result
    
    def _build_chunks_list_text(self, chunks_index: Dict[str, Any]) -> str:
        """构建分块列表文本（用于筛选）
        
        输出分块ID、标题、摘要，供模型判断相关性并返回选中的分块ID
        
        支持两种结构：
        1. 旧结构: {"chunks": [...]}
        2. 新结构: {"sections": [...]}
        """
        # 优先使用新结构sections
        sections = chunks_index.get("sections", [])
        if sections:
            lines = []
            for section in sections:
                section_id = section.get("section_id", "")
                title = section.get("title", "")
                summary = section.get("summary", "")
                
                # 输出分块ID、标题、摘要
                lines.append(f"【分块 {section_id}】")
                if title:
                    lines.append(f"标题: {title}")
                if summary:
                    lines.append(f"摘要: {summary}")
                lines.append("")
            
            return "\n".join(lines)
        
        # 兼容旧结构chunks
        chunks = chunks_index.get("chunks", [])
        lines = []
        for idx, chunk in enumerate(chunks, start=1):
            chunk_id = chunk.get("chunk_id") or chunk.get("id") or chunk.get("section_id") or str(idx)
            title = chunk.get("title", "")
            summary = chunk.get("summary", "")
            
            # 输出分块ID、标题、摘要
            lines.append(f"【分块 {chunk_id}】")
            if title:
                lines.append(f"标题: {title}")
            if summary:
                lines.append(f"摘要: {summary}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _load_chunks_content(self,
                            chunks_index: Dict[str, Any],
                            chunks_dir: str,
                            relevant_chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """加载筛选出的分块内容
        
        支持两种方式：
        1. 新结构：直接从chunks_index的sections中获取content
        2. 旧结构：从chunks_dir读取单独的.md文件
        
        注意：模型返回的是分块ID（如 h1_1），需要用ID来匹配分块
        """
        loaded_chunks = []
        
        # 检查是否有sections（新结构）
        sections = chunks_index.get("sections", [])
        if sections:
            # 创建section_id到section的映射
            section_map = {s.get("section_id"): s for s in sections}
            
            for chunk_info in relevant_chunks:
                # 获取模型返回的分块ID
                section_id = str(chunk_info.get("section_id", "") or chunk_info.get("chunk_id", "")).strip()
                
                # 通过分块ID查找分块
                section = section_map.get(section_id)
                
                if not section:
                    logger.warning(f"未找到分块ID: {section_id}")
                    continue
                
                loaded_chunks.append({
                    "section_id": section_id,
                    "chunk_id": section_id,  # 保持兼容
                    "heading": section.get("title", ""),
                    "title": section.get("title", ""),  # 使用统一字段名
                    "relevance_score": chunk_info.get("relevance_score", 0),
                    "content": section.get("content", "")
                })
        else:
            # 兼容旧结构：直接从 JSON 的 chunks 中取内容（当前版本不再生成每块的 .md 文件）
            chunks_list = chunks_index.get("chunks", [])
            # 建立多重索引：chunk_id/id/序号/零基序号
            index_map = {}
            for i, ch in enumerate(chunks_list, start=1):
                keys = [
                    ch.get("chunk_id"),
                    ch.get("id"),
                    ch.get("section_id"),
                    str(i),
                    str(i - 1),
                ]
                for k in keys:
                    if k is not None and k != "":
                        index_map[str(k)] = ch
            for chunk_info in relevant_chunks:
                # 优先使用 section_id，兼容旧字段名 chunk_id
                req_id = str(chunk_info.get("section_id", "") or chunk_info.get("chunk_id", "")).strip()
                ch = index_map.get(req_id)

                # 回退1：若ID包含下划线，取后缀再试（兼容类似 sec_0002 → 0002）
                if not ch and "_" in req_id:
                    suffix = req_id.split("_")[-1]
                    ch = index_map.get(suffix)

                # 回退2：零填充数字 → 整数索引（兼容 0002 → "2"；并兼容1基/0基）
                if not ch:
                    num_str = req_id
                    if "_" in num_str:
                        num_str = num_str.split("_")[-1]
                    if num_str.isdigit():
                        try:
                            n = int(num_str)
                            # 先尝试1基
                            ch = index_map.get(str(n)) or ch
                            # 再尝试0基
                            if not ch and n > 0:
                                ch = index_map.get(str(n - 1))
                        except Exception as e:
                            import traceback
                            traceback.print_exc()
                            pass

                if not ch:
                    logger.warning(f"未在chunks索引中找到分块: {req_id}")
                    continue
                # 提取内容与标题
                content = ch.get("content") or ch.get("text") or ""
                title_path = ch.get("title_path") or []
                heading = " / ".join(title_path) if title_path else (chunk_info.get("title", "") or chunk_info.get("heading", ""))
                loaded_chunks.append({
                    "section_id": req_id,
                    "chunk_id": req_id,  # 保持兼容
                    "title": heading,
                    "heading": heading,  # 保持兼容
                    "relevance_score": chunk_info.get("relevance_score", 0),
                    "content": content
                })
        
        return loaded_chunks
    
    def _extract_from_selected_chunks(self,
                                     chunks_content: List[Dict[str, Any]],
                                     extraction_query: str,
                                     task_name: Optional[str] = None,
                                     doc_type: str = "word") -> Dict[str, Any]:
        """
        阶段2：从筛选的分块中提取内容
        
        Args:
            chunks_content: 筛选出的分块内容列表
            extraction_query: 提取需求
            task_name: 任务名称
            doc_type: 文档类型，"word" 或 "pdf"
        
        Returns:
            Dict: 提取结果
        """
        try:
            # 构建分块内容文本
            selected_chunks_text = self._build_selected_chunks_text(chunks_content)
            
            # 根据文档类型选择提示词模板
            template_name = "chunk_based_extraction_pdf" if doc_type == "pdf" else "chunk_based_extraction"
            
            # 构建提示词（不包含task_name）
            variables = {
                "extraction_query": extraction_query,
                "selected_chunks_content": selected_chunks_text,
                "project_desc": os.getenv("CURRENT_PROJECT_DESC", "")
            }
            prompt = system_prompt_manager.build_prompt(template_name, variables)
            
            # 调用模型
            model_output = self.llm.generate_single(prompt)
            
            # ✅ 保存提取阶段完整溯源数据
            from datetime import datetime
            extraction_result = {
                "success": True,
                "stage": "chunk_extraction",
                "timestamp": datetime.now().isoformat(),
                "input_data": {
                    "extraction_query": extraction_query,
                    "task_name": task_name or "内容提取",
                    "doc_type": doc_type,
                    "chunks_count": len(chunks_content)
                },
                "chunks_used": [
                    {
                        "section_id": c.get("section_id") or c.get("chunk_id"),
                        "chunk_id": c.get("section_id") or c.get("chunk_id"),  # 保持兼容
                        "title": c.get("title") or c.get("heading"),
                        "heading": c.get("title") or c.get("heading"),  # 保持兼容
                        "relevance_score": c.get("relevance_score"),
                        "content_length": len(c.get("content", ""))
                    }
                    for c in chunks_content
                ],
                "variables": variables,
                "full_prompt": prompt,
                "prompt_length": len(prompt),
                "model_output": model_output,
                "content": model_output,
                "output_length": len(model_output),
                "extraction_query": extraction_query,
                "source_chunks": selected_chunks_text  # 保存源分块内容
            }
            
            # 可选：保存提取结果到文件
            self._save_extraction_result(extraction_result)
            
            return extraction_result
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"从分块中提取内容失败: {e}", exc_info=True)
            # ✅ 异常时也保存溯源数据
            from datetime import datetime
            import traceback
            error_result = {
                "success": False,
                "stage": "chunk_extraction",
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "error_type": type(e).__name__,
                "traceback": traceback.format_exc(),
                "input_data": {
                    "extraction_query": extraction_query,
                    "task_name": task_name or "内容提取",
                    "doc_type": doc_type,
                    "chunks_count": len(chunks_content) if 'chunks_content' in locals() else 0
                }
            }
            self._save_extraction_result(error_result)
            return error_result
    
    def _build_selected_chunks_text(self, chunks_content: List[Dict[str, Any]]) -> str:
        """构建筛选出的分块内容文本（用于提取）
        
        只输出分块内容，不包含分块ID、标题等元信息
        """
        contents = []
        
        for chunk in chunks_content:
            content = chunk.get("content", "")
            if content:
                contents.append(content)
        
        return "\n\n".join(contents)
    
    def _parse_json_response(self, response: str) -> Optional[Dict[str, Any]]:
        """解析JSON响应"""
        import re
        
        def clean_json_string(s: str) -> str:
            """清理JSON字符串中的非法控制字符"""
            # 移除除了\n, \r, \t之外的控制字符，但保留转义后的合法字符
            # 将制表符、回车符、换行符替换为空格
            s = s.replace('\t', ' ').replace('\r', ' ')
            # 移除其他C0控制字符（0x00-0x1F，除了\n）
            cleaned = ''.join(char if ord(char) >= 0x20 or char == '\n' else ' ' for char in s)
            return cleaned
        
        # 尝试直接解析
        try:
            return json.loads(response)
        except:
            pass
        
        # 方法1: 提取 ```json ... ``` 代码块（贪婪匹配）
        json_match = re.search(r'```json\s*(\{.*\})\s*```', response, re.DOTALL)
        if json_match:
            json_str = clean_json_string(json_match.group(1))
            try:
                return json.loads(json_str)
            except Exception as e:
                import traceback
                traceback.print_exc()
                logger.warning(f"JSON代码块解析失败: {e}")
        
        # 方法2: 提取 ``` ... ``` 代码块（可能没有json标记）
        json_match = re.search(r'```\s*(\{.*\})\s*```', response, re.DOTALL)
        if json_match:
            json_str = clean_json_string(json_match.group(1))
            try:
                return json.loads(json_str)
            except Exception as e:
                import traceback
                traceback.print_exc()
                logger.warning(f"普通代码块解析失败: {e}")
        
        # 方法3: 查找第一个完整的JSON对象（贪婪匹配）
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            json_str = clean_json_string(json_match.group(0))
            try:
                return json.loads(json_str)
            except Exception as e:
                import traceback
                traceback.print_exc()
                logger.warning(f"裸JSON解析失败: {e}")
        
        logger.error("无法解析JSON响应")
        return None
    
    def _get_paragraph_prompts_dir(self) -> Path:
        """获取当前段落的生成基目录：output/generation/single/<paragraph_id>
        注意：下游保存函数会在该基目录下创建 prompts/ outputs/ provenance/ 子目录。
        """
        from pathlib import Path as _Path
        
        # 使用线程安全的方式获取当前会话目录
        output_dir = get_current_output_dir(default="output")
        
        # 尝试从paragraph_id环境变量获取
        from utils.context_manager import get_paragraph_id
        paragraph_id = get_paragraph_id("")
        if not paragraph_id:
            # fallback: 使用默认目录
            paragraph_id = "default"
        
        # 生成阶段：单段落级别目录
        base_dir = _Path(output_dir) / "generation" / "single" / paragraph_id
        base_dir.mkdir(parents=True, exist_ok=True)
        
        return base_dir
    
    def _save_loaded_chunks_data(self, chunks_index: Dict[str, Any], chunks_index_path: str) -> None:
        """保存读取的chunks数据（始终保存）"""
        try:
            import json
            from datetime import datetime
            
            base_dir = self._get_paragraph_prompts_dir()
            provenance_dir = base_dir / "provenance"
            provenance_dir.mkdir(parents=True, exist_ok=True)
            
            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            
            # 构建数据摘要（不保存完整content，只保存统计信息）
            sections = chunks_index.get("sections", chunks_index.get("chunks", []))
            chunks_summary = {
                "timestamp": timestamp,
                "source_file": chunks_index_path,
                "total_sections": len(sections),
                "sections_info": [
                    {
                        "section_id": s.get("section_id", ""),
                        "title": s.get("title", ""),
                        "word_count": s.get("word_count", 0),
                        "content_length": len(s.get("content", "")),
                        "summary": s.get("summary", "")
                    }
                    for s in sections
                ]
            }
            
            # 保存摘要JSON
            json_filename = f"chunks_loaded_{timestamp}.json"
            json_filepath = provenance_dir / json_filename
            with open(json_filepath, 'w', encoding='utf-8') as f:
                json.dump(chunks_summary, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ 读取的chunks数据已保存: {json_filepath.name}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.warning(f"保存chunks数据失败: {e}")
    
    def _save_filtering_result(self, result: Dict[str, Any]) -> None:
        """保存筛选阶段完整溯源数据（始终保存）"""
        try:
            import json
            from datetime import datetime
            
            base_dir = self._get_paragraph_prompts_dir()
            provenance_dir = base_dir / "provenance"
            prompts_dir = base_dir / "prompts"
            outputs_dir = base_dir / "outputs"
            provenance_dir.mkdir(parents=True, exist_ok=True)
            prompts_dir.mkdir(parents=True, exist_ok=True)
            outputs_dir.mkdir(parents=True, exist_ok=True)
            
            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            
            # 1. 保存完整溯源数据JSON
            json_filename = f"filtering_provenance_{timestamp}.json"
            json_filepath = provenance_dir / json_filename
            with open(json_filepath, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            
            # 2. 保存提示词TXT
            prompt_filename = f"filtering_prompt_{timestamp}.txt"
            prompt_filepath = prompts_dir / prompt_filename
            with open(prompt_filepath, 'w', encoding='utf-8') as f:
                f.write(result.get("full_prompt", ""))
            
            # 3. 保存模型输出TXT
            output_filename = f"filtering_output_{timestamp}.txt"
            output_filepath = outputs_dir / output_filename
            with open(output_filepath, 'w', encoding='utf-8') as f:
                f.write(result.get("model_output", ""))
            
            logger.info(f"✅ 筛选阶段溯源数据已保存: {json_filepath.name}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.warning(f"保存筛选溯源数据失败: {e}")
    
    def _save_extraction_result(self, result: Dict[str, Any]) -> None:
        """保存提取阶段完整溯源数据（始终保存）"""
        try:
            import json
            from datetime import datetime
            
            base_dir = self._get_paragraph_prompts_dir()
            provenance_dir = base_dir / "provenance"
            prompts_dir = base_dir / "prompts"
            outputs_dir = base_dir / "outputs"
            provenance_dir.mkdir(parents=True, exist_ok=True)
            prompts_dir.mkdir(parents=True, exist_ok=True)
            outputs_dir.mkdir(parents=True, exist_ok=True)
            
            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            
            # 1. 保存完整溯源数据JSON（限制source_chunks大小）
            save_result = result.copy()
            
            json_filename = f"extraction_provenance_{timestamp}.json"
            json_filepath = provenance_dir / json_filename
            with open(json_filepath, 'w', encoding='utf-8') as f:
                json.dump(save_result, f, ensure_ascii=False, indent=2)
            
            # 2. 保存提示词TXT
            prompt_filename = f"extraction_prompt_{timestamp}.txt"
            prompt_filepath = prompts_dir / prompt_filename
            with open(prompt_filepath, 'w', encoding='utf-8') as f:
                f.write(result.get("full_prompt", ""))
            
            # 3. 保存模型输出TXT
            output_filename = f"extraction_output_{timestamp}.txt"
            output_filepath = outputs_dir / output_filename
            with open(output_filepath, 'w', encoding='utf-8') as f:
                f.write(result.get("model_output", ""))
            
            logger.info(f"✅ 提取阶段溯源数据已保存: {json_filepath.name}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.warning(f"保存提取溯源数据失败: {e}")
    
    def _calculate_avg_score(self, chunks: List[Dict[str, Any]]) -> float:
        """计算平均相关性评分"""
        if not chunks:
            return 0.0
        
        scores = [c.get("relevance_score", 0) for c in chunks]
        return sum(scores) / len(scores) if scores else 0.0


# 创建全局单例
two_stage_extraction_service = TwoStageExtractionService()


__all__ = [
    "TwoStageExtractionService",
    "two_stage_extraction_service"
]
