#!/usr/bin/env python3
"""
完整的CSR内容生成流水线
整合配置解析、数据提取、内容生成三个模块

技术特点：
- 两阶段屏障：先并发提取所有段落，再并发生成所有段落
- 支持并发数限制，避免API限流
- 支持请求间隔控制
"""

import logging
import json
import time
import threading
from typing import Any, Dict, List, Optional
from pathlib import Path
import traceback
import re
from config import get_settings
from service.linux.generation.data_extractor import DataExtractorV2
from utils.config_parser import ConfigParser
from service.linux.generation.paragraph_generation_service import ParagraphGenerationService
from utils.task_logger import get_task_logger
from utils.tag_error_manager import (
    reset_error_manager,
    record_tag_error,
    is_tag_failed
)
from utils.ragflow_client import ragflow_client
from utils.context_manager import inherit_context
logger = logging.getLogger(__name__)


def _task_log(level: str, message: str, **extra):
    """辅助函数：同时记录到标准日志和任务日志"""
    task_logger = get_task_logger()
    if task_logger:
        getattr(task_logger, level)(message, logger_name="pipeline", **extra)
    getattr(logger, level)(message)


class CSRGenerationPipeline:
    """CSR内容生成流水线"""

    def __init__(self,
                 config_path: str = None,  # 段落配置路径（已废弃）
                 base_data_dir: str = "AAA/project_data",
                 cache_dir: str = "cache",
                 use_mock_services: bool = False,
                 model_service=None,
                 project_id:Optional[str] = None,
                 project_name:Optional[str] = None):
        """
        初始化生成流水线

        Args:
            config_path: 配置文件路径
            base_data_dir: 数据目录
            cache_dir: 缓存目录
            use_mock_services: 是否使用模拟服务
            model_service: 模型服务实例
            project_id: 项目ID（用于回调标识）
            project_name: 项目名称（用于RAGFlow知识库查找）
        """
        self.config_path = config_path
        self.use_mock_services = use_mock_services
        self.project_id = project_id
        self.project_name = project_name

        # 阶段回调钩子（由外部设置）
        self.on_extraction_completed = None  # 提取完毕回调
        self.on_generation_started = None  # 开始生成回调

        # 初始化各个模块
        self.config_parser = ConfigParser(config_path)
        self.data_extractor = DataExtractorV2(
            base_data_dir=base_data_dir,
            cache_dir=cache_dir
        )
        self.paragraph_generation_service = ParagraphGenerationService()

        # 解析配置
        self.paragraphs = self.config_parser.parse()
        logger.info(f"成功加载 {len(self.paragraphs)} 个段落配置")


    def generate_all_paragraphs(self) -> List[Dict[str, Any]]:
        """
        生成所有段落内容（两阶段屏障：先并发提取，提取全部完成后再并发生成）
        
        并发控制：
        - 段落级并发数受 max_paragraph_workers 配置限制
        - 每次LLM请求间隔受 llm_request_interval 配置控制
        
        错误处理策略：
        - 每个段落（标签）是独立的处理单元
        - 如果一个标签处理遇到错误：记录错误 → 停止该标签后续处理 → 标记为失败
        - 其他标签继续正常处理
        
        Returns:
            所有段落的生成结果列表
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        results: List[Dict[str, Any]] = []
        extracted_by_id: Dict[str, Dict[str, Any]] = {}

        # 重置错误管理器（新任务开始）
        error_manager = reset_error_manager()

        # 获取并发配置
        settings = get_settings()
        max_paragraph_workers = settings.max_paragraph_workers
        llm_request_interval = settings.llm_request_interval

        logger.info(f"📌 并发配置: 段落并发数={max_paragraph_workers}, 请求间隔={llm_request_interval}s")

        # 阶段A：并发提取全部段落
        def _extract_para(para) -> Dict[str, Any]:
            """
            内部函数：提取单个段落的数据
            
            【执行步骤】
            1. 遍历段落中的每个数据项（para.data）
            2. 构建data_item字典，包含关键字段：
               - extract: 提取提示词
               - datas: 文件名列表
               - original_mode: 是否原文模式
               - quote: 引用标签（用于在生成内容前添加标识）
            3. 调用enrichment方法，从预处理结果中补充元数据（file_type, chunks_file等）
            4. 将enriched数据合并回data_item（使用update，确保不覆盖quote等原始字段）
            5. 调用DataExtractor执行数据提取
            
            【错误处理】
            - 如果提取过程中遇到错误，记录错误并标记标签失败
            - 失败的标签不会继续后续的生成阶段
            
            【关键】确保quote字段在enrichment前就存在于data_item中，
            这样后续的enrichment逻辑会保留该字段，最终传递给DataExtractor
            """
            paragraph_id = para.id

            # 开始处理标签
            error_manager.start_tag(paragraph_id)
            error_manager.set_stage(paragraph_id, "extraction")

            try:
                # 构建data items并从preprocessed.json中enrichment元数据
                enriched_data = []
                for item in para.data:
                    # ✅ 关键：在enrichment前就包含所有溯源字段
                    data_item = {
                        "extract": item.extract,
                        "datas": item.datas,
                        "original_mode": item.original_mode,
                        "insert_original": item.insert_original,
                        "quote": getattr(item, "quote", None),  # 从DataItem对象读取quote属性
                        "file_name": item.file_name,
                        "item_id": item.item_id,                # 资料编号
                        "directory": item.directory,            # 来源目录
                    }
                    # 1. 先获取 dataset_id，判断知识库是否存在
                    dataset_id = None
                    file_names_for_rag = item.file_name or []  # ✅ 安全处理 None
                    if self.project_name:
                        dataset_id = ragflow_client.get_dataset_id(self.project_name)
                        if dataset_id:
                            logger.info(f"📌 找到知识库 ID: {dataset_id}")
                        else:
                            logger.info(f"📌 未找到知识库: {self.project_name}")

                    # 2. 批量获取 document_ids
                    rag_file_ids = []
                    if dataset_id and file_names_for_rag:
                        logger.info(f"📌 批量获取 document_ids...")
                        logger.info(f"   - file_name: {file_names_for_rag}")
                        logger.info(f"   - project_name: {self.project_name}")
                        rag_file_ids = ragflow_client.get_document_ids_batch(file_names_for_rag, self.project_name)

                    # 3. 保存文件名到 document_id 的映射，供后续 per-file 检索使用
                    if rag_file_ids:
                        # 构建 file_name -> document_id 映射
                        file_to_doc_id = {}
                        for idx, fn in enumerate(file_names_for_rag):
                            if idx < len(rag_file_ids):
                                file_to_doc_id[fn] = rag_file_ids[idx]

                        data_item["rag_file_to_doc_id"] = file_to_doc_id
                        data_item["rag_question"] = item.extract
                        data_item["rag_dataset_id"] = dataset_id
                        logger.info(f"📌 设置 RAGFlow 映射: {file_to_doc_id}")

                    # 建立预处理文件 ID(datas) -> 原始文件名(file_name) 的映射
                    # 用于在 _build_doc_structure 中查找 RAGFlow document_id
                    if item.datas:
                        preprocessed_to_original = {}
                        for idx, data_entry in enumerate(item.datas):
                            pp_id = None
                            original_name = None
                            if isinstance(data_entry, dict):
                                # 格式1：datas 数组元素是字典，包含 fileId 和 fileName
                                pp_id = data_entry.get("fileId") or data_entry.get("file", "")
                                original_name = data_entry.get("fileName") or data_entry.get("file_name", "")
                            else:
                                # 格式2：datas 是字符串（fileId），从 item.file_name 获取对应文件名
                                pp_id = data_entry
                                if item.file_name and idx < len(item.file_name):
                                    original_name = item.file_name[idx]

                            if pp_id and original_name:
                                preprocessed_to_original[pp_id] = original_name

                        if preprocessed_to_original:
                            data_item["preprocessed_to_original"] = preprocessed_to_original
                            logger.info(f"📌 设置预处理ID映射: {preprocessed_to_original}")

                    if self.project_id:
                        data_item["project_id"] = self.project_id


                    # Enrichment: 从preprocessed.json读取file_type, chunks_file等
                    # enrichment会保留data_item中已有的quote字段
                    try:
                        enriched = self._enrich_data_item_from_preprocessing(data_item)
                        if enriched:
                            data_item.update(enriched)  # 合并enriched数据，不覆盖已有的quote
                    except Exception as e:
                        traceback.print_exc()
                        logger.warning(f"Enrichment失败: {e}")

                    enriched_data.append(data_item)

                paragraph_dict = {
                    "id": para.id,
                    "data": enriched_data,
                    "generate": para.generate,
                    "example": para.example,
                    "insert_original": para.insert_original
                }

                # 执行数据提取
                result = self.data_extractor.extract_data_for_paragraph(paragraph_dict)

                # 检查提取结果中是否有错误项
                extracted_items = result.get("extracted_items", [])
                error_items = [item for item in extracted_items if item.get("status") == "error"]

                if error_items:
                    # 有数据项提取失败，记录错误并标记标签失败
                    first_error = error_items[0]
                    error_msg = first_error.get("error", "数据提取失败")
                    actual_error_type = first_error.get("error_type", "DATA_EXTRACT_ERROR")
                    stack_trace = first_error.get("stack_trace", "")
                    record_tag_error(
                        tag_id=paragraph_id,
                        stage="extraction",
                        error_type=actual_error_type,
                        message=f"数据提取失败: {error_msg}",
                        context={
                            "failed_items": len(error_items),
                            "total_items": len(extracted_items),
                            "first_error": first_error
                        },
                        exception=stack_trace
                    )
                    logger.error(
                        f"❌ 段落 {paragraph_id} 提取失败: {len(error_items)}/{len(extracted_items)} 个数据项失败")
                    # 仍然返回结果，但标签已被标记为失败
                    result["tag_failed"] = True
                    result["error_message"] = error_msg

                return result

            except Exception as e:
                traceback.print_exc()
                stack_trace = traceback.format_exc()
                logger.warning(stack_trace)
                # 提取过程中遇到异常，记录错误并标记标签失败
                record_tag_error(
                    tag_id=paragraph_id,
                    stage="extraction",
                    error_type="EXTRACTION_EXCEPTION",
                    message=str(e),
                    exception=e,
                    context={"paragraph_id": paragraph_id}
                )
                logger.error(f"❌ 段落 {paragraph_id} 提取异常: {e}", exc_info=True)

                # 返回错误结果
                return {
                    "paragraph_id": paragraph_id,
                    "generate_prompt": para.generate,
                    "example": para.example,
                    "extracted_items": [
                        {"status": "error", "error": str(e), "content": None}
                    ],
                    "tag_failed": True,
                    "error_message": str(e)
                }

        # 请求间隔控制（阶段A）
        _extract_request_lock = threading.Lock()
        _extract_last_request_time = [0.0]

        # 限制并发数
        actual_extract_workers = min(max_paragraph_workers, len(self.paragraphs)) if self.paragraphs else 0

        if actual_extract_workers <= 1:
            for para in self.paragraphs:
                logger.info(f"并发阶段A(串行回退) 提取段落: {para.id}")
                extracted = _extract_para(para)
                extracted_by_id[para.id] = extracted
        else:
            def _extract_para_with_lock(para):
                """包装函数：请求间隔控制后执行提取（上下文由 inherit_context 继承）"""
                # 请求间隔控制
                with _extract_request_lock:
                    elapsed = time.time() - _extract_last_request_time[0]
                    if elapsed < llm_request_interval:
                        time.sleep(llm_request_interval - elapsed)
                    _extract_last_request_time[0] = time.time()

                return _extract_para(para)

            logger.info(f"📂 阶段A并发提取: {len(self.paragraphs)}个段落, 并发数: {actual_extract_workers}")

            with ThreadPoolExecutor(max_workers=actual_extract_workers) as executor:
                future_to_para = {
                    executor.submit(inherit_context(_extract_para_with_lock), p): p
                    for p in self.paragraphs
                }
                for future in as_completed(future_to_para):
                    para = future_to_para[future]
                    try:
                        extracted = future.result()
                    except Exception as e:
                        traceback.print_exc()
                        stack_trace = traceback.format_exc()
                        logger.warning(stack_trace)
                        # ✅ 修复：并发执行异常时也要标记标签失败
                        record_tag_error(
                            tag_id=para.id,
                            stage="extraction",
                            error_type="THREAD_EXCEPTION",
                            message=f"提取线程异常: {str(e)}",
                            exception=e,
                            context={"paragraph_id": para.id}
                        )
                        logger.error(f"❌ 段落 {para.id} 提取线程异常: {e}", exc_info=True)
                        extracted = {
                            "paragraph_id": para.id,
                            "generate_prompt": para.generate,
                            "example": para.example,
                            "extracted_items": [
                                {"status": "error", "error": str(e), "content": None}
                            ],
                            "tag_failed": True,
                            "error_message": str(e)
                        }
                    extracted_by_id[para.id] = extracted

        # ===== 阶段A完成：触发"提取完毕"回调 =====
        logger.info(f"✓ 阶段A完成：全部 {len(extracted_by_id)} 个段落提取完毕")
        if self.on_extraction_completed:
            try:
                self.on_extraction_completed()
            except Exception as e:
                traceback.print_exc()
                logger.warning(f"提取完毕回调执行失败: {e}")

        # ===== 阶段B开始：触发"开始生成"回调 =====
        logger.info(f"✓ 阶段B开始：开始生成全部段落")
        if self.on_generation_started:
            try:
                self.on_generation_started()
            except Exception as e:
                traceback.print_exc()
                logger.warning(f"开始生成回调执行失败: {e}")

        # 阶段B：全部提取完成后，并发生成全部段落
        def _generate_para(para) -> Dict[str, Any]:
            paragraph_id = para.id

            # ✅ 关键检查：如果标签在提取阶段已失败，跳过生成阶段
            if is_tag_failed(paragraph_id):
                logger.warning(f"⏭️ 跳过段落 {paragraph_id} 的生成阶段（提取阶段已失败）")
                extracted_data = extracted_by_id.get(paragraph_id, {})
                return {
                    "paragraph_id": paragraph_id,
                    "status": "error",
                    "error_message": extracted_data.get("error_message", "提取阶段失败"),
                    "generated_content": "",
                    "extracted_data": extracted_data,
                    "skipped_generation": True,
                    "skip_reason": "extraction_failed"
                }

            # 设置当前阶段为生成
            error_manager.set_stage(paragraph_id, "generation")

            extracted_data = extracted_by_id.get(paragraph_id) or {
                "paragraph_id": paragraph_id,
                "generate_prompt": para.generate,
                "example": para.example,
                "insert_original": para.insert_original,
                "extracted_items": []
            }

            # 仅依据 generate 是否为空判断是否跳过
            if not (para.generate or "").strip() and not para.is_table:
                parts: List[str] = []
                tfl_placeholders: List[str] = []  # 收集TFL占位符（RTF/Excel原文模式）
                placeholders_to_insert: List[str] = []  # 收集要插入的占位符

                for item in extracted_data.get("extracted_items", []):
                    if item.get("status") == "success":
                        content = item.get("content", "")

                        # ✅ 处理内容（Word/PDF的content已包含{{Table_1_Start}}等占位符）
                        if content:
                            # 清理可能的调试信息（如 ## Source: xxx）
                            cleaned_content = content
                            # 移除 "## Source: xxx" 行
                            cleaned_content = re.sub(r'^##\s*Source:.*$', '', cleaned_content, flags=re.MULTILINE)
                            # 新增：无 generate 时直接展示给用户，去掉 Excel Sheet 标头
                            # 去除 "### Sheet: xxx" 行
                            cleaned_content = re.sub(r'^###\s*Sheet:\s*.+\n?', '', cleaned_content, flags=re.MULTILINE)
                            # 去除多 Sheet 之间的分隔线 "---"
                            cleaned_content = re.sub(r'^\s*---\s*\n?', '', cleaned_content, flags=re.MULTILINE)
                            # 去除 "## 来源: xxx" 行
                            cleaned_content = re.sub(r'^##\s*来源:\s*.+\n?', '', cleaned_content, flags=re.MULTILINE)
                            # 收拢多余空行
                            cleaned_content = re.sub(r'\n{3,}', '\n\n', cleaned_content)
                            # ✅ 清理模型可能幻觉出的无效占位符（如 {{ORIGINAL_CONTENT:...}}）
                            cleaned_content = re.sub(r'\{\{ORIGINAL_CONTENT:[^}]*\}\}', '', cleaned_content)
                            cleaned_content = cleaned_content.strip()
                            if cleaned_content:
                                parts.append(cleaned_content)
                        # ✅ 收集该数据项的占位符（如果该项需要插入图表）
                        if item.get("insert_original") and item.get("placeholders"):
                            placeholders_to_insert.extend(item.get("placeholders", []))

                        # ✅ 处理 TFL 占位符映射（RTF/Excel 插入图表）
                        tfl_mappings = item.get("tfl_insert_mappings", [])
                        if tfl_mappings:
                            for mapping in tfl_mappings:
                                tfl_placeholder = mapping.get("Placeholder", "")
                                if tfl_placeholder:
                                    tfl_placeholders.append(tfl_placeholder)

                # 去重占位符
                placeholders_to_insert = list(set(placeholders_to_insert))

                # 组合内容：提取的内容 + TFL占位符
                all_parts = parts[:]
                # 插入图表模式
                if para.insert_original:
                    if tfl_placeholders:
                        # TFL占位符
                        all_parts.extend(tfl_placeholders)
                        logger.info(f"✅ 已附加{len(tfl_placeholders)}个TFL占位符到段落末尾: {tfl_placeholders}")
                    # word 可以提取到表格占位符，不需要手动添加
                    # if placeholders_to_insert:
                    #     # 图表占位符
                    #     all_parts.extend(placeholders_to_insert)
                    #     logger.info(f"✅ 已附加{len(placeholders_to_insert)}个图表占位符到段落末尾: {placeholders_to_insert}")


                generated_content = "\n\n".join(all_parts).strip()
                # 去掉MD格式
                generated_content = self._remove_md_formatting(generated_content)
                if not para.insert_original:
                    # word在提取的时候文本中可能包含了占位符，非图表引用模式要移除掉文件中原有的占位符{{}}
                    generated_content = re.sub(r"\{\{(Table|Image)_\d+_start\}\}", "", generated_content)

                # 标记标签成功（跳过生成也算成功）
                error_manager.mark_success(paragraph_id)

                return {
                    "paragraph_id": paragraph_id,
                    "status": "success",
                    "generated_content": generated_content,
                    "extracted_data": extracted_data,
                    "generation_input": {
                        "generate_prompt": para.generate,
                        "example": para.example
                    },
                    "skipped_generation": True,
                    "placeholders_used": placeholders_to_insert, # 添加使用的占位符信息
                    # ✅ 溯源信息（与单段落生成保持一致）
                    "extraction_traceability": extracted_data.get('traceability', {}),
                    "generation_traceability": {}
                }
            else:
                generation_result = {}  # 初始化，避免未定义
                try:
                    if self.use_mock_services:
                        generated_content = f"[模拟生成] 段落 {paragraph_id} 的内容\n根据提取的数据和生成要求生成的内容..."
                    else:
                        generation_result = self.paragraph_generation_service.generate_paragraph(
                            generate_prompt=para.generate,
                            extracted_data=extracted_data,
                            insert_original=para.insert_original,
                            example=para.example,
                            paragraph_id=para.id,
                            is_table=para.is_table,
                            html=para.html
                        )

                        # 检查生成结果是否成功
                        if not generation_result.get('success', True):
                            stack_trace = traceback.format_exc()
                            logger.warning(stack_trace)
                            error_msg = generation_result.get('error', '生成失败')
                            record_tag_error(
                                tag_id=paragraph_id,
                                stage="generation",
                                error_type="GENERATION_ERROR",
                                message=error_msg,
                                context={"generation_result": generation_result}
                            )
                            return {
                                "paragraph_id": paragraph_id,
                                "status": "error",
                                "error_message": error_msg,
                                "generated_content": "",
                                "extracted_data": extracted_data
                            }

                        generated_content = generation_result.get('generated_content', '')
                except Exception as e:
                    traceback.print_exc()
                    # 生成过程中遇到异常，记录错误并标记标签失败
                    stack_trace = traceback.format_exc()
                    logger.warning(stack_trace)
                    record_tag_error(
                        tag_id=paragraph_id,
                        stage="generation",
                        error_type="GENERATION_EXCEPTION",
                        message=str(e),
                        exception=stack_trace,
                        context={"paragraph_id": paragraph_id}
                    )
                    logger.error(f"❌ 段落 {paragraph_id} 生成异常: {e}", exc_info=True)
                    return {
                        "paragraph_id": paragraph_id,
                        "status": "error",
                        "error_message": str(e),
                        "generated_content": "",
                        "extracted_data": extracted_data
                    }

                # 标记标签成功
                error_manager.mark_success(paragraph_id)

                return {
                    "paragraph_id": paragraph_id,
                    "status": "success",
                    "generated_content": generated_content,
                    "extracted_data": extracted_data,
                    "generation_input": {
                        "generate_prompt": para.generate,
                        "example": para.example
                    },
                    "skipped_generation": False,
                    "placeholders_used": extracted_data.get("all_placeholders", []),    # 添加使用的占位符信息
                    # ✅ 溯源信息（与单段落生成保持一致）
                    "extraction_traceability": extracted_data.get('traceability', {}),
                    "generation_traceability": generation_result.get('traceability', {})
                }

        # 请求间隔控制（阶段B）
        _generate_request_lock = threading.Lock()
        _generate_last_request_time = [0.0]

        # 限制并发数
        actual_generate_workers = min(max_paragraph_workers, len(self.paragraphs)) if self.paragraphs else 0

        if actual_generate_workers <= 1:
            for para in self.paragraphs:
                res = _generate_para(para)
                results.append(res)
        else:
            def _generate_para_with_lock(para):
                """包装函数：请求间隔控制后执行生成（上下文由 inherit_context 继承）"""
                # 请求间隔控制
                with _generate_request_lock:
                    elapsed = time.time() - _generate_last_request_time[0]
                    if elapsed < llm_request_interval:
                        time.sleep(llm_request_interval - elapsed)
                    _generate_last_request_time[0] = time.time()

                return _generate_para(para)

            logger.info(f"📝 阶段B并发生成: {len(self.paragraphs)}个段落, 并发数: {actual_generate_workers}")

            with ThreadPoolExecutor(max_workers=actual_generate_workers) as executor:
                future_to_para = {
                    executor.submit(inherit_context(_generate_para_with_lock), p): p
                    for p in self.paragraphs
                }
                for future in as_completed(future_to_para):
                    para = future_to_para[future]
                    try:
                        res = future.result()
                    except Exception as e:
                        traceback.print_exc()
                        stack_trace = traceback.format_exc()
                        logger.warning(stack_trace)
                        # ✅ 修复：并发执行异常时也要标记标签失败
                        record_tag_error(
                            tag_id=para.id,
                            stage="generation",
                            error_type="THREAD_EXCEPTION",
                            message=f"生成线程异常: {str(e)}",
                            exception=e,
                            context={"paragraph_id": para.id}
                        )
                        logger.error(f"❌ 段落 {para.id} 生成线程异常: {e}", exc_info=True)
                        res = {
                            "paragraph_id": para.id,
                            "status": "error",
                            "error_message": str(e),
                            "generated_content": "",
                            "extracted_data": extracted_by_id.get(para.id, {})
                        }
                    results.append(res)


        # 输出错误摘要
        error_summary = error_manager.get_summary()
        if error_summary["failed"] > 0:
            logger.warning(
                f"⚠️ 处理完成，但有 {error_summary['failed']}/{error_summary['total']} 个标签失败:\n"
                f"   失败标签: {error_summary['failed_tags']}"
            )
        else:
            logger.info(f"✅ 处理完成，{error_summary['success']}/{error_summary['total']} 个标签成功")

        return results


    def get_paragraph_list(self) -> List[Dict[str, Any]]:
        """
        获取所有段落的列表信息
        
        Returns:
            段落信息列表
        """
        return [
            {
                "id": para.id,
                "generate": para.generate,
                "example": para.example or "",
                "data": [
                    {
                        "extract": item.extract,
                        "datas": item.datas,
                        "insert_original": item.insert_original
                    }
                    for item in para.data
                ],
                "data_count": len(para.data)
            }
            for para in self.paragraphs
        ]

    def _enrich_data_item_from_preprocessing(self, data_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        从预处理的preprocessed.json中读取元数据来enrichment data_item
        
        【核心功能】
        根据datas字段中的文件名查找对应的预处理结果，补充file_type、chunks_file等元数据
        
        【关键字段保留】
        在enrichment过程中，会保留原始data_item的关键字段（extract, original_mode, quote），
        确保这些字段不会在enrichment后丢失
        
        【数据流】
        1. 从data_item.datas提取文件名列表
        2. 根据文件名查找预处理结果（优先使用索引，fallback到目录遍历）
        3. 读取preprocessed.json和chunks_structured.json
        4. 合并元数据并保留原始字段
        
        Args:
            data_item: 原始数据项（必须包含datas字段，可选包含extract、original_mode、quote）
            
        Returns:
            包含file_type, chunks_file, source_file等字段的字典，如果找不到则返回None
            返回的字典会保留原始data_item中的extract、original_mode、quote字段
        """
        # 使用datas字段指定文件列表
        datas = data_item.get("datas", [])
        if not datas:
            logger.warning("数据项缺少datas字段，无法enrichment")
            return None

        # 从datas中提取文件名
        file_names = []
        for data_obj in datas:
            if isinstance(data_obj, dict):
                file_name = data_obj.get("file", "")
                if file_name:
                    file_names.append(file_name)
            elif isinstance(data_obj, str):
                # 兼容直接传字符串的情况
                file_names.append(data_obj)

        if not file_names:
            logger.warning("datas中没有找到有效的文件名")
            return None

        # 根据文件名查找预处理结果
        return self._enrich_by_files(data_item, file_names)

    def _enrich_by_files(self, data_item: Dict[str, Any], file_names: List[str]) -> Optional[Dict[str, Any]]:
        """
        根据文件名列表查找预处理结果，合并多个文件的数据
        
        【核心功能】
        - 根据文件名列表在预处理目录中查找对应的preprocessed.json
        - 支持多文件合并（合并chunks_file_list和available_resources）
        - 保留原始data_item的关键字段（extract, original_mode, quote）
        
        【匹配策略】
        1. 优先使用preprocessing_index.json索引快速查找
        2. Fallback: 遍历AAA/Preprocessing目录，使用多种匹配方式：
           - 文件名完全匹配
           - 文件名包含匹配
           - 父目录名匹配（去扩展名）
        
        【返回结构】
        {
            "file_type": "word",  # 文件类型
            "chunks_file_list": [...],  # 分块文件列表
            "available_resources": [...],  # 可用资源（表格/图片）
            "source_file": "xxx.docx",  # 源文件名
            "extract": "...",  # 保留自原始data_item
            "original_mode": True/False,  # 保留自原始data_item
            "quote": "引用标签"  # 保留自原始data_item（如果存在）
        }
        
        Args:
            data_item: 原始数据项（包含extract、original_mode、quote等字段）
            file_names: 要查找的文件名列表
            
        Returns:
            enriched数据字典，如果找不到任何文件则返回None
        """
        logger.info(f"🔍 enrichment by files: {file_names}")
        logger.info(f"🔍 原始data_item包含字段: {list(data_item.keys())}")

        # 方式0：优先使用 file_mappings.json 快速查找
        matched_files = []
        found_file_names = set()  # ✅ 记录已找到的文件名，避免重复
        try:
            from service.windows.preprocessing.preprocessing_function.file_mappings_manager import (
                get_preprocessed_json_path as _get_pp_from_map,
                find_file_mapping as _find_map,
            )
            for file_name in file_names:
                mapping_path = None
                try:
                    mapping_path = _get_pp_from_map(file_name)
                except Exception:
                    mapping_path = None
                if not mapping_path:
                    try:
                        _info = _find_map(file_name)
                        if _info and _info.get("status") == "success":
                            mapping_path = _info.get("preprocessed_json")
                    except Exception:
                        pass
                if not mapping_path:
                    continue
                # 归一化为本机可访问路径（若给的是Windows绝对路径，则基于 AAA 相对化）
                p_str = str(mapping_path).replace("\\", "/")
                local_pp = Path(p_str)
                if not local_pp.exists():
                    try:
                        low = p_str.lower()
                        idx = low.find("aaa/")
                        if idx != -1:
                            rel = p_str[idx + 4:].lstrip("/")
                            local_pp = Path("AAA") / Path(rel)
                    except Exception:
                        pass
                if local_pp.exists():
                    try:
                        with open(local_pp, 'r', encoding='utf-8') as f:
                            pp_data = json.load(f)
                        logger.info(f"✓ 从file_mappings找到: {file_name} -> {local_pp.parent.name}")
                        matched_files.append((file_name, local_pp, pp_data))
                        found_file_names.add(file_name)  # ✅ 记录已找到
                    except Exception as _e:
                        traceback.print_exc()
                        logger.debug(f"读取preprocessed.json失败(file_mappings): {_e}")
        except Exception as e:
            traceback.print_exc()
            logger.debug(f"读取file_mappings失败: {e}")

        # 方式1：使用索引文件快速查找（若0未命中或需补充）
        try:
            from service.windows.preprocessing.preprocessing_function.preprocessing_index import \
                find_preprocessing_result

            for file_name in file_names:
                # ✅ 跳过已在方式0找到的文件
                if file_name in found_file_names:
                    logger.debug(f"跳过已找到的文件: {file_name}")
                    continue

                file_info = find_preprocessing_result(file_name)
                if file_info:
                    # 从索引获取preprocessed.json路径
                    preprocessed_json_path = file_info.get('preprocessed_json')
                    if preprocessed_json_path:
                        preprocessed_file = Path(preprocessed_json_path)
                        if preprocessed_file.exists():
                            try:
                                with open(preprocessed_file, 'r', encoding='utf-8') as f:
                                    pp_data = json.load(f)

                                logger.info(f"✓ 从索引找到: {file_name} -> {preprocessed_file.parent.name}")
                                matched_files.append((file_name, preprocessed_file, pp_data))
                                continue
                            except Exception as e:
                                traceback.print_exc()
                                logger.warning(f"读取preprocessed.json失败: {e}")

                # 如果索引查找失败，fallback到遍历目录
                logger.info(f"⚠️ 索引未找到 {file_name}，尝试遍历目录")
                # 使用AAA/Preprocessing作为fallback目录（预处理结果通常在这里）
                file_count = 0
                for preprocessed_file in Path('AAA/Preprocessing').rglob('preprocessed.json'):
                    file_count += 1
                    try:
                        with open(preprocessed_file, 'r', encoding='utf-8') as f:
                            pp_data = json.load(f)

                        source_file = pp_data.get('source_file', '')
                        source_file_name = Path(source_file).name

                        # 多种匹配方式
                        # 1. 完全匹配文件名
                        # 2. 文件名包含在source_file中
                        # 3. preprocessed_file的父目录名匹配（去掉.docx后缀）
                        parent_dir_name = preprocessed_file.parent.name
                        file_name_stem = Path(file_name).stem  # 去掉扩展名

                        is_match = (
                                source_file_name == file_name or
                                file_name in source_file or
                                parent_dir_name == file_name_stem or
                                parent_dir_name == file_name
                        )

                        # 详细日志
                        if parent_dir_name == file_name_stem or parent_dir_name == file_name:
                            logger.info(
                                f">>> 匹配检查: parent='{parent_dir_name[:30]}' vs stem='{file_name_stem[:30]}'  result={is_match}")

                        if is_match:
                            logger.info(f"✓ 遍历找到: {file_name} -> {preprocessed_file.parent.name}")
                            matched_files.append((file_name, preprocessed_file, pp_data))
                            break

                    except Exception as e:
                        traceback.print_exc()
                        logger.warning(f"读取preprocessed.json失败: {e}")
                        continue

                logger.info(f"遍历完成，共检查了{file_count}个preprocessed.json文件，未找到匹配")

        except ImportError:
            # 如果索引模块不可用，fallback到遍历目录
            logger.warning("索引模块不可用，使用遍历目录方式")
            for file_name in file_names:
                for preprocessed_file in Path('AAA/Preprocessing').rglob('preprocessed.json'):
                    try:
                        with open(preprocessed_file, 'r', encoding='utf-8') as f:
                            pp_data = json.load(f)

                        source_file = pp_data.get('source_file', '')

                        if Path(source_file).name == file_name or file_name in source_file:
                            logger.info(f"✓ 找到匹配: {file_name} -> {preprocessed_file.parent.name}")
                            matched_files.append((file_name, preprocessed_file, pp_data))
                            break

                    except Exception as e:
                        traceback.print_exc()
                        logger.debug(f"读取preprocessed.json失败: {e}")
                        continue

        if not matched_files:
            logger.warning(f"❌ 未找到匹配的预处理文件: {file_names}")
            return None

        # 分类：Word/PDF vs Excel/RTF
        doc_files = []  # Word/PDF
        table_files = []  # Excel/RTF

        for file_name, preprocessed_file, pp_data in matched_files:
            file_type = pp_data.get('file_type', '').lower()

            if file_type in ['word', 'doc', 'docx', 'pdf']:
                doc_files.append((file_name, preprocessed_file, pp_data))
            elif file_type in ['excel', 'xlsx', 'rtf']:
                table_files.append((file_name, preprocessed_file, pp_data))

        # 根据文件类型构建不同的结构
        if doc_files:
            return self._build_doc_structure(doc_files, Path('AAA/Preprocessing'), original_data_item=data_item)
        elif table_files:
            return self._build_table_structure(table_files, Path('AAA/Preprocessing'), original_data_item=data_item)
        else:
            return None

    def _build_doc_structure(self, doc_files: List, base_dir: Path, original_data_item: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        构建Word/PDF文件的数据结构
        
        返回chunks_file数组，data_extractor会处理合并逻辑
        """
        chunks_file_list = []
        all_regions = []
        source_files = []
        available_resources = []  # ✅ 新增：可用的资源（表格/图片）
        per_file_ragflow_list = []  # ✅ 新增：收集每个文件的 RAGFlow 内容

        for file_name, preprocessed_file, pp_data in doc_files:
            # 收集chunks_file
            structured_chunks = pp_data.get('processing_info', {}).get('structured_chunks_file')
            logger.info(f"📄 处理文件: {file_name}")
            logger.info(f"  structured_chunks_file: {structured_chunks}")

            if structured_chunks:
                # structured_chunks_file通常已经是完整路径（绝对或相对于项目根目录）
                s = str(structured_chunks)
                s_norm = s.replace('\\', '/')
                candidate_aaa = None
                try:
                    low = s_norm.lower()
                    idx = low.find('aaa/')
                    if idx != -1:
                        rel = s_norm[idx + 4:].lstrip('/')
                        rel = rel.replace('\\', '/')
                        candidate_aaa = (Path('AAA') / Path(rel))
                except Exception:
                    candidate_aaa = None
                chunks_path = Path(structured_chunks)
                if candidate_aaa and candidate_aaa.exists():
                    chunks_path = candidate_aaa
                logger.info(f"  初始路径: {chunks_path}")
                logger.info(f"  路径存在: {chunks_path.exists()}")

                # 优先选择结构化sections文件（*_chunks_structured.json）
                try:
                    if chunks_path.name.endswith('_chunks.json'):
                        alt = chunks_path.with_name(chunks_path.name.replace('_chunks.json', '_chunks_structured.json'))
                        if alt.exists():
                            logger.info(f"  ✓ 发现结构化分块文件，优先使用: {alt}")
                            chunks_path = alt
                except Exception:
                    pass

                # 如果路径不存在，说明可能是相对于preprocessed_file的
                if not chunks_path.exists():
                    # 尝试相对于preprocessed_file.parent
                    chunks_path = preprocessed_file.parent / structured_chunks
                    logger.info(f"  尝试拼接路径: {chunks_path}")
                    logger.info(f"  拼接后存在: {chunks_path.exists()}")
                    # 再次尝试结构化版本
                    try:
                        if chunks_path.name.endswith('_chunks.json'):
                            alt = chunks_path.with_name(
                                chunks_path.name.replace('_chunks.json', '_chunks_structured.json'))
                            if alt.exists():
                                logger.info(f"  ✓ 发现结构化分块文件(相对路径)，优先使用: {alt}")
                                chunks_path = alt
                    except Exception:
                        pass

                if chunks_path.exists():
                    try:
                        aaa_base = Path.cwd() / 'AAA'
                        rel = chunks_path.absolute().resolve().relative_to(aaa_base)
                        norm_path = str(Path('AAA') / rel)
                    except Exception:
                        norm_path = str(chunks_path.absolute())
                    # 改为字典结构，携带 fileId 和原始文件名信息
                    file_id = preprocessed_file.parent.name  # 预处理目录名就是 fileId
                    # 使用 preprocessed_to_original 映射将 fileId 转换为原始文件名
                    preprocessed_to_original = original_data_item.get("preprocessed_to_original", {}) if original_data_item else {}
                    original_file_name = preprocessed_to_original.get(file_name, file_name)  # file_name 可能是 fileId，需要转换
                    chunks_file_list.append({
                        "chunks_path": norm_path,
                        "file_id": file_id,
                        "original_file_name": original_file_name  # 真正的原始文件名
                    })
                    source_files.append(pp_data.get('source_file', original_file_name))
                    logger.info(f"  ✓ 添加chunks文件: {norm_path}, fileId: {file_id}, original_file_name: {original_file_name}")
                else:
                    logger.warning(f"  ❌ chunks文件不存在: {chunks_path}")
            else:
                logger.warning(f"  ⚠️ processing_info中没有structured_chunks_file字段")

            # ✅ 提取regions信息（表格/图片的Label和路径）
            regions = pp_data.get('regions', [])
            current_source_file = pp_data.get('source_file', file_name)

            for region in regions:
                label = region.get('Label', '')
                # 只处理表格和图片（排除Sheet等其他类型）
                if label.startswith('Table_') or label.startswith('Image_'):
                    # ✅ 统一Label格式：确保带_Start后缀（用于占位符匹配）
                    if not label.endswith('_Start') and not label.endswith('_End'):
                        label = f"{label}_Start"

                    region_path = region.get('path', '')

                    # 构建完整路径
                    if region_path:
                        # 统一分隔符，优先处理包含AAA的绝对/半绝对路径，避免重复拼接
                        rp_str = str(region_path)
                        rp_norm = rp_str.replace('\\', '/')
                        full_path = None

                        # 优先：若包含 AAA/ 前缀（忽略大小写），直接相对到本机 AAA 根
                        try:
                            low = rp_norm.lower()
                            idx = low.find('aaa/')
                            if idx != -1:
                                rel = rp_norm[idx + 4:].lstrip('/\\')
                                candidate = Path('AAA') / Path(rel)
                                if candidate.exists():
                                    full_path = candidate
                        except Exception as e:
                            traceback.print_exc()
                            pass

                        # 方式1: POSIX绝对路径
                        if not full_path:
                            if Path(rp_norm).is_absolute():
                                candidate = Path(rp_norm)
                                if candidate.exists():
                                    full_path = candidate

                        # 方式1.1: 字面字符串存在（处理工作目录下相对路径）
                        if not full_path:
                            candidate = Path(rp_norm)
                            if candidate.exists():
                                full_path = candidate

                        # 方式2: 相对于preprocessed_file.parent（注意此时使用规范化后的rp_norm，避免重复AAA前缀）
                        if not full_path:
                            candidate = preprocessed_file.parent / rp_norm
                            if candidate.exists():
                                full_path = candidate

                        # 方式3: 回退到父目录的regions_word（仅按文件名查找）
                        if not full_path:
                            resource_filename = Path(rp_norm).name
                            candidate = preprocessed_file.parent.parent / 'regions_word' / resource_filename
                            if candidate.exists():
                                full_path = candidate
                                try:
                                    logger.info(f"  ✓ 使用回退路径: {candidate.relative_to(Path.cwd())}")
                                except Exception as e:
                                    traceback.print_exc()
                                    logger.info(f"  ✓ 使用回退路径: {candidate}")

                        if full_path:
                            try:
                                aaa_base = Path.cwd() / 'AAA'
                                rel = full_path.absolute().resolve().relative_to(aaa_base)
                                res_path = str(Path('AAA') / rel)
                            except Exception as e:
                                traceback.print_exc()
                                res_path = str(full_path.absolute())
                            available_resources.append({
                                'label': label,  # "Table_1_Start"
                                'type': 'table' if label.startswith('Table_') else 'image',
                                'path': res_path,
                                'source_file': current_source_file
                            })
                            # logger.info(f"  ✓ 添加资源: {label} -> {full_path.name}")
                        else:
                            logger.warning(f"  ⚠️ 资源文件不存在: {region_path}")

            # 收集regions目录（保留旧逻辑兼容性）
            regions_dir = preprocessed_file.parent / 'regions_word'
            if regions_dir.exists():
                try:
                    rel_path = regions_dir.relative_to(Path.cwd() / 'AAA')
                    all_regions.append({
                        'name': file_name,
                        'regions': f"AAA/{rel_path.as_posix()}"
                    })
                except ValueError:
                    all_regions.append({
                        'name': file_name,
                        'regions': str(regions_dir)
                    })

            # Per-file RAGFlow 检索：使用 preprocessed_to_original 映射获取原始文件名
            # 然后用原始文件名查找 rag_file_to_doc_id 映射获取 document_id
            preprocessed_to_original = original_data_item.get("preprocessed_to_original", {}) if original_data_item else {}
            original_file_name = preprocessed_to_original.get(file_name, "")  # file_name 是预处理文件 ID
            if not original_file_name:
                # 回退：尝试从 source_file 获取（可能仍是预处理文件 ID）
                source_file_name = pp_data.get('source_file', '')
                original_file_name = Path(source_file_name).name if source_file_name else file_name

            per_file_rag = self._retrieve_ragflow_for_file(
                file_to_doc_id=original_data_item.get("rag_file_to_doc_id", {}) if original_data_item else {},
                file_name=original_file_name,
                question=original_data_item.get("rag_question", "") if original_data_item else "",
                dataset_id=original_data_item.get("rag_dataset_id", "") if original_data_item else ""
            )
            if per_file_rag:
                # 同时记录预处理文件 ID（用于匹配）和原始文件名（用于显示）
                per_file_ragflow_list.append({
                    "file_name": original_file_name,
                    "preprocessed_file_id": file_name,  # 预处理文件 ID，用于匹配 chunks_file 的父目录名
                    "ragflow_content": per_file_rag.get("marked_content", ""),  # 用于 LLM生成
                    "ragflow_chunks": per_file_rag.get("chunks", [])  # 用于溯源
                })
                logger.info(f"  ✓ RAGFlow 检索成功: {original_file_name} (ID: {file_name}), {len(per_file_rag.get('chunks', []))} 个分块")

        if not chunks_file_list:
            logger.warning("❌ 未找到任何chunks文件")
            return None

        # 构建返回结构
        enriched = {
            'file_type': doc_files[0][2].get('file_type', 'word'),
            'chunks_file': chunks_file_list if len(chunks_file_list) > 1 else chunks_file_list[0],  # 单个返回字符串，多个返回数组
            'source_file': ', '.join(source_files),
            'available_resources': available_resources  # ✅ 新增：可用的资源列表
        }

        # 附加 per-file RAGFlow 内容列表
        if per_file_ragflow_list:
            enriched['per_file_ragflow_list'] = per_file_ragflow_list

        # 保留原始data_item的关键字段（extract, original_mode, quote, file_name）
        if original_data_item:
            if 'extract' in original_data_item:
                enriched['extract'] = original_data_item['extract']
            if 'original_mode' in original_data_item:
                enriched['original_mode'] = original_data_item['original_mode']
            if 'quote' in original_data_item:
                enriched['quote'] = original_data_item['quote']
            # 传递 file_name 字段，用于构建 scheme_data
            if 'file_name' in original_data_item:
                enriched['file_name'] = original_data_item['file_name']
            # 传递 fileId -> 原始文件名的映射，用于建立 per_file_contents
            if 'preprocessed_to_original' in original_data_item:
                enriched['preprocessed_to_original'] = original_data_item['preprocessed_to_original']

        # 添加regions信息（保留旧字段兼容性）
        if all_regions:
            if len(all_regions) == 1:
                enriched['region'] = all_regions[0]['regions']  # 单个返回字符串
            else:
                enriched['regions'] = all_regions  # 多个返回数组

        logger.info(f"✓ 构建文档结构: {len(chunks_file_list)} 个chunks文件, {len(available_resources)} 个可用资源")

        return enriched

    def _build_table_structure(self, table_files: List, base_dir: Path, original_data_item: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        构建Excel/RTF文件的数据结构

        data_extractor期望的结构：
        - markdown_files: 文件列表 (List[str])
        - source_file: 字符串(单文件) 或 列表(多文件)
        """

        # 改为字典结构，按目录组织，携带 file_id 和原始文件名
        markdown_file_groups = []
        source_files_list = []
        processed_dirs = set()

        for file_name, preprocessed_file, pp_data in table_files:
            # Excel: 查找所有markdown文件
            markdown_dir = preprocessed_file.parent / 'sheets' / 'markdown'
            if not markdown_dir.exists():
                markdown_dir = preprocessed_file.parent / 'markdown'

            # 跳过已处理的目录
            dir_key = str(markdown_dir.absolute().resolve()) if markdown_dir.exists() else None
            if dir_key and dir_key in processed_dirs:
                logger.debug(f"跳过已处理的目录: {markdown_dir}")
                continue

            if markdown_dir.exists():
                processed_dirs.add(dir_key)

                # 获取目录中所有.md文件
                md_files = list(markdown_dir.glob('*.md'))
                md_file_paths = []
                for md_file in md_files:
                    try:
                        aaa_base = Path.cwd() / 'AAA'
                        rel = md_file.absolute().resolve().relative_to(aaa_base)
                        md_norm = str(Path('AAA') / rel)
                    except Exception:
                        md_norm = str(md_file)
                    md_file_paths.append(md_norm)

                # 构建字典结构，携带 file_id 和原始文件名
                file_id = preprocessed_file.parent.name  # 预处理目录名就是 fileId
                # 使用 preprocessed_to_original 映射将 fileId 转换为原始文件名
                preprocessed_to_original = original_data_item.get("preprocessed_to_original", {}) if original_data_item else {}
                original_file_name = preprocessed_to_original.get(file_name, file_name)  # file_name 可能是 fileId，需要转换
                markdown_file_groups.append({
                    "markdown_dir": str(markdown_dir),
                    "markdown_files": md_file_paths,
                    "file_id": file_id,
                    "original_file_name": original_file_name  # 真正的原始文件名
                })

                logger.info(f"找到 {len(md_files)} 个markdown文件: {original_file_name}, fileId: {file_id}")

            # 添加source_file
            source_file = pp_data.get('source_file', '')
            if source_file and source_file not in source_files_list:
                source_files_list.append(source_file)

        if not markdown_file_groups:
            logger.warning("❌ 未找到任何markdown文件")
            return None

        # 构建返回结构
        enriched = {
            'file_type': table_files[0][2].get('file_type', 'excel'),
            'markdown_files': markdown_file_groups,  # 字典结构列表
        }

        # source_file: 单个返回字符串，多个返回列表
        if len(source_files_list) == 1:
            enriched['source_file'] = source_files_list
        else:
            enriched['source_file'] = source_files_list

        # 保留原始 data_item 中的关键字段
        if original_data_item:
            if original_data_item.get('project_id'):
                enriched['project_id'] = original_data_item['project_id']
            # 传递 file_name 字段，用于构建 scheme_data
            if 'file_name' in original_data_item:
                enriched['file_name'] = original_data_item['file_name']
            if 'extract' in original_data_item:
                enriched['extract'] = original_data_item['extract']
            if 'original_mode' in original_data_item:
                enriched['original_mode'] = original_data_item['original_mode']
            if 'quote' in original_data_item:
                enriched['quote'] = original_data_item['quote']

        logger.info(f"✓ 构建表格结构: {len(markdown_file_groups)} 个文件组")
        return enriched

    def _retrieve_ragflow_for_file(self, file_to_doc_id: Dict[str, str], file_name: str,
                                    question: str, dataset_id: str) -> Dict[str, Any]:
        """
        为单个文件执行 RAGFlow 检索

        Args:
            file_to_doc_id: 文件名 -> RAGFlow document_id 映射
            file_name: 当前文件名
            question: 检索问题（即 extract 提示词）
            dataset_id: 知识库 ID

        Returns:
            {
                "marked_content": str,  # 标记后的 RAGFlow 分块文本（用于 LLM）
                "chunks": List[Dict],   # 原始分块列表（用于溯源）
            }
            失败返回 None
        """
        doc_id = file_to_doc_id.get(file_name)
        if not doc_id:
            logger.debug(f"文件 {file_name} 无对应的 RAGFlow document_id")
            return None
        if not question or not dataset_id:
            return None

        try:
            # 获取 10 个 RAGFlow 分块
            chunks = ragflow_client.retrieve(
                document_ids=[doc_id],
                question=question,
                dataset_id=dataset_id,
                page_size=10
            )
            if not chunks:
                return None

            marked_parts = []
            for chunk in chunks:
                cid = chunk.get('chunk_id', 'unknown')
                content = chunk.get('content', '')
                marked_parts.append(f"【RAGFlow分块-{cid}】\n{content}")

            return {
                "marked_content": "\n\n".join(marked_parts),  # 用于 LLM
                "chunks": chunks  # 用于溯源
            }
        except Exception as e:
            logger.warning(f"RAGFlow 单文件检索失败: {file_name} - {e}")
            return None

    def _remove_md_formatting(self,text: str) -> str:
        """
        去除MD格式，保留特定标记，去除表格、图片引用、标题标记和空行

        Args:
            text: 输入的Markdown文本

        Returns:
            处理后的文本
        """
        if not text:
            return text

        lines = text.split('\n')
        result_lines = []
        in_html_table = False

        for line in lines:
            # 检查是否在HTML表格内
            if '<table>' in line:
                in_html_table = True
            if '</table>' in line:
                in_html_table = False
                continue
            # 如果在HTML表格内，跳过该行
            if in_html_table:
                continue

            # 检查是否是{{Table_X_Start}}或{{Table_X_End}}格式的标记，这些要保留
            if re.match(r'\{\{Table_\d+_(Start|End)\}\}', line.strip()):
                result_lines.append(line)
                continue
            # 去除单独的代码块标记（整行都是```的行）
            if line.strip() == '```':
                continue
            # 去除图片引用（但保留其他内容）
            line = re.sub(r'!\[.*?\]\(.*?\)', '', line)
            # 去除标题开头的#（保留#后面的内容）
            line = re.sub(r'^#+\s*', '', line)
            #去除加粗格式(**text** 或 __text__)
            line = re.sub(r'\*\*(.+?)\*\*', r'\1', line)
            line = re.sub(r'__(.+?)__', r'\1', line)
            # 如果处理后的行不为空（去除空白字符后），添加到结果中
            if line.strip():
                result_lines.append(line)
        after_text = '\n'.join(result_lines)
        logger.info(
            f"去除MD格式，处理前字数: {len(text)}, 处理后字数: {len(after_text)}")
        return after_text

