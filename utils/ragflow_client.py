#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RAGFlow 客户端工具类

功能说明：
- 封装 RAGFlow SDK 的常用操作
- 提供延迟初始化的客户端管理
- 支持从文件名映射到 document_id
- 支持从 project_name 映射到 dataset_id

使用方式：
    from utils.ragflow_client import ragflow_client

    # 1. 获取知识库 ID
    dataset_id = ragflow_client.get_dataset_id("项目名称")
    if not dataset_id:
        print("知识库不存在")

    # 2. 批量获取文档 ID
    doc_ids = ragflow_client.get_document_ids_batch(["方案.docx", "报告.docx"], "项目名称")

    # 3. 检索
    chunks = ragflow_client.retrieve(
        document_ids=doc_ids,
        question="提取试验设计内容",
        dataset_id=dataset_id
    )
"""

import logging
from ragflow_sdk import RAGFlow, DataSet
from typing import Any, Dict, List, Optional
import requests
from urllib3.exceptions import NewConnectionError, MaxRetryError

logger = logging.getLogger(__name__)


class RAGFlowClient:
    """
    RAGFlow 客户端工具类

    【核心功能】
    1. 客户端连接管理（延迟初始化、单例复用）
    2. 知识库检索
    3. project_name → dataset_id 映射
    4. 批量获取文件名对应的 document_id

    【设计原则】
    - 延迟初始化：首次使用时创建客户端
    - 单例复用：全局共享一个客户端实例
    - 容错处理：配置缺失或服务不可用时不影响主流程
    """

    def __init__(self):
        """初始化 RAGFlow 客户端管理器"""
        self._client: Optional[Any] = None
        self._initialized: bool = False
        self._settings_cache: Optional[Any] = None

        # 文件名 → document_id 映射缓存
        self._document_id_cache: Dict[str, Optional[str]] = {}

        # project_name → dataset_id 映射缓存
        self._dataset_id_cache: Dict[str, Optional[str]] = {}



    def _get_settings(self) -> Optional[Any]:
        """
        获取配置（带缓存）

        Returns:
            Settings 对象，失败返回 None
        """
        if self._settings_cache is not None:
            return self._settings_cache

        try:
            from config import get_settings
            self._settings_cache = get_settings()
            return self._settings_cache
        except Exception as e:
            logger.error(f"获取配置失败：{e}", exc_info=True)
            return None

    def _get_client(self) -> Optional[Any]:
        """
        获取 RAGFlow 客户端（延迟初始化）

        第一次调用时创建客户端，后续复用。
        如果配置缺失或初始化失败，返回 None。

        Returns:
            RAGFlow 客户端实例，或 None
        """
        # 已初始化过，直接返回
        if self._client is not None:
            return self._client

        # 已尝试过但失败，不再重复尝试
        if self._initialized:
            return None

        self._initialized = True

        try:


            settings = self._get_settings()
            if not settings:
                logger.warning("无法获取配置，RAGFlow 功能不可用")
                return None

            api_key = getattr(settings, 'ragflow_api_key', '')
            base_url = getattr(settings, 'ragflow_base_url', '')

            if not api_key or not base_url:
                logger.warning("RAGFlow 配置缺失（api_key 或 base_url），RAGFlow 功能不可用")
                return None

            # 创建客户端
            self._client = RAGFlow(api_key=api_key, base_url=base_url)
            logger.info("✅ RAGFlow 客户端初始化成功")
            return self._client

        except ImportError:
            logger.error("ragflow_sdk 未安装，请执行：pip install ragflow-sdk")
            return None
        except Exception as e:
            logger.error(f"RAGFlow 客户端初始化失败：{e}", exc_info=True)
            return None

    @property
    def is_available(self) -> bool:
        """
        检查 RAGFlow 服务是否可用

        Returns:
            True 表示可用，False 表示不可用
        """
        return self._get_client() is not None


    def _check_documents_status(self, dataset_id: str, document_ids: List[str]) -> tuple[bool, List[str]]:
        """
        检查文档解析状态

        Args:
            dataset_id: 知识库 ID
            document_ids: 文档 ID 列表

        Returns:
            (是否全部解析完成, 解析完成的文档ID列表)
        """
        client = self._get_client()
        if not client:
            return False, []

        try:
            # 获取数据集
            datasets = client.list_datasets(id=dataset_id)
            if not datasets:
                logger.warning(f"未找到数据集: {dataset_id}")
                return False, []
            dataset = datasets[0]

            # 检查每个文档的解析状态
            ready_doc_ids = []
            for doc_id in document_ids:
                documents = dataset.list_documents(id=doc_id)
                if not documents:
                    logger.warning(f"未找到文档: {doc_id}")
                    continue

                document = documents[0]
                doc_status = getattr(document, 'run', None) or getattr(document, 'status', None)

                if doc_status in ('DONE', 'success'):
                    logger.info(f"✅ 文档 '{document.name}' 解析完成，状态: {doc_status}")
                    ready_doc_ids.append(doc_id)
                elif doc_status in ('RUNNING', 'processing'):
                    logger.warning(f"⏳ 文档 '{document.name}' 解析进行中，跳过")
                elif doc_status in ('FAIL', 'failed'):
                    logger.warning(f"❌ 文档 '{document.name}' 解析失败")
                else:
                    logger.warning(f"📌 文档 '{document.name}' 未知状态: {doc_status}")

            return len(ready_doc_ids) == len(document_ids), ready_doc_ids

        except Exception as e:
            if self._is_network_error(e):
                logger.warning(f"无法连接 RAGFlow 服务，跳过文档状态检查: {e}")
            else:
                logger.error(f"检查文档解析状态失败：{e}", exc_info=True)
            return False, []

    def retrieve(
            self,
            document_ids: List[str],
            question: str,
            dataset_id: str,
            page_size: Optional[int] = None
    ) -> Optional[List[str]]:
        """
        从 RAGFlow 知识库检索相关分块

        Args:
            document_ids: RAGFlow 文档 ID 列表
            question: 检索问题
            dataset_id: RAGFlow 知识库 ID（调用方需提前获取）
            page_size: 返回分块数量（可选，默认使用配置值）

        Returns:
            检索到的分块文本列表，失败返回 None

        Example:
            chunks = ragflow_client.retrieve(
                document_ids=["doc_123"],
                question="提取试验设计内容",
                dataset_id="dataset_abc"
            )
        """
        # 验证必要参数
        if not dataset_id:
            logger.warning("dataset_id 为空，跳过检索")
            return None

        # 验证问题是否为空
        if not question or not question.strip():
            logger.warning("检索问题为空，跳过检索")
            return None

        # 1. 获取客户端
        client = self._get_client()
        if not client:
            logger.warning("RAGFlow 客户端不可用，跳过检索")
            return None

        # 2. 过滤空值
        document_ids = [doc_id for doc_id in document_ids if doc_id]
        if not document_ids:
            logger.info("document_ids 列表为空，跳过检索")
            return None

        # 3. 检查文档解析状态
        all_ready, ready_doc_ids = self._check_documents_status(dataset_id, document_ids)
        if not ready_doc_ids:
            logger.warning("没有解析完成的文档，跳过检索")
            return None
        if not all_ready:
            logger.info(f"部分文档未解析完成，仅检索已完成的 {len(ready_doc_ids)}/{len(document_ids)} 个文档")

        # 4. 获取 page_size
        if page_size is None:
            settings = self._get_settings()
            page_size = getattr(settings, 'ragflow_page_size', 5) if settings else 5

        logger.info(f"RAGFlow 检索中....")

        try:
            # 5. 执行检索（使用解析完成的文档）
            retriever = client.retrieve(
                dataset_ids=[dataset_id],
                document_ids=ready_doc_ids,
                question=question,
                page_size=page_size
            )

            # 6. 提取分块内容
            if not retriever:
                logger.warning("RAGFlow 检索结果为空")
                return None

            chunks = []
            for id,chunk in enumerate(retriever, start=1):
                content = chunk.content if hasattr(chunk, 'content') else str(chunk)
                similarity = chunk.similarity if hasattr(chunk, 'similarity') else None
                chunks_info = {
                    "chunk_id": f"chunk_{id}",
                    "similarity": similarity,
                    "content": content
                }
                chunks.append(chunks_info)

            logger.info(f"✅ RAGFlow 检索成功：{len(chunks)} 个分块")
            for chunk in chunks:
                try:
                    content_length = len(chunk.get('content', ''))
                    logger.info(f"   - 分块 {chunk.get('chunk_id', 'unknown')}: {content_length} 字符")
                except Exception as e:
                    logger.warning(f"记录分块日志时出错: {e}")

            return chunks

        except Exception as e:
            if self._is_network_error(e):
                logger.warning(f"无法连接 RAGFlow 服务，跳过检索: {e}")
            else:
                logger.error(f"RAGFlow 检索失败：{e}", exc_info=True)
            return None

    def get_dataset_id(self, project_name: Optional[str]) -> Optional[str]:
        """
        根据 project_name 获取 RAGFlow 的 dataset_id
        Args:
            project_name: 项目名称（RAGFlow 知识库名称）
        Returns:
            dataset_id，未找到返回 None
        """
        # 验证必要参数
        if project_name is None:
            logger.info("project_name 为 None，无法确定对应的知识库")
            return None

        # 检查缓存
        if project_name in self._dataset_id_cache:
            return self._dataset_id_cache[project_name]

        client = self._get_client()
        if client is None:
            logger.info("RAGFlow 客户端未初始化，无法获取知识库 ID")
            return None

        try:
            datasets = client.list_datasets(page_size=10000)
            if not datasets:
                logger.info("RAGFlow 返回的数据集列表为空")
                return None

            for dataset in datasets:
                if dataset.name == project_name:
                    logger.info(f"找到项目 '{project_name}' 对应的知识库 ID: {dataset.id}")
                    self._dataset_id_cache[project_name] = dataset.id
                    return dataset.id
        except Exception as e:
            if self._is_network_error(e):
                logger.warning(f"无法连接 RAGFlow 服务 (project_name={project_name}): {e}")
            else:
                logger.error(f"获取知识库 ID 时发生错误 (project_name={project_name}): {e}", exc_info=True)
            return None

    def get_document_ids_batch(self, file_names: List[str], project_name: str) -> List[str]:
        """
        批量获取多个文件的 document_id

        相比多次调用 get_document_id()，此方法只调用一次 API 获取数据集和文档列表，
        然后在内存中批量匹配，效率更高。

        Args:
            file_names: 文件名列表
            project_name: 项目名称（RAGFlow 知识库名称）

        Returns:
            找到的 document_id 列表（不包含未匹配的文件）
        """
        if not file_names or not project_name:
            logger.info("file_names 或 project_name 为空，无法批量获取文档 ID")
            return []

        client = self._get_client()
        if client is None:
            logger.info("RAGFlow 客户端未初始化，无法批量获取文档 ID")
            return []

        try:
            # 1. 获取数据集
            try:
                dataset = client.get_dataset(name=project_name)
            except Exception:
                dataset = None

            if dataset is None:
                logger.warning(f"未找到项目 '{project_name}' 对应的数据集")
                return []

            # 2. 获取所有文档（只调用一次 API）
            documents = dataset.list_documents()
            if not documents:
                logger.warning(f"数据集 '{project_name}' 中没有文档")
                return []

            # 3. 构建文件名到文档的映射
            doc_map = {doc.name: doc for doc in documents}

            # 4. 批量匹配并收集结果
            document_ids = []
            for file_name in file_names:
                cache_key = f"{project_name}:{file_name}"
                if file_name in doc_map:
                    doc_id = doc_map[file_name].id
                    self._document_id_cache[cache_key] = doc_id
                    document_ids.append(doc_id)
                    logger.info(f"   ✅ 获取到 document_id: {file_name} -> {doc_id}")
                else:
                    self._document_id_cache[cache_key] = None
                    logger.info(f"   ❌ 未获取到 document_id: {file_name}")

            return document_ids

        except AttributeError as e:
            logger.error(f"访问数据集或文档属性失败 (project_name={project_name}): {e}", exc_info=True)
            return []
        except Exception as e:
            if self._is_network_error(e):
                logger.warning(f"无法连接 RAGFlow 服务 (project_name={project_name}): {e}")
            else:
                logger.error(f"批量获取文档 ID 时发生错误 (project_name={project_name}): {e}", exc_info=True)
            return []

    def _is_network_error(self, error: Exception) -> bool:
        """
        判断是否为网络连接相关的异常

        网络错误属于可预期的场景，应该使用简洁的日志输出，
        而非打印完整堆栈信息。

        Args:
            error: 异常对象

        Returns:
            True 表示是网络错误，False 表示其他类型错误
        """
        # requests 库的网络错误
        if isinstance(error, requests.exceptions.ConnectionError):
            return True
        if isinstance(error, requests.exceptions.Timeout):
            return True

        # urllib3 的网络错误（可能被包装在其他异常中）
        if isinstance(error, (NewConnectionError, MaxRetryError)):
            return True

        # 检查异常链中是否包含网络错误
        cause = error.__cause__ or error.__context__
        if cause:
            return self._is_network_error(cause)

        # 检查异常消息中是否包含网络错误特征
        error_msg = str(error).lower()
        network_keywords = [
            'connection refused',
            'connectionerror',
            'max retries exceeded',
            'failed to establish a new connection',
            'connection timed out',
            'timed out',
            '目标计算机积极拒绝',  # Windows 中文错误消息
        ]
        return any(kw in error_msg for kw in network_keywords)

# 全局 RAGFlow 客户端单例实例
# 通过 from utils.ragflow_client import ragflow_client 使用
ragflow_client = RAGFlowClient()

# 模块公开接口
__all__ = ['ragflow_client']
