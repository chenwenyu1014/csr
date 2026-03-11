"""
进度回调服务

功能说明：
- 负责将任务进度推送到前端回调接口
- 自动更新Java系统中的标签状态
- 支持多个标签同时更新
- 提供完整的错误处理和重试机制

主要功能：
1. 任务进度推送：向callback_url发送进度更新
2. 标签状态更新：调用Java系统接口更新标签状态
3. 标签结果推送：任务完成后推送结果到Java系统
4. 单标签状态更新：支持单独更新某个段落标签的状态

回调机制：
- 进度回调（轻量）：推送任务进度和状态
- 结果回调（重量）：推送完整的生成结果
- 标签回调（自动）：自动更新Java系统中的标签状态

配置说明：
- TAG_STATUS_API_BASE: 标签状态更新接口基础URL（默认http://192.168.3.32:8088）
- TAG_RESULT_API_BASE: 标签结果回调接口基础URL（默认http://192.168.3.32:8088）
- JAVA_API_TOKEN: Java系统认证Token（用于Shiro认证）
"""

import os
import logging
import httpx
import asyncio
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor

from .task_manager import TaskManager, TaskStage, get_task_manager
from utils.task_logger import get_task_logger

logger = logging.getLogger(__name__)


def _task_log_error(message: str, exc: Exception = None, **extra):
    """记录错误到任务日志"""
    task_logger = get_task_logger()
    if task_logger:
        task_logger.error(message, exc=exc, logger_name="progress_callback", **extra)

# ========== 全局线程池 ==========
# 用于在同步代码中异步发送回调请求
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="callback_")

# ========== 标签状态更新接口配置 ==========
# 可通过环境变量 TAG_STATUS_API_BASE 配置基础URL
# 默认使用 192.168.3.32:8088（所有回调统一IP和端口）
TAG_STATUS_API_BASE = os.getenv("TAG_STATUS_API_BASE", "http://192.168.3.32:8088")
TAG_STATUS_API_PATH = "/ky/sys/projectTagsSourceInfo/updateStatus"

# ========== 标签结果回调接口配置 ==========
TAG_RESULT_API_BASE = os.getenv("TAG_RESULT_API_BASE", "http://192.168.3.32:8088")
TAG_RESULT_API_PATH = "/ky/sys/projectTagsSourceInfo/getTagAIResult"


# ========== 阶段到标签状态的映射 ==========
# 简化为4个核心状态：开始提取 -> 提取完毕 -> 开始生成 -> 生成完毕
STAGE_TO_TAG_STATUS = {
    TaskStage.PENDING: "等待处理",        # 接口同步返回，不走回调
    TaskStage.PREPROCESSING: "开始提取",  # 预处理阶段合并到提取
    TaskStage.EXTRACTION: "开始提取",     # 提取中
    TaskStage.GENERATION: "开始生成",      # 生成中
    TaskStage.POSTPROCESSING: "开始生成", # 后处理合并到生成
    TaskStage.COMPLETED: "生成完毕",      # 最终结果回调中返回
    TaskStage.FAILED: "生成失败",
}


class ProgressCallback:
    """
    进度回调服务
    
    这是CSR文档生成系统中负责进度通知和状态更新的核心组件。
    它会自动将任务进度推送到前端，并更新Java系统中的标签状态。
    
    主要功能：
    1. 向 callback_url 推送任务进度（轻量级，仅状态信息）
    2. 调用标签状态更新接口更新标签状态（自动）
    3. 推送完整的生成结果到Java系统（任务完成时）
    4. 支持单独更新某个段落标签的状态
    
    使用示例：
        # 创建回调实例
        callback = ProgressCallback(
            task_id="task_123",
            callback_url="http://example.com/callback",
            project_id="proj_456",
            tag_ids=["tag_1", "tag_2"]  # 可以同时更新多个标签
        )
        
        # 通知阶段变化
        callback.notify_extraction_started()  # 开始提取
        callback.notify_extraction_completed()  # 提取完毕
        callback.notify_generation_started()  # 开始生成
        
        # 通知单个段落完成
        callback.update_single_tag_status("tag_1", "生成完毕")
        
        # 通知任务完成
        callback.notify_complete(result_data)
        
        # 通知错误
        callback.notify_error("发生错误")
    """
    
    def __init__(
        self,
        task_id: str,
        callback_url: Optional[str] = None,
        result_callback_url: Optional[str] = None,
        project_id: Optional[str] = None,
        tag_ids: Optional[List[str]] = None,
        timeout: float = 30.0,
        auth_token: Optional[str] = None
    ):
        """
        Args:
            task_id: 任务ID
            callback_url: 进度状态回调URL（轻量，推送进度）
            result_callback_url: 结果回调URL（重量，推送完整结果）
            project_id: 项目ID（用于标签状态更新）
            tag_ids: 标签ID列表（用于标签状态更新，不传则使用task_id）
            timeout: HTTP请求超时时间
            auth_token: 认证Token（用于调用Java系统的Shiro认证）
        """
        self.task_id = task_id
        self.callback_url = callback_url
        self.result_callback_url = result_callback_url
        self.project_id = project_id
        self.tag_ids = tag_ids or []  # 可以同时更新多个标签
        self.timeout = timeout
        # ✅ 新增：认证Token支持
        self.auth_token = auth_token or os.getenv("JAVA_API_TOKEN")
        self.task_manager = get_task_manager()
        
        # 标签API URL
        self.tag_status_api_url = self._build_tag_status_url()
        self.tag_result_api_url = self._build_tag_result_url()
        
        logger.info(
            f"ProgressCallback 初始化: task_id={task_id}, "
            f"callback_url={callback_url}, result_callback_url={result_callback_url}, "
            f"project_id={project_id}, "
            f"tag_status_api={self.tag_status_api_url}, "
            f"tag_result_api={self.tag_result_api_url}"
        )
    
    def _build_tag_status_url(self) -> str:
        """
        构建标签状态更新API URL
        
        默认使用配置的基础URL: http://192.168.3.32:8088
        """
        base_url = TAG_STATUS_API_BASE
        
        if base_url:
            return f"{base_url.rstrip('/')}{TAG_STATUS_API_PATH}"
        
        # 如果没有配置，返回相对路径（不应该发生）
        logger.warning("标签状态API基础URL未配置")
        return TAG_STATUS_API_PATH
    
    def _build_tag_result_url(self) -> str:
        """
        构建标签结果回调API URL
        
        默认使用配置的基础URL: http://192.168.3.32:8088
        """
        base_url = TAG_RESULT_API_BASE
        
        if base_url:
            return f"{base_url.rstrip('/')}{TAG_RESULT_API_PATH}"
        
        logger.warning("标签结果API基础URL未配置")
        return TAG_RESULT_API_PATH
    
    def _build_payload(
        self,
        stage: str,
        message: str,
        progress: int,
        current_step: int = 0,
        total_steps: int = 0,
        detail: Optional[dict] = None,
        result: Optional[dict] = None,
        error: Optional[str] = None
    ) -> dict:
        """构建回调数据"""
        payload = {
            "task_id": self.task_id,
            "stage": stage,
            "message": message,
            "progress": progress,
            "current_step": current_step,
            "total_steps": total_steps,
        }
        
        # 添加 project_id
        if self.project_id:
            payload["project_id"] = self.project_id
        
        if detail:
            payload["detail"] = detail
        if result is not None:
            payload["result"] = result
        if error:
            payload["error"] = error
        
        return payload
    
    def _send_callback_sync(self, payload: dict):
        """同步发送状态回调（用于线程池）"""
        if not self.callback_url:
            return
        
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(
                    self.callback_url,
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    logger.debug(f"状态回调成功: {self.task_id} -> {payload.get('stage')}")
                else:
                    logger.warning(
                        f"状态回调返回非200: {response.status_code} - {response.text[:200]}"
                    )
        except Exception as e:
            logger.warning(f"状态回调发送失败: {e}")
    
    def _send_result_callback_sync(self, payload: dict):
        """同步发送结果回调（用于线程池）"""
        if not self.result_callback_url:
            return
        
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(
                    self.result_callback_url,
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    logger.info(f"结果回调成功: {self.task_id}")
                else:
                    logger.warning(
                        f"结果回调返回非200: {response.status_code} - {response.text[:200]}"
                    )
        except Exception as e:
            logger.warning(f"结果回调发送失败: {e}")
    
    def _update_tag_status_sync(self, tag_id: str, status: str):
        """
        同步更新标签状态
        
        调用 POST /ky/sys/projectTagsSourceInfo/updateStatus
        参数：id（标签标识）、status（标签状态）、project_id（项目ID）
        """
        if not self.tag_status_api_url:
            logger.debug("标签状态API未配置，跳过更新")
            return
        
        try:
            data = {
                "id": tag_id,
                "status": status,
            }
            
            if self.project_id:
                data["project_id"] = self.project_id
            
            logger.info(f"更新标签状态: {data} -> {self.tag_status_api_url}")
            
            # ✅ 构建请求头（包含认证Token）
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            if self.auth_token:
                headers["X-Access-Token"] = self.auth_token
            
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(
                    self.tag_status_api_url,
                    data=data,  # 使用 form data
                    headers=headers
                )
                
                if response.status_code == 200:
                    logger.info(f"标签状态更新成功: id={tag_id}, status={status}")
                else:
                    logger.warning(
                        f"标签状态更新失败: {response.status_code} - {response.text[:200]}"
                    )
        except Exception as e:
            logger.warning(f"标签状态更新失败: {e}")
    
    def _send_tag_result_sync(self, tag_id: str, result_data: dict):
        """
        同步发送标签结果
        
        调用 POST /ky/sys/projectTagsSourceInfo/getTagAIResult
        参数：id（标签唯一标识）、dataJson（生成结果JSON字符串）、project_id（项目ID）
        """
        if not self.tag_result_api_url:
            logger.debug("标签结果API未配置，跳过发送")
            return
        
        try:
            import json as json_module
            
            # 将结果数据序列化为JSON字符串
            data_json_str = json_module.dumps(result_data, ensure_ascii=False)
            
            data = {
                "id": tag_id,
                "dataJson": data_json_str,
            }
            
            if self.project_id:
                data["project_id"] = self.project_id
            
            logger.info(f"发送标签结果: id={tag_id}, project_id={self.project_id}")
            logger.debug(f"结果数据长度: {len(data_json_str)} 字符")
            
            # ✅ 构建请求头（包含认证Token）
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            if self.auth_token:
                headers["X-Access-Token"] = self.auth_token
            
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(
                    self.tag_result_api_url,
                    data=data,  # 使用 form data
                    headers=headers
                )
                
                if response.status_code == 200:
                    logger.info(f"标签结果发送成功: id={tag_id}")
                else:
                    logger.warning(
                        f"标签结果发送失败: {response.status_code} - {response.text[:200]}"
                    )
        except Exception as e:
            logger.error(f"标签结果发送失败: {e}", exc_info=True)
            _task_log_error("标签结果发送失败", exc=e)
    
    def _update_all_tags_status(self, stage: TaskStage):
        """更新所有相关标签的状态"""
        tag_status = STAGE_TO_TAG_STATUS.get(stage, "processing")
        
        # 如果有指定的 tag_ids，更新它们
        if self.tag_ids:
            for tag_id in self.tag_ids:
                _executor.submit(self._update_tag_status_sync, tag_id, tag_status)
        else:
            # 否则使用 task_id 作为标签ID
            _executor.submit(self._update_tag_status_sync, self.task_id, tag_status)
    
    async def _send_callback_async(self, payload: dict):
        """异步发送回调"""
        if not self.callback_url:
            return
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    self.callback_url,
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    logger.debug(f"回调成功: {self.task_id} -> {payload.get('stage')}")
                else:
                    logger.warning(
                        f"回调返回非200: {response.status_code} - {response.text[:200]}"
                    )
        except Exception as e:
            logger.warning(f"回调发送失败: {e}")
    
    def _update_and_send(
        self,
        stage: TaskStage,
        message: str,
        progress: int,
        current_step: int = 0,
        total_steps: int = 0,
        detail: Optional[dict] = None,
        result: Optional[dict] = None,
        error: Optional[str] = None,
        update_tag_status: bool = True
    ):
        """更新任务状态并发送回调"""
        # 更新任务管理器中的状态
        self.task_manager.update_progress(
            task_id=self.task_id,
            stage=stage,
            message=message,
            progress=progress,
            current_step=current_step,
            total_steps=total_steps,
            detail=detail
        )
        
        # 构建并发送回调
        payload = self._build_payload(
            stage=stage.value,
            message=message,
            progress=progress,
            current_step=current_step,
            total_steps=total_steps,
            detail=detail,
            result=result,
            error=error
        )
        
        # 在线程池中异步发送，不阻塞主流程
        if self.callback_url:
            _executor.submit(self._send_callback_sync, payload)
        
        # 更新标签状态
        if update_tag_status:
            self._update_all_tags_status(stage)
    
    # # ========== 便捷方法 ==========
    #
    # def set_tag_ids(self, tag_ids: List[str]):
    #     """
    #     设置要更新的标签ID列表
    #
    #     可以在任务执行过程中动态设置，比如根据配置中的 paragraph_id 设置
    #     """
    #     self.tag_ids = tag_ids
    #     logger.info(f"设置标签ID列表: {tag_ids}")
    #
    # def add_tag_id(self, tag_id: str):
    #     """添加一个标签ID"""
    #     if tag_id not in self.tag_ids:
    #         self.tag_ids.append(tag_id)
    #
    # def notify_accepted(self, message: str = "任务已接收"):
    #     """通知任务已接收"""
    #     self._update_and_send(
    #         stage=TaskStage.PENDING,
    #         message=message,
    #         progress=0
    #     )
    #
    # def notify_preprocessing(
    #     self,
    #     message: str,
    #     progress: int = 10,
    #     current: int = 0,
    #     total: int = 0,
    #     detail: Optional[dict] = None
    # ):
    #     """通知预处理进度"""
    #     self._update_and_send(
    #         stage=TaskStage.PREPROCESSING,
    #         message=message,
    #         progress=progress,
    #         current_step=current,
    #         total_steps=total,
    #         detail=detail
    #     )
    #
    # def notify_extraction(
    #     self,
    #     message: str,
    #     progress: int = 30,
    #     current: int = 0,
    #     total: int = 0,
    #     detail: Optional[dict] = None
    # ):
    #     """通知数据提取进度"""
    #     self._update_and_send(
    #         stage=TaskStage.EXTRACTION,
    #         message=message,
    #         progress=progress,
    #         current_step=current,
    #         total_steps=total,
    #         detail=detail
    #     )
    #
    # def notify_generation(
    #     self,
    #     message: str,
    #     progress: int = 60,
    #     current: int = 0,
    #     total: int = 0,
    #     detail: Optional[dict] = None
    # ):
    #     """通知内容生成进度"""
    #     self._update_and_send(
    #         stage=TaskStage.GENERATION,
    #         message=message,
    #         progress=progress,
    #         current_step=current,
    #         total_steps=total,
    #         detail=detail
    #     )
    #
    # def notify_postprocessing(
    #     self,
    #     message: str,
    #     progress: int = 90,
    #     detail: Optional[dict] = None
    # ):
    #     """通知后处理进度"""
    #     self._update_and_send(
    #         stage=TaskStage.POSTPROCESSING,
    #         message=message,
    #         progress=progress,
    #         detail=detail
    #     )
    
    def notify_complete(self, result: dict, message: str = "生成完成"):
        """
        通知任务完成
        
        完整回调流程：
        1. callback_url: 发送轻量的状态通知
        2. result_callback_url: 发送完整的结果数据
        3. 为每个段落调用标签结果接口
        4. 更新所有标签状态为"生成完成"
        """
        # ✅ 确保标签终态一定更新为“生成完毕”
        # 之前依赖外部显式调用 notify_generation_completed，但异步链路里经常遗漏，导致Java侧状态停留在“开始生成”。
        try:
            self._update_all_tags_status_with_custom("生成完毕")
        except Exception:
            # 标签状态更新失败不应影响最终完成回调
            pass

        # 先更新任务管理器
        self.task_manager.complete_task(self.task_id, result)
        
        # 1. 发送状态完成回调（轻量）
        if self.callback_url:
            status_payload = self._build_payload(
                stage=TaskStage.COMPLETED.value,
                message=message,
                progress=100
                # 不包含 result，保持轻量
            )
            _executor.submit(self._send_callback_sync, status_payload)
            logger.info(f"状态完成回调已发送: {self.task_id}")
        
        # 2. 发送结果回调（重量，包含完整数据）
        if self.result_callback_url:
            result_payload = {
                "task_id": self.task_id,
                "project_id": self.project_id,
                "success": True,
                "message": message,
                **result  # 展开完整结果
            }
            _executor.submit(self._send_result_callback_sync, result_payload)
            logger.info(f"结果回调已发送: {self.task_id}")
        
        # 3. 为每个段落调用标签结果接口
        self._send_tag_results(result)
        
        # 4. 标签状态已经在生成完成时更新过了，这里不再重复更新
        
        logger.info(f"任务完成通知已发送: {self.task_id}")
    
    def _send_tag_results(self, result: dict):
        """
        为每个段落发送结果到标签结果接口
        
        调用 POST /ky/sys/projectTagsSourceInfo/getTagAIResult
        为每个段落推送完整的生成结果（与同步接口返回格式一致）
        
        dataJson 格式：
        {
            "id": "9_1_study_plan",           # 段落ID
            "project_id": "proj_xxx",         # 项目ID
            "status": "生成完毕",              # ✅ 状态字段
            "success": true,
            "generated_content": {
                "paragraphs": [{...}],        # 该段落的内容
                "resource_mappings": {...}
            },
            "traceability": [{...}],          # 该段落的溯源信息
            "insertion_config": {...}         # 插入配置
        }
        """
        try:
            # 提取段落结果
            paragraphs = result.get("generated_content", {}).get("paragraphs", [])
            
            if not paragraphs:
                logger.warning("没有段落结果可推送")
                return
            
            # 提取溯源信息（建立 paragraph_id -> provenance 映射）
            traceability_list = result.get("traceability", [])
            traceability_map = {}
            for item in traceability_list:
                pid = item.get("paragraph_id")
                if pid:
                    traceability_map[pid] = item
            
            # 提取资源映射
            resource_mappings = result.get("generated_content", {}).get("resource_mappings", {})
            
            # 提取其他共享字段
            run_dir = result.get("run_dir", "")
            
            logger.info(f"开始为 {len(paragraphs)} 个段落发送结果到标签结果接口")
            
            # 为每个段落推送完整结果
            for para in paragraphs:
                paragraph_id = para.get("paragraph_id")
                if not paragraph_id:
                    continue
                
                # 构建该段落的完整结果（与同步接口格式一致）
                para_success = para.get("status", "success") == "success"
                paragraph_result = {
                    # 标识字段
                    "id": paragraph_id,
                    "project_id": self.project_id,
                    "status": "生成完毕" if para_success else "生成失败",  # ✅ 状态字段
                    "success": para_success,
                    
                    # 运行目录
                    "run_dir": run_dir,
                    
                    # 生成内容（该段落）
                    "generated_content": {
                        "paragraphs": [
                            {
                                "paragraph_id": paragraph_id,
                                "generated_content": para.get("generated_content", ""),
                                "status": para.get("status", "success")
                            }
                        ],
                        "resource_mappings": resource_mappings
                    },
                    
                    # 溯源信息（该段落）
                    "traceability": [
                        traceability_map.get(paragraph_id, {
                            "paragraph_id": paragraph_id,
                            "generated_content": para.get("generated_content", ""),
                            "status": para.get("status", "success"),
                            "provenance": {"extracted_items": []}
                        })
                    ],
                    
                    # 插入配置（该段落）
                    "insertion_config": {
                        "generation_results": [
                            {
                                "paragraph_id": paragraph_id,
                                "generated_content": para.get("generated_content", ""),
                                "status": para.get("status", "success")
                            }
                        ],
                        "resource_mappings": resource_mappings
                    }
                }
                
                # 异步发送该段落的结果
                _executor.submit(self._send_tag_result_sync, paragraph_id, paragraph_result)
            
            logger.info(f"所有段落结果已提交队列")
            
        except Exception as e:
            logger.error(f"发送标签结果失败: {e}", exc_info=True)
    
    def notify_error(self, error: str, message: str = "生成失败"):
        """通知任务失败"""
        # 先更新任务管理器
        self.task_manager.fail_task(self.task_id, error)
        
        # 发送失败回调
        payload = self._build_payload(
            stage=TaskStage.FAILED.value,
            message=message,
            progress=0,
            error=error
        )
        
        if self.callback_url:
            _executor.submit(self._send_callback_sync, payload)
        
        # 更新标签状态为失败
        self._update_all_tags_status(TaskStage.FAILED)
        
        logger.error(f"任务失败通知已发送: {self.task_id} - {error}")
        _task_log_error(f"任务失败: {error}", task_id=self.task_id)
    
    # def notify_custom(
    #     self,
    #     stage: str,
    #     message: str,
    #     progress: int,
    #     current: int = 0,
    #     total: int = 0,
    #     detail: Optional[dict] = None
    # ):
    #     """
    #     自定义进度通知
    #
    #     Args:
    #         stage: 自定义阶段名称
    #         message: 进度消息
    #         progress: 进度百分比
    #         current: 当前步骤
    #         total: 总步骤
    #         detail: 额外详情
    #     """
    #     # 尝试映射到标准阶段
    #     stage_map = {
    #         "pending": TaskStage.PENDING,
    #         "preprocessing": TaskStage.PREPROCESSING,
    #         "extraction": TaskStage.EXTRACTION,
    #         "generation": TaskStage.GENERATION,
    #         "postprocessing": TaskStage.POSTPROCESSING,
    #         "completed": TaskStage.COMPLETED,
    #         "failed": TaskStage.FAILED,
    #     }
    #
    #     task_stage = stage_map.get(stage.lower(), TaskStage.GENERATION)
    #
    #     self._update_and_send(
    #         stage=task_stage,
    #         message=message,
    #         progress=progress,
    #         current_step=current,
    #         total_steps=total,
    #         detail=detail
    #     )
    
    def update_single_tag_status(self, tag_id: str, status: str):
        """
        单独更新某个标签的状态
        
        用于更精细的控制，比如更新单个段落的状态
        
        Args:
            tag_id: 标签ID（如 paragraph_id）
            status: 状态值（如 "开始提取", "提取完毕", "开始生成", "生成完毕"）
        """
        _executor.submit(self._update_tag_status_sync, tag_id, status)
    
    def notify_extraction_started(self, message: str = "开始提取数据"):
        """通知开始提取阶段"""
        # 更新任务进度 + 可选回调（但不重复用映射更新标签状态）
        self._update_and_send(
            stage=TaskStage.EXTRACTION,
            message=message,
            progress=10,
            update_tag_status=False
        )
        # 标签状态：开始提取
        self._update_all_tags_status_with_custom("开始提取")
        logger.info(f"通知开始提取: {self.task_id}")
    
    def notify_extraction_completed(self, message: str = "提取完毕"):
        """通知提取完成"""
        # 提取完毕属于“提取阶段”的一个里程碑：更新任务进度并发送回调，但标签状态用自定义文案
        self._update_and_send(
            stage=TaskStage.EXTRACTION,
            message=message,
            progress=40,
            update_tag_status=False
        )
        self._update_all_tags_status_with_custom("提取完毕")
        logger.info(f"通知提取完毕: {self.task_id}")
    
    def notify_generation_started(self, message: str = "开始生成内容"):
        """通知开始生成阶段"""
        self._update_and_send(
            stage=TaskStage.GENERATION,
            message=message,
            progress=60,
            update_tag_status=False
        )
        self._update_all_tags_status_with_custom("开始生成")
        logger.info(f"通知开始生成: {self.task_id}")
    
    def notify_generation_completed(self, message: str = "生成完毕"):
        """通知生成完成（所有段落）"""
        # 生成完毕仍然可能在 notify_complete 之前发生：先把任务进度推进（不切到 completed）
        self._update_and_send(
            stage=TaskStage.GENERATION,
            message=message,
            progress=90,
            update_tag_status=False
        )
        self._update_all_tags_status_with_custom("生成完毕")
        logger.info(f"通知生成完毕: {self.task_id}")
    
    def _update_all_tags_status_with_custom(self, status: str):
        """使用自定义状态更新所有标签"""
        if self.tag_ids:
            for tag_id in self.tag_ids:
                _executor.submit(self._update_tag_status_sync, tag_id, status)
        else:
            _executor.submit(self._update_tag_status_sync, self.task_id, status)


def create_progress_callback(
    task_id: str, 
    callback_url: Optional[str] = None,
    result_callback_url: Optional[str] = None,
    project_id: Optional[str] = None,
    tag_ids: Optional[List[str]] = None,
    auth_token: Optional[str] = None
) -> ProgressCallback:
    """
    创建进度回调实例
    
    Args:
        task_id: 任务ID
        callback_url: 进度状态回调URL
        result_callback_url: 结果回调URL
        project_id: 项目ID（用于标签状态更新）
        tag_ids: 标签ID列表
        auth_token: 认证Token（用于调用Java系统的Shiro认证）
        
    Returns:
        ProgressCallback 实例
    """
    return ProgressCallback(
        task_id=task_id, 
        callback_url=callback_url,
        result_callback_url=result_callback_url,
        project_id=project_id,
        tag_ids=tag_ids,
        auth_token=auth_token
    )
