"""
预处理任务服务模块

功能说明：
- 封装文件预处理的业务逻辑
- 提供异步批量处理能力
- 处理 Windows Bridge 调用和回调

主要类：
- PreprocessingTaskService: 预处理任务服务类
"""

# ========== 标准库导入 ==========
import hashlib
import json
import logging
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

# ========== 本地导入 ==========
from config import get_settings

# ========== 模块配置 ==========
logger = logging.getLogger(__name__)
settings = get_settings()


class PreprocessingTaskService:
    """
    预处理任务服务类
    
    封装了文件预处理的核心业务逻辑，支持批量异步处理。
    """
    
    def __init__(self):
        """初始化预处理任务服务"""
        self.settings = settings
    
    # ============================================================
    # 公开方法
    # ============================================================
    
    async def process_files_async(
        self,
        task_id: str,
        files_list: List[Dict[str, Any]],
        folder_path: str,
        force_ocr: bool = False,
        extract_regions: bool = True,
        extract_assets: bool = True,
        chunking_enabled: bool = True,
        callback_url: Optional[str] = None
    ):
        """
        异步处理文件列表
        
        使用异步 HTTP 调用 Windows Bridge，不阻塞事件循环
        
        Args:
            task_id: 任务ID
            files_list: 文件列表
            folder_path: 文件夹路径
            force_ocr: 是否强制OCR
            extract_regions: 是否提取区域
            extract_assets: 是否提取资源
            chunking_enabled: 是否启用分块
            callback_url: 回调URL
        """
        from service.linux.bridge.windows_bridge_client import WindowsBridgeClient
        
        logger.info(f"🚀 [异步] 开始后台处理任务: {task_id}")
        
        results = []
        succeeded = 0
        failed = 0
        
        # Windows Bridge 客户端
        client = WindowsBridgeClient(self.settings.windows_bridge_url)
        
        for file_item in files_list:
            # 解析文件项
            filename, file_id = self._parse_file_item(file_item)
            if not filename:
                continue
            
            # 计算文件 SHA256
            sha256_val = self._compute_file_sha256(folder_path, filename)
            
            try:
                logger.info(f"  [异步] 处理文件: {filename}")
                file_path_rel = str((Path(folder_path) / filename)).replace('\\', '/')
                
                # 使用异步方法调用 Windows Bridge
                data = await client.preprocess_file_async(
                    file_path=file_path_rel,
                    folder_path=folder_path,
                    filename=filename,
                    file_id=file_id,
                    force_ocr=force_ocr,
                    extract_regions=extract_regions,
                    extract_assets=extract_assets,
                    chunking_enabled=chunking_enabled
                )
                
                if data is None:
                    raise RuntimeError("Windows Bridge 无响应")
                
                if not data.get('success'):
                    raise RuntimeError(
                        data.get('error_message') or data.get('error') or 'Windows Bridge处理失败'
                    )
                
                # 成功
                results.append({
                    "id": file_id or filename,
                    "filename": filename,
                    "status": "success",
                    "sha256": sha256_val,
                })
                succeeded += 1
                logger.info(f"  ✅ [异步] 成功: {filename}")
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"  ❌ [异步] 失败：{filename}, 错误：{e}", exc_info=True)
                results.append({
                    "id": file_id or filename,
                    "filename": filename,
                    "status": "fail",
                    "sha256": "",
                    "err_msg": "处理异常",
                    "error": error_msg,
                })
                failed += 1

        logger.info(f"✅ [异步] 任务完成: {task_id}, 成功: {succeeded}, 失败: {failed}")

        # 异步回调通知
        if callback_url:
            await self._send_callback(callback_url, results)

    # ============================================================
    # 私有辅助方法
    # ============================================================
    
    def _parse_file_item(self, file_item) -> tuple:
        """
        解析文件项
        
        支持两种格式：
        - 字符串: 直接作为文件名
        - 字典: {"id": "xxx", "filename": "xxx"}
        
        Returns:
            (filename, file_id) 元组
        """
        if isinstance(file_item, str):
            return file_item, None
        elif isinstance(file_item, dict):
            filename = file_item.get('filename', file_item.get('name', ''))
            file_id = file_item.get('id', file_item.get('file_id', ''))
            return filename, file_id
        else:
            logger.warning(f"  ⚠️ 无效的文件项: {file_item}")
            return None, None
    
    def _compute_file_sha256(self, folder_path: str, filename: str) -> str:
        """计算文件 SHA256"""
        sha256_val = ""
        try:
            src_path = Path('AAA') / 'project_data' / folder_path / filename
            h = hashlib.sha256()
            with open(src_path, 'rb') as rf:
                for chunk in iter(lambda: rf.read(1024 * 1024), b''):
                    h.update(chunk)
            sha256_val = h.hexdigest()
        except Exception as e:
            logger.debug(f"计算sha256失败: {filename}, {e}")
        return sha256_val
    
    async def _send_callback(self, callback_url: str, results: List[dict]):
        """
        发送回调通知
        
        Args:
            callback_url: 回调URL
            results: 处理结果列表

        回调格式 (application/json):
        POST {callback_url}
        Content-Type: application/json

        {
            "id": "file_001",
            "filename": "document.pdf",
            "status": "success",
            "sha256": "a1b2c3d4e5f6...",
            "err_msg": "",
            "error": ""
        }

        失败场景:
        {
            "id": "file_001",
            "filename": "document.pdf",
            "status": "fail",
            "sha256": "",
            "err_msg": "处理异常",
            "error": "File format not supported"
        }
        """
        try:
            logger.info(f"📤 [异步] 回调通知：{callback_url}")

            # 逐个文件发送回调（每个文件一个请求）
            for result in results:
                await self._send_single_callback(callback_url, result)

        except Exception as e:
            logger.error(f"[异步] 回调失败：{e}", exc_info=True)

    async def _send_single_callback(self, callback_url: str, result: dict):
        """
        发送单个文件的回调通知

        Args:
            callback_url: 回调 URL
            result: 单个文件的处理结果
        """
        try:
            logger.info(f"📤 [异步] 回调通知: {callback_url}")

            # 使用 aiohttp 进行异步回调
            try:
                import aiohttp

                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        callback_url,
                        json=result,
                        headers={"Content-Type": "application/json"},
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as resp:
                        status = resp.status
                        try:
                            resp_json = await resp.json()
                        except Exception:
                            resp_json = None

                        ok = (status == 200) and (
                            not isinstance(resp_json, dict) or bool(resp_json.get("success", True))
                        )
                        if ok:
                            logger.info(f"✅ [异步] 回调成功：id={result.get('id')}, status={result.get('status')}")
                        else:
                            text = await resp.text()
                            logger.warning(f"[异步] 回调失败: status={status}, body={text[:200]}")
            except ImportError:
                # 如果 aiohttp 不可用，回退到同步 requests
                import requests
                response = requests.post(
                    callback_url,
                    json=result,
                    headers={"Content-Type": "application/json"},
                    timeout=30
                )
                logger.info(f"回调响应 (同步回退): {response.status_code}")

        except Exception as e:
            logger.error(f"[异步] 回调失败: {e}", exc_info=True)


# ============================================================
# 全局单例（线程安全）
# ============================================================

_preprocessing_task_service: Optional[PreprocessingTaskService] = None
_preprocessing_task_service_lock = threading.Lock()


def get_preprocessing_task_service() -> PreprocessingTaskService:
    """获取预处理任务服务单例（线程安全）"""
    global _preprocessing_task_service
    if _preprocessing_task_service is None:
        with _preprocessing_task_service_lock:
            if _preprocessing_task_service is None:
                _preprocessing_task_service = PreprocessingTaskService()
    return _preprocessing_task_service
