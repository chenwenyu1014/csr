"""
接口4: 批量预处理
POST /api/v1/preprocessing/batch-simple

本模块只负责路由定义和参数验证，业务逻辑由 PreprocessingTaskService 处理。
"""

# ========== 标准库导入 ==========
import asyncio
import json
import logging
import os
import uuid
from datetime import datetime

# ========== 第三方库导入 ==========
from fastapi import APIRouter, Form, HTTPException
from fastapi.responses import JSONResponse

# ========== 本地导入 ==========
from config import get_settings

# ========== 模块配置 ==========
router = APIRouter()
settings = get_settings()
logger = logging.getLogger(__name__)


# ============================================================
# API 路由
# ============================================================

@router.post("/preprocessing/batch-simple")
async def preprocess_batch_simple(
    folder_path: str = Form(..., description="文件夹路径"),
    files: str = Form(..., description="文件列表JSON数组"),
    extract_regions: bool = Form(True),
    extract_assets: bool = Form(True),
    chunking_enabled: bool = Form(True),
    force_ocr: bool = Form(False),
    callback_url: str = Form("http://192.168.3.32:8088/ky/KM/kmFile/updateFileStatus"),
    combinationId: str | None = Form(None, description="组合ID"),
    project_desc: str | None = Form(None, description="项目背景"),
):
    """
    批量预处理接口（异步）
    
    功能：
    - 立即返回202 Accepted
    - 后台异步处理文件
    - 完成后回调通知
    
    请求示例：
        POST /api/v1/preprocessing/batch-simple
        
        folder_path=项目A/文档
        files=[{"id":"f1","filename":"test.docx"}]
    
    立即响应：
        {
          "success": true,
          "task_id": "batch_20241206_123456",
          "total_files": 1
        }
    
    完成后回调到callback_url：
        {
          "dataJson": [{
            "id": "f1",
            "status": "success",
            "preprocessed_json": "..."
          }]
        }
    """
    try:
        # 解析文件列表
        files_list = json.loads(files)
        
        # 设置环境变量
        _setup_environment(combinationId, project_desc)

        # 生成任务ID
        task_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        logger.info(f"接受预处理任务: {task_id}, 文件数: {len(files_list)}")
        
        # 检查 Windows Bridge 配置
        if not settings.windows_bridge_url:
            raise HTTPException(status_code=500, detail="需要配置WINDOWS_BRIDGE_URL用于预处理转发")
        
        # 启动后台异步任务
        from service.linux.preprocessing.preprocessing_task_service import get_preprocessing_task_service
        
        preprocessing_service = get_preprocessing_task_service()
        asyncio.create_task(preprocessing_service.process_files_async(
            task_id=task_id,
            files_list=files_list,
            folder_path=folder_path,
            force_ocr=force_ocr,
            extract_regions=extract_regions,
            extract_assets=extract_assets,
            chunking_enabled=chunking_enabled,
            callback_url=callback_url
        ))
        
        # 立即返回202
        return JSONResponse(
            status_code=202,
            content={
                "success": True,
                "message": "预处理任务已接受",
                "task_id": task_id,
                "total_files": len(files_list),
                "callback_url": callback_url
            }
        )
    
    except HTTPException:
        raise
    except json.JSONDecodeError as e:
        logger.error(f"文件列表JSON解析失败: {e}")
        raise HTTPException(status_code=400, detail=f"files参数JSON格式错误: {e}")
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"接受预处理任务失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 私有辅助函数
# ============================================================

def _setup_environment(combinationId: str | None, project_desc: str | None):
    """设置环境变量"""
    os.environ["CURRENT_COMBINATION_ID"] = str(combinationId or "")
    os.environ["CURRENT_PROJECT_DESC"] = str(project_desc or "")
