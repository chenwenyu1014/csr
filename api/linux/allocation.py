"""
接口6: 文件分配（文件匹配）
POST /api/v1/datasource/allocate

本模块只负责路由定义和参数验证，业务逻辑由 AllocationService 处理。
异步非阻塞执行，单个标签完成后通过 /callBackTagMatchFile 回调，
全部完成后通过 /callBackAllTagMatchFile 回调全量结果。
"""

# ========== 标准库导入 ==========
import asyncio
import json
import logging
import os
import uuid
from datetime import datetime

# ========== 第三方库导入 ==========
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import JSONResponse

# ========== 本地导入 ==========
from utils.context_manager import set_project_desc, set_combination_id
from config import get_settings

# ========== 模块配置 ==========
router = APIRouter()
settings = get_settings()
logger = logging.getLogger(__name__)


# ============================================================
# API 路由
# ============================================================

@router.post("/datasource/allocate")
async def allocate_datasource(
    request: Request,
    items_json: str = Form(..., description="请求数据JSON数组（[{tagId, data:[...]}]）"),
    combinationId: str | None = Form(None, description="组合ID"),
    project_desc: str | None = Form(None, description="项目背景")
):
    """
    数据源分配接口（异步非阻塞，完成后通过回调通知）
    
    功能：
    - 仅支持数组输入：每个元素为一个分组 {"tagId": "...", "data": [ ... ]}
    - 对 data[] 的每个项：使用 first_match_logic + source_full_file[].fileName 调用模型匹配
    - 在原项基础上新增 matched_files（字符串数组）
    - 单个标签完成 → 回调 /ky/sys/projectTagsSourceInfo/callBackTagMatchFile
    - 全部标签完成 → 回调 /ky/sys/projectTagsSourceInfo/callBackAllTagMatchFile
    
    返回示例（立即返回）：
        {
          "code": 202,
          "success": true,
          "message": "数据匹配任务已接受",
          "task_id": "allocation_20250605_123456_abc12345"
        }
        
    全量回调示例：
        POST /ky/sys/projectTagsSourceInfo/callBackAllTagMatchFile
        {
          "code": 200,
          "message": "匹配成功",
          "data": [{"tagId": "...", "data": [{"id": "...", "matched_files": [...]}]}]
        }
    """
    try:
        # 设置环境变量
        _setup_environment(combinationId, project_desc)

        # 解析 items_json
        data = _parse_items_json(items_json)

        logger.info(f"数据匹配请求: {len(data)} 个段落标签")

        # 生成任务ID
        task_id = f"allocation_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

        # 启动后台异步任务（非阻塞）
        from service.linux.file_service.allocation_service import get_allocation_service

        allocation_service = get_allocation_service()
        asyncio.create_task(
            allocation_service.allocate_batch_async(data, task_id=task_id)
        )

        logger.info(f"数据匹配任务已提交 - task_id={task_id}")

        return JSONResponse(
            status_code=202,
            content={
                "code": 202,
                "success": True,
                "message": "数据匹配任务已接受",
                "task_id": task_id,
            },
        )
    
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"数据匹配失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 私有辅助函数
# ============================================================

def _setup_environment(combinationId: str | None, project_desc: str | None):
    """设置项目上下文（contextvars，每请求线程独立）"""
    set_combination_id(str(combinationId or ""))
    set_project_desc(str(project_desc or ""))


def _parse_items_json(items_json: str) -> list:
    """解析 items_json 参数"""
    try:
        data = json.loads(items_json)
        if not isinstance(data, list):
            raise HTTPException(
                status_code=400, 
                detail="items_json必须是JSON数组（[{tagId, data:[...]}]）"
            )
        return data
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"items_json解析失败: {e}")
