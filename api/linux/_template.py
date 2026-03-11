"""
API路由模板
用于创建新的接口模块时参考
"""

from fastapi import APIRouter, Form, File, UploadFile, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse, Response
import logging

from config import get_settings

router = APIRouter()
settings = get_settings()
logger = logging.getLogger(__name__)


@router.post("/example")
async def example_endpoint(
    request: Request,
    param1: str = Form(..., description="参数1"),
    param2: str | None = Form(None, description="参数2"),
):
    """
    示例接口
    
    Args:
        request: FastAPI请求对象
        param1: 必需参数
        param2: 可选参数
    
    Returns:
        JSONResponse: 返回结果
    """
    try:
        # 实现逻辑
        result = {
            "success": True,
            "message": "处理成功",
            "data": {}
        }
        return JSONResponse(result)
    
    except Exception as e:
        logger.error(f"处理失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
