"""
接口7: 文档后处理
POST /api/v1/document/clean - 清理文档（Content Control）

纯 python-docx 实现，不依赖 Windows Bridge / COM，可跨平台运行。
"""

# ========== 标准库导入 ==========
import asyncio
import logging

# ========== 第三方库导入 ==========
from fastapi import APIRouter, Form, HTTPException
from fastapi.responses import JSONResponse

# ========== 本地导入 ==========
from config import get_settings
from service.linux.postprocessing import DocumentCleaner

# ========== 模块配置 ==========
router = APIRouter()
settings = get_settings()
logger = logging.getLogger(__name__)


# ============================================================
# API 路由
# ============================================================

@router.post("/document/clean")
async def clean_document(
    file_path: str = Form(..., description="文件路径（相对于AAA目录）"),
    output_path: str = Form(None, description="输出文件路径（可选，默认覆盖原文件）"),
    remove_content_controls: bool = Form(True, description="是否清理Content Control控件"),
):
    """
    清理Word文档接口（本地 python-docx 实现）

    功能：
    1. 清理Content Control控件（保留控件内的内容）

    Args:
        file_path: 文件路径，相对于AAA目录
        output_path: 输出文件路径（可选），如果不提供则生成 _cleaned 后缀文件
        remove_content_controls: 是否清理Content Control（默认True）

    Returns:
        JSON响应，包含清理结果

    示例请求:
        POST /api/v1/document/clean
        Content-Type: application/x-www-form-urlencoded

        file_path=output/result_20251212.docx&remove_content_controls=true

    返回示例:
        {
            "success": true,
            "output_file": "AAA/output/result_20251212.docx",
            "controls_removed": 5,
        }
    """
    try:
        logger.info("=" * 70)
        logger.info("文档清理服务（Linux本地）")
        logger.info("=" * 70)
        logger.info(f"文件路径: {file_path}")
        logger.info(f"输出路径: {output_path or '(自动生成)'}")
        logger.info(f"清理控件: {remove_content_controls}")

        cleaner = DocumentCleaner()
        # 在线程池中执行同步的 python-docx 操作，避免阻塞事件循环
        result = await asyncio.to_thread(
            cleaner.clean,
            file_path=file_path,
            output_path=output_path,
            remove_content_controls=remove_content_controls,
        )

        if result.success:
            logger.info("文档清理成功")
            logger.info(f"  - 清理控件: {result.controls_removed} 个")
            logger.info(f"  - 输出文件: {result.output_file}")

            return JSONResponse({
                "success": True,
                "output_file": result.output_file,
                "controls_removed": result.controls_removed,
                "markers_removed": result.markers_removed,
            })
        else:
            error_msg = result.error or "未知错误"
            logger.error(f"文档清理失败: {error_msg}")
            raise HTTPException(status_code=500, detail=f"清理失败: {error_msg}")

    except FileNotFoundError as e:
        logger.error(f"文件不存在: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"文档清理失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"清理失败: {str(e)}")