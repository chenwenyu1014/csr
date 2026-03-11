"""
接口7: 文档后处理
POST /api/v1/document/clean - 清理文档（Content Control + 首行水印）

本模块只负责路由定义和参数验证，通过 Windows Bridge 执行实际清理操作。
"""

# ========== 标准库导入 ==========
import logging

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

@router.post("/document/clean")
async def clean_document(
    file_path: str = Form(..., description="文件路径（相对于AAA目录）"),
    output_path: str = Form(None, description="输出文件路径（可选，默认覆盖原文件）"),
    remove_first_line: bool = Form(True, description="是否删除首行（水印）"),
    remove_content_controls: bool = Form(True, description="是否清理Content Control控件"),
):
    """
    清理Word文档接口（转发到Windows Bridge）
    
    功能：
    1. 清理Content Control控件（保留控件内的内容）
    2. 删除文件首行（通常是水印）
    
    Args:
        file_path: 文件路径，相对于AAA目录
        output_path: 输出文件路径（可选），如果不提供则覆盖原文件
        remove_first_line: 是否删除首行（默认True）
        remove_content_controls: 是否清理Content Control（默认True）
    
    Returns:
        JSON响应，包含清理结果
        
    示例请求:
        POST /api/v1/document/clean
        Content-Type: application/x-www-form-urlencoded
        
        file_path=output/result_20251212.docx&remove_first_line=true&remove_content_controls=true
    
    返回示例:
        {
            "success": true,
            "output_file": "AAA/output/result_20251212.docx",
            "controls_removed": 5,
            "first_line_removed": true
        }
    """
    try:
        logger.info("=" * 70)
        logger.info("文档清理服务（Linux转发）")
        logger.info("=" * 70)
        logger.info(f"📄 文件路径: {file_path}")
        logger.info(f"📄 输出路径: {output_path or '(覆盖原文件)'}")
        logger.info(f"🔧 删除首行: {remove_first_line}")
        logger.info(f"🔧 清理控件: {remove_content_controls}")
        
        # 调用 Windows Bridge（异步）
        from service.linux.bridge.windows_bridge_client import WindowsBridgeClient
        
        client = WindowsBridgeClient()
        
        if not client.is_configured():
            logger.error("Windows Bridge未配置")
            raise HTTPException(
                status_code=503, 
                detail="Windows Bridge服务未配置，请设置 WINDOWS_BRIDGE_URL 环境变量"
            )
        
        logger.info(f"🌐 Windows Bridge URL: {client.base_url}")
        
        result = await client.clean_document_async(
            file_path=file_path,
            output_path=output_path,
            remove_first_line=remove_first_line,
            remove_content_controls=remove_content_controls
        )
        
        if result is None:
            raise HTTPException(status_code=500, detail="调用Windows Bridge失败")
        
        if result.get("success"):
            logger.info("✅ 文档清理成功")
            logger.info(f"   - 清理控件: {result.get('controls_removed', 0)} 个")
            logger.info(f"   - 删除首行: {result.get('first_line_removed', False)}")
            logger.info(f"   - 输出文件: {result.get('output_file', '')}")
            
            return JSONResponse({
                "success": True,
                "output_file": result.get("output_file", ""),
                "controls_removed": result.get("controls_removed", 0),
                "first_line_removed": result.get("first_line_removed", False)
            })
        else:
            error_msg = result.get("error", "未知错误")
            logger.error(f"❌ 文档清理失败: {error_msg}")
            raise HTTPException(status_code=500, detail=f"清理失败: {error_msg}")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"文档清理失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"清理失败: {str(e)}")
