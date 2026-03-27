"""
接口5: 模板插入
POST /api/v1/template/insert

本模块只负责路由定义和参数验证，通过 Windows Bridge 执行实际插入操作。
"""

# ========== 标准库导入 ==========
import logging
import os

# ========== 第三方库导入 ==========
from fastapi import APIRouter, Form, HTTPException, Request
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

@router.post("/template/insert")
async def insert_content_to_template(
    request: Request,
    template_file: str = Form(None, description="模板文件路径（相对于AAA）"),
    data_json: str = Form(None, description="生成结果JSON"),
    project_desc: str | None = Form(None, description="项目背景"),
):
    """
    模板插入接口（Linux转发到Windows Bridge）
    
    功能：
    - 将生成内容插入Word模板
    - Linux环境自动转发到Windows Bridge
    - 返回最终的.docx文件路径
    
    请求示例：
        POST /api/v1/template/insert
        
        template_file=Template/模板.docx
        data_json={"generation_results":[...],"resource_mappings":{}}
    
    返回示例：
        {
          "success": true,
          "output_file": "AAA/output/result_xxx.docx",
          "inserted_controls": 5
        }
    """
    try:
        import traceback
        # 参数验证
        if not template_file or not data_json:
            logger.error("缺少必需参数")
            return {"success": False, "error": "缺少必需参数"}
            # raise HTTPException(status_code=400, detail="缺少必需参数")
        
        logger.info(f"模板插入请求: template={template_file}")
        
        # 设置环境变量
        os.environ["CURRENT_PROJECT_DESC"] = str(project_desc or "")
        
        # 检查 Windows Bridge 配置
        if not settings.windows_bridge_url:
            logger.error("需要配置WINDOWS_BRIDGE_URL用于Word文档处理")
            return {"success": False, "error": "需要配置WINDOWS_BRIDGE_URL用于Word文档处理"}

            # raise HTTPException(
            #     status_code=500,
            #     detail="需要配置WINDOWS_BRIDGE_URL用于Word文档处理"
            # )
        
        # 调用 Windows Bridge（异步）
        from service.linux.bridge.windows_bridge_client import WindowsBridgeClient
        
        client = WindowsBridgeClient(settings.windows_bridge_url)
        
        logger.info(f"📡 [异步] 转发到Windows Bridge")
        logger.info(f"   模板: {template_file}")
        
        result = await client.insert_content_async(
            template_file=template_file,
            data_json=data_json
        )
        
        if result is None:
            logger.error(f"❌ Windows Bridge调用失败")
            traceback.print_exc()
            return {"success": False, "error": "Windows Bridge调用失败"}
            # raise HTTPException(
            #     status_code=500,
            #     detail="Windows Bridge插入失败: 无响应"
            # )
        
        if not result.get("success"):
            logger.error(f"❌ Windows Bridge返回错误: {result.get('error')}")
            traceback.print_exc()
            return {"success": False, "error": "Windows Bridge插入失败"}
            # raise HTTPException(
            #     status_code=500,
            #     detail=f"Windows Bridge插入失败: {result.get('error', '未知错误')}"
            # )
        
        logger.info("✅ [异步] Windows Bridge插入成功")
        
        return JSONResponse(result)
    
    except HTTPException as e:
        logger.error(f"模板插入失败: {e}", exc_info=True)
        traceback.print_exc()
        return {"success": False, "error": "模板插入失败"}
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"模板插入失败: {e}", exc_info=True)
        return {"success": False, "error": "模板插入失败"}
        # raise HTTPException(status_code=500, detail=str(e))
