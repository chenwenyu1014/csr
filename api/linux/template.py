"""
模板处理接口
POST /api/v1/template/process  - 异步处理模板文件

功能说明：
- 处理Word模板文件，标记所有表格
- 导出每个表格为独立Word文件
- 转换为HTML和图片
- 通过回调返回结果

本模块只负责路由定义和参数验证，业务逻辑由 TemplateProcessingService 处理。
"""

# ========== 标准库导入 ==========
import logging
import time
import uuid
import os

# ========== 第三方库导入 ==========
from fastapi import APIRouter, Form, HTTPException
from fastapi.responses import JSONResponse

# ========== 模块配置 ==========
router = APIRouter()
logger = logging.getLogger(__name__)


# ============================================================
# API 路由
# ============================================================

@router.post("/template/process")
async def process_template(
    template_file: str = Form(..., description="模板文件路径（相对于AAA/）"),
    output_dir: str | None = Form(None, description="输出目录"),
    callback_base_url: str | None = Form(os.getenv("CALLBACK_BASE_URL"), description="回调基础URL"),
    file_id: str | None = Form(None, description="文件ID（用于回调标识）"),
    template_id: str | None = Form(None, description="模板ID用于回调标识）"),
    auth_token: str | None = Form(None, description="认证Token"),
):
    """
    模板处理接口（异步）

    功能：
    - 接收请求后【立即返回】task_id
    - 后台异步执行模板处理任务
    - 通过回调接口返回处理结果

    处理流程：
    1. 标记Word文档中的所有表格（添加{{Table_N_Start/End}}标记）
    2. 获取每个表格的标题（表格前一段落文本）
    3. 导出每个表格区域为独立Word文件
    4. 转换为HTML和PNG图片
    5. 删除中间文件，保留标记文件和图片

    请求示例：
        POST /api/v1/template/process
        template_file=项目A/模板.docx
        callback_url=http://xxx/callback

    立即响应：
        {
            "success": true,
            "task_id": "template_20241206_123456",
            "message": "任务已创建，正在后台执行"
        }

    回调结果：
        {
            "success": true,
            "task_id": "template_xxx",
            "file_id": "xxx",
            "template_id": "xxx",
            "result": {
                "file": "原始路径",
                "processed_file": "处理后路径",
                "resources": [{"title": "...", "html": "...", "pic": "..."}]
                "paragraphs_ids":[{"paragraphs_ids":"xxx",...},{}]
            },
            "error": null
        }
    """
    try:
        # 生成任务ID
        task_id = f"template_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        logger.info(f"接受模板处理任务: {task_id}, 文件: {template_file}")

        # 使用后台线程执行任务
        from service.linux.template.template_service import get_template_service
        template_service = get_template_service()
        template_service.start_async_task(
            task_id=task_id,
            template_file=template_file,
            output_dir=output_dir,
            callback_base_url=callback_base_url,
            file_id=file_id,
            template_id=template_id,
            auth_token=auth_token
        )

        # 立即返回
        return JSONResponse({
            "success": True,
            "task_id": task_id,
            "message": "任务已创建，正在后台执行"
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"接受模板处理任务失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/template/download")
async def download_template(
    file_path: str = Form(..., description="Word文件路径（相对于AAA/）"),
    paragraph_ids: str = Form(..., description="paragraph_ids JSON字符串"),
    output_file: str | None = Form(None, description="输出文件路径（可选）"),
):
    """
    模板下载处理

    将经过模板处理后带有内容控件的Word文档，
    根据传入的paragraph_ids信息，将控件转换回批注，
    同时清理控件和模板标记。

    请求示例:
        POST /api/v1/template/download
        file_path=AAA/Preprocessing/Template/模板/模板_marked.docx
        paragraph_ids=[{"paragraphId":"方案编号",...}, ...]

    响应:
        {
            "success": true,
            "output_file": "AAA/.../模板_commented.docx",
            "comments_created": 15,
            "error": null
        }
    """
    try:
        import json as _json
        from service.linux.template.control_to_comment_converter import convert_controls_to_comments

        # 解析 paragraph_ids JSON
        try:
            paragraph_ids_list = _json.loads(paragraph_ids)
            if not isinstance(paragraph_ids_list, list):
                raise HTTPException(status_code=400, detail="paragraph_ids 必须是 JSON 数组")
        except _json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"paragraph_ids JSON 解析失败: {e}")

        logger.info(f"接受模板下载任务: 文件={file_path}, 段落标签数={len(paragraph_ids_list)}")

        result = convert_controls_to_comments(
            file_path=file_path,
            paragraph_ids=paragraph_ids_list,
            output_path=output_file,
        )

        if not result["success"]:
            failed_step = result.get("failed_step", "")
            error_msg = result.get("error", "未知错误")
            if failed_step:
                error_msg = f"[{failed_step}] {error_msg}"
            return JSONResponse({
                "success": False,
                "output_file": None,
                "comments_created": 0,
                "error": error_msg,
            }, status_code=200)

        return JSONResponse({
            "success": True,
            "output_file": result["output_file"],
            "comments_created": result["comments_created"],
            "error": None,
        })

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"模板下载处理失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
