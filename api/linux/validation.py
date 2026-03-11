"""
接口1: 数据源校验
POST /api/v1/validation/data-source

本模块只负责路由定义和参数验证，业务逻辑由 ValidationService 处理。
"""

# ========== 标准库导入 ==========
import json
import logging
import os
from typing import Any, Dict

# ========== 第三方库导入 ==========
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
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

@router.post("/validation/data-source")
async def validate_data_source(
    request: Request,
    spec_json: str | None = Form(None, description="数据源校验规范(JSON字符串)"),
    spec_file: UploadFile | None = File(None, description="数据源校验规范(.json)"),
    task_name: str | None = Form(None, description="任务名称(可覆盖JSON中的task_name)"),
    prompt_id: str | None = Form("data_source_validation", description="提示词模板ID"),
    model_name: str | None = Form(None, description="指定模型名称"),
    return_prompt: bool | None = Form(False, description="是否在返回中包含完整提示词"),
    combinationId: str | None = Form(None, description="组合ID"),
    project_desc: str | None = Form(None, description="项目背景"),
) -> JSONResponse:
    """
    数据源校验接口
    
    功能：
    - 使用LLM对上传的文件进行智能校验
    - 判断文件是否符合要求
    - 返回合格/不合格文件列表
    
    请求示例：
        POST /api/v1/validation/data-source
        Content-Type: multipart/form-data或application/json
        
        {
          "task_name": "项目A数据校验",
          "categories": [{
            "name": "方案文档",
            "type": "docx",
            "num": 2,
            "rules": "必须包含研究方案",
            "files": ["研究方案v1.0.docx"]
          }]
        }
    
    返回示例：
        {
          "code": 200,
          "message": "校验成功",
          "data": {
            "name": {
              "方案文档": {
                "Qualified": ["研究方案v1.0.docx"],
                "Unqualified": []
              }
            }
          }
        }
    """
    try:
        # 解析规范JSON
        spec_obj = await _parse_spec(request, spec_json, spec_file)
        
        # 设置环境变量
        _setup_environment(combinationId, project_desc, spec_obj)

        # 调用校验服务（异步版本）
        from service.linux.validation.validation_service import validation_service
        
        logger.info(f"开始数据源校验（异步），任务名称: {task_name or spec_obj.get('task_name', '未命名')}")
        svc_result = await validation_service.validate_pure_async(
            spec_obj, 
            task_name=task_name
        )
        
        # 处理结果
        if not svc_result.get("success"):
            logger.warning("数据源校验失败: %s", svc_result.get("error"))
            return JSONResponse({
                "code": 500,
                "message": "校验失败",
                "error": svc_result.get("error"),
                "details": svc_result.get("details"),
                "prompt_path": svc_result.get("prompt_path"),
                "model_output_path": svc_result.get("model_output_path"),
            })
        
        # 返回结果
        return JSONResponse({
            "code": 200,
            "message": "校验成功",
            "data": svc_result.get("data"),
            "summary": None,
            "pre_checks": None,
            "prompt_path": svc_result.get("prompt_path"),
            "model_output_path": svc_result.get("model_output_path"),
        })
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"数据源校验失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"校验失败: {str(e)}")


# ============================================================
# 私有辅助函数
# ============================================================

async def _parse_spec(
    request: Request, 
    spec_json: str | None, 
    spec_file: UploadFile | None
) -> Dict[str, Any]:
    """
    解析规范JSON
    
    支持三种方式：
    1. spec_json: Form字段中的JSON字符串
    2. spec_file: 上传的JSON文件
    3. request.body: 直接POST的JSON请求体
    """
    spec_obj: Dict[str, Any] | None = None
    
    if spec_json:
        try:
            spec_obj = json.loads(spec_json)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"spec_json 无法解析: {e}")
    
    elif spec_file is not None:
        if not (spec_file.filename or '').lower().endswith('.json'):
            raise HTTPException(status_code=415, detail="spec_file 必须为 .json")
        try:
            raw = await spec_file.read()
            spec_obj = json.loads(raw.decode('utf-8', errors='ignore'))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"spec_file 无法解析: {e}")
    
    else:
        try:
            body = await request.body()
            if body:
                spec_obj = json.loads(body.decode('utf-8', errors='ignore'))
        except Exception:
            spec_obj = None
    
    if not isinstance(spec_obj, dict):
        raise HTTPException(
            status_code=400,
            detail="必须提供合法的JSON对象作为数据源校验规范"
        )
    
    return spec_obj


def _setup_environment(
    combinationId: str | None, 
    project_desc: str | None, 
    spec_obj: Dict[str, Any]
):
    """设置环境变量"""
    # 组合ID
    _cid = combinationId or spec_obj.get("combinationId")
    os.environ["CURRENT_COMBINATION_ID"] = str(_cid or "")
    
    # 项目背景（优先级：接口参数 > JSON中的project_desc）
    _desc = (project_desc or "") or spec_obj.get("project_desc", "")
    os.environ["CURRENT_PROJECT_DESC"] = str(_desc)
