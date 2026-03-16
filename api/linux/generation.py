"""
接口2: 文本生成
POST /api/v1/flow/run-text       - 异步生成（立即返回，带回调）
GET  /api/v1/flow/task/{task_id} - 查询任务状态
GET  /api/v1/flow/tasks          - 列出任务

本模块只负责路由定义和参数验证，业务逻辑由 GenerationService 处理。
"""

# ========== 标准库导入 ==========
import json
import logging
from typing import Optional

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
# API 路由 - 任务查询
# ============================================================

@router.get("/flow/task/{task_id}")
async def get_task_status(task_id: str):
    """
    查询任务状态
    
    返回示例：
        {
            "task_id": "task_xxx",
            "callback_url": "http://...",
            "created_at": 1702345678.123,
            "started_at": 1702345679.456,
            "completed_at": null,
            "progress": {
                "stage": "generation",
                "message": "正在生成段落 3/10",
                "progress": 60,
                "current_step": 3,
                "total_steps": 10
          },
            "result": null,
            "error": null
        }
    """
    from service.linux.generation.task_manager import get_task_manager
    
    task_manager = get_task_manager()
    task = task_manager.get_task(task_id)
    
    if not task:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
    
    return JSONResponse(task.to_dict())


@router.get("/flow/tasks")
async def list_tasks(limit: int = 50):
    """
    列出最近的任务
    
    Args:
        limit: 返回数量限制，默认50
    """
    from service.linux.generation.task_manager import get_task_manager
    
    task_manager = get_task_manager()
    tasks = task_manager.list_tasks(limit=limit)
    
    return JSONResponse({
        "success": True,
        "total": len(tasks),
        "tasks": tasks
    })


# ============================================================
# API 路由 - 文本生成
# ============================================================

@router.post("/flow/run-text")
async def run_flow_text_only(
    request: Request,
    config_json: str | None = Form(None, description="主流程配置JSON"),
    config_file: UploadFile | None = File(None, description="主流程配置JSON文件"),
    base_data_dir: str | None = Form(None),
    output_dir: str | None = Form(None),
    combinationId: str | None = Form(None, description="组合ID"),
    project_desc: str | None = Form(None, description="项目背景"),
    project_id: str | None = Form(None, description="项目ID（用于回调）"),
    callback_url: str | None = Form(None, description="进度状态回调URL（轻量，推送阶段/进度）"),
    result_callback_url: str | None = Form(None, description="结果回调URL（重量，推送完整结果）"),
    skip_validation: bool = Form(True, description="是否跳过提取校验（默认True跳过以加快处理速度，设为False可提高提取质量但更慢）"),
):
    """
    文本生成接口（异步，带回调）
    
    功能：
    - 接收请求后【立即返回】task_id
    - 后台异步执行生成任务
    - 通过回调接口实时推送状态和结果
    
    回调机制：
    1. 标签状态更新（自动）：
       POST http://192.168.3.32:8088/ky/sys/projectTagsSourceInfo/updateStatus
       参数: id=段落ID, status=中文状态, project_id=项目ID
       
    2. 标签结果推送（自动）：
       POST http://192.168.3.32:8088/ky/sys/projectTagsSourceInfo/getTagAIResult
       参数: id=段落ID, dataJson=结果JSON字符串, project_id=项目ID
    
    请求示例：
        POST /api/v1/flow/run-text
        Content-Type: multipart/form-data
        
        config_json: {"paragraphs": [...], "project_desc": "..."}
        project_id: "proj_123456"
    
    返回示例：
        {
            "success": true,
            "task_id": "task_1702345678_abc12345",
            "message": "任务已创建，正在后台执行",
            "project_id": "proj_123456"
        }
    """
    try:
        # 解析配置JSON
        cfg_obj = await _parse_config(request, config_json, config_file)
        
        # 提取 project_id
        final_project_id = _extract_project_id(project_id, cfg_obj)
        logger.info(f"最终使用的 project_id: {final_project_id}")
        
        # 创建任务
        from service.linux.generation.task_manager import get_task_manager
        
        task_manager = get_task_manager()
        task_id = task_manager.create_task(
            callback_url=callback_url,
            config={
                "config": cfg_obj,
                "base_data_dir": base_data_dir,
                "output_dir": output_dir,
                "project_id": final_project_id,
                "combinationId": combinationId,
                "project_desc": project_desc,
                "callback_url": callback_url,
                "result_callback_url": result_callback_url,
            }
        )
        
        logger.info(f"创建异步任务: {task_id}, project_id={final_project_id}")
        
        # 获取认证Token
        auth_token = request.headers.get("X-Access-Token") or request.headers.get("Authorization")
        if auth_token:
            logger.info(f"获取到认证Token: {auth_token[:20]}...")
        
        # 启动后台线程执行生成任务
        from service.linux.generation.generation_service import get_generation_service
        
        generation_service = get_generation_service()
        generation_service.start_async_task(
            task_id=task_id,
            cfg_obj=cfg_obj,
            base_data_dir=base_data_dir,
            output_dir=output_dir,
            combinationId=combinationId,
            project_desc=project_desc,
            callback_url=callback_url,
            result_callback_url=result_callback_url,
            project_id=final_project_id,
            auth_token=auth_token,
            skip_validation=skip_validation
        )
        
        # 立即返回
        return JSONResponse({
            "success": True,
            "task_id": task_id,
            "status": "等待处理",
            "message": "任务已创建，正在后台执行",
            "project_id": final_project_id
        })
    
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"创建异步任务失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"创建任务失败: {str(e)}")


# ============================================================
# 私有辅助函数
# ============================================================

async def _parse_config(
    request: Request, 
    config_json: Optional[str], 
    config_file: Optional[UploadFile]
) -> dict:
    """
    解析配置JSON
    
    支持三种方式：
    1. config_json: Form字段中的JSON字符串
    2. config_file: 上传的JSON文件
    3. request.body: 直接POST的JSON请求体
    """
    cfg_obj: dict | None = None
    
    if config_json:
        try:
            cfg_obj = json.loads(config_json)
            logger.info(f"✓ 从 config_json 参数解析配置成功")
        except json.JSONDecodeError as e:
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=400, detail=f"config_json JSON格式错误: {str(e)}")
    
    elif config_file:
        try:
            raw = await config_file.read()
            cfg_obj = json.loads(raw.decode('utf-8'))
            logger.info(f"✓ 从 config_file 文件解析配置成功")
        except json.JSONDecodeError as e:
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=400, detail=f"config_file JSON格式错误: {str(e)}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=400, detail=f"读取config_file失败: {str(e)}")
    
    else:
        # 尝试从请求体读取
        try:
            body = await request.body()
            if body:
                cfg_obj = json.loads(body.decode('utf-8'))
                logger.info(f"✓ 从 request.body 解析配置成功")
        except json.JSONDecodeError as e:
            import traceback
            traceback.print_exc()
            pass
        except Exception as e:
            import traceback
            traceback.print_exc()
            pass
    
    if cfg_obj is None:
        raise HTTPException(
            status_code=400,
            detail="请提供以下任一参数：1) config_json (Form字段), 2) config_file (文件上传), 3) JSON格式的请求体"
        )
    
    if not isinstance(cfg_obj, dict):
        raise HTTPException(
            status_code=400,
            detail=f"配置必须是JSON对象，当前类型: {type(cfg_obj).__name__}"
        )
    
    return cfg_obj


def _extract_project_id(project_id: Optional[str], cfg_obj: dict) -> Optional[str]:
    """
    提取 project_id
    
    优先级：Form参数 > paragraphs[0].project_id > 顶层project_id
    """
    final_project_id = project_id
    
    if not final_project_id:
        # 尝试从 paragraphs[0].project_id 提取
        paragraphs = cfg_obj.get("paragraphs", [])
        if paragraphs and isinstance(paragraphs[0], dict):
            final_project_id = paragraphs[0].get("project_id")
            if final_project_id:
                logger.info(f"从 paragraphs[0].project_id 提取到 project_id: {final_project_id}")
    
    if not final_project_id:
        # 尝试从顶层提取
        final_project_id = cfg_obj.get("project_id")
        if final_project_id:
            logger.info(f"从顶层 project_id 提取到 project_id: {final_project_id}")
    
    return final_project_id
