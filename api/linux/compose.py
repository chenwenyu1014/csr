"""
接口3: 完整流程（异步）
POST /api/v1/documents/compose       - 异步执行（立即返回状态，完成后回调）
GET  /api/v1/documents/compose/task/{task_id} - 查询任务状态

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

@router.get("/documents/compose/task/{task_id}")
async def get_compose_task_status(task_id: str):
    """
    查询完整流程任务状态
    
    返回示例：
        {
            "task_id": "task_xxx",
            "created_at": 1702345678.123,
            "started_at": 1702345679.456,
            "completed_at": 1702345700.789,
            "progress": {
                "stage": "completed",
                "message": "生成完成",
                "progress": 100
            },
            "result": {...},
            "error": null
        }
    """
    from service.linux.generation.task_manager import get_task_manager
    
    task_manager = get_task_manager()
    task = task_manager.get_task(task_id)
    
    if not task:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
    
    return JSONResponse(task.to_dict())


# ============================================================
# API 路由 - 完整流程
# ============================================================

@router.post("/documents/compose")
async def compose_document(
    request: Request,
    config_json: str | None = Form(None, description="段落配置JSON字符串"),
    config_file: UploadFile | None = File(None, description="段落配置JSON文件"),
    template_file: str | None = Form(None, description="模板文件路径"),
    filename: str | None = Form(None, description="自定义输出文件名"),
    base_data_dir: str | None = Form(None),
    output_dir: str | None = Form(None),
    combinationId: str | None = Form(None, description="组合ID"),
    project_desc: str | None = Form(None, description="项目背景"),
    project_id: str | None = Form(None, description="项目ID"),
    callback_url: str | None = Form(
        "http://192.168.3.32:8088/ky/sys/projectCreateManage/getReportAIResult", 
        description="完成时回调URL"
    ),
    skip_validation: bool = Form(True, description="是否跳过提取校验（默认True跳过以加快处理速度，设为False可提高提取质量但更慢）"),
):
    """
    【接口3】完整流程接口（生成 + 插入）- 异步版本
    
    ===== 执行方式 =====
    - 异步执行，接收请求后【立即返回】task_id和状态
    - 后台异步执行生成和插入任务
    - 完成或失败时，通过 callback_url 回调结果
    
    ===== 请求参数 =====
    - config_json: 段落配置JSON字符串（与config_file二选一）
    - config_file: 段落配置JSON文件（与config_json二选一）
    - template_file: Word模板文件路径，相对于AAA目录（可选）
    - callback_url: 完成时回调URL（可选）
    - project_id: 项目ID（可选）
    
    ===== 立即返回格式 =====
    {
      "success": true,
      "task_id": "task_1702345678_abc12345",
      "status": "处理中",
      "message": "任务已创建，正在后台执行"
    }
    
    ===== 完成时回调格式 =====
    POST {callback_url}
    Content-Type: application/json
    
    成功时：
    {
      "success": true,
      "status": "完成",
      "task_id": "task_xxx",
      "project_id": "proj_xxx",
      "run_dir": "输出目录路径",
      "output_file": "最终Word文件路径",
      "generated_content": {...},
      "traceability": [...],
      "insertion_config": {...}
    }
    
    失败时：
    {
      "success": false,
      "status": "失败",
      "task_id": "task_xxx",
      "project_id": "proj_xxx",
      "error": "错误信息"
    }
    
    ===== 查询任务状态 =====
    GET /api/v1/documents/compose/task/{task_id}
    """
    try:
        logger.info("=" * 60)
        logger.info("【接口3】完整流程接口（异步版本）")
        logger.info("=" * 60)
        
        # 解析配置JSON
        cfg_obj = await _parse_config(request, config_json, config_file)
        
        # 提取 project_id
        final_project_id = _extract_project_id(project_id, cfg_obj)
        logger.info(f"project_id: {final_project_id}")
        logger.info(f"callback_url: {callback_url}")
        
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
                "template_file": template_file,
                "filename": filename,
                "callback_url": callback_url
            }
        )
        
        logger.info(f"创建异步任务: {task_id}")
        
        # 获取认证Token
        auth_token = request.headers.get("X-Access-Token") or request.headers.get("Authorization")
        if auth_token:
            logger.info(f"获取到认证Token: {auth_token[:20]}...")
        
        # 启动后台线程执行完整流程任务
        from service.linux.generation.generation_service import get_generation_service
        
        generation_service = get_generation_service()
        generation_service.start_compose_async_task(
            task_id=task_id,
            cfg_obj=cfg_obj,
            base_data_dir=base_data_dir,
            output_dir=output_dir,
            combinationId=combinationId,
            project_desc=project_desc,
            template_file=template_file,
            project_id=final_project_id,
            auth_token=auth_token,
            callback_url=callback_url,
            skip_validation=skip_validation
        )
        
        # 立即返回
        return JSONResponse({
            "success": True,
            "task_id": task_id,
            "status": "处理中",
            "message": "任务已创建，正在后台执行"
        })
    
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"创建完整流程异步任务失败: {e}", exc_info=True)
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
            raise HTTPException(status_code=400, detail=f"config_json JSON格式错误: {str(e)}")
    
    elif config_file:
        try:
            raw = await config_file.read()
            cfg_obj = json.loads(raw.decode('utf-8'))
            logger.info(f"✓ 从 config_file 文件解析配置成功")
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"config_file JSON格式错误: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"读取config_file失败: {str(e)}")
    
    else:
        # 尝试从请求体读取
        try:
            body = await request.body()
            if body:
                cfg_obj = json.loads(body.decode('utf-8'))
                logger.info(f"✓ 从 request.body 解析配置成功")
        except json.JSONDecodeError:
            pass
        except Exception:
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
