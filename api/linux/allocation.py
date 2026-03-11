"""
接口6: 数据源分配（数据匹配）
POST /api/v1/datasource/allocate

本模块只负责路由定义和参数验证，业务逻辑由 AllocationService 处理。
"""

# ========== 标准库导入 ==========
import json
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

@router.post("/datasource/allocate")
async def allocate_datasource(
    request: Request,
    items_json: str = Form(..., description="请求数据JSON数组（[{tagId, data:[...]}]）"),
    combinationId: str | None = Form(None, description="组合ID"),
    project_desc: str | None = Form(None, description="项目背景")
):
    """
    数据源分配接口（批量匹配版，新版Schema）
    
    功能：
    - 仅支持数组输入：每个元素为一个分组 {"tagId": "...", "data": [ ... ]}
    - 对 data[] 的每个项：使用 first_match_logic + source_full_file[].fileName 调用模型匹配
    - 在原项基础上新增 matched_files（字符串数组）
    
    请求示例（新版）：
        POST /api/v1/datasource/allocate
        Content-Type: application/json
        
        [
          {
            "tagId": "4028941b9af839c8019af839cd5c0008",
            "data": [
              {
                "id": "1764922037671_rnz2q1c01",
                "first_match_logic": "临床研究方案",
                "source_full_file": [
                  {"fileName": "I期CSR模版-V0.1-20250311.docx", "fileId": "..."}
                ]
              }
            ]
          }
        ]
    
    返回示例：
        {
          "code": 200,
          "message": "匹配成功",
          "data": [
            {
              "tagId": "4028941b9af839c8019af839cd5c0008",
              "data": [
                {
                  "id": "1764922037671_rnz2q1c01",
                  "first_match_logic": "临床研究方案",
                  "source_full_file": [...],
                  "matched_files": ["I期CSR模版-V0.1-20250311.docx"]
                }
              ]
            }
          ]
        }
    """
    try:
        # 设置环境变量
        _setup_environment(combinationId, project_desc)

        # 解析 items_json
        data = _parse_items_json(items_json)
        
        logger.info(f"数据匹配请求(新版Schema): {len(data)} 个分组")
        
        # 使用服务层处理
        from service.linux.allocation.allocation_service import get_allocation_service
        
        allocation_service = get_allocation_service()
        result_groups = allocation_service.allocate_batch(data)

        return JSONResponse({
            "code": 200,
            "message": "匹配成功",
            "data": result_groups
        })
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"数据匹配失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 私有辅助函数
# ============================================================

def _setup_environment(combinationId: str | None, project_desc: str | None):
    """设置环境变量"""
    os.environ["CURRENT_COMBINATION_ID"] = str(combinationId or "")
    os.environ["CURRENT_PROJECT_DESC"] = str(project_desc or "")


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
