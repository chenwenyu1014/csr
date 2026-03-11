"""
CSR API Service - 主应用入口

这是CSR文档生成系统的FastAPI主应用，包含以下6个核心接口：
1. validation - 数据源验证接口
2. generation - 内容生成接口
3. compose - 文档合成接口
4. preprocessing - 预处理接口
5. insertion - 内容插入接口
6. allocation - 数据分配接口
7. postprocessing - 后处理接口

主要功能：
- 提供RESTful API接口
- 请求ID追踪和日志记录
- CORS跨域支持
- 健康检查端点
"""

import os
import time
import uuid
import logging
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv, find_dotenv

from config import get_settings
from utils import setup_json_logging, request_id_ctx

# ========== 环境变量预加载 ==========
# 预加载环境变量，优先查找当前工作目录下的.env文件
# 如果找不到则回退到项目根目录
try:
    _env_path = find_dotenv(usecwd=True)
    if not _env_path:
        _env_path = str(Path(__file__).resolve().parents[2] / ".env")
    load_dotenv(_env_path, override=False, encoding="utf-8")
except Exception:
    # 如果加载失败，静默忽略，使用默认配置
    pass

# ========== 配置和日志初始化 ==========
# 初始化应用配置
settings = get_settings()

# 设置JSON格式的结构化日志
setup_json_logging(service="api", level=settings.log_level)

# 获取当前模块的日志记录器
logger = logging.getLogger(__name__)

# ========== FastAPI应用创建 ==========
# 创建FastAPI应用实例
app = FastAPI(
    title="CSR API Service",
    version="1.0.0",
    description="六大核心接口的独立服务"
)

# ========== CORS中间件配置 ==========
# 如果启用了CORS，添加跨域资源共享中间件
# 允许所有来源、方法和请求头（开发环境配置）
if settings.enable_cors:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],          # 允许所有来源
        allow_credentials=True,       # 允许携带凭证
        allow_methods=["*"],          # 允许所有HTTP方法
        allow_headers=["*"],          # 允许所有请求头
    )


# ========== 请求ID中间件 ==========
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """
    为每个HTTP请求添加唯一ID并记录请求日志
    
    功能：
    1. 从请求头获取X-Request-Id，如果没有则生成新的UUID
    2. 将请求ID设置到上下文变量中，便于后续日志记录
    3. 记录请求处理时间和状态码
    4. 在响应头中返回请求ID
    
    Args:
        request: FastAPI请求对象
        call_next: 下一个中间件或路由处理函数
        
    Returns:
        Response: HTTP响应对象
    """
    # 从请求头获取或生成新的请求ID
    rid = request.headers.get("X-Request-Id") or f"req_{uuid.uuid4().hex}"
    # 设置到上下文变量中
    token = request_id_ctx.set(rid)
    # 记录请求开始时间
    started = time.perf_counter()
    
    try:
        # 调用下一个中间件或路由处理函数
        response = await call_next(request)
        return response
    finally:
        # 无论成功或失败，都执行清理和日志记录
        try:
            # 计算请求处理耗时（毫秒）
            duration_ms = int((time.perf_counter() - started) * 1000)
            # 获取响应状态码
            status = getattr(response, "status_code", 0) if 'response' in locals() else 0
            
            # 记录访问日志
            logging.getLogger("server.access").info(
                "request.done",
                extra={
                    "event": "request.done",
                    "path": request.url.path,      # 请求路径
                    "method": request.method,      # HTTP方法
                    "status": status,              # 响应状态码
                    "duration_ms": duration_ms,    # 处理耗时
                    "request_id": rid,             # 请求ID
                }
            )
        except Exception:
            # 日志记录失败不影响请求处理
            pass
        
        try:
            # 在响应头中添加请求ID
            if 'response' in locals():
                response.headers["X-Request-Id"] = rid
        except Exception:
            pass
        
        try:
            # 重置上下文变量
            request_id_ctx.reset(token)
        except Exception:
            pass


# ========== 健康检查端点 ==========
@app.get("/healthz")
def healthz():
    """
    健康检查端点
    
    用于监控服务是否正常运行，通常用于负载均衡器或监控系统。
    
    Returns:
        JSONResponse: 包含服务状态和版本信息的JSON响应
    """
    return JSONResponse({
        "status": "ok",
        "service": "CSR API Service",
        "version": "1.0.0",
    })


# ========== 应用启动事件 ==========
# 注意：这里使用延迟导入来避免循环依赖
@app.on_event("startup")
async def startup_event():
    """
    应用启动时的初始化函数
    
    执行以下操作：
    1. 记录启动日志
    2. 确保必要的目录存在
    3. 注册所有API路由模块
    """
    logger.info("CSR API Service启动中...")
    logger.info(f"输出目录: {settings.compose_output_dir}")
    logger.info(f"数据目录: {settings.base_data_dir}")
    
    # 确保必要的目录存在
    settings.ensure_dirs()
    
    # 延迟导入所有API路由模块（避免循环依赖）
    from api.linux import (
        validation,      # 数据源验证接口
        generation,      # 内容生成接口
        compose,         # 文档合成接口
        preprocessing,   # 预处理接口
        insertion,       # 内容插入接口
        allocation,      # 数据分配接口
        postprocessing   # 后处理接口
    )
    
    # 注册所有路由到主应用，统一使用/api/v1前缀
    app.include_router(validation.router, prefix="/api/v1", tags=["validation"])
    app.include_router(generation.router, prefix="/api/v1", tags=["generation"])
    app.include_router(compose.router, prefix="/api/v1", tags=["compose"])
    app.include_router(preprocessing.router, prefix="/api/v1", tags=["preprocessing"])
    app.include_router(insertion.router, prefix="/api/v1", tags=["insertion"])
    app.include_router(allocation.router, prefix="/api/v1", tags=["allocation"])
    app.include_router(postprocessing.router, prefix="/api/v1", tags=["postprocessing"])
    
    logger.info("所有路由注册完成")


# ========== 应用关闭事件 ==========
@app.on_event("shutdown")
async def shutdown_event():
    """
    应用关闭时的清理函数
    
    执行资源清理操作，如关闭数据库连接、清理临时文件等。
    """
    import threading
    logger.info("CSR API Service关闭中...")
    
    # 打印当前活跃线程（用于调试）
    active_threads = threading.enumerate()
    non_main_threads = [t for t in active_threads if t.name != "MainThread"]
    if non_main_threads:
        logger.info(f"当前活跃的后台线程 ({len(non_main_threads)}个):")
        for t in non_main_threads:
            logger.info(f"  - {t.name} (daemon={t.daemon}, alive={t.is_alive()})")
    
    logger.info("CSR API Service关闭完成")


# ========== 主程序入口 ==========
if __name__ == "__main__":
    # 直接运行此文件时，使用uvicorn启动开发服务器
    import uvicorn
    uvicorn.run(
        "api.linux.main:app",  # 应用模块路径
        host=settings.host,     # 监听地址
        port=settings.port,     # 监听端口
        reload=False            # 是否启用自动重载（生产环境建议关闭）
    )
