"""
CSR API Service - 主应用入口

这是CSR文档生成系统的FastAPI主应用，包含以下10个核心接口：
1. constraint_validation - 数据源约束验证接口
2. generation - 内容生成接口
3. compose - 文档合成接口
4. preprocessing - 预处理接口
5. insertion - 内容插入接口
6. allocation - 文件分配接口
7. postprocessing - 后处理接口
8. template - 模板处理接口
9. file_validation - 文件校验接口
10. manifest - 清单生成接口

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
import threading
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv, find_dotenv

from config import get_settings
from utils import request_id_ctx, setup_logging

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

# 设置统一日志配置（控制台 + 文件双输出）
setup_logging(service_name="API")

# 获取当前模块的日志记录器
logger = logging.getLogger(__name__)


# ========== Lifespan 生命周期管理 ==========
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    应用生命周期管理函数（替代已废弃的 on_event）

    yield 之前：应用启动时执行（原 startup_event）
    yield 之后：应用关闭时执行（原 shutdown_event）
    """
    # ===== Startup =====
    logger.info("CSR API Service启动中...")
    logger.info(f"输出目录: {settings.compose_output_dir}")
    logger.info(f"数据目录: {settings.base_data_dir}")

    # 确保必要的目录存在
    settings.ensure_dirs()

    # 延迟导入所有API路由模块（避免循环依赖）
    from api.linux import (
        constraint_validation,  # 数据源约束验证接口
        generation,             # 内容生成接口
        compose,                # 文档合成接口
        preprocessing,          # 预处理接口
        insertion,              # 内容插入接口
        allocation,             # 文件分配接口
        postprocessing,         # 后处理接口
        template,               # 模板处理接口
        file_validation,        # 文件校验接口
        manifest,               # 清单生成接口
    )

    # 注册所有路由到主应用，统一使用 /api/v1 前缀
    app.include_router(constraint_validation.router, prefix="/api/v1", tags=["validation"])
    app.include_router(generation.router,            prefix="/api/v1", tags=["generation"])
    app.include_router(compose.router,               prefix="/api/v1", tags=["compose"])
    app.include_router(preprocessing.router,         prefix="/api/v1", tags=["preprocessing"])
    app.include_router(insertion.router,             prefix="/api/v1", tags=["insertion"])
    app.include_router(allocation.router,            prefix="/api/v1", tags=["allocation"])
    app.include_router(postprocessing.router,        prefix="/api/v1", tags=["postprocessing"])
    app.include_router(template.router,              prefix="/api/v1", tags=["template"])
    app.include_router(file_validation.router,       prefix="/api/v1", tags=["validation"])
    app.include_router(manifest.router,              prefix="/api/v1", tags=["manifest"])

    logger.info("所有路由注册完成")

    yield  # ← 分界线：yield 前是 startup，yield 后是 shutdown

    # ===== Shutdown =====
    logger.info("CSR API Service关闭中...")

    # 打印当前活跃线程（用于调试）
    active_threads = threading.enumerate()
    non_main_threads = [t for t in active_threads if t.name != "MainThread"]
    if non_main_threads:
        logger.info(f"当前活跃的后台线程 ({len(non_main_threads)}个):")
        for t in non_main_threads:
            logger.info(f"  - {t.name} (daemon={t.daemon}, alive={t.is_alive()})")

    logger.info("CSR API Service关闭完成")


# ========== FastAPI应用创建 ==========
# 创建FastAPI应用实例，传入 lifespan 生命周期管理函数
app = FastAPI(
    title="CSR API Service",
    version="1.0.0",
    description="六大核心接口的独立服务",
    lifespan=lifespan,
)

# ========== CORS中间件配置 ==========
# 如果启用了CORS，添加跨域资源共享中间件
# 允许所有来源、方法和请求头（开发环境配置）
if settings.enable_cors:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],      # 允许所有来源
        allow_credentials=True,   # 允许携带凭证
        allow_methods=["*"],      # 允许所有HTTP方法
        allow_headers=["*"],      # 允许所有请求头
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
                    "event":       "request.done",
                    "path":        request.url.path,   # 请求路径
                    "method":      request.method,     # HTTP方法
                    "status":      status,             # 响应状态码
                    "duration_ms": duration_ms,        # 处理耗时
                    "request_id":  rid,                # 请求ID
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
