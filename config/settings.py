"""
配置管理模块

使用pydantic-settings管理环境变量，提供统一的配置管理接口。
支持从环境变量和.env文件加载配置，并提供配置验证和默认值。
"""

import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache
from dotenv import load_dotenv, find_dotenv

# 预加载 .env 文件（将键值注入 os.environ，便于 os.getenv 使用）
# 优先查找当前工作目录下的.env文件，如果找不到则回退到项目根目录
try:
    _env_file = find_dotenv(usecwd=True)
    if not _env_file:
        # 回退到项目根目录（config/ 上两级）
        _env_file = str(Path(__file__).resolve().parents[1] / ".env")
    load_dotenv(_env_file, override=False, encoding="utf-8")
except Exception:
    # 如果加载失败，静默忽略，使用默认配置
    pass


class Settings(BaseSettings):
    """
    应用配置类
    
    使用pydantic-settings管理所有应用配置项，支持从环境变量自动加载。
    所有配置项都有默认值，可以通过环境变量或.env文件覆盖。
    """
    
    # ========== 基础配置 ==========
    # 项目数据根目录，存储项目相关数据
    base_data_dir: str = "AAA/project_data"
    # 文档合成输出目录，存储生成的CSR文档
    compose_output_dir: str = "AAA/output"
    # 缓存目录，存储临时缓存文件
    cache_dir: str = "AAA/cache"
    
    # ========== 服务配置 ==========
    # API服务监听地址，0.0.0.0表示监听所有网络接口
    host: str = "0.0.0.0"
    # API服务监听端口
    port: int = 8000
    
    # ========== LLM配置 ==========
    # 使用的LLM模型名称（用于通用生成）
    llm_model: str = "qwen3.6-flash"
    # LLM API密钥，用于身份验证
    llm_api_key: str = ""
    # LLM API基础URL，兼容OpenAI格式的API接口
    llm_api_base: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    
    # ========== 任务专用模型配置 ==========
    # 提取任务专用模型（默认使用思考模式）
    extraction_model: str = "qwen3.6-plus"
    # 验证任务专用模型
    validation_model: Optional[str] =  "qwen3.6-flash"
    # 生成任务专用模型
    generation_model: Optional[str] = "qwen3.7-plus"

    # ========== 按模型独立配置 ==========
    # 模型配置文件路径（JSON格式，包含限流/思考模式/超时/重试等）
    # .env 通过 LLM_MODEL_CONFIGS_FILE 指向，此处为默认空值
    llm_model_configs_file: str = ""
    # 备用：内联JSON字符串（不推荐，优先用文件）
    llm_model_configs: str = ""

    # ========== Windows Bridge配置 ==========
    # Windows桥接服务URL，用于跨平台调用Windows特定功能
    windows_bridge_url: Optional[str] = None
    # Windows桥接服务请求超时时间（秒）
    windows_bridge_timeout: int = 300
    
    # ========== 日志配置 ==========
    # 日志级别：DEBUG, INFO, WARNING, ERROR, CRITICAL
    log_level: str = "INFO"
    # 日志格式：json 或 text
    log_format: str = "json"
    
    # ========== 可选配置 ==========
    # 提取任务的增量流式输出间隔（用于进度报告）
    extraction_delta_stream: int = 1
    # 是否启用CORS跨域支持
    enable_cors: bool = True
    
    # ========== 并发控制配置 ==========
    # 文件级并发提取的最大并发数（避免API限流）
    # 适用于：多个chunks文件提取、多个Excel Sheet提取
    max_file_extraction_workers: int = 5
    # LLM请求间隔（秒），避免短时间内高频请求
    llm_request_interval: float = 0.5
    # 标签（段落）级并发的最大并发数
    max_paragraph_workers: int = 8
    # 标签内数据项的最大并发数
    max_data_item_workers: int = 8
    # 摘要生成的最大并发数
    max_summary_workers: int = 8
    # 生成/全流程任务的最大并发执行数（有界线程池，防止突发流量下线程失控）
    generation_max_concurrent_tasks: int = 10

    # ========== RAGFlow 配置 ==========
    ragflow_api_key: str = os.getenv('RAGFLOW_API_KEY')
    ragflow_base_url: str = os.getenv('RAGFLOW_BASE_URL')
    ragflow_page_size: int = os.getenv('RAGFLOW_PAGE_SIZE', 10)
    
    # Pydantic配置：指定环境变量文件和相关设置
    model_config = SettingsConfigDict(
        env_file=".env",              # 环境变量文件路径
        env_file_encoding="utf-8",    # 文件编码
        case_sensitive=False,         # 环境变量名不区分大小写
        extra="ignore"                # 忽略额外的环境变量
    )
    
    @property
    def output_dir(self) -> str:
        """
        输出目录别名属性
        
        Returns:
            str: 输出目录路径，等同于compose_output_dir
        """
        return self.compose_output_dir

    def ensure_dirs(self):
        """
        确保必要的目录存在
        
        创建配置中定义的所有目录（如果不存在），包括：
        - base_data_dir: 项目数据目录
        - compose_output_dir: 输出目录
        - cache_dir: 缓存目录
        """
        for dir_path in [self.base_data_dir, self.compose_output_dir, self.cache_dir]:
            Path(dir_path).mkdir(parents=True, exist_ok=True)


@lru_cache()
def get_settings() -> Settings:
    """
    获取配置单例（使用LRU缓存）
    
    首次调用时创建Settings实例并确保目录存在，后续调用返回缓存的实例。
    这样可以避免重复创建和目录检查，提高性能。
    
    Returns:
        Settings: 配置单例对象
    """
    settings = Settings()
    settings.ensure_dirs()
    return settings
