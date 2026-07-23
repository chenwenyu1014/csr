"""
统一日志配置模块

设计原则：
1. 每个服务写入专属日志文件，避免多进程写入同一文件
2. 文件名包含环境标识、系统标识和服务标识
3. 控制台 + 文件双输出
4. 服务级日志使用专用 Logger，不污染 Root Logger

日志文件命名规则：
- {environment}_{system}_{service_name}.log  服务日志
- {environment}_{system}_{service_name}.log.1 轮转备份

使用方式：
    # 在入口文件（main.py 或 app.py）开头调用，传入服务名
    from utils.logging_config import setup_logging
    setup_logging(service_name="linux_api")    # Linux API 服务
    setup_logging(service_name="windows_bridge")  # Windows Bridge 服务

    # 其他模块正常使用
    import logging
    logger = logging.getLogger(__name__)
"""

import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

# 多进程安全的轮转文件 Handler
from concurrent_log_handler import ConcurrentRotatingFileHandler


def get_log_filename(environment: str, system: str, service_name: str = "main") -> str:
    """
    生成日志文件名

    Args:
        environment: 环境标识 (prod/test/dev)
        system: 系统标识 (linux/windows)
        service_name: 服务标识 (linux_api/windows_bridge/generation 等)

    Returns:
        日志文件名，如 prod_linux_linux_api.log
    """
    return f"{environment}_{system}_{service_name}.log"


def detect_system() -> str:
    """
    自动检测当前系统类型

    Returns:
        'linux' 或 'windows'
    """
    if sys.platform.startswith('win'):
        return 'windows'
    else:
        return 'linux'


def detect_environment() -> str:
    """
    检测当前环境

    优先从环境变量读取，如果没有则默认为 dev

    Returns:
        'prod'、'test' 或 'dev'
    """
    env = os.getenv('ENVIRONMENT', 'dev').lower().strip()
    
    # 环境名称映射表
    env_mapping = {
        'prod': 'prod',
        'production': 'prod',
        'test': 'test',
        'testing': 'test',
        'dev': 'dev',
        'development': 'dev'
    }
    
    return env_mapping.get(env, 'dev')


def setup_logging(
    log_level: Optional[str] = None,
    log_dir: Optional[str] = None,
    to_file: Optional[bool] = None,
    environment: Optional[str] = None,
    system: Optional[str] = None,
    service_name: str = "main",
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
) -> logging.Logger:
    """
    设置统一日志配置

    在入口文件开头调用此函数，会：
    1. 配置 Root Logger 的级别和格式
    2. 添加控制台输出 Handler
    3. 可选添加文件输出 Handler

    Args:
        log_level: 日志级别，默认从环境变量 LOG_LEVEL 读取
        log_dir: 日志目录，默认从环境变量 LOG_DIR 读取
        to_file: 是否写入文件，默认从环境变量 LOG_TO_FILE 读取
        environment: 环境标识，默认自动检测
        system: 系统标识，默认自动检测
        service_name: 服务名称，由入口文件传入（如 'api', 'main'）
        max_bytes: 单个日志文件最大大小
        backup_count: 保留的备份文件数量

    Returns:
        Root Logger 实例
    """
    # 获取配置参数
    if log_level is None:
        log_level = os.getenv('LOG_LEVEL', 'INFO')
    if log_dir is None:
        log_dir = os.getenv('LOG_DIR', 'AAA/logs')
    if to_file is None:
        to_file = os.getenv('LOG_TO_FILE', 'true').lower() in ('true', '1', 'yes', 'on')
    if environment is None:
        environment = detect_environment()
    if system is None:
        system = detect_system()

    # 获取日志级别
    level = getattr(logging, log_level.upper(), logging.INFO)

    # 日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s(%(lineno)s) - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 获取 Root Logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # 清理已有的 handlers（避免重复添加）
    # 保留已有的文件 handlers（可能是 session.log 等）
    existing_stream_handlers = [h for h in root_logger.handlers if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)]
    existing_file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]

    # 只保留 session.log 等 session 级别的 handler
    for h in existing_stream_handlers:
        root_logger.removeHandler(h)

    # 添加控制台 Handler（总是添加）
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    # 添加文件 Handler（如果启用）
    if to_file:
        # 确保日志目录存在
        log_dir_path = Path(log_dir)
        try:
            log_dir_path.mkdir(parents=True, exist_ok=True)
        except Exception:
            # 如果创建失败（如权限问题），回退到当前目录
            log_dir_path = Path('.')

        # 生成日志文件名
        log_filename = get_log_filename(environment, system, service_name)
        log_file_path = log_dir_path / log_filename

        # 检查是否已有相同的文件 handler
        same_file_handler = None
        for h in existing_file_handlers:
            if isinstance(h, (RotatingFileHandler, ConcurrentRotatingFileHandler)):
                try:
                    if str(Path(h.baseFilename).resolve()) == str(log_file_path.resolve()):
                        same_file_handler = h
                        break
                except Exception:
                    pass

        # 如果没有相同的 handler，则添加新的
        if same_file_handler is None:
            try:
                file_handler = ConcurrentRotatingFileHandler(
                    str(log_file_path),
                    maxBytes=max_bytes,
                    backupCount=backup_count,
                    encoding='utf-8',
                    use_gzip=False,
                )
                file_handler.setLevel(level)
                file_handler.setFormatter(formatter)
                root_logger.addHandler(file_handler)

                # 记录日志配置信息
                root_logger.info(f"[日志系统] 初始化完成: {log_file_path}, 级别={log_level}, 环境={environment}, 系统={system}, 服务={service_name}")
            except Exception as e:
                root_logger.warning(f"[日志系统] 创建文件日志失败: {e}, 仅使用控制台输出")

    return root_logger


def get_service_logger(
    service_name: str,
    log_dir: Optional[str] = None,
    environment: Optional[str] = None,
    system: Optional[str] = None,
    max_bytes: int = 5 * 1024 * 1024,  # 5MB
    backup_count: int = 3,
) -> logging.Logger:
    """
    获取服务专用 Logger

    用于特定服务的独立日志记录，不通过 Root Logger 传播。

    Args:
        service_name: 服务名称（如 'preprocessing', 'generation'）
        log_dir: 日志目录
        environment: 环境标识
        system: 系统标识
        max_bytes: 单个日志文件最大大小
        backup_count: 保留的备份文件数量

    Returns:
        专用 Logger 实例
    """
    if log_dir is None:
        log_dir = os.getenv('LOG_DIR', 'AAA/logs')
    if environment is None:
        environment = detect_environment()
    if system is None:
        system = detect_system()

    # 获取日志级别
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    level = getattr(logging, log_level.upper(), logging.INFO)

    # 日志格式
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(name)s - %(message)s'
    )

    # 创建专用 Logger
    logger_name = f"service.{service_name}"
    service_logger = logging.getLogger(logger_name)
    service_logger.setLevel(level)

    # 关闭传播（不传给 Root Logger）
    service_logger.propagate = False

    # 确保日志目录存在
    log_dir_path = Path(log_dir)
    try:
        log_dir_path.mkdir(parents=True, exist_ok=True)
    except Exception:
        log_dir_path = Path('.')

    # 生成日志文件名
    log_filename = f"{environment}_{system}_{service_name}.log"
    log_file_path = log_dir_path / log_filename

    # 检查是否已有 handler
    has_file_handler = any(
        isinstance(h, (RotatingFileHandler, ConcurrentRotatingFileHandler)) and
        str(Path(h.baseFilename).resolve()) == str(log_file_path.resolve())
        for h in service_logger.handlers
    )

    if not has_file_handler:
        try:
            file_handler = ConcurrentRotatingFileHandler(
                str(log_file_path),
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding='utf-8',
                use_gzip=False,
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            service_logger.addHandler(file_handler)
        except Exception as e:
            # 如果创建失败，使用控制台输出
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setLevel(level)
            stream_handler.setFormatter(formatter)
            service_logger.addHandler(stream_handler)
            service_logger.warning(f"创建文件日志失败: {e}")

    return service_logger