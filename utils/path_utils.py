"""
路径工具模块

功能说明：
- 提供统一的路径处理工具
- 规范化路径格式
- 确保目录存在

使用示例：
    from utils.path_utils import (
        normalize_aaa_path,
        ensure_dir_exists,
        get_prompts_dir
    )
    
    # 规范化路径
    path = normalize_aaa_path("AAA\\project_data\\file.txt")
    # 结果：project_data/file.txt
    
    # 确保目录存在
    dir_path = ensure_dir_exists("AAA/output/session_123")
    
    # 获取提示词目录
    prompts_dir = get_prompts_dir("extraction")
"""

# ========== 标准库导入 ==========
import os
from pathlib import Path
from typing import Optional


# ============================================================
# 路径规范化
# ============================================================

def normalize_path(path: str) -> str:
    """
    规范化路径（统一使用正斜杠）
    
    Args:
        path: 原始路径
    
    Returns:
        规范化后的路径
    """
    return path.replace("\\", "/")


def normalize_aaa_path(path: str) -> str:
    """
    规范化AAA相对路径
    
    将绝对路径转换为相对于AAA目录的路径
    例如：C:/xxx/AAA/project_data/file.txt -> project_data/file.txt
    
    Args:
        path: 原始路径
    
    Returns:
        相对于AAA的路径
    """
    normalized = normalize_path(path)
    idx = normalized.lower().find("aaa/")
    if idx != -1:
        normalized = normalized[idx + 4:]
    return normalized


def to_absolute_path(relative_path: str, base_dir: str = "AAA") -> str:
    """
    转换为绝对路径
    
    Args:
        relative_path: 相对路径
        base_dir: 基础目录
    
    Returns:
        绝对路径
    """
    if os.path.isabs(relative_path):
        return relative_path
    return str(Path(base_dir) / relative_path)


# ============================================================
# 目录管理
# ============================================================

def ensure_dir_exists(dir_path: str) -> Path:
    """
    确保目录存在
    
    Args:
        dir_path: 目录路径
    
    Returns:
        Path对象
    """
    p = Path(dir_path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def ensure_parent_dir_exists(file_path: str) -> Path:
    """
    确保文件的父目录存在
    
    Args:
        file_path: 文件路径
    
    Returns:
        文件的父目录Path对象
    """
    p = Path(file_path).parent
    p.mkdir(parents=True, exist_ok=True)
    return p


# ============================================================
# 特定目录获取
# ============================================================

def get_prompts_dir(task_type: str = "default", create: bool = True) -> Path:
    """
    获取提示词保存目录
    
    Args:
        task_type: 任务类型（extraction/validation/generation等）
        create: 是否创建目录（如果不存在）
    
    Returns:
        提示词目录Path对象
    """
    from utils.context_manager import get_current_output_dir
    
    session_dir = get_current_output_dir()
    prompts_dir = Path(session_dir) / "prompts" / task_type
    
    if create:
        prompts_dir.mkdir(parents=True, exist_ok=True)
    
    return prompts_dir


def get_session_dir(session_id: Optional[str] = None) -> Path:
    """
    获取会话目录
    
    Args:
        session_id: 会话ID（可选，如果为None则使用当前输出目录）
    
    Returns:
        会话目录Path对象
    """
    if session_id:
        from config import get_settings
        settings = get_settings()
        return Path(settings.compose_output_dir) / session_id
    else:
        from utils.context_manager import get_current_output_dir
        return Path(get_current_output_dir())


def get_cache_dir(cache_type: str = "default", create: bool = True) -> Path:
    """
    获取缓存目录
    
    Args:
        cache_type: 缓存类型
        create: 是否创建目录
    
    Returns:
        缓存目录Path对象
    """
    from config import get_settings
    settings = get_settings()
    cache_dir = Path(settings.cache_dir) / cache_type
    
    if create:
        cache_dir.mkdir(parents=True, exist_ok=True)
    
    return cache_dir


# ============================================================
# 文件操作辅助
# ============================================================

def safe_file_name(name: str) -> str:
    """
    生成安全的文件名（移除非法字符）
    
    Args:
        name: 原始文件名
    
    Returns:
        安全的文件名
    """
    # 移除或替换非法字符
    illegal_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    safe_name = name
    for char in illegal_chars:
        safe_name = safe_name.replace(char, '_')
    return safe_name


def get_unique_file_path(base_path: str, extension: str = "") -> Path:
    """
    获取唯一的文件路径（如果文件已存在，自动添加序号）
    
    Args:
        base_path: 基础路径（不含扩展名）
        extension: 文件扩展名
    
    Returns:
        唯一的文件路径
    """
    if extension and not extension.startswith('.'):
        extension = f'.{extension}'
    
    path = Path(f"{base_path}{extension}")
    
    if not path.exists():
        return path
    
    # 文件已存在，添加序号
    counter = 1
    while True:
        path = Path(f"{base_path}_{counter}{extension}")
        if not path.exists():
            return path
        counter += 1

