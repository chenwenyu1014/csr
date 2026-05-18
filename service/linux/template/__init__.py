"""
模板处理模块

功能说明：
- 处理Word模板文件中的表格
- 标记表格、获取标题、导出区域、转换格式
"""

from .template_service import TemplateProcessingService, get_template_service
from .template_marker import TemplateMarker
from .table_converter import TableConverter

__all__ = [
    'TemplateProcessingService',
    'get_template_service',
    'TemplateMarker',
    'TableConverter',
]