"""
模板处理模块

文件说明：
- template_service.py               模板处理主服务，编排标记→导出→转换→清理流程
- template_marker.py                表格标记器，识别并标记 Word 模板中的表格区域
- table_converter.py                表格转换器，将表格区域转为 LibreOffice 可渲染格式
- template_comment_parser.py        模板批注解析器，将批注转为 Word 内容控件（SDT）
- comment_field_parser.py           批注字段解析器（纯规则）+ CommentValidator 批量封装
- xml_utils.py                      WordprocessingML XML 工具函数与批注 XML 管理器
- control_to_comment_converter.py   内容控件转批注转换器，将 SDT 转换回批注
"""

from .template_service import TemplateProcessingService, get_template_service
from .template_marker import TemplateMarker
from .table_converter import TableConverter
from .template_comment_parser import (
    TemplateCommentParser,
    get_template_comment_parser,
    parse_and_convert,
)
from .xml_utils import CommentXmlManager
from .control_to_comment_converter import (
    ControlToCommentConverter,
    convert_controls_to_comments,
    format_comment_text,
)

__all__ = [
    'TemplateProcessingService',
    'get_template_service',
    'TemplateMarker',
    'TableConverter',
    'TemplateCommentParser',
    'get_template_comment_parser',
    'parse_and_convert',
    'CommentXmlManager',
    'ControlToCommentConverter',
    'convert_controls_to_comments',
    'format_comment_text',
]