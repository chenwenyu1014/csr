"""
预处理层 (Preprocessing Layer)

将各种格式的文件转换为标准化数据包，供主流程使用。

主要模块：
- file_detector: 文件类型检测
- format_converter: 格式转换（PDF/Word/Markdown）
- document_marker: 文档标记（插入标签）
- content_splitter: 内容切分
- asset_extractor: 资源提取（图片/表格）
"""

from .file_processor import FileProcessor, PreprocessedDocument, ContentType, FileType
from .service import PreprocessingService
# from .batch_processor import BatchProcessor, BatchProcessResult, batch_processor

# 创建全局服务实例
preprocessing_service = PreprocessingService()

__all__ = [
    "FileProcessor",
    "PreprocessedDocument",
    "ContentType",
    "FileType",
    "PreprocessingService",
    "preprocessing_service",
    # "BatchProcessor",
    # "BatchProcessResult",
    # "batch_processor",
]






