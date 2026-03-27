"""
结构化JSON日志
"""

import logging
import sys
from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """自定义JSON格式化器"""
    
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        
        # 添加自定义字段
        log_record["level"] = record.levelname
        log_record["logger"] = record.name
        
        # 添加请求ID（如果有）
        from .request_context import get_request_id
        request_id = get_request_id()
        if request_id:
            log_record["request_id"] = request_id


def setup_json_logging(service: str = "csr-api", level: str = "INFO"):
    """
    设置JSON日志
    
    Args:
        service: 服务名称
        level: 日志级别
    """
    # 创建handler
    handler = logging.StreamHandler(sys.stdout)
    # 设置JSON格式化器
    formatter=logging.Formatter(
        '%(asctime)s - %(name)s(%(lineno)s) - %(levelname)s - %(message)s')

    # formatter = CustomJsonFormatter(
    #     "%(timestamp)s %(level)s %(name)s %(message)s",
    #     timestamp=True,
    #     json_ensure_ascii=False
    # )
    handler.setFormatter(formatter)
    
    # 配置root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    if root_logger.handlers:
        root_logger.handlers.clear()
    root_logger.addHandler(handler)

    # 配置特定logger
    for logger_name in ["uvicorn", "uvicorn.access", "uvicorn.error"]:
        logger = logging.getLogger(logger_name)
        logger.setLevel(getattr(logging, level.upper()))


# 别名：兼容 flow_controller.py 等模块中对 JSONLogFormatter 的引用
JSONLogFormatter = CustomJsonFormatter
