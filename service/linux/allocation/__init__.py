"""
数据分配服务模块
================

提供数据源分配（文件匹配）相关的服务功能。

主要组件：
- AllocationService: 数据分配服务
- DataSourceValidator: 数据源验证器
"""

from service.linux.allocation.allocation_service import AllocationService, get_allocation_service
from service.linux.allocation.data_source_validator import DataSourceValidator

__all__ = [
    'AllocationService',
    'get_allocation_service',
    'DataSourceValidator',
]
