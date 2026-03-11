"""
API 路由模块
============

包含7个核心接口：

1. validation      - 数据源验证接口 (POST /api/v1/validation/data-source)
2. generation      - 内容生成接口 (POST /api/v1/flow/run-text)
3. compose         - 文档合成接口 (POST /api/v1/documents/compose)
4. preprocessing   - 预处理接口 (POST /api/v1/preprocessing/batch-simple)
5. insertion       - 内容插入接口 (POST /api/v1/template/insert)
6. allocation      - 数据分配接口 (POST /api/v1/datasource/allocate)
7. postprocessing  - 后处理接口 (POST /api/v1/document/clean)

注意：这些模块在 main.py 的 startup 事件中导入，以避免循环依赖。
"""
