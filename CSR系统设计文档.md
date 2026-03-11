# CSR 文档生成系统设计文档

## 一、系统架构概览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          客户端 (前端/外部系统)                           │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Linux API Service (FastAPI)                        │
│                          端口: 8000                                     │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                       7 个核心接口                               │    │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐               │    │
│  │  │接口1    │ │接口2    │ │接口3     │ │接口4    │               │    │
│  │  │校验     │ │生成     │ │完整流程  │ │预处理    │               │    │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘               │    │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐                           │    │
│  │  │接口5    │ │接口6    │ │接口7    │                            │    │
│  │  │插入    │ │分配    │ │清理    │                               │    │
│  │  └─────────┘ └─────────┘ └─────────┘                           │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                     │                                    │
│                    ┌────────────────┼────────────────┐                  │
│                    ▼                ▼                ▼                  │
│  ┌─────────────────────┐ ┌─────────────────┐ ┌─────────────────┐       │
│  │   Service 层        │ │   Bridge 层     │ │   Model 层      │       │
│  │ (ValidationService) │ │(WindowsBridge)  │ │ (LLM Service)   │       │
│  │ (GenerationService) │ │                 │ │                 │       │
│  │ (AllocationService) │ │                 │ │                 │       │
│  └─────────────────────┘ └─────────────────┘ └─────────────────┘       │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     │ HTTP (aiohttp 异步)
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Windows Bridge Service (FastAPI)                      │
│                          端口: 8081                                      │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    Word/Excel 处理能力                           │    │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐                │    │
│  │  │ Word COM    │ │ 内容插入   │ │ 文档清理   │               │    │
│  │  │ 预处理      │ │ 模板填充   │ │ 控件清理   │               │    │
│  │  └─────────────┘ └─────────────┘ └─────────────┘               │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         共享存储: AAA 目录                               │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐       │
│  │project_data │ │Preprocessing│ │  Template   │ │   output    │       │
│  │  (源文件)   │ │(预处理结果) │ │  (模板)     │ │  (输出)     │       │
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 二、分层设计原则

### 设计理念：三层分离

```
┌──────────────────────────────────────────────────┐
│  API 层 (api/linux/*.py)                         │
│  - 只负责路由定义和参数验证                       │
│  - 不包含业务逻辑                                │
│  - 调用 Service 层                               │
├──────────────────────────────────────────────────┤
│  Service 层 (service/linux/*/*.py)               │
│  - 封装核心业务逻辑                              │
│  - 处理数据转换和结果构建                        │
│  - 调用 Model 层和 Bridge 层                     │
├──────────────────────────────────────────────────┤
│  Model/Bridge 层                                 │
│  - Model: LLM 调用、提示词管理                     │
│  - Bridge: 跨平台调用 (Windows COM)               │
└──────────────────────────────────────────────────┘
```

---

## 三、7 个核心接口详解

### 接口1: 数据源校验 `/api/v1/validation/data-source`

**功能**: 使用 LLM 判断上传的文件是否符合要求

**调用链路**:
```
API (validation.py)
    │
    ▼ await validation_service.validate_pure_async()
Service (validation_service.py)
    │
    ▼ await validator.validate_pure_async()
Validator (data_source_validator.py)
    │
    ▼ await self.llm.generate_single_async()
LLM Service (llm_service.py)
    │
    ▼ 调用大模型 API
```

**关键代码**:

```python
# api/linux/validation.py (路由层 - 简洁)
@router.post("/validation/data-source")
async def validate_data_source(...):
    # 1. 解析参数
    spec_obj = await _parse_spec(request, spec_json, spec_file)
    
    # 2. 设置环境变量
    _setup_environment(combinationId, project_desc, spec_obj)
    
    # 3. 调用服务层（异步）
    svc_result = await validation_service.validate_pure_async(spec_obj, task_name)
    
    # 4. 返回结果
    return JSONResponse({...})
```

```python
# service/linux/validation/validation_service.py (服务层)
async def validate_pure_async(self, spec, task_name):
    # 调用底层异步校验
    result = await self.validator.validate_pure_async(spec, task_name)
    return {...}
```

```python
# service/linux/allocation/data_source_validator.py (底层)
async def validate_pure_async(self, spec, task_name, prompt_id):
    # 1. 构建提示词
    prompt_text = self.build_prompt_from_spec_raw(spec, task_name, prompt_id)
    
    # 2. 异步调用 LLM
    model_output_raw = await self.llm.generate_single_async(prompt_text)
    
    # 3. 解析结果
    parsed = self._parse_json_response(model_output_raw)
    return ValidationResult(...)
```

---

### 接口2: 文本生成 `/api/v1/flow/run-text`

**功能**: 异步执行内容生成，立即返回 task_id

**执行方式**: 后台线程 + 回调通知

**调用链路**:
```
API (generation.py)
    │
    ▼ generation_service.start_async_task()
Service (generation_service.py)
    │
    ├─▶ threading.Thread (后台执行)
    │       │
    │       ▼ CSRFlowController.generate_all_paragraphs()
    │       │
    │       ▼ callback.notify_complete()
    │
    ▼ 立即返回 task_id
```

**关键代码**:

```python
# api/linux/generation.py (路由层)
@router.post("/flow/run-text")
async def run_flow_text_only(...):
    # 1. 解析配置
    cfg_obj = await _parse_config(request, config_json, config_file)
    
    # 2. 创建任务
    task_id = task_manager.create_task(...)
    
    # 3. 启动后台线程
    generation_service.start_async_task(
        task_id=task_id,
        cfg_obj=cfg_obj,
        ...
    )
    
    # 4. 立即返回（不等待完成）
    return JSONResponse({
        "success": True,
        "task_id": task_id,
        "status": "等待处理"
    })
```

```python
# service/linux/generation/generation_service.py (服务层)
def start_async_task(self, task_id, cfg_obj, ...):
    """启动后台线程"""
    thread = threading.Thread(
        target=self._run_async,
        args=(task_id, cfg_obj, ...),
        daemon=True
    )
    thread.start()
    return thread

def _run_async(self, task_id, cfg_obj, ...):
    """后台线程执行"""
    try:
        # 1. 设置环境
        self._setup_environment(cfg_obj, combinationId, project_desc)
        
        # 2. 执行生成
        controller = CSRFlowController(fc)
        flow_res = controller.generate_all_paragraphs()
        
        # 3. 构建结果
        result = self._build_result(flow_res, run_dir)
        
        # 4. 通知完成（回调）
        callback.notify_complete(result)
    except Exception as e:
        callback.notify_error(str(e))
```

---

### 接口3: 完整流程 `/api/v1/documents/compose`

**功能**: 生成 + 插入模板 的完整流程（异步）

**执行方式**: 后台线程执行，完成后回调

**调用链路**:
```
API (compose.py)
    │
    ▼ generation_service.start_compose_async_task()
Service (generation_service.py)
    │
    ├─▶ threading.Thread (后台执行)
    │       │
    │       ▼ 步骤1: CSRFlowController.generate_all_paragraphs()
    │       │
    │       ▼ 步骤2: WindowsBridgeClient.insert_content()
    │       │
    │       ▼ 步骤3: _send_compose_callback()
    │
    ▼ 立即返回 task_id
```

**关键代码**:

```python
# api/linux/compose.py (路由层)
@router.post("/documents/compose")
async def compose_document(...):
    # 1. 解析配置
    cfg_obj = await _parse_config(request, config_json, config_file)
    
    # 2. 创建任务
    task_id = task_manager.create_task(callback_url=callback_url, ...)
    
    # 3. 启动后台线程执行完整流程
    generation_service.start_compose_async_task(
        task_id=task_id,
        cfg_obj=cfg_obj,
        template_file=template_file,
        callback_url=callback_url,
        ...
    )
    
    # 4. 立即返回
    return JSONResponse({
        "success": True,
        "task_id": task_id,
        "status": "处理中"
    })
```

```python
# service/linux/generation/generation_service.py
def _run_compose_async(self, task_id, cfg_obj, template_file, callback_url, ...):
    """异步执行完整流程"""
    try:
        # 步骤1: 生成内容
        flow_res = controller.generate_all_paragraphs()
        result = self._build_result(flow_res, run_dir)
        
        # 步骤2: 插入模板（如果提供了模板）
        if template_file:
            client = WindowsBridgeClient(self.settings.windows_bridge_url)
            insertion_result = client.insert_content(template_file, data_json_str)
            result["output_file"] = insertion_result.get("output_file")
        
        # 步骤3: 发送回调
        self._send_compose_callback(callback_url, auth_token, result)
        
    except Exception as e:
        self._send_compose_callback(callback_url, auth_token, {"success": False, "error": str(e)})
```

---

### 接口4: 批量预处理 `/api/v1/preprocessing/batch-simple`

**功能**: 对文件进行预处理（Word/Excel/PDF → 结构化数据）

**执行方式**: asyncio.create_task 后台异步执行

**调用链路**:
```
API (preprocessing.py)
    │
    ▼ asyncio.create_task(preprocessing_service.process_files_async())
Service (preprocessing_task_service.py)
    │
    ▼ await client.preprocess_file_async()  (异步 HTTP)
Bridge (windows_bridge_client.py)
    │
    ▼ aiohttp POST to Windows Bridge
Windows Bridge (app.py)
    │
    ▼ PreprocessingService.preprocess()
```

**关键代码**:

```python
# api/linux/preprocessing.py (路由层)
@router.post("/preprocessing/batch-simple")
async def preprocess_batch_simple(...):
    # 1. 解析参数
    files_list = json.loads(files)
    task_id = f"batch_{...}"
    
    # 2. 启动后台异步任务（不阻塞）
    asyncio.create_task(preprocessing_service.process_files_async(
        task_id=task_id,
        files_list=files_list,
        ...
    ))
    
    # 3. 立即返回 202
    return JSONResponse(status_code=202, content={
        "success": True,
        "task_id": task_id,
        "message": "预处理任务已接受"
    })
```

```python
# service/linux/preprocessing/preprocessing_task_service.py
async def process_files_async(self, task_id, files_list, ...):
    """异步处理文件列表"""
    for file_item in files_list:
        # 异步调用 Windows Bridge
        data = await client.preprocess_file_async(
            file_path=file_path_rel,
            filename=filename,
            ...
        )
    
    # 异步回调通知
    await self._send_callback_async(callback_url, results)
```

---

### 接口5: 模板插入 `/api/v1/template/insert`

**功能**: 将生成的内容插入 Word 模板

**调用链路**:
```
API (insertion.py)
    │
    ▼ await client.insert_content_async()  (异步 HTTP)
Bridge (windows_bridge_client.py)
    │
    ▼ aiohttp POST to Windows Bridge
Windows Bridge (app.py)
    │
    ▼ WordControlContentInserter.insert_to_template()
```

**关键代码**:

```python
# api/linux/insertion.py (路由层)
@router.post("/template/insert")
async def insert_content_to_template(...):
    # 1. 参数验证
    if not template_file or not data_json:
        raise HTTPException(status_code=400, detail="缺少必需参数")
    
    # 2. 异步调用 Windows Bridge
    client = WindowsBridgeClient(settings.windows_bridge_url)
    result = await client.insert_content_async(
        template_file=template_file,
        data_json=data_json
    )
    
    # 3. 返回结果
    return JSONResponse(result)
```

```python
# service/linux/bridge/windows_bridge_client.py
async def insert_content_async(self, template_file, data_json):
    """异步插入内容"""
    async with await self._get_aiohttp_session() as session:
        async with session.post(url, data=data, headers=headers) as resp:
            return await resp.json()
```

---

### 接口6: 数据源分配 `/api/v1/datasource/allocate`

**功能**: 使用 LLM 匹配文件与数据需求

**调用链路**:
```
API (allocation.py)
    │
    ▼ allocation_service.allocate_batch()
Service (allocation_service.py)
    │
    ▼ matching_service.match()
Validator (data_source_validator.py)
    │
    ▼ self.llm.generate_single()
LLM Service
```

**关键代码**:

```python
# api/linux/allocation.py (路由层)
@router.post("/datasource/allocate")
async def allocate_datasource(...):
    # 1. 解析参数
    data = _parse_items_json(items_json)
    
    # 2. 调用服务层
    result_groups = allocation_service.allocate_batch(data)
    
    # 3. 返回结果
    return JSONResponse({
        "code": 200,
        "message": "匹配成功",
        "data": result_groups
    })
```

---

### 接口7: 文档清理 `/api/v1/document/clean`

**功能**: 清理 Word 文档的 Content Control 和首行水印

**调用链路**:
```
API (postprocessing.py)
    │
    ▼ await client.clean_document_async()  (异步 HTTP)
Bridge (windows_bridge_client.py)
    │
    ▼ aiohttp POST to Windows Bridge
Windows Bridge (app.py)
    │
    ▼ _clean_content_controls_preserve_content()  (Word COM)
```

**关键代码**:

```python
# api/linux/postprocessing.py (路由层)
@router.post("/document/clean")
async def clean_document(...):
    # 异步调用 Windows Bridge
    client = WindowsBridgeClient()
    result = await client.clean_document_async(
        file_path=file_path,
        output_path=output_path,
        remove_first_line=remove_first_line,
        remove_content_controls=remove_content_controls
    )
    return JSONResponse(result)
```

---

## 四、异步设计详解

### 同步 vs 异步对比

| 操作类型 | 同步方式（旧） | 异步方式（新） | 说明 |
|---------|--------------|--------------|------|
| LLM 调用 | `llm.generate_single()` | `await llm.generate_single_async()` | 不阻塞事件循环 |
| HTTP 请求 | `requests.post()` | `async with aiohttp.post()` | 真正异步 I/O |
| 后台任务 | `threading.Thread` | `asyncio.create_task()` | 共享事件循环 |

### 异步调用示例

```python
# Windows Bridge 异步客户端
class WindowsBridgeClient:
    
    async def clean_document_async(self, file_path, ...):
        """异步清理文档 - 不阻塞事件循环"""
        async with await self._get_aiohttp_session() as session:
            async with session.post(url, data=data, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    return {"success": False, "error": f"HTTP {resp.status}"}
```

### 并发能力对比

```
【改造前】
请求1: 校验   ████████████████████  (LLM阻塞20秒)
请求2: 清理         等待...   ████████  (需要等请求1释放线程)

【改造后】
请求1: 校验   ████████████████████  (异步LLM，不阻塞)
请求2: 清理   ████████              (同时执行)
```

---

## 五、任务管理机制

### TaskManager（任务管理器）

```python
# service/linux/generation/task_manager.py

class TaskManager:
    """
    任务管理器（单例模式）
    - 线程安全：使用 threading.Lock
    - 支持任务状态追踪和查询
    """
    
    def create_task(self, callback_url, config) -> str:
        """创建任务，返回 task_id"""
        task_id = f"task_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        with self._task_lock:
            self._tasks[task_id] = TaskInfo(task_id=task_id, ...)
        return task_id
    
    def get_task(self, task_id) -> TaskInfo:
        """查询任务状态"""
        with self._task_lock:
            return self._tasks.get(task_id)
    
    def complete_task(self, task_id, result):
        """标记任务完成"""
        with self._task_lock:
            task = self._tasks.get(task_id)
            task.progress.stage = TaskStage.COMPLETED
            task.result = result
```

### 任务生命周期

```
PENDING → EXTRACTION → GENERATION → COMPLETED
    │                       │
    └──────────────────────▶ FAILED
```

---

## 六、目录结构

```
csr文档生成/
├── api/                          # API 层
│   ├── linux/                    # Linux 端接口
│   │   ├── main.py              # FastAPI 主应用
│   │   ├── validation.py        # 接口1: 数据源校验
│   │   ├── generation.py        # 接口2: 文本生成
│   │   ├── compose.py           # 接口3: 完整流程
│   │   ├── preprocessing.py     # 接口4: 批量预处理
│   │   ├── insertion.py         # 接口5: 模板插入
│   │   ├── allocation.py        # 接口6: 数据分配
│   │   └── postprocessing.py    # 接口7: 文档清理
│   └── windows/
│       └── app.py               # Windows Bridge 服务
│
├── service/                      # Service 层
│   ├── linux/
│   │   ├── validation/
│   │   │   └── validation_service.py
│   │   ├── generation/
│   │   │   ├── generation_service.py   # 生成服务
│   │   │   ├── task_manager.py         # 任务管理器
│   │   │   ├── flow_controller.py      # 流程控制器
│   │   │   └── progress_callback.py    # 回调处理
│   │   ├── allocation/
│   │   │   ├── allocation_service.py
│   │   │   └── data_source_validator.py
│   │   ├── preprocessing/
│   │   │   └── preprocessing_task_service.py
│   │   └── bridge/
│   │       └── windows_bridge_client.py  # Windows Bridge 客户端
│   ├── models/
│   │   └── llm_service.py       # LLM 调用服务
│   └── prompts/                  # 提示词模板
│
├── config/
│   └── settings.py              # 配置管理
│
├── utils/                        # 工具函数
│
└── AAA/                          # 共享存储目录
    ├── project_data/            # 源文件
    ├── Preprocessing/           # 预处理结果
    ├── Template/                # Word 模板
    └── output/                  # 输出结果
```

---

## 七、关键配置

### 环境变量

| 变量名 | 说明 | 默认值 |
|-------|------|--------|
| `WINDOWS_BRIDGE_URL` | Windows Bridge 服务地址 | `http://192.168.3.70:8081` |
| `BASE_DATA_DIR` | 基础数据目录 | `AAA/project_data` |
| `COMPOSE_OUTPUT_DIR` | 输出目录 | `AAA/output` |
| `LLM_ASYNC` | 是否使用异步 LLM | `1` |

### 启动命令

```bash
# Linux API 服务
cd csr文档生成
uvicorn api.linux.main:app --host 0.0.0.0 --port 8000

# Windows Bridge 服务
cd csr文档生成
python api/windows/app.py
```

---

## 八、总结

### 核心设计原则

1. **三层分离**: API 层只做路由，Service 层处理业务，底层处理具体实现
2. **异步优先**: 所有 I/O 操作（LLM调用、HTTP请求）都使用异步方式
3. **任务化管理**: 耗时操作通过任务管理器追踪，支持状态查询和回调通知
4. **跨平台协作**: Linux 端处理逻辑，Windows 端处理 Office 文档

### 接口执行方式对照表

| 接口 | 执行方式 | 返回时机 | 结果通知方式 |
|-----|---------|---------|-------------|
| 校验 | 异步等待 | 完成后返回 | 直接返回结果 |
| 生成 | 后台线程 | 立即返回 | 回调通知 |
| 完整流程 | 后台线程 | 立即返回 | 回调通知 |
| 预处理 | asyncio任务 | 立即返回 | 回调通知 |
| 插入 | 异步等待 | 完成后返回 | 直接返回结果 |
| 分配 | 同步执行 | 完成后返回 | 直接返回结果 |
| 清理 | 异步等待 | 完成后返回 | 直接返回结果 |





