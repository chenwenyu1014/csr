# CSR 文档生成系统

基于 FastAPI 的 CSR 文档自动生成服务，支持 Linux/Windows 跨平台协作。

## 系统架构

```
Linux API Service (port 8000)
        │
        │ HTTP (aiohttp 异步)
        ▼
Windows Bridge Service (port 8081)
        │
        ▼
Microsoft Word/Excel (COM)
```

## 项目结构

```
csr-api-service/
├── api/                              # API 层
│   ├── linux/                        # Linux 端接口
│   │   ├── main.py                   # FastAPI 主应用
│   │   ├── validation.py             # 接口1: 数据源校验
│   │   ├── generation.py             # 接口2: 文本生成
│   │   ├── compose.py                # 接口3: 完整流程
│   │   ├── preprocessing.py          # 接口4: 批量预处理
│   │   ├── insertion.py              # 接口5: 模板插入
│   │   ├── allocation.py             # 接口6: 数据分配
│   │   └── postprocessing.py         # 接口7: 文档清理
│   └── windows/
│       └── app.py                    # Windows Bridge 服务
│
├── service/                          # Service 层
│   ├── linux/
│   │   ├── validation/               # 校验服务
│   │   ├── generation/               # 生成服务
│   │   │   ├── generation_service.py
│   │   │   ├── flow_controller.py
│   │   │   ├── task_manager.py
│   │   │   └── progress_callback.py
│   │   ├── allocation/               # 分配服务
│   │   ├── preprocessing/            # 预处理服务
│   │   └── bridge/                   # Windows Bridge 客户端
│   ├── models/                       # LLM 服务
│   ├── prompts/                      # 提示词模板
│   └── windows/                      # Windows 端服务
│
├── config/
│   └── settings.py                   # 配置管理
│
├── utils/                            # 工具函数
│
└── AAA/                              # 共享存储目录
    ├── project_data/                 # 源文件
    ├── Preprocessing/                # 预处理结果
    ├── Template/                     # Word 模板
    └── output/                       # 输出结果
```

## 快速开始

### 安装依赖
```bash
pip install -r requirements.txt
```

### 配置环境
```bash
cp .env.example .env
# 编辑 .env 文件配置必要参数
```

### 启动服务

Linux API 服务：
```bash
uvicorn api.linux.main:app --host 0.0.0.0 --port 8000
```

Windows Bridge 服务：
```bash
python api/windows/app.py
```

## 7 个核心接口

| 接口 | 路径 | 执行方式 | 说明 |
|-----|------|---------|------|
| 校验 | `POST /api/v1/validation/data-source` | 异步等待 | 使用 LLM 校验数据源 |
| 生成 | `POST /api/v1/flow/run-text` | 后台线程 | 异步执行内容生成 |
| 完整流程 | `POST /api/v1/documents/compose` | 后台线程 | 生成 + 插入模板 |
| 预处理 | `POST /api/v1/preprocessing/batch-simple` | asyncio任务 | 文件预处理 |
| 插入 | `POST /api/v1/template/insert` | 异步等待 | 内容插入 Word 模板 |
| 分配 | `POST /api/v1/datasource/allocate` | 同步执行 | LLM 匹配文件与数据需求 |
| 清理 | `POST /api/v1/document/clean` | 异步等待 | 清理 Content Control |

## 环境变量

```env
# 基础配置
BASE_DATA_DIR=AAA/project_data
COMPOSE_OUTPUT_DIR=AAA/output
CACHE_DIR=AAA/cache

# LLM 配置
LLM_MODEL=qwen-max
LLM_API_KEY=your_api_key
LLM_ASYNC=1

# Windows Bridge
WINDOWS_BRIDGE_URL=http://192.168.3.70:8081
WINDOWS_BRIDGE_TIMEOUT=300
```

## 部署说明

### 分布式部署（推荐）
- Linux 服务器：运行 API 服务 (`api/linux/`)
- Windows 服务器：运行 Bridge 服务 (`api/windows/`)
- 通过 `WINDOWS_BRIDGE_URL` 配置连接

### 单机部署
Windows 机器上可同时运行两个服务：
```bash
# 终端1：API 服务
uvicorn api.linux.main:app --host 0.0.0.0 --port 8000

# 终端2：Bridge 服务
python api/windows/app.py
```

⚠️ Windows Bridge 必须使用单 worker，因为 Word COM 不支持多线程。

## 设计原则

- 三层分离：API 层 → Service 层 → Model/Bridge 层
- 异步优先：所有 I/O 操作使用异步方式
- 任务化管理：耗时操作通过 TaskManager 追踪
- 跨平台协作：Linux 处理逻辑，Windows 处理 Office 文档

详细设计文档请参考 [CSR系统设计文档.md](./CSR系统设计文档.md)
