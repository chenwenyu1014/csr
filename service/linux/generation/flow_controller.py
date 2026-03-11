#!/usr/bin/env python3
"""
CSR内容生成流程控制器
统一调用所有模块化功能，实现完整的业务流程
"""

import logging  # 标准库：日志记录
from datetime import datetime  # 标准库：时间与时间戳
import uuid  # 标准库：运行随机后缀
import time  # 标准库：耗时与时间戳
import os  # 标准库：重复导入（向下兼容旧代码）
import re  # 标准库：正则处理
import json as _json_mod  # 标准库：JSON 序列化（使用别名避免命名冲突）
from pathlib import Path  # 标准库：路径对象
from typing import Any, Dict, List, Optional  # 标准库：类型注解
from dataclasses import dataclass  # 标准库：配置/结果的数据类

from service.linux.generation.pipeline import CSRGenerationPipeline  # 核心：加载段落并负责生成/处理流水线
from service.linux.generation.data_extractor import DataExtractorV2  # 核心：各数据类型的提取（方案/TFL等）
# from service.windows.insertion.word_post_processor import CSRWordPostProcessor  # 核心：对生成结果做后处理
from service.linux.generation.paragraph_generation_service import ParagraphGenerationService  # 核心：段落生成服务
from utils.config_parser import ConfigParser  # 解析段落配置 JSON
from service.models import create_llm_service, create_vision_model_service  # 初始化模型/视觉服务
from utils.context_manager import set_current_output_dir, get_current_output_dir  # 工具：线程安全的上下文管理

from utils.output_manager import (  # 工具：输出目录与文件落盘
    save_json,
    save_text,
)
from utils.task_logger import get_task_logger

logger = logging.getLogger(__name__)


def _task_log_error(message: str, exc: Exception = None, **extra):
    """记录错误到任务日志"""
    task_logger = get_task_logger()
    if task_logger:
        task_logger.error(message, exc=exc, logger_name="flow_controller", **extra)

@dataclass
class FlowConfig:
    """流程配置"""
    config_path: str = None  # 段落配置路径（已废弃）
    base_data_dir: str = "data/rtf&index"
    cache_dir: str = "cache"
    output_dir: str = "output"
    use_mock_services: bool = False
    log_level: str = "INFO"
    # Word集成配置
    word_template_file: Optional[str] = None  # Word模板文件路径
    word_output_file: Optional[str] = None  # Word输出文件路径
    word_placeholder_format: str = "{{%s}}"  # Word占位符格式
    enable_word_integration: bool = False  # 是否启用Word集成
    # 全局：无论是否走"原文"分支，都先对方案Word做一次标记与导出
    premark_scheme_tables_images: bool = True
    # 是否在流程开头就按标签导出（固定为开启，尾部仅做映射）
    early_export_regions: bool = True
    # 外部插入工具接口（若不为空则主流程末尾会打包并调用）
    compose_insert_url: Optional[str] = None
    # 是否跳过提取校验阶段（设为True可加快处理速度，但可能降低提取质量）
    skip_validation: bool = True  # 默认跳过校验，加快处理速度

@dataclass
class FlowResult:
    """流程执行结果"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class CSRFlowController:
    """CSR内容生成流程控制器"""
    
    def __init__(self, config: FlowConfig):
        self.config = config
        # 1. 先初始化基础日志（控制台）
        self._setup_logging()
        # 2. 初始化会话目录（此时日志还未挂载到文件）
        self._initialize_session_dir()
        # 3. 立即挂载文件日志（确保后续所有日志都能写入文件）
        self._attach_file_loggers()
        # 4. 记录启动信息（现在会写入session.log）
        self._log_session_start()
        # 5. 初始化其他组件
        self._initialize_detailed_logger()
        self._initialize_services()
        self._initialize_pipeline()
    
    def _log_session_start(self):
        """记录会话启动信息到日志文件"""
        logger.info("=" * 60)
        logger.info("CSR内容生成流程启动")
        logger.info(f"会话目录: {self.session_dir}")
        logger.info(f"配置路径: {self.config.config_path}")
        logger.info(f"基础数据目录: {self.config.base_data_dir}")
        logger.info(f"输出目录: {self.config.output_dir}")
        logger.info(f"跳过校验: {self.config.skip_validation}")
        logger.info("=" * 60)
        
    def _initialize_session_dir(self):
        """初始化会话输出目录，通过线程安全的方式传递给各模块。"""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        rand = uuid.uuid4().hex[:6]
        # 若外部已指定 CURRENT_OUTPUT_DIR（通过线程本地或环境变量），则优先使用
        env_dir = get_current_output_dir(default="").strip()
        if env_dir:
            session_dir = env_dir
        else:
            base_output = self.config.output_dir or "output"
            # 统一命名为 run_YYYYMMDD_HHMMSS_xxxxxx
            session_dir = os.path.join(base_output, f"run_{ts}_{rand}")
        
        # 设置是否跳过提取校验的环境变量
        if self.config.skip_validation:
            os.environ["SKIP_EXTRACTION_VALIDATION"] = "1"
            # 注意：此日志在文件handler挂载前，不会写入session.log
        else:
            os.environ["SKIP_EXTRACTION_VALIDATION"] = "0"
        # 创建基础目录结构（仅 inputs/ 与 outputs，prompts 受开关控制）
        os.makedirs(os.path.join(session_dir, "inputs"), exist_ok=True)
        os.makedirs(os.path.join(session_dir, "outputs"), exist_ok=True)
        try:
            sp = (os.getenv("SAVE_PROMPTS", "0").strip().lower() in ("1", "true", "yes", "y", "on", "是"))
            if sp:
                os.makedirs(os.path.join(session_dir, "prompts"), exist_ok=True)
        except Exception:
            pass
        # 使用线程安全的方式设置当前输出目录
        set_current_output_dir(session_dir)
        # 保存实例属性
        self.session_dir = session_dir

        # 环境与配置快照
        try:
            import sys
            import platform
            import json as _json
            from pathlib import Path as _Path
            import shutil as _shutil

            inputs_dir = _Path(session_dir) / "inputs"
            env_snapshot = {
                "timestamp": ts,
                "python_version": sys.version,
                "platform": platform.platform(),
                "executable": sys.executable,
                "argv": sys.argv,
                "cwd": os.getcwd(),
                "env": {
                    # 脱敏关键变量
                    k: (os.getenv(k)[:4] + "***" if os.getenv(k) else None)
                    for k in ["DASHSCOPE_API_KEY"]
                },
                "config_files": {
                    "paragraphs": self.config.config_path,
                    "system_prompts": "prompts/system/system_prompts.json",
                    "unified_models": "prompts/models/unified_models.json"
                }
            }
            # 应用版本与Git提交
            try:
                try:
                    from src import __version__ as _app_version
                except Exception:
                    _app_version = None
                _git_commit = None
                try:
                    import subprocess as _subp
                    _git_commit = _subp.check_output(["git", "rev-parse", "--short", "HEAD"], stderr=_subp.DEVNULL, text=True).strip()
                except Exception:
                    try:
                        head = _Path(".git/HEAD")
                        if head.exists():
                            ref = head.read_text(encoding="utf-8").strip()
                            if ref.startswith("ref:"):
                                ref_path = _Path(".git") / ref.split(":", 1)[1].strip()
                                if ref_path.exists():
                                    _git_commit = ref_path.read_text(encoding="utf-8").strip()[:7]
                            else:
                                _git_commit = ref[:7]
                    except Exception:
                        _git_commit = None
                env_snapshot["app"] = {"version": _app_version, "git_commit": _git_commit}
            except Exception:
                pass
            # 写入 environment.json
            with open(inputs_dir / "environment.json", "w", encoding="utf-8") as f:
                f.write(_json.dumps(env_snapshot, ensure_ascii=False, indent=2))

            # 复制关键配置文件到 inputs/configs
            cfg_out = inputs_dir / "configs"
            cfg_out.mkdir(parents=True, exist_ok=True)
            for rel in [
                "prompts/system/system_prompts.json",
                "prompts/models/unified_models.json",
            ]:
                try:
                    src_p = _Path(rel)
                    if src_p.exists():
                        _shutil.copy2(src_p, cfg_out / src_p.name)
                except Exception as _e:
                    logger.warning(f"复制配置文件失败 {rel}: {_e}")
            
            # 保存config_path（如果提供）
            if self.config.config_path:
                try:
                    src_p = _Path(self.config.config_path)
                    if src_p.exists():
                        _shutil.copy2(src_p, cfg_out / src_p.name)
                except Exception as _e:
                    logger.warning(f"复制配置文件失败 {self.config.config_path}: {_e}")
        except Exception as e:
            logger.warning(f"保存环境与配置快照失败: {e}")

    def _attach_file_loggers(self) -> None:
        """为当前会话目录挂载文件日志：
        - session.log: 主流程日志（INFO）
        - system.log: 系统调试日志（DEBUG）
        
        注意：
        - 使用 FlushingFileHandler 确保日志实时写入文件
        - 使用 SessionFilter 基于会话目录过滤（支持多线程/子线程日志记录）
        - Handler 引用保存到 self._session_file_handlers，便于任务完成后清理
        """
        import threading
        
        # 初始化 handler 列表
        self._session_file_handlers = []
        
        # 记录当前线程ID（用于标识会话所有者，但不用于过滤）
        self._owner_thread_id = threading.current_thread().ident
        
        try:
            session_dir = getattr(self, "session_dir", None)
            if not session_dir:
                return
            session_dir_path = Path(session_dir)
            session_dir_path.mkdir(parents=True, exist_ok=True)
            # JSON 日志（可配置），默认启用
            use_json = False
            try:
                use_json = (os.getenv("JSON_LOGS", "1").strip().lower() in ("1","true","yes","y","on","是"))
            except Exception:
                use_json = True
            if use_json:
                try:
                    from utils.json_logging import JSONLogFormatter  # type: ignore
                    formatter = JSONLogFormatter(service="flow")
                except Exception:
                    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            else:
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

            # 自定义 FileHandler：每条日志后立即 flush，确保实时写入
            class FlushingFileHandler(logging.FileHandler):
                """每条日志后立即刷新到文件的 FileHandler"""
                def emit(self, record):
                    super().emit(record)
                    self.flush()  # 立即将缓冲区内容写入文件
            
            # 会话过滤器：基于会话目录过滤，支持主线程及其创建的所有子线程
            # 不再使用线程ID过滤，而是使用环境变量中的会话目录匹配
            class SessionFilter(logging.Filter):
                """基于会话目录的日志过滤器（支持多线程）"""
                def __init__(self, session_dir: str):
                    super().__init__()
                    self.session_dir = session_dir
                
                def filter(self, record):
                    # 检查当前线程的输出目录是否匹配当前会话
                    try:
                        from utils.context_manager import get_current_output_dir
                        current_dir = get_current_output_dir(default="")
                        if current_dir and current_dir == self.session_dir:
                            return True
                    except Exception:
                        pass
                    
                    # 兜底：检查环境变量（子线程可能继承）
                    try:
                        env_dir = os.getenv("CURRENT_OUTPUT_DIR", "")
                        if env_dir and env_dir == self.session_dir:
                            return True
                    except Exception:
                        pass
                    
                    # 默认接受（避免过度过滤导致日志丢失）
                    # 在单任务模式下，接受所有日志
                    return True
            
            # 创建会话过滤器
            session_filter = SessionFilter(session_dir)

            # 主日志（用户可读的全过程日志）- 实时写入
            main_fh = FlushingFileHandler(session_dir_path / 'session.log', encoding='utf-8')
            main_fh.setLevel(logging.INFO)
            main_fh.setFormatter(formatter)
            main_fh.addFilter(session_filter)  # 使用会话过滤器（支持子线程）

            # 系统日志（更详细，便于排障）- 实时写入
            sys_fh = FlushingFileHandler(session_dir_path / 'system.log', encoding='utf-8')
            sys_fh.setLevel(logging.DEBUG)
            sys_fh.setFormatter(formatter)
            sys_fh.addFilter(session_filter)  # 使用会话过滤器（支持子线程）

            root_logger = logging.getLogger()
            # 直接添加 handler（每个会话有独立的文件路径，不需要检查重复）
            root_logger.addHandler(main_fh)
            self._session_file_handlers.append(main_fh)
            root_logger.addHandler(sys_fh)
            self._session_file_handlers.append(sys_fh)
            
            logger.info("文件日志已挂载（实时写入模式，支持子线程）")
        except Exception as e:
            logger.warning(f"挂载文件日志失败: {e}")

    def _setup_logging(self):
        """设置日志"""
        try:
            use_json = (os.getenv("JSON_LOGS", "1").strip().lower() in ("1","true","yes","y","on","是"))
        except Exception:
            use_json = True
        if use_json:
            try:
                from utils.json_logging import setup_json_logging  # type: ignore
                setup_json_logging(service="flow", level=self.config.log_level)
            except Exception:
                logging.basicConfig(
                    level=getattr(logging, self.config.log_level.upper()),
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
        else:
            logging.basicConfig(
                level=getattr(logging, self.config.log_level.upper()),
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        logger.info("流程控制器初始化完成")
    
    def _initialize_detailed_logger(self):
        """简化日志系统"""
        self.detailed_logger = None
        logger.info("日志系统初始化完成")
        
    def _initialize_services(self):
        """初始化所有服务"""
        try:
            # 初始化LLM服务
            self.llm_service = create_llm_service()
            logger.info("LLM服务初始化完成")
            
            # 初始化视觉服务
            self.vision_service = create_vision_model_service()
            logger.info("视觉服务初始化完成")
            
            # 初始化数据提取器
            self.data_extractor = DataExtractorV2(
                base_data_dir=self.config.base_data_dir,
                cache_dir=self.config.cache_dir
            )
            # 注入服务
            self.data_extractor.llm_service = self.llm_service
            self.data_extractor.vision_service = self.vision_service
            # 简化日志注入
            self.data_extractor.detailed_logger = None
            logger.info("数据提取器初始化完成")
            
            # 初始化段落生成服务（已合并ContentGenerator功能）
            self.paragraph_generation_service = ParagraphGenerationService()
            # 简化日志注入
            self.paragraph_generation_service.detailed_logger = None
            logger.info("段落生成服务初始化完成")
            
            # 初始化配置解析器（如果提供config_path）
            if self.config.config_path:
                self.config_parser = ConfigParser(self.config.config_path)
                logger.info("配置解析器初始化完成")
            else:
                self.config_parser = None
                logger.info("配置解析器未初始化（无config_path）")
            
        except Exception as e:
            logger.error(f"服务初始化失败: {e}")
            _task_log_error("服务初始化失败", exc=e)
            raise
            
    def _initialize_pipeline(self):
        """初始化流水线"""
        try:
            self.pipeline = CSRGenerationPipeline(
                config_path=self.config.config_path,
                base_data_dir=self.config.base_data_dir,
                cache_dir=self.config.cache_dir,
                use_mock_services=self.config.use_mock_services,
                model_service=self.llm_service
            )
            # 将服务注入到pipeline的data_extractor中
            self.pipeline.data_extractor.llm_service = self.llm_service
            self.pipeline.data_extractor.vision_service = self.vision_service
            # 简化日志注入
            self.pipeline.data_extractor.detailed_logger = None
            self.pipeline.paragraph_generation_service.detailed_logger = None
            logger.info("流水线初始化完成")
        except Exception as e:
            logger.error(f"流水线初始化失败: {e}")
            _task_log_error("流水线初始化失败", exc=e)
            raise
    
    def set_stage_callbacks(self, on_extraction_completed=None, on_generation_started=None):
        """
        设置阶段回调钩子（用于实时通知进度）
        
        Args:
            on_extraction_completed: 提取完毕回调函数
            on_generation_started: 开始生成回调函数
        """
        if self.pipeline:
            self.pipeline.on_extraction_completed = on_extraction_completed
            self.pipeline.on_generation_started = on_generation_started
            logger.info("已设置阶段回调钩子")
            #
            # # 初始化Word后处理器（仅在明确启用时）
            # self.word_post_processor = None
            #
            # # ✅ 只有明确启用Word集成时才初始化Word后处理器
            # if self.config.enable_word_integration and self.config.word_template_file:
            #     self.word_post_processor = CSRWordPostProcessor(
            #         template_file=self.config.word_template_file,
            #         output_file=self.config.word_output_file,
            #         placeholder_format=self.config.word_placeholder_format
            #     )
            #     logger.info("Word后处理器初始化完成")
            # else:
            #     logger.info("Word集成已禁用，跳过Word后处理器初始化")
    
    def get_paragraph_list(self) -> List[Dict[str, Any]]:
        """获取所有段落列表"""
        try:
            return self.pipeline.get_paragraph_list()
        except Exception as e:
            logger.error(f"获取段落列表失败: {e}")
            return []
    
    # def generate_single_paragraph(self, paragraph_id: str) -> FlowResult:
    #     """生成单个段落"""
    #     try:
    #         logger.info(f"开始生成段落: {paragraph_id}")
    #         result = self.pipeline.generate_single_paragraph(paragraph_id)
    #
    #         if result['status'] == 'success':
    #             return FlowResult(
    #                 success=True,
    #                 message=f"段落 {paragraph_id} 生成成功",
    #                 data=result
    #             )
    #         else:
    #             return FlowResult(
    #                 success=False,
    #                 message=f"段落 {paragraph_id} 生成失败",
    #                 error=result.get('error_message', '未知错误')
    #             )
    #     except Exception as e:
    #         logger.error(f"生成段落 {paragraph_id} 失败: {e}")
    #         _task_log_error(f"生成段落 {paragraph_id} 失败", exc=e, paragraph_id=paragraph_id)
    #         return FlowResult(
    #             success=False,
    #             message=f"生成段落 {paragraph_id} 失败",
    #             error=str(e)
    #         )
    #
    def generate_all_paragraphs(self) -> FlowResult:
        """生成所有段落"""
        try:
            logger.info("开始生成所有段落")
            # 旧的预标记逻辑已删除，现在使用基于preprocessing_index.json的新逻辑
            results = self.pipeline.generate_all_paragraphs()

            # ✅ 新增：为每个生成结果解析占位符并建立资源映射
            self._build_resource_mappings_for_results(results)

            # 汇总待人工选择的 TFL 项（当 LLM 给出 find=false 时）
            tfl_selections_required: List[Dict[str, Any]] = []
            try:
                for r in (results or []):
                    pid = r.get('paragraph_id')
                    items = (r.get('extracted_data') or {}).get('extracted_items', [])
                    for it in items:
                        try:
                            if (it or {}).get('data_type') == 'tfl' and (it or {}).get('status') == 'needs_selection':
                                tfl_selections_required.append({
                                    'paragraph_id': pid,
                                    'title': it.get('title', ''),
                                    'candidates': it.get('tfl_list', []) or [],
                                    'rationale': it.get('rationale', ''),
                                    'selection_result_file': it.get('tfl_selection_result_file', '')
                                })
                        except Exception:
                            continue
            except Exception as _e:
                logger.warning(f"收集待人工选择的TFL项失败: {_e}")
            
            success_count = sum(1 for r in results if r['status'] == 'success')
            total_count = len(results)
            
            # 统一输出：仅使用初始化的会话目录，不再创建 runs 结构
            run_dir = Path(self.session_dir)
            prompts_dir = run_dir / "prompts"
            outputs_dir = run_dir / "outputs"
            # 仅确保 outputs 存在；prompts 与 tfl 调试目录受开关控制
            outputs_dir.mkdir(parents=True, exist_ok=True)
            try:
                sp = (os.getenv("SAVE_PROMPTS", "0").strip().lower() in ("1", "true", "yes", "y", "on", "是"))
                if sp:
                    prompts_dir.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
            try:
                std = (os.getenv("SAVE_TFL_DEBUG", "0").strip().lower() in ("1", "true", "yes", "y", "on", "是"))
                if std:
                    tfl_dir = outputs_dir / "tfl_processing"
                    tfl_dir.mkdir(parents=True, exist_ok=True)
            except Exception as _e:
                logger.warning(f"创建tfl_processing目录失败: {_e}")

            # 保持环境变量为会话目录（不覆写）
            run_timestamp = time.strftime("%Y%m%d_%H%M%S")
            run_output_path = outputs_dir / f"run_output_{run_timestamp}.json"
            run_summary = {
                "timestamp": run_timestamp,
                "run_dir": str(run_dir),
                "success_count": success_count,
                "total_count": total_count,
                "results": results,
                "selection_needed": True if tfl_selections_required else False,
                "tfl_selections_required": tfl_selections_required,
                # 溯源直达文件指针（运行过程中逐步填充）
                "complete_json_file": None,
                "insert_payload_file": None,
                "bundle_file": None,
                "external_result_file": None,
                "external_insert_meta_file": None,
            }
            
            # ====== 不再使用Word预插入，word_result保留供后续逻辑使用 ======
            word_result = None
            
            # 运行时指标与制品索引容器
            timings: Dict[str, Any] = {
                "start_ts": time.time()
            }
            artifacts: Dict[str, Any] = {
                "logs": [],
                "prompts": [],
                "outputs": [],
                "tfl_processing": []
            }

            # 落盘本次运行汇总（JSON）
            save_json(run_output_path, run_summary)

            # 独立保存：聚合的“数据提取结果”文件（JSON 与可读文本）
            try:
                extracted_json_path = outputs_dir / f"extracted_data_{run_timestamp}.json"
                extracted_txt_path = outputs_dir / f"extracted_data_{run_timestamp}.txt"
                extracted_payload = {
                    "timestamp": run_timestamp,
                    "run_dir": str(run_dir),
                    "paragraphs": []
                }
                readable_lines: List[str] = []
                for r in results:
                    pid = r.get('paragraph_id', '')
                    para_items = (r.get('extracted_data') or {}).get('extracted_items', [])
                    extracted_payload["paragraphs"].append({
                        "paragraph_id": pid,
                        "items": para_items
                    })
                    readable_lines.append(f"=== 段落ID: {pid} 的数据提取结果 ===")
                    if para_items:
                        for idx, it in enumerate(para_items, 1):
                            data_type = it.get('data_type', '未知类型')
                            status = it.get('status')
                            content = it.get('content') or ''
                            error = it.get('error')
                            readable_lines.append(f"{idx}. [{data_type}] status={status}")
                            if status == 'success':
                                readable_lines.append(content)
                            else:
                                readable_lines.append(f"错误: {error}")
                            readable_lines.append("")
                    else:
                        readable_lines.append("(无数据)")
                        readable_lines.append("")

                # 保存JSON与TXT（TXT仅保留纯内容，不包含编号/状态/段落ID/解释）
                save_json(extracted_json_path, extracted_payload)
                try:
                    pure_lines = []
                    for r in results:
                        pid = r.get('paragraph_id', '')
                        items = (r.get('extracted_data') or {}).get('extracted_items', [])
                        for it in items:
                            try:
                                if it.get('status') == 'success':
                                    ct = it.get('content') or ''
                                    if ct:
                                        pure_lines.append(str(ct))
                                        pure_lines.append("")
                            except Exception:
                                continue
                    save_text(extracted_txt_path, "\n".join(pure_lines).strip())
                except Exception:
                    # 兜底：仍回退原有可读文本
                    save_text(extracted_txt_path, "\n".join(readable_lines))
            except Exception as e:
                logger.warning(f"保存提取数据文件失败: {e}")

            # 生成友好阅读的聚合文本输出（生成结果）
            readable_path = outputs_dir / f"content_{run_timestamp}.txt"
            lines = []
            for r in results:
                pid = r.get('paragraph_id', '')
                if r.get('status') == 'success':
                    lines.append(f"=== 段落ID: {pid} ===")
                    _content = r.get('generated_content', '') or ''
                    # 剥离“标签信息 (JSON)”并落盘为独立文件，文本仅保留清理后的正文
                    try:
                        m = re.search(r"##\s*标签信息\s*\(JSON\)\s*```json\s*(\{[\s\S]*?\})\s*```", _content)
                        if m:
                            json_str = m.group(1)
                            try:
                                data = _json_mod.loads(json_str)
                            except Exception:
                                data = None
                            # 保存到当前会话 outputs 目录（以段落ID与本次run时间戳命名，避免重复）
                            try:
                                plan_json_path = outputs_dir / f"plan_labels_{pid}_{run_timestamp}.json"
                                if data is not None:
                                    save_json(plan_json_path, data)
                            except Exception:
                                pass
                            # 从内容中移除整段“标签信息 (JSON)”块
                            _content = re.sub(r"##\s*标签信息\s*\(JSON\)[\s\S]*?```\s*", "", _content, count=1).strip()
                            # 若存在“清理后的原文内容”，则仅保留其后的正文
                            parts = _content.split("## 清理后的原文内容", 1)
                            if len(parts) > 1:
                                _content = parts[1].strip()
                    except Exception:
                        pass
                    lines.append(_content)
                    lines.append("")
                else:
                    lines.append(f"=== 段落ID: {pid} (生成失败) ===")
                    lines.append(f"错误: {r.get('error_message', '未知错误')}")
                    lines.append("")
            save_text(readable_path, "\n".join(lines))

            # 用户级日志：仅在开启 SAVE_PROMPTS 时生成关联清单
            try:
                sp = (os.getenv("SAVE_PROMPTS", "0").strip().lower() in ("1", "true", "yes", "y", "on", "是"))
                if sp:
                    user_log = {
                        "timestamp": run_timestamp,
                        "run_dir": str(run_dir),
                        "paragraphs": []
                    }
                    prompts_dir = run_dir / 'prompts'
                    prompt_files = list(prompts_dir.glob('*.txt')) if prompts_dir.exists() else []
                    # 记录制品：prompts
                    artifacts["prompts"] = [str(p) for p in prompt_files]
                    for r in results:
                        pid = r.get('paragraph_id', '')
                        para_entry = {
                            "paragraph_id": pid,
                            "generate_prompt": r.get('generation_input', {}).get('generate_prompt'),
                            "example": r.get('generation_input', {}).get('example'),
                            "extracted_data": r.get('extracted_data', {}).get('extracted_items', []) if r.get('extracted_data') else [],
                            "generated_content": r.get('generated_content', ''),
                            "generation_prompt_files": [str(p) for p in prompt_files if p.name.startswith(f'generation_prompt_{pid}_')],
                        }
                        user_log["paragraphs"].append(para_entry)

                    # 全局提示词（数据提取/TFL相关）仅记录文件路径
                    user_log["global_prompts"] = {
                        "data_extraction": [str(p) for p in prompt_files if p.name.startswith('data_extraction_prompt_') or p.name.startswith('data_extraction_summary_prompt_')],
                        "original_extraction": [str(p) for p in prompt_files if p.name.startswith('original_extraction_prompt_')],
                        "tfl": [str(p) for p in prompt_files if p.name.startswith('tfl_')]
                    }

                    save_json(outputs_dir / f'user_log_{run_timestamp}.json', user_log)
            except Exception as e:
                logger.warning(f"保存用户级日志失败: {e}")

            # 创建本次运行的独立日志文件（受 SAVE_LOGS 控制）
            run_log_path = None
            try:
                sl = (os.getenv("SAVE_LOGS", "0").strip().lower() in ("1", "true", "yes", "y", "on", "是"))
            except Exception:
                sl = False
            if sl:
                logs_dir = run_dir / "logs"
                logs_dir.mkdir(parents=True, exist_ok=True)
                run_log_path = logs_dir / f"run_{run_timestamp}.log"
                save_text(run_log_path, f"run: {run_timestamp}\nsuccess: {success_count}/{total_count}\n")
                # 记录制品：logs与outputs
                artifacts["logs"].append(str(run_dir / 'session.log'))
                artifacts["logs"].append(str(run_dir / 'system.log'))
                artifacts["logs"].append(str(run_log_path))
            artifacts["outputs"].extend([
                str(run_output_path),
                str(readable_path),
                str(extracted_json_path) if 'extracted_json_path' in locals() else None,
                str(extracted_txt_path) if 'extracted_txt_path' in locals() else None,
            ])
            artifacts["outputs"] = [p for p in artifacts["outputs"] if p]

            # 构建“按配置解析结构聚合”的单一JSON（session_report）
            try:
                timings["report_build_start"] = time.time()
                para_configs = self.get_paragraph_list()  # 含 id/generate/example/data
                # 索引结果按段落
                result_by_id = {r.get('paragraph_id'): r for r in results}
                # 收集所有提示词文件
                all_prompt_files = list((run_dir / 'prompts').glob('*.txt'))
                # 生成每段的报告条目（并发构建）
                from concurrent.futures import ThreadPoolExecutor, as_completed
                report_paragraphs: List[Dict[str, Any]] = []

                def _build_entry(pc: Dict[str, Any]) -> Dict[str, Any]:
                    pid = pc.get('id')
                    r = result_by_id.get(pid, {})
                    gen_prompt_files = [str(p) for p in all_prompt_files if p.name.startswith(f'generation_prompt_{pid}_')]
                    data_types = [d.get('type') for d in pc.get('data', [])]
                    extraction_prompt_files: List[str] = []
                    for dt in data_types:
                        if not dt:
                            continue
                        extraction_prompt_files.extend([str(p) for p in all_prompt_files if p.name.startswith(f'data_extraction_prompt_{pid}_{dt}_')])
                        if not any(p for p in extraction_prompt_files if f'data_extraction_prompt_{pid}_{dt}_' in p):
                            extraction_prompt_files.extend([str(p) for p in all_prompt_files if p.name.startswith(f'data_extraction_prompt_{dt}_')])
                    # 追加原文提取提示词（按段落精确匹配）
                    # 优先匹配包含段落ID前缀的文件名 original_extraction_prompt_{pid}_*
                    _orig_prompts_precise = [
                        str(p) for p in all_prompt_files
                        if p.name.startswith(f'original_extraction_prompt_{pid}_')
                    ]
                    # 回退：若无段落ID前缀文件，再匹配不含段落ID的（极少数降级路径）
                    if not _orig_prompts_precise:
                        _orig_prompts_precise = [
                            str(p) for p in all_prompt_files
                            if p.name.startswith('original_extraction_prompt_')
                        ]
                    extraction_prompt_files.extend(_orig_prompts_precise)
                    has_tfl = any((d.get('type') or '').lower() == 'tfl' for d in pc.get('data', []))
                    tfl_prompt_files = [str(p) for p in all_prompt_files if p.name.startswith('tfl_')] if has_tfl else []
                    tfl_selection: List[Dict[str, Any]] = []
                    tfl_verifications: List[Dict[str, Any]] = []
                    parallel_outputs: List[Dict[str, Any]] = []
                    items = (r.get('extracted_data') or {}).get('extracted_items', [])
                    for it in items:
                        if (it or {}).get('data_type') == 'tfl':
                            if it.get('tfl_saved_outputs'):
                                parallel_outputs.extend(it.get('tfl_saved_outputs') or [])
                            if it.get('tfl_verification'):
                                tfl_verifications.append(it.get('tfl_verification'))
                            if it.get('tfl_selection'):
                                tfl_selection.append(it.get('tfl_selection'))
                            # 将TFL增强后的提取内容标注信息汇入
                            # 兼容已在提取阶段完成增强的结构
                            if it.get('tfl_processing_info'):
                                tfl_verifications.append({'processing_info': it.get('tfl_processing_info')})
                    for po in parallel_outputs:
                        files = (po or {}).get('files') or {}
                        for _, fp in files.items():
                            artifacts["tfl_processing"].append(fp)
                    return {
                        'id': pid,
                        'config': {
                            'generate': pc.get('generate'),
                            'example': pc.get('example'),
                            'data': pc.get('data', [])
                        },
                        'steps': {
                            'data_extraction': {
                                'items': (r.get('extracted_data') or {}).get('extracted_items', []),
                                'prompt_files': extraction_prompt_files
                            },
                            'generation': {
                                'prompt_files': gen_prompt_files,
                                'generated_content': r.get('generated_content', '')
                            },
                            # TFL处理信息已在提取阶段完成，这里作为步骤信息汇总
                            'tfl_processing': {
                                'prompt_files': tfl_prompt_files,
                                'parallel_outputs': parallel_outputs,
                                'verification_results': tfl_verifications,
                                'selection': tfl_selection
                            }
                        }
                    }

                max_workers = len(para_configs) if para_configs else 0
                if max_workers <= 1:
                    for pc in para_configs:
                        report_paragraphs.append(_build_entry(pc))
                else:
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = {executor.submit(_build_entry, pc): pc for pc in para_configs}
                        for future in as_completed(futures):
                            try:
                                entry = future.result()
                                report_paragraphs.append(entry)
                            except Exception as _e:
                                logger.warning(f"构建报告条目失败: {_e}")

                session_report = {
                    'timestamp': run_timestamp,
                    'run_dir': str(run_dir),
                    'paragraphs': report_paragraphs,
                }
                session_report_path = outputs_dir / f'session_report_{run_timestamp}.json'
                save_json(session_report_path, session_report)
                artifacts["outputs"].append(str(session_report_path))
                timings["report_build_end"] = time.time()

                # === 追加：构建简明 trace.json（每段落 -> 生成提示词/原始输出路径）===
                try:
                    def _latest_generation_output_file(pid: str) -> Optional[str]:
                        try:
                            files = [p for p in (outputs_dir).glob(f'generation_output_{pid}_*.txt')]
                            if not files:
                                return None
                            files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                            return str(files[0])
                        except Exception:
                            return None

                    trace_entries: List[Dict[str, Any]] = []
                    for pc in (para_configs or []):
                        try:
                            pid = pc.get('id')
                            gen_prompt = _latest_generation_prompt_file(pid)
                            gen_output = _latest_generation_output_file(pid)
                            # 关联提取类提示词（与上方 session_report 逻辑一致）
                            data_types = [d.get('type') for d in pc.get('data', [])]
                            extraction_prompt_files: List[str] = []
                            for dt in data_types:
                                if not dt:
                                    continue
                                extraction_prompt_files.extend([str(p) for p in all_prompt_files if p.name.startswith(f'data_extraction_prompt_{pid}_{dt}_')])
                                if not any(p for p in extraction_prompt_files if f'data_extraction_prompt_{pid}_{dt}_' in p):
                                    extraction_prompt_files.extend([str(p) for p in all_prompt_files if p.name.startswith(f'data_extraction_prompt_{dt}_')])
                            # 原文类提示词
                            _orig_precise = [str(p) for p in all_prompt_files if p.name.startswith(f'original_extraction_prompt_{pid}_')]
                            if not _orig_precise:
                                _orig_precise = [str(p) for p in all_prompt_files if p.name.startswith('original_extraction_prompt_')]
                            extraction_prompt_files.extend(_orig_precise)

                            trace_entries.append({
                                'paragraph_id': pid,
                                'generation': {
                                    'prompt_file': gen_prompt,
                                    'raw_output_file': gen_output,
                                },
                                'extraction': {
                                    'prompt_files': extraction_prompt_files
                                }
                            })
                        except Exception:
                            continue

                    trace_payload = {
                        'timestamp': run_timestamp,
                        'run_dir': str(run_dir),
                        'paragraphs': trace_entries,
                    }
                    trace_path = outputs_dir / 'trace.json'
                    save_json(trace_path, trace_payload)
                    artifacts["outputs"].append(str(trace_path))
                    try:
                        run_summary['trace_file'] = str(trace_path)
                    except Exception:
                        pass
                except Exception as _e:
                    logger.warning(f"构建trace.json失败: {_e}")
            except Exception as e:
                logger.warning(f"构建session_report失败: {e}")

            # 构建更清晰的分层溯源（provenance）：
            # 1) 顶层为每个段落的最终结果
            # 2) 其下为生成前给到模型的输入（生成提示词 + 提取到的输入数据拆分为scheme/tfl）
            # 3) 再下为各提取项的来源与提示词文件
            try:
                prov_build_start = time.time()
                prov_paragraphs: List[Dict[str, Any]] = []

                # 工具：选择某段的最新生成提示词文件
                def _latest_generation_prompt_file(pid: str) -> Optional[str]:
                    try:
                        files = [p for p in (run_dir / 'prompts').glob(f'generation_prompt_{pid}_*.txt')]
                        if not files:
                            return None
                        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                        return str(files[0])
                    except Exception:
                        return None

                # 构建每个段落的溯源条目
                for pc in (para_configs or []):
                    try:
                        pid = pc.get('id')
                        r = result_by_id.get(pid, {})
                        gen_input = (r.get('generation_input') or {})
                        items = (r.get('extracted_data') or {}).get('extracted_items', [])
                        cfg_list = pc.get('data', []) or []

                        # 将用于生成阶段的输入按 scheme 与 tfl 分类（与 ContentGenerator._build_prompt 保持一致）
                        scheme_inputs: List[Dict[str, Any]] = []
                        tfl_inputs: List[Dict[str, Any]] = []
                        for it in (items or []):
                            try:
                                data_type_raw = (it or {}).get('data_type', '未知类型')
                                data_type = (str(data_type_raw) if data_type_raw is not None else '').lower()
                                entry = {
                                    'data_type': it.get('data_type'),
                                    'status': it.get('status'),
                                    'content': it.get('content') or '',
                                    'source_file': it.get('source_file'),
                                    'marked_file': it.get('marked_file'),
                                    'ocr_cache_file': it.get('ocr_cache_file'),
                                    'ocr_clean_cache_file': it.get('ocr_clean_cache_file')
                                }
                                if data_type == 'tfl':
                                    # 附带 TFL 相关元信息
                                    entry.update({
                                        'tfl_source_files': it.get('tfl_source_files'),
                                        'tfl_insert_mappings': it.get('tfl_insert_mappings'),
                                        'tfl_per_file_results': it.get('tfl_per_file_results'),
                                        'tfl_processing_info': it.get('tfl_processing_info')
                                    })
                                    tfl_inputs.append(entry)
                                else:
                                    scheme_inputs.append(entry)
                            except Exception:
                                continue

                        # 关联提示词文件（生成与提取）
                        gen_prompt_file = _latest_generation_prompt_file(pid)
                        data_types = [d.get('type') for d in pc.get('data', [])]
                        extraction_prompt_files: List[str] = []
                        for dt in data_types:
                            if not dt:
                                continue
                            # 精确匹配当前段落ID优先，其次为全局该类型
                            extraction_prompt_files.extend([str(p) for p in all_prompt_files if p.name.startswith(f'data_extraction_prompt_{pid}_{dt}_')])
                            if not any(p for p in extraction_prompt_files if f'data_extraction_prompt_{pid}_{dt}_' in p):
                                extraction_prompt_files.extend([str(p) for p in all_prompt_files if p.name.startswith(f'data_extraction_prompt_{dt}_')])
                        # 原文类提示词
                        _orig_precise = [str(p) for p in all_prompt_files if p.name.startswith(f'original_extraction_prompt_{pid}_')]
                        if not _orig_precise:
                            _orig_precise = [str(p) for p in all_prompt_files if p.name.startswith('original_extraction_prompt_')]
                        extraction_prompt_files.extend(_orig_precise)

                        # 按提取项细化：为每个 item 附带其配置与提示词文件（基于顺序与类型匹配）
                        items_detailed: List[Dict[str, Any]] = []
                        for idx, it in enumerate(items or []):
                            try:
                                cfg_item = cfg_list[idx] if idx < len(cfg_list) else None
                            except Exception:
                                cfg_item = None
                            # 解析类型标识（优先用配置中的 type）
                            try:
                                _t = str((cfg_item or {}).get('type') or (it or {}).get('data_type') or '').strip()
                            except Exception:
                                _t = ''
                            item_prompt_files: List[str] = []
                            if _t:
                                try:
                                    # data_extraction 提示词（段内优先，其次全局类型）
                                    item_prompt_files.extend([
                                        str(p) for p in all_prompt_files if p.name.startswith(f'data_extraction_prompt_{pid}_{_t}_')
                                    ])
                                    if not item_prompt_files:
                                        item_prompt_files.extend([
                                            str(p) for p in all_prompt_files if p.name.startswith(f'data_extraction_prompt_{_t}_')
                                        ])
                                    # 原文提取提示词（段内优先，其次全局）
                                    _orig_pf = [str(p) for p in all_prompt_files if p.name.startswith(f'original_extraction_prompt_{pid}_{_t}_')]
                                    if not _orig_pf:
                                        _orig_pf = [str(p) for p in all_prompt_files if p.name.startswith(f'original_extraction_prompt_{_t}_')]
                                    item_prompt_files.extend(_orig_pf)
                                except Exception:
                                    pass
                            items_detailed.append({
                                'item': it,
                                'config': cfg_item,
                                'prompt_files': item_prompt_files
                            })

                        # ✅ 增强溯源：收集提取和生成的详细溯源
                        extraction_traceability = r.get('extraction_traceability', {})
                        generation_traceability = r.get('generation_traceability', {})
                        
                        prov_paragraphs.append({
                            'paragraph_id': pid,
                            'result': {
                                'status': r.get('status'),
                                'generated_content': r.get('generated_content', '')
                            },
                            'generation': {
                                'generate_prompt': gen_input.get('generate_prompt'),
                                'example': gen_input.get('example'),
                                'generation_prompt_file': gen_prompt_file,
                                'inputs': {
                                    'scheme_items': scheme_inputs,
                                    'tfl_items': tfl_inputs
                                },
                                # ✅ 溯源：生成阶段的详细信息
                                'traceability': generation_traceability
                            },
                            'extraction': {
                                'config_data': pc.get('data', []),
                                'prompt_files': extraction_prompt_files,
                                'items': items,
                                'items_detailed': items_detailed,
                                # ✅ 溯源：提取阶段的详细信息
                                'traceability': extraction_traceability
                            }
                        })
                    except Exception as _e:
                        logger.warning(f"构建段落溯源失败 {pc}: {_e}")

                provenance_payload = {
                    'timestamp': run_timestamp,
                    'run_dir': str(run_dir),
                    'paragraphs': prov_paragraphs
                }
                prov_path = outputs_dir / f'provenance_{run_timestamp}.json'
                save_json(prov_path, provenance_payload)
                try:
                    artifacts['outputs'].append(str(prov_path))
                except Exception:
                    pass
                try:
                    run_summary['provenance_file'] = str(prov_path)
                except Exception:
                    pass
                try:
                    timings['provenance_build_seconds'] = round(time.time() - prov_build_start, 3)
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"构建provenance失败: {e}")

            try:
                ts_obj = {
                    'timestamp': run_timestamp,
                    'run_dir': str(run_dir),
                    'summary': {
                        'success_count': success_count,
                        'total_count': total_count,
                        'selection_needed': True if tfl_selections_required else False
                    },
                    'artifacts': {
                        'run_output_file': str(run_output_path),
                        'readable_output_file': str(readable_path),
                        'extracted_data_file': str(extracted_json_path) if 'extracted_json_path' in locals() else None,
                        'provenance_file': str(prov_path) if 'prov_path' in locals() else None,
                        'session_report_file': str(session_report_path) if 'session_report_path' in locals() else None,
                        'complete_json_file': str(complete_path) if 'complete_path' in locals() else None
                    },
                    'paragraphs': []
                }
                try:
                    if 'prov_path' in locals() and Path(str(prov_path)).exists():
                        with open(prov_path, 'r', encoding='utf-8') as f:
                            d = _json_mod.load(f)
                        ts_obj['paragraphs'] = d.get('paragraphs') or []
                except Exception:
                    pass
                try:
                    if 'complete_path' in locals() and Path(str(complete_path)).exists():
                        with open(complete_path, 'r', encoding='utf-8') as f:
                            cj = _json_mod.load(f)
                        ts_obj['resources'] = {
                            'tfl': cj.get('tfl') or [],
                            'plan': cj.get('plan') or {'table': [], 'Image': []}
                        }
                except Exception:
                    pass
                trace_state_path = outputs_dir / f"trace_state_{run_timestamp}.json"
                save_json(trace_state_path, ts_obj)
                try:
                    artifacts['outputs'].append(str(trace_state_path))
                except Exception:
                    pass
                try:
                    run_summary['trace_state_file'] = str(trace_state_path)
                except Exception:
                    pass
            except Exception:
                pass

            # 保存运行制品索引与timings
            try:
                save_json(outputs_dir / f'artifacts_{run_timestamp}.json', artifacts)
                timings["end_ts"] = time.time()
                save_json(outputs_dir / f'timings_{run_timestamp}.json', timings)
            except Exception as e:
                logger.warning(f"保存artifacts失败: {e}")

            # 单独落盘待选择清单（便于对接端使用）
            try:
                if tfl_selections_required:
                    sel_path = outputs_dir / f'tfl_selections_{run_timestamp}.json'
                    save_json(sel_path, { 'timestamp': run_timestamp, 'items': tfl_selections_required })
                    try:
                        artifacts['outputs'].append(str(sel_path))
                    except Exception:
                        pass
            except Exception as _e:
                logger.warning(f"保存 tfl_selections 清单失败: {_e}")

            # 构建并保存本次运行的“完整JSON”（先合并标签与导出，再标准化为对外结构）
            try:
                # 1) 收集 TFL 数据项（所有段落） - 大小写兼容
                tfl_items: List[Dict[str, Any]] = []
                for r in results:
                    items = (r.get('extracted_data') or {}).get('extracted_items', [])
                    for it in items:
                        if isinstance(it, dict) and ((it.get('data_type') or '').lower() == 'tfl'):
                            tfl_items.append(it)

                # 2) 合并本次会话 outputs 下的 plan_labels_*.json（原始 Start/End）
                merged_plan: Dict[str, Any] = {"Path": "", "table": [], "Image": []}
                try:
                    import json as _json
                    seen_pairs = set()
                    plan_files = list(outputs_dir.glob('plan_labels_*.json'))
                    for pf in plan_files:
                        try:
                            with pf.open('r', encoding='utf-8') as f:
                                data = _json.load(f)
                            plan = (data or {}).get('plan') or {}
                            if (not merged_plan.get('Path')) and plan.get('Path'):
                                merged_plan['Path'] = plan.get('Path')
                            for entry in (plan.get('table') or []):
                                key = ('table', entry.get('Start'), entry.get('End'))
                                if key not in seen_pairs:
                                    merged_plan['table'].append(entry)
                                    seen_pairs.add(key)
                            for entry in (plan.get('Image') or []):
                                key = ('image', entry.get('Start'), entry.get('End'))
                                if key not in seen_pairs:
                                    merged_plan['Image'].append(entry)
                                    seen_pairs.add(key)
                        except Exception as _e:
                            logger.warning(f"合并方案标签JSON失败 {pf}: {_e}")
                except Exception as _e:
                    logger.warning(f"读取方案标签JSON失败: {_e}")

                # 3) 输出文件路径（若启用了Word后处理）
                final_word_path = ""
                try:
                    if word_result and getattr(word_result, 'output_file', None):
                        final_word_path = str(word_result.output_file)
                except Exception:
                    final_word_path = ""

                # 3.5) 直接从“生成内容”中抽取 Start 标签，覆盖 merged_plan（不依赖 plan_labels_*）
                try:
                    import re as _re
                    table_starts: list[str] = []
                    image_starts: list[str] = []
                    pat = _re.compile(r"\{\{(Table|Image)_(\d+)_Start\}\}")
                    for _r in (results or []):
                        s = (_r or {}).get('generated_content') or ''
                        if not s:
                            continue
                        for m in pat.finditer(s):
                            full = f"{{{{{m.group(1)}_{m.group(2)}_Start}}}}"
                            if m.group(1) == 'Table':
                                if full not in table_starts:
                                    table_starts.append(full)
                            else:
                                if full not in image_starts:
                                    image_starts.append(full)
                    if table_starts or image_starts:
                        _mpath = (merged_plan or {}).get('Path') or ''
                        merged_plan = {
                            'Path': _mpath,
                            'table': [ { 'Start': t, 'End': '' } for t in table_starts ],
                            'Image': [ { 'Start': i, 'End': '' } for i in image_starts ],
                        }
                except Exception:
                    pass

                # 4) 基于导出结果，构建新的 plan 结构（按标签 -> 导出文件路径），并标准化为 {id, Path}
                try:
                    marked_path = (merged_plan or {}).get('Path') or ''
                    # 兜底：若未从 plan_labels 获取标记文件路径，则从提取结果中寻找任一方案/伦理的 marked_file
                    if not marked_path:
                        try:
                            for _r in (results or []):
                                items = (_r.get('extracted_data') or {}).get('extracted_items', [])
                                for it in items:
                                    if not isinstance(it, dict):
                                        continue
                                    if (it.get('data_type') in ('方案', '伦理资料')) and it.get('marked_file'):
                                        marked_path = str(it.get('marked_file'))
                                        break
                                if marked_path:
                                    break
                        except Exception:
                            marked_path = ''
                    if not marked_path:
                        # 兜底：直接在 data/index 与当前会话目录搜索最新 *_marked.docx
                        try:
                            cands = []
                            try:
                                cands += list(Path('data/index').glob('*_marked.docx'))
                            except Exception:
                                pass
                            try:
                                cands += list(Path(self.session_dir).glob('**/*_marked.docx'))
                            except Exception:
                                pass
                            if cands:
                                cands.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                                marked_path = str(cands[0])
                        except Exception:
                            marked_path = ''
                    if not marked_path:
                        raise RuntimeError('无标记文件路径，跳过表/图导出与映射构建')

                    from service.windows.insertion.word_document_service import word_document_service as _wds
                    mp = Path(marked_path)
                    try:
                        export_dir = str(mp.parent / mp.stem)
                    except Exception:
                        export_dir = str(outputs_dir / 'plan_exports')

                    # 统一构造 regions（用于导出或映射）；若 plan 中没有标签，则基于标记文件扫描
                    regions = []
                    plan_tables = (merged_plan.get('table') or [])
                    plan_images = (merged_plan.get('Image') or [])
                    if (not plan_tables) and (not plan_images):
                        try:
                            scanned = _wds.scan_all_tag_regions(str(mp)) or []
                            for reg in scanned:
                                if reg.get('start') and reg.get('end'):
                                    regions.append({
                                        'start': reg.get('start'),
                                        'end': reg.get('end'),
                                        'name': (reg.get('name') or '').replace('{', '').replace('}', '')
                                    })
                        except Exception:
                            regions = []
                    else:
                        for entry in plan_tables:
                            s = entry.get('Start'); e = entry.get('End')
                            if s:
                                regions.append({'start': s, 'end': e or '', 'name': s.replace('{', '').replace('}', '')})
                        for entry in plan_images:
                            s = entry.get('Start'); e = entry.get('End')
                            if s:
                                regions.append({'start': s, 'end': e or '', 'name': s.replace('{', '').replace('}', '')})

                    def _sanitize(n: str) -> str:
                        return (n.replace('\\', '_').replace('/', '_').replace(':', '_')
                                  .replace('*', '_').replace('?', '_').replace('"', '_')
                                  .replace('<', '_').replace('>', '_').replace('|', '_'))

                    # 若开启 early_export_regions：不在此处导出，只构建映射到现有文件
                    exported = []
                    if getattr(self.config, 'early_export_regions', False):
                        from pathlib import Path as _Path
                        for reg in regions:
                            name = reg.get('name') or ''  # e.g. Table_1_Start / Image_2_Start
                            # 规范化：将 *_Start 去掉，映射到已导出的 Table_X / Image_X
                            base = name[:-6] if name.endswith('_Start') else name  # remove suffix '_Start'
                            safe_base = _sanitize(base)
                            p1 = _Path(export_dir) / f"{safe_base}.docx"
                            p2 = _Path(export_dir) / f"{safe_base}.DOCX"
                            p3 = _Path(export_dir) / f"{safe_base}.doc"
                            chosen = None
                            if p1.exists():
                                chosen = str(p1)
                            elif p2.exists():
                                chosen = str(p2)
                            elif p3.exists():
                                chosen = str(p3)
                            # 兼容极端：若导出名包含下划线清洗差异，再尝试直接用原 name
                            if chosen is None:
                                safe_full = _sanitize(name)
                                p_full = _Path(export_dir) / f"{safe_full}.docx"
                                if p_full.exists():
                                    chosen = str(p_full)
                            if chosen is not None:
                                # name 仍保留为原始开始标签名（无花括号），以便后续 key=Start 匹配
                                exported.append({'name': name, 'path': chosen})
                    else:
                        # 正常路径：此处执行导出（带内置缓存）
                        exported = _wds.export_regions_to_word(
                            marked_file=marked_path,
                            regions=regions,
                            export_dir=export_dir
                        )

                    # 记录制品
                    for it in exported:
                        try:
                            artifacts['outputs'].append(it.get('path'))
                        except Exception:
                            pass

                    # 基于导出结果，构建新的 plan 结构（按标签 -> 导出文件路径）
                    try:
                        import os as _os
                        name_to_path = { (it or {}).get('name'): (it or {}).get('path') for it in (exported or []) }
                        def _rel(p: str) -> str:
                            try:
                                return _os.path.relpath(p, _os.getcwd())
                            except Exception:
                                return p
                        plan_new = { 'table': [], 'Image': [] }
                        for entry in (merged_plan.get('table') or []):
                            sid = entry.get('Start') or entry.get('id')
                            if not sid:
                                continue
                            key = sid.replace('{', '').replace('}', '')
                            pth = name_to_path.get(key)
                            if pth:
                                plan_new['table'].append({ 'id': sid, 'Path': _rel(pth) })
                        for entry in (merged_plan.get('Image') or []):
                            sid = entry.get('Start') or entry.get('id')
                            if not sid:
                                continue
                            key = sid.replace('{', '').replace('}', '')
                            pth = name_to_path.get(key)
                            if pth:
                                plan_new['Image'].append({ 'id': sid, 'Path': _rel(pth) })
                        merged_plan = plan_new
                    except Exception as _e:
                        logger.warning(f"构建按标签导出的plan失败: {_e}")
                except Exception as _e:
                    logger.warning(f"导出表/图到独立Word失败: {_e}")
            except Exception as e:
                logger.warning(f"构建完整JSON失败: {e}")

            # 基于 merged_plan（已转为 {id, Path} 且本地路径）与 tfl_items，规范化生成对外 complete_json 并落盘
            try:
                def _plan_arc_from_src(src: Path) -> str:
                    parent = src.parent.name
                    if parent.endswith('_marked'):
                        return f"datas/data/index/{parent}/{src.name}"
                    return f"datas/data/index/{src.name}"

                # 标准化 TFL 列表
                tfl_norm: List[Dict[str, str]] = []
                for it in (tfl_items or []):
                    # 仅使用段落提取阶段明确给出的映射（不再做其它猜测/回退）
                    mappings = (it or {}).get('tfl_insert_mappings') or []
                    for m in mappings:
                        try:
                            pth = (m or {}).get('Path')
                            # 优先使用占位符作为 id（期望格式如 {{TFL_t14_1_1_2_1}}），无则回退到映射中的 id，再无则根据路径推导
                            tid = (m or {}).get('Placeholder') or (m or {}).get('id') or ''
                            if not pth:
                                continue
                            # 若仍无 tid，则基于文件名推导占位符
                            if not tid:
                                try:
                                    from pathlib import Path as _P
                                    tid = f"{{{{TFL_{_P(pth).stem}}}}}"
                                except Exception:
                                    tid = ''
                            if not tid:
                                continue
                            src_p = Path(pth) if Path(pth).is_absolute() else (Path.cwd() / str(pth))
                            if not src_p.exists():
                                name = Path(pth).name
                                cands = list(Path(self.session_dir).glob(f"**/{name}")) or list(Path('data').glob(f"**/{name}"))
                                if cands:
                                    cands.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                                    src_p = cands[0]
                            if src_p.exists():
                                arc = f"datas/data/rtf&index/{src_p.name}"
                                tfl_norm.append({ 'id': tid, 'Path': arc })
                        except Exception:
                            continue

                # 标准化 Plan（把本地导出路径映射为 datas/data/index/**），找不到本地文件也写入基于目录名的回退路径
                plan_norm: Dict[str, List[Dict[str, str]]] = { 'table': [], 'Image': [] }
                for key in ('table', 'Image'):
                    for entry in (merged_plan.get(key) or []):
                        pth = (entry or {}).get('Path')
                        sid = (entry or {}).get('id')
                        if not pth:
                            continue
                        try:
                            src_p = Path(pth) if Path(pth).is_absolute() else (Path.cwd() / str(pth))
                            if not src_p.exists():
                                name = Path(pth).name
                                cands = list(Path(self.session_dir).glob(f"**/{name}")) or list(Path('output').glob(f"**/{name}"))
                                if cands:
                                    cands.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                                    src_p = cands[0]
                            if src_p.exists():
                                arc = _plan_arc_from_src(src_p)
                            else:
                                # 回退：基于路径的父目录名与文件名构造 datas/data/index/**
                                try:
                                    parent = Path(pth).parent.name
                                except Exception:
                                    parent = 'index'
                                arc = f"datas/data/index/{parent}/{Path(pth).name}"
                            plan_norm['table' if key=='table' else 'Image'].append({ 'id': sid or '', 'Path': arc })
                        except Exception:
                            # 兜底：直接基于字符串拼装
                            try:
                                parent = Path(str(pth)).parent.name
                            except Exception:
                                parent = 'index'
                            arc = f"datas/data/index/{parent}/{Path(str(pth)).name}"
                            plan_norm['table' if key=='table' else 'Image'].append({ 'id': sid or '', 'Path': arc })

                # 输出模板路径：以“插入后模板”的文件名放置到 datas/output/<name>.docx
                try:
                    tpl_name = Path(str(word_result.output_file)).name if (word_result and getattr(word_result, 'output_file', None)) else 'report_template.docx'
                except Exception:
                    tpl_name = 'report_template.docx'
                output_arc = f"datas/output/{tpl_name}"

                complete_payload = {
                    'tfl': tfl_norm,
                    'plan': plan_norm,
                    'output': output_arc
                }
                complete_path = outputs_dir / f'complete_{run_timestamp}.json'
                save_json(complete_path, complete_payload)
                artifacts['outputs'].append(str(complete_path))
                try:
                    run_summary['complete_json_file'] = str(complete_path)
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"标准化 complete.json 失败: {e}")
            
            # ====== Word集成已移至独立接口 ======
            # 生成阶段只负责内容生成，不执行插入
            # 插入操作由 /api/v1/template/insert 或完整流程接口调用
            logger.info("✅ 内容生成完成，跳过插入（由独立接口处理）")
            # 在完成外部阶段后，刷新 run_output 文件（带上快捷指针）
            try:
                save_json(run_output_path, run_summary)
            except Exception:
                pass

            # 生成 outputs/index.md 汇总入口，便于快速浏览溯源
            try:
                index_lines: List[str] = []
                index_lines.append(f"运行时间: {run_timestamp}")
                index_lines.append("")
                index_lines.append("关键文件：")
                def _add(label: str, pth: Optional[str]):
                    try:
                        if pth and Path(str(pth)).exists():
                            index_lines.append(f"- {label}: {pth}")
                    except Exception:
                        pass
                _add('run_output', str(run_output_path))
                _add('session_report', str(session_report_path) if 'session_report_path' in locals() else None)
                _add('provenance', run_summary.get('provenance_file'))
                _add('readable_content', str(readable_path))
                _add('extracted_data_json', str(extracted_json_path) if 'extracted_json_path' in locals() else None)
                _add('extracted_data_text', str(extracted_txt_path) if 'extracted_txt_path' in locals() else None)
                _add('complete_json', run_summary.get('complete_json_file'))
                _add('insert_payload', run_summary.get('insert_payload_file'))
                _add('bundle_zip', run_summary.get('bundle_file'))
                try:
                    if run_summary.get('bundle_file'):
                        sha = Path(str(run_summary['bundle_file']) + '.sha256')
                        _add('bundle_zip.sha256', str(sha) if sha.exists() else None)
                except Exception:
                    pass
                _add('external_result_docx', run_summary.get('external_result_file'))
                try:
                    if run_summary.get('external_result_file'):
                        sha = Path(str(run_summary['external_result_file']) + '.sha256')
                        _add('external_result_docx.sha256', str(sha) if sha.exists() else None)
                except Exception:
                    pass
                _add('external_insert_meta', run_summary.get('external_insert_meta_file'))
                # 目录指引
                index_lines.append("")
                index_lines.append("目录指引：")
                _add('prompts_dir', str(run_dir / 'prompts') if (run_dir / 'prompts').exists() else None)
                _add('tfl_processing_dir', str(outputs_dir / 'tfl_processing') if (outputs_dir / 'tfl_processing').exists() else None)
                _add('inputs_environment', str(Path(self.session_dir) / 'inputs' / 'environment.json'))
                _add('inputs_configs', str(Path(self.session_dir) / 'inputs' / 'configs'))
                save_text(outputs_dir / 'index.md', "\n".join(index_lines).strip())
            except Exception:
                pass

            return FlowResult(
                success=True,
                message=f"批量生成完成: {success_count}/{total_count} 成功",
                data={
                    'run_dir': str(run_dir),
                    'run_output_file': str(run_output_path),
                    'readable_output_file': str(readable_path),
                    'extracted_data_file': str(extracted_json_path) if 'extracted_json_path' in locals() else None,
                    'extracted_data_text_file': str(extracted_txt_path) if 'extracted_txt_path' in locals() else None,
                    'run_log_file': str(run_log_path) if run_log_path else None,
                    'session_report_file': str(session_report_path) if 'session_report_path' in locals() else None,
                    'artifacts_file': str(outputs_dir / f'artifacts_{run_timestamp}.json'),
                    'success_count': success_count,
                    'total_count': total_count,
                    'word_integration': word_result.__dict__ if word_result else None,
                    'selection_needed': True if tfl_selections_required else False,
                    'tfl_selections_required': tfl_selections_required
                }
            )
        except Exception as e:
            logger.error(f"批量生成失败: {e}")
            return FlowResult(
                success=False,
                message="批量生成失败",
                error=str(e)
            )
    
    # def generate_paragraphs_by_ids(self, paragraph_ids: List[str]) -> FlowResult:
    #     """根据ID列表生成指定段落"""
    #     try:
    #         logger.info(f"开始生成指定段落: {paragraph_ids}")
    #         results = self.pipeline.generate_paragraphs_by_ids(paragraph_ids)
    #
    #         success_count = sum(1 for r in results if r['status'] == 'success')
    #         total_count = len(results)
    #
    #         return FlowResult(
    #             success=True,
    #             message=f"指定段落生成完成: {success_count}/{total_count} 成功",
    #             data={
    #                 'results': results,
    #                 'success_count': success_count,
    #                 'total_count': total_count
    #             }
    #         )
    #     except Exception as e:
    #         logger.error(f"生成指定段落失败: {e}")
    #         return FlowResult(
    #             success=False,
    #             message="生成指定段落失败",
    #             error=str(e)
    #         )
    #
    # def save_results(self, results: List[Dict[str, Any]],
    #                 output_file: Optional[str] = None) -> FlowResult:
    #     """保存生成结果"""
    #     try:
    #         if output_file is None:
    #             output_file = f"{self.config.output_dir}/generated_paragraphs.txt"
    #
    #         self.pipeline.save_results(results, output_file)
    #
    #         return FlowResult(
    #             success=True,
    #             message=f"结果已保存到: {output_file}",
    #             data={'output_file': output_file}
    #         )
    #     except Exception as e:
    #         logger.error(f"保存结果失败: {e}")
    #         return FlowResult(
    #             success=False,
    #             message="保存结果失败",
    #             error=str(e)
    #         )
    #
    # def get_system_status(self) -> FlowResult:
    #     """获取系统状态"""
    #     try:
    #         status = {
    #             'config_loaded': self.config_parser is not None,
    #             'services_ready': all([
    #                 self.llm_service is not None,
    #                 self.vision_service is not None,
    #                 self.data_extractor is not None,
    #                 self.paragraph_generation_service is not None
    #             ]),
    #             'pipeline_ready': self.pipeline is not None,
    #             'paragraph_count': len(self.get_paragraph_list()),
    #             'cache_dir_exists': Path(self.config.cache_dir).exists(),
    #             'output_dir_exists': Path(self.config.output_dir).exists(),
    #             'word_integration_enabled': self.config.enable_word_integration,
    #             'word_post_processor_ready': self.word_post_processor is not None
    #         }
    #
    #         return FlowResult(
    #             success=True,
    #             message="系统状态正常",
    #             data=status
    #         )
    #     except Exception as e:
    #         logger.error(f"获取系统状态失败: {e}")
    #         return FlowResult(
    #             success=False,
    #             message="获取系统状态失败",
    #             error=str(e)
    #         )
    #
    def cleanup(self):
        """清理资源，包括移除会话级别的日志 Handler"""
        # 清理 detailed_logger
        if hasattr(self, 'detailed_logger') and self.detailed_logger:
            self.detailed_logger.cleanup()
        
        # 移除并关闭会话级别的文件日志 Handler
        if hasattr(self, '_session_file_handlers') and self._session_file_handlers:
            # 在移除 handler 之前，先记录清理完成的日志
            logger.info("=" * 60)
            logger.info("会话清理完成，日志记录结束")
            logger.info(f"会话目录: {getattr(self, 'session_dir', 'unknown')}")
            logger.info("=" * 60)
            
            root_logger = logging.getLogger()
            for handler in self._session_file_handlers:
                try:
                    # 先 flush 确保所有日志写入
                    handler.flush()
                    # 从 root logger 移除
                    root_logger.removeHandler(handler)
                    # 关闭文件
                    handler.close()
                except Exception as e:
                    # 这条日志可能不会写入文件（handler已被移除）
                    pass
            self._session_file_handlers.clear()
        
        # 清除线程本地的输出目录（避免影响同一线程后续任务）
        from utils.context_manager import clear_thread_output_dir
        clear_thread_output_dir()
    
    # def create_word_template(self) -> FlowResult:
    #     """创建Word模板文件（仅用于测试）"""
    #     try:
    #         if not self.word_post_processor:
    #             return FlowResult(
    #                 success=False,
    #                 message="Word后处理器未初始化",
    #                 error="请先启用Word集成功能"
    #             )
    #
    #         # 使用Word集成服务创建模板
    #         from .word_integration import CSRWordIntegrator
    #         temp_integrator = CSRWordIntegrator(
    #             template_file=self.config.word_template_file,
    #             auto_create_template=True
    #         )
    #
    #         paragraph_config = self.get_paragraph_list()
    #         success = temp_integrator.create_template(paragraph_config)
    #
    #         if success:
    #             return FlowResult(
    #                 success=True,
    #                 message=f"Word模板文件已创建: {self.config.word_template_file}",
    #                 data={'template_file': self.config.word_template_file}
    #             )
    #         else:
    #             return FlowResult(
    #                 success=False,
    #                 message="Word模板文件创建失败",
    #                 error="请检查文件路径和权限"
    #             )
    #
    #     except Exception as e:
    #         logger.error(f"创建Word模板失败: {e}")
    #         return FlowResult(
    #             success=False,
    #             message="创建Word模板失败",
    #             error=str(e)
    #         )
    #
    # def get_word_template_status(self) -> FlowResult:
    #     """获取Word模板状态"""
    #     try:
    #         if not self.word_post_processor:
    #             return FlowResult(
    #                 success=False,
    #                 message="Word后处理器未初始化",
    #                 error="请先启用Word集成功能"
    #             )
    #
    #         paragraph_config = self.get_paragraph_list()
    #         status = self.word_post_processor.validate_template(paragraph_config)
    #
    #         return FlowResult(
    #             success=True,
    #             message="Word模板状态获取成功",
    #             data=status
    #         )
    #
    #     except Exception as e:
    #         logger.error(f"获取Word模板状态失败: {e}")
    #         return FlowResult(
    #             success=False,
    #             message="获取Word模板状态失败",
    #             error=str(e)
    #         )
    #
    # # def build_new_complete_json(
    # #     self,
    # #     results: List[Dict[str, Any]],
    # #     word_result: Any,
    # #     config_data: Dict[str, Any],
    # #     outputs_dir: Path,
    # #     run_timestamp: str
    # # ) -> Optional[Path]:
    # #     """
    # #     构建新版 complete.json（用于本地插入服务）
    # #
    # #     包含：
    # #     - metadata: 模板路径、输出路径等元数据
    # #     - generation_results: 段落生成结果（包含 extracted_data）
    # #     - resource_mappings: 占位符到文件路径的映射
    # #     """
    # #     try:
    # #         from pathlib import Path
    # #         import glob
    # #
    # #         # 1. 构建 metadata
    # #         metadata = {
    # #             "template_file": str(word_result.output_file) if word_result else "",
    # #             "output_file": str(outputs_dir / f"最终报告_{run_timestamp}.docx"),
    # #             "config_file": str(self.config.config_path),
    # #             "timestamp": run_timestamp,
    # #             "session_dir": str(Path(self.session_dir))
    # #         }
    # #
    # #         # 2. 构建 generation_results（保留完整的 extracted_data）
    # #         generation_results = []
    # #         for result in results:
    # #             generation_results.append({
    # #                 "paragraph_id": result.get('paragraph_id', ''),
    # #                 "generated_content": result.get('generated_content', ''),
    # #                 "status": result.get('status', 'error'),
    # #                 "extracted_data": result.get('extracted_data', {})
    # #             })
    # #
    # #         # 3. 构建 resource_mappings
    # #         resource_mappings = self._build_resource_mappings_from_results(results, config_data)
    # #
    # #         # 4. 组装完整 JSON
    # #         complete_json = {
    # #             "metadata": metadata,
    # #             "generation_results": generation_results,
    # #             "resource_mappings": resource_mappings
    # #         }
    # #
    # #         # 5. 保存
    # #         complete_path = outputs_dir / f"complete_new_{run_timestamp}.json"
    # #         save_json(complete_path, complete_json)
    # #
    # #         logger.info(f"✅ 已保存新版 complete.json: {complete_path}")
    # #         logger.info(f"   - generation_results: {len(generation_results)} 个段落")
    # #         logger.info(f"   - resource_mappings: {len(resource_mappings)} 个占位符")
    # #
    # #         return complete_path
    # #
    # #     except Exception as e:
    # #         logger.error(f"构建新版 complete.json 失败: {e}")
    # #         return None
    
    def _build_resource_mappings_for_results(self, results: List[Dict[str, Any]]) -> None:
        """
        为每个生成结果解析占位符并建立资源映射（段落级别）
        
        Args:
            results: 生成结果列表，会被原地修改添加resource_mappings字段
        """
        import re
        
        for result in results:
            generated_content = result.get("generated_content", "")
            extracted_data = result.get("extracted_data", {})
            available_resources = extracted_data.get("available_resources", [])
            
            # 1. 从生成内容中提取所有占位符
            placeholders = re.findall(r'\{\{[^}]+\}\}', generated_content)
            
            # 2. 段落级别建立映射
            resource_mappings = {}
            used_placeholders = []
            
            # 🆕 首先从 tfl_insert_mappings 获取TFL占位符映射
            extracted_items = extracted_data.get("extracted_items", [])
            for item in extracted_items:
                tfl_mappings = item.get("tfl_insert_mappings", [])
                for mapping in tfl_mappings:
                    placeholder = mapping.get("Placeholder", "")
                    if placeholder and placeholder in placeholders:
                        map_path = mapping.get("Path", "")
                        # 确保路径是绝对路径
                        if map_path and not Path(map_path).is_absolute():
                            abs_path = Path(map_path).absolute()
                            if abs_path.exists():
                                map_path = str(abs_path)
                        
                        resource_mappings[placeholder] = {
                            "placeholder": placeholder,
                            "type": "table",  # RTF/Excel默认为表格类型
                            "path": map_path,
                            "source_file": mapping.get("Source", "")
                        }
                        used_placeholders.append({
                            "placeholder": placeholder,
                            "type": "table",
                            "label": placeholder.replace("{{", "").replace("}}", "")
                        })
                        logger.info(f"📌 TFL资源映射: {placeholder} -> {mapping.get('Source', '')}")
            
            # 然后处理其他占位符（从available_resources查找）
            for placeholder in placeholders:
                if placeholder in resource_mappings:
                    continue  # 已经在TFL映射中处理过了
                    
                # 去除花括号得到label
                label = placeholder.replace("{{", "").replace("}}", "")
                
                # 从available_resources中查找匹配的资源
                for resource in available_resources:
                    res_label = resource.get("label", "")
                    
                    # ✅ 增强匹配逻辑：支持多种Label格式
                    # 1. 完全匹配
                    # 2. 占位符带_Start后缀，资源不带 (Table_1_Start vs Table_1)
                    # 3. 资源带_Start后缀，占位符不带 (Table_1 vs Table_1_Start)
                    is_match = (
                        res_label == label or
                        (label.endswith('_Start') and res_label == label[:-6]) or
                        (res_label.endswith('_Start') and label == res_label[:-6]) or
                        (label.endswith('_End') and res_label == label[:-4]) or
                        (res_label.endswith('_End') and label == res_label[:-4])
                    )
                    
                    if is_match:
                        # ✅ 确保路径是绝对路径或AAA相对路径
                        res_path = resource.get("path", "")
                        if res_path and not Path(res_path).is_absolute():
                            # 如果是相对路径，转换为绝对路径
                            abs_path = Path(res_path).absolute()
                            if abs_path.exists():
                                res_path = str(abs_path)
                        
                        resource_mappings[placeholder] = {
                            "placeholder": placeholder,
                            "type": resource["type"],
                            "path": res_path,
                            "source_file": resource.get("source_file", "")
                        }
                        used_placeholders.append({
                            "placeholder": placeholder,
                            "type": resource["type"],
                            "label": label
                        })
                        break
            
            # 3. 添加到result中
            result["resource_mappings"] = resource_mappings
            result["placeholders"] = used_placeholders
            
            # 日志
            if resource_mappings:
                logger.info(f"✓ 段落 {result.get('paragraph_id')} 资源映射: {len(resource_mappings)} 个占位符")
                for ph, mapping in resource_mappings.items():
                    logger.info(f"  - {ph} -> {Path(mapping['path']).name}")
    
    # def _build_resource_mappings_from_results(
    #     self,
    #     results: List[Dict[str, Any]],
    #     config_data: Dict[str, Any]
    # ) -> Dict[str, Dict[str, str]]:
    #     """
    #     从生成结果和配置构建资源映射
    #
    #     优先从 results 的 tfl_insert_mappings 提取，
    #     补充从配置的 region 文件夹扫描
    #     """
    #     from pathlib import Path
    #     import glob
    #
    #     resource_mappings = {}
    #
    #     # 方式1: 从 results 的 tfl_insert_mappings 提取
    #     for result in results:
    #         extracted_data = result.get('extracted_data', {})
    #         items = extracted_data.get('extracted_items', [])
    #
    #         for item in items:
    #             mappings = item.get('tfl_insert_mappings', [])
    #             for mapping in mappings:
    #                 placeholder = mapping.get('Placeholder', '')
    #                 if placeholder:
    #                     # ✅ 确保路径是绝对路径
    #                     map_path = mapping.get('Path', '')
    #                     if map_path and not Path(map_path).is_absolute():
    #                         abs_path = Path(map_path).absolute()
    #                         if abs_path.exists():
    #                             map_path = str(abs_path)
    #
    #                     resource_mappings[placeholder] = {
    #                         "placeholder": placeholder,
    #                         "path": map_path,
    #                         "type": "table",  # 默认为表格类型
    #                         "source_file": mapping.get('Source', '')
    #                     }
    #
    #     # 方式2: 从配置的 region 文件夹扫描（Word类型）
    #     try:
    #         paragraphs = config_data.get('paragraphs', [])
    #         for paragraph in paragraphs:
    #             for data_item in paragraph.get('data', []):
    #                 file_type = data_item.get('file_type', '')
    #                 options = data_item.get('options', '')
    #
    #                 # Word类型 + 原文模式：扫描 region 文件夹
    #                 if file_type in ['docx', 'pdf'] and options == '原文':
    #                     regions_list = data_item.get('region', [])
    #
    #                     for region_item in regions_list:
    #                         if not isinstance(region_item, dict):
    #                             continue
    #
    #                         regions_folder = region_item.get('regions', '')
    #                         file_name = region_item.get('name', '')
    #
    #                         if not regions_folder or not Path(regions_folder).exists():
    #                             continue
    #
    #                         # 扫描文件夹中的所有文件
    #                         try:
    #                             for file_path in glob.glob(f"{regions_folder}/*"):
    #                                 stem = Path(file_path).stem
    #                                 placeholder = f"{{{{{stem}}}}}"
    #
    #                                 # 判断类型
    #                                 ext = Path(file_path).suffix.lower()
    #                                 if ext in ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp']:
    #                                     file_type_val = 'image'
    #                                 else:
    #                                     file_type_val = 'table'
    #
    #                                 if placeholder not in resource_mappings:
    #                                     resource_mappings[placeholder] = {
    #                                         "placeholder": placeholder,
    #                                         "path": str(Path(file_path).absolute()),
    #                                         "type": file_type_val,
    #                                         "source_file": file_name
    #                                     }
    #                         except Exception as e:
    #                             logger.warning(f"扫描 region 文件夹失败: {regions_folder} - {e}")
    #     except Exception as e:
    #         logger.warning(f"从配置构建 resource_mappings 失败: {e}")
    #
    #     return resource_mappings
    #
    # def process_to_word(self, results: List[Dict[str, Any]]) -> FlowResult:
    #     """将生成结果处理到Word文档"""
    #     try:
    #         if not self.word_post_processor:
    #             return FlowResult(
    #                 success=False,
    #                 message="Word后处理器未初始化",
    #                 error="请先启用Word集成功能"
    #             )
    #
    #         paragraph_config = self.get_paragraph_list()
    #         word_result = self.word_post_processor.process_results(results, paragraph_config)
    #
    #         if word_result.success:
    #             return FlowResult(
    #                 success=True,
    #                 message=f"Word文档处理成功: {word_result.message}",
    #                 data={
    #                     'output_file': word_result.output_file,
    #                     'inserted_paragraphs': word_result.inserted_paragraphs,
    #                     'template_info': word_result.template_info
    #                 }
    #             )
    #         else:
    #             return FlowResult(
    #                 success=False,
    #                 message="Word文档处理失败",
    #                 error=word_result.error
    #             )
    #
    #     except Exception as e:
    #         logger.error(f"Word文档处理失败: {e}")
    #         return FlowResult(
    #             success=False,
    #             message="Word文档处理失败",
    #             error=str(e)
    #         )

def create_flow_controller(config: Optional[FlowConfig] = None) -> CSRFlowController:
    """创建流程控制器实例"""
    if config is None:
        config = FlowConfig()
    return CSRFlowController(config)
