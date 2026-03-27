"""
生成服务模块

功能说明：
- 统一封装文本生成的业务逻辑
- 提供同步和异步两种生成方式
- 处理结果格式化和溯源信息构建

主要类：
- GenerationService: 生成服务类，提供同步/异步生成能力
"""

# ========== 标准库导入 ==========
import json
import logging
import os
import threading
import time as time_module
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

# ========== 本地导入 ==========
from config import get_settings
from utils.context_manager import set_current_output_dir, get_current_output_dir
from utils.task_logger import TaskLogger, set_task_logger, clear_task_logger

# 导入耗时记录工具
try:
    from utils.timing import Timer, generation_timer, log_timing, print_global_summary
except ImportError:
    class Timer:
        def __init__(self, *args, **kwargs): pass
        def start(self): return self
        def stop(self): return 0
        def __enter__(self): return self
        def __exit__(self, *args): pass
        @property
        def duration(self): return 0
        @property
        def duration_str(self): return "0ms"
    generation_timer = None
    def log_timing(*args, **kwargs): pass
    def print_global_summary(): pass

# ========== 模块配置 ==========
logger = logging.getLogger(__name__)
settings = get_settings()


class GenerationService:
    """
    生成服务类
    
    封装了文本生成的核心业务逻辑，支持同步和异步两种调用方式。
    """
    
    def __init__(self):
        """初始化生成服务"""
        self.settings = settings
    
    # ============================================================
    # 公开方法 - 同步执行
    # ============================================================
    
    # def run_sync(
    #     self,
    #     cfg_obj: dict,
    #     base_data_dir: Optional[str] = None,
    #     output_dir: Optional[str] = None,
    #     combinationId: Optional[str] = None,
    #     project_desc: Optional[str] = None,
    #     skip_validation: bool = True,  # 默认跳过校验
    # ) -> dict:
    #     """
    #     同步执行生成任务（不走回调，直接返回结果）
    #
    #     用于 compose 接口，同步返回完整结果
    #
    #     Args:
    #         cfg_obj: 配置对象
    #         base_data_dir: 基础数据目录
    #         output_dir: 输出目录
    #         combinationId: 组合ID
    #         project_desc: 项目描述
    #         skip_validation: 是否跳过提取校验（默认False）
    #
    #     Returns:
    #         包含生成结果的字典
    #     """
    #     # 开始总计时
    #     total_timer = Timer("同步生成任务", parent="生成服务")
    #     total_timer.start()
    #
    #     try:
    #         # 设置环境变量
    #         with Timer("设置环境变量", parent="生成服务"):
    #             self._setup_environment(cfg_obj, combinationId, project_desc)
    #
    #         # 创建输出目录
    #         with Timer("创建输出目录", parent="生成服务"):
    #             run_dir = self._create_output_dir(output_dir)
    #
    #         # 保存配置
    #         with Timer("保存配置文件", parent="生成服务"):
    #             cfg_path = self._save_config(run_dir, cfg_obj)
    #
    #         # 执行生成流程
    #         from service.linux.generation.flow_controller import CSRFlowController, FlowConfig
    #
    #         with Timer("初始化流程控制器", parent="生成服务"):
    #             fc = FlowConfig(
    #                 config_path=str(cfg_path),
    #                 base_data_dir=base_data_dir or self.settings.base_data_dir,
    #                 cache_dir=self.settings.cache_dir,
    #                 output_dir=output_dir or self.settings.compose_output_dir,
    #                 word_template_file=None,
    #                 word_output_file=None,
    #                 enable_word_integration=False,
    #                 skip_validation=skip_validation,  # 传递跳过校验参数
    #             )
    #
    #             fc.early_export_regions = True
    #             fc.compose_insert_url = None
    #
    #             controller = CSRFlowController(fc)
    #             controller.word_post_processor = None
    #
    #         try:
    #             # 执行生成（同步）
    #             with Timer("执行段落生成", parent="生成服务") as gen_timer:
    #                 flow_res = controller.generate_all_paragraphs()
    #
    #             if generation_timer:
    #                 generation_timer.record("段落生成", gen_timer.duration, parent="生成服务")
    #
    #             if not flow_res.success:
    #                 raise Exception(flow_res.error or flow_res.message or "生成失败")
    #
    #             # 处理结果
    #             with Timer("构建返回结果", parent="生成服务"):
    #                 result = self._build_result(flow_res, run_dir)
    #
    #             total_timer.stop()
    #
    #             # 记录总耗时
    #             if generation_timer:
    #                 generation_timer.record("同步生成完成", total_timer.duration, parent="生成服务")
    #
    #             logger.info(f"✅ 同步生成任务完成 [总耗时: {total_timer.duration_str}]")
    #
    #             # 打印耗时摘要
    #             try:
    #                 if generation_timer:
    #                     generation_timer.print_summary()
    #             except Exception:
    #                 pass
    #
    #             return result
    #         finally:
    #             # 清理会话级别的日志 Handler，避免后续日志混入本次会话
    #             controller.cleanup()
    #
    #     except Exception as e:
    #         total_timer.stop()
    #         logger.error(f"❌ 同步生成失败 [耗时: {total_timer.duration_str}]: {e}", exc_info=True)
    #         raise
    
    # ============================================================
    # 公开方法 - 异步执行（后台线程）
    # ============================================================
    
    def start_async_task(
        self,
        task_id: str,
        cfg_obj: dict,
        base_data_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        combinationId: Optional[str] = None,
        project_desc: Optional[str] = None,
        callback_url: Optional[str] = None,
        result_callback_url: Optional[str] = None,
        project_id: Optional[str] = None,
        auth_token: Optional[str] = None,
        skip_validation: bool = True  # 默认跳过校验
    ) -> threading.Thread:
        """
        启动后台线程执行异步生成任务
        
        Args:
            task_id: 任务ID
            cfg_obj: 配置对象
            base_data_dir: 基础数据目录
            output_dir: 输出目录
            combinationId: 组合ID
            project_desc: 项目描述
            callback_url: 进度状态回调URL
            result_callback_url: 结果回调URL
            project_id: 项目ID
            auth_token: 认证Token
            skip_validation: 是否跳过提取校验（默认True）
        
        Returns:
            启动的线程对象
        """
        thread = threading.Thread(
            target=self._run_async,
            args=(
                task_id, cfg_obj, base_data_dir, output_dir, 
                combinationId, project_desc, callback_url, 
                result_callback_url, project_id, auth_token,
                skip_validation
            ),
            daemon=True
        )
        thread.start()
        return thread
    
    def start_compose_async_task(
        self,
        task_id: str,
        cfg_obj: dict,
        base_data_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        combinationId: Optional[str] = None,
        project_desc: Optional[str] = None,
        template_file: Optional[str] = None,
        project_id: Optional[str] = None,
        auth_token: Optional[str] = None,
        callback_url: Optional[str] = None,
        skip_validation: bool = True  # 默认跳过校验
    ) -> threading.Thread:
        """
        启动后台线程执行异步完整流程任务（生成 + 插入）
        
        Args:
            task_id: 任务ID
            cfg_obj: 配置对象
            base_data_dir: 基础数据目录
            output_dir: 输出目录
            combinationId: 组合ID
            project_desc: 项目描述
            template_file: 模板文件路径
            project_id: 项目ID
            auth_token: 认证Token
            callback_url: 完成时回调URL
            skip_validation: 是否跳过提取校验（默认True）
        
        Returns:
            启动的线程对象
        """
        thread = threading.Thread(
            target=self._run_compose_async,
            args=(
                task_id, cfg_obj, base_data_dir, output_dir, 
                combinationId, project_desc, template_file,
                project_id, auth_token, callback_url,
                skip_validation
            ),
            daemon=True
        )
        thread.start()
        return thread
    
    # ============================================================
    # 私有方法 - 异步任务执行
    # ============================================================
    
    def _run_async(
        self,
        task_id: str,
        cfg_obj: dict,
        base_data_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        combinationId: Optional[str] = None,
        project_desc: Optional[str] = None,
        callback_url: Optional[str] = None,
        result_callback_url: Optional[str] = None,
        project_id: Optional[str] = None,
        auth_token: Optional[str] = None,
        skip_validation: bool = True  # 默认跳过校验
    ):
        """
        在后台线程中执行生成任务（带回调）
        """
        from service.linux.generation.progress_callback import create_progress_callback

        
        # 提取所有 paragraph_id 作为标签ID
        paragraph_ids = self._extract_paragraph_ids(cfg_obj)
        logger.info(f"提取到 {len(paragraph_ids)} 个段落ID: {paragraph_ids}")
        
        # 创建回调实例
        callback = create_progress_callback(
            task_id=task_id, 
            callback_url=callback_url,
            result_callback_url=result_callback_url,
            project_id=project_id,
            tag_ids=paragraph_ids,
            auth_token=auth_token
        )

        
        # 创建输出目录（提前创建以便初始化日志器）
        run_dir = None
        task_logger = None
        
        try:
            # 设置环境变量
            self._setup_environment(cfg_obj, combinationId, project_desc)
            
            # 创建输出目录
            run_dir = self._create_output_dir(output_dir)
            
            # 初始化任务日志器
            task_logger = TaskLogger(
                task_id=task_id,
                output_dir=run_dir / "logs",
                auto_flush_on_error=True
            )
            set_task_logger(task_logger)
            task_logger.info("异步任务开始", task_id=task_id, project_id=project_id)
            
            # 保存配置
            cfg_path = self._save_config(run_dir, cfg_obj)
            task_logger.info("配置文件已保存", config_path=str(cfg_path))
            
            # 执行生成流程
            from service.linux.generation.flow_controller import CSRFlowController, FlowConfig
            
            fc = FlowConfig(
                config_path=str(cfg_path),
                base_data_dir=base_data_dir or self.settings.base_data_dir,
                cache_dir=self.settings.cache_dir,
                output_dir=output_dir or self.settings.compose_output_dir,
                word_template_file=None,
                word_output_file=None,
                enable_word_integration=False,
                skip_validation=skip_validation,  # 传递跳过校验参数
            )
            
            fc.early_export_regions = True
            fc.compose_insert_url = None
            
            controller = CSRFlowController(fc)
            # controller.word_post_processor = None
            
            try:
                # 设置实时阶段回调
                controller.set_stage_callbacks(
                    on_extraction_completed=callback.notify_extraction_completed,
                    on_generation_started=callback.notify_generation_started
                )
                
                # 回调：开始提取
                callback.notify_extraction_started()
                task_logger.info("开始数据提取")
                
                # 执行生成
                flow_res = controller.generate_all_paragraphs()
                
                if not flow_res.success:
                    error_msg = flow_res.error or flow_res.message or "生成失败"
                    task_logger.error("生成流程失败", error_msg=error_msg)
                    callback.notify_error(error_msg)
                    return
                
                task_logger.info("生成流程完成")
                
                # 处理结果
                result = self._build_result(flow_res, run_dir)
                
                # 更新每个段落的标签状态
                self._update_paragraph_status(result, callback)

                # ✅ 明确推送“生成完毕”阶段（标签/进度）
                # notify_complete 会保证终态，但这里提前打点，避免调用方只看阶段回调时漏掉“生成完毕”。
                try:
                    callback.notify_generation_completed()
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    pass
                
                # 通知完成
                callback.notify_complete(result)
                task_logger.info("任务完成", paragraphs_count=len(paragraph_ids))
            finally:
                # 清理会话级别的日志 Handler，避免后续日志混入本次会话
                controller.cleanup()
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"异步任务执行失败: {e}", exc_info=True)
            if task_logger:
                task_logger.error("异步任务执行失败", exc=e, task_id=task_id)
            callback.notify_error(str(e))
        finally:
            # 确保日志写入文件
            if task_logger:
                task_logger.flush()
            clear_task_logger()
    
    def _run_compose_async(
        self,
        task_id: str,
        cfg_obj: dict,
        base_data_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        combinationId: Optional[str] = None,
        project_desc: Optional[str] = None,
        template_file: Optional[str] = None,
        project_id: Optional[str] = None,
        auth_token: Optional[str] = None,
        callback_url: Optional[str] = None,
        skip_validation: bool = True  # 默认跳过校验
    ):
        """
        异步执行完整流程（生成 + 插入模板）
        """
        from service.linux.generation.task_manager import get_task_manager
        
        task_manager = get_task_manager()
        run_dir = None
        task_logger = None
        flow_steps = ""  # 用于记录流程执行步骤
        try:

            # ===== 步骤1：执行生成流程 =====
            logger.info(f"[Compose异步] 开始步骤1: 文本生成, task_id={task_id}")
            
            # 设置环境变量
            self._setup_environment(cfg_obj, combinationId, project_desc)
            
            # 创建输出目录
            run_dir = self._create_output_dir(output_dir)
            
            # 初始化任务日志器
            task_logger = TaskLogger(
                task_id=task_id,
                output_dir=run_dir / "logs",
                auto_flush_on_error=True
            )
            set_task_logger(task_logger)
            task_logger.info("Compose异步任务开始", task_id=task_id, project_id=project_id, template_file=template_file)
            
            # 保存配置
            cfg_path = self._save_config(run_dir, cfg_obj)
            task_logger.info("配置文件已保存", config_path=str(cfg_path))
            
            # 执行生成流程
            from service.linux.generation.flow_controller import CSRFlowController, FlowConfig
            
            fc = FlowConfig(
                config_path=str(cfg_path),
                base_data_dir=base_data_dir or self.settings.base_data_dir,
                cache_dir=self.settings.cache_dir,
                output_dir=output_dir or self.settings.compose_output_dir,
                word_template_file=None,
                word_output_file=None,
                enable_word_integration=False,
                skip_validation=skip_validation,  # 传递跳过校验参数
            )
            
            fc.early_export_regions = True
            fc.compose_insert_url = None
            
            controller = CSRFlowController(fc)
            # controller.word_post_processor = None
            
            try:
                flow_steps = "执行生成流程"
                # 执行生成
                task_logger.info("开始执行生成流程")
                flow_res = controller.generate_all_paragraphs()
                
                if not flow_res.success:
                    error_msg = flow_res.error or flow_res.message or "生成失败"
                    logger.error(f"[Compose异步] 生成失败: {error_msg}")
                    task_logger.error("生成流程失败", error_msg=error_msg)
                    task_manager.fail_task(task_id, error_msg)
                    self._send_compose_callback(callback_url, auth_token, {
                        "success": False,
                        "status": "失败",
                        "task_id": task_id,
                        "project_id": project_id,
                        "err_msg": "生成流程失败",
                        "error": error_msg
                    })
                    return
                flow_steps = "处理生成结果"
                # 处理生成结果
                result = self._build_result(flow_res, run_dir)
                logger.info(f"[Compose异步] 步骤1完成: 文本生成成功")
                task_logger.info("步骤1完成: 文本生成成功")
                
                # ===== 步骤2：调用模板插入（如果提供了模板）=====
                if template_file:
                    flow_steps = "执行模板插入"
                    logger.info(f"[Compose异步] 开始步骤2: 模板插入 - {template_file}")
                    task_logger.info("开始步骤2: 模板插入", template_file=template_file)

                    insertion_config = result.get("insertion_config", {})
                    data_json_str = json.dumps(insertion_config, ensure_ascii=False)

                    from service.linux.bridge.windows_bridge_client import WindowsBridgeClient

                    client = WindowsBridgeClient(self.settings.windows_bridge_url)
                    insertion_result = client.insert_content(
                        template_file=template_file,
                        data_json=data_json_str
                    )

                    if insertion_result and insertion_result.get("success"):
                        logger.info(f"[Compose异步] 步骤2完成: 模板插入成功")
                        task_logger.info("步骤2完成: 模板插入成功")
                        result["output_file"] = insertion_result.get("output_file")
                    else:
                        flow_steps = "模板插入失败"
                        error_msg = insertion_result.get("error", "未知错误") if insertion_result else "Windows Bridge无响应"
                        logger.warning(f"[Compose异步] 步骤2失败: {error_msg}")
                        task_logger.warning("步骤2失败: 模板插入失败", error_msg=error_msg)
                        result["insertion_error"] = error_msg
                else:
                    flow_steps = "未提供模板文件"
                    logger.info(f"[Compose异步] 跳过步骤2: 未提供模板文件")
                    task_logger.info("跳过步骤2: 未提供模板文件")
                
                # ===== 步骤3：完成，发送回调 =====
                result["status"] = "完成"
                result["task_id"] = task_id
                result["project_id"] = project_id
                
                task_manager.complete_task(task_id, result)
                self._send_compose_callback(callback_url, auth_token, result)
                
                logger.info(f"[Compose异步] 完整流程完成: task_id={task_id}")
                task_logger.info("Compose异步任务完成", task_id=task_id)
            finally:
                # 清理会话级别的日志 Handler，避免后续日志混入本次会话
                controller.cleanup()
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"[Compose异步] 任务执行失败: {e}", exc_info=True)
            if task_logger:
                task_logger.error("Compose异步任务执行失败", exc=e, task_id=task_id)
            task_manager.fail_task(task_id, str(e))
            self._send_compose_callback(callback_url, auth_token, {
                "success": False,
                "status": "失败",
                "task_id": task_id,
                "project_id": project_id,
                "err_msg": "任务执行失败"+flow_steps,
                "error": str(e)
            })
        finally:
            # 确保日志写入文件
            if task_logger:
                task_logger.flush()
            clear_task_logger()
    
    # ============================================================
    # 私有方法 - 环境与配置
    # ============================================================
    
    def _setup_environment(
        self, 
        cfg_obj: dict, 
        combinationId: Optional[str], 
        project_desc: Optional[str]
    ):
        """设置环境变量"""
        os.environ["CURRENT_PROJECT_DESC"] = str(
            (project_desc or "") or (cfg_obj.get("project_desc", "") or "")
        )
        _cid = combinationId or cfg_obj.get("combinationId")
        os.environ["CURRENT_COMBINATION_ID"] = str(_cid or "")
    
    def _create_output_dir(self, output_dir: Optional[str]) -> Path:
        """创建输出目录（使用线程安全的方式设置当前输出目录）"""
        base_output = output_dir or self.settings.compose_output_dir
        _rid = uuid.uuid4().hex[:6]
        run_dir = Path(base_output) / f"compose_{time_module.strftime('%Y%m%d_%H%M%S')}_{_rid}"
        in_dir = run_dir / "inputs"
        out_dir = run_dir / "outputs"
        in_dir.mkdir(parents=True, exist_ok=True)
        out_dir.mkdir(parents=True, exist_ok=True)
        # 使用线程安全的方式设置当前输出目录
        set_current_output_dir(str(run_dir))
        logger.info(f"输出目录已创建: {run_dir}")
        return run_dir
    
    def _save_config(self, run_dir: Path, cfg_obj: dict) -> Path:
        """保存配置文件"""
        cfg_path = run_dir / "inputs" / "paragraphs.json"
        cfg_path.write_text(json.dumps(cfg_obj, ensure_ascii=False, indent=2), encoding='utf-8')
        return cfg_path
    
    # ============================================================
    # 私有方法 - 结果处理
    # ============================================================
    
    def _extract_paragraph_ids(self, cfg_obj: dict) -> List[str]:
        """从配置中提取所有段落ID"""
        paragraph_ids = []
        paragraphs_list = cfg_obj.get("paragraphs", [])
        for para in paragraphs_list:
            if isinstance(para, dict) and para.get("id"):
                paragraph_ids.append(para["id"])
        return paragraph_ids
    
    def _build_result(self, flow_res, run_dir: Path) -> dict:
        """构建返回结果"""
        data_obj = flow_res.data or {}
        results = []
        
        # 读取运行结果
        try:
            run_output_file = data_obj.get('run_output_file')
            if run_output_file and Path(run_output_file).exists():
                with open(run_output_file, 'r', encoding='utf-8') as f:
                    run_json = json.load(f)
                results = run_json.get('results', []) or []
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.warning(f"读取运行结果失败: {e}")
        
        # 构建返回数据
        all_resource_mappings = {}
        paragraphs = []
        traceability_list = []

        for r in results:
            pid = r.get("paragraph_id", "")
            gen_txt = r.get("generated_content", "")
            status_str = r.get("status", "success")
            paragraphs.append({
                "paragraph_id": pid,
                "generated_content": gen_txt,
                "status": status_str
            })

            # 构建溯源信息
            prov_items = self._build_provenance_items(r)
            
            traceability_list.append({
                "paragraph_id": pid,
                "generated_content": gen_txt,
                "status": status_str,
                "provenance": {"extracted_items": prov_items}
            })

            if "resource_mappings" in r:
                all_resource_mappings.update(r["resource_mappings"])

        return {
            "success": True,
            "status": "生成完毕",
            "run_dir": data_obj.get('run_dir') or str(run_dir),
            "generated_content": {
                "paragraphs": paragraphs,
                "resource_mappings": all_resource_mappings
            },
            "traceability": traceability_list,
            "insertion_config": {
                "generation_results": paragraphs,
                "resource_mappings": all_resource_mappings
            }
        }
    
    def _build_provenance_items(self, result: dict) -> List[dict]:
        """构建溯源信息项"""
        prov_items = []
        try:
            eitems = (result.get("extracted_data") or {}).get("extracted_items", [])
            for it in eitems:
                if not isinstance(it, dict) or it.get("status") != "success":
                    continue
                
                data_type = (it.get("data_type") or "").lower()
                extract_item = it.get("extract_item") or it.get("extract") or ""
                extract_text = it.get("content", "")

                # 构建 used_data
                used_data = self._build_used_data(it, data_type)
                
                # 构建 data_source
                data_source = self._build_data_source(it, data_type)

                prov_items.append({
                    "extract_item": extract_item,
                    "extract_text": extract_text,
                    "used_data": used_data,
                    "data_source": data_source
                })
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.warning(f"构建溯源失败: {e}")
        
        return prov_items
    
    def _build_used_data(self, item: dict, data_type: str) -> Dict[str, Any]:
        """构建 used_data 字段"""
        used_data = {}
        
        if data_type in ("word", "pdf"):
            used_data["kind"] = "chunk"
            cfile = item.get("chunks_file")
            
            # 处理 chunks_file
            if isinstance(cfile, list) and cfile:
                used_data["structured_chunks_file"] = self._normalize_path(str(cfile[0]))
            elif isinstance(cfile, str) and cfile:
                used_data["structured_chunks_file"] = self._normalize_path(cfile)
            
            # 处理 sections
            sections = item.get("chunks_used_sections") or []
            if sections:
                used_data["section_id"] = sections[0]
            
            # 处理标题
            try:
                cu = (item.get("extraction_result") or {}).get("chunks_used", [])
                if cu:
                    title0 = cu[0].get("heading")
                    if title0:
                        used_data["title"] = title0
            except Exception as e:
                import traceback
                traceback.print_exc()
                pass
                
        elif data_type in ("excel", "rtf"):
            used_data["kind"] = "sheet"
            sres = item.get("sheets_results") or (item.get("extraction_result") or {}).get("sheets_results") or []
            if sres:
                used_data["sheet_name"] = sres[0].get("sheet_name")
                used_data["sheet_index"] = 1
                md_file = sres[0].get("md_file")
                if md_file:
                    used_data["markdown_path"] = self._normalize_path(str(md_file))
        
        return used_data
    
    def _build_data_source(self, item: dict, data_type: str) -> Dict[str, str]:
        """构建 data_source 字段"""
        src = item.get("source_file") or ""
        if isinstance(src, list):
            src = src[0] if src else ""
        if isinstance(src, str) and "," in src:
            src = src.split(",")[0].strip()
        
        ds_type = "word" if data_type in ("word", "pdf") else ("excel" if data_type in ("excel", "rtf") else data_type)
        normalized_src = self._normalize_path(str(src)) if src else ""
        
        return {
            "name": (Path(normalized_src).name if normalized_src else ""),
            "path": normalized_src,
            "type": ds_type or ""
        }
    
    def _normalize_path(self, path: str) -> str:
        """规范化路径，移除 AAA/ 前缀"""
        normalized = path.replace("\\", "/")
        idx = normalized.lower().find("aaa/")
        if idx != -1:
            normalized = normalized[idx + 4:]
        return normalized
    
    # ============================================================
    # 私有方法 - 回调
    # ============================================================
    
    def _update_paragraph_status(self, result: dict, callback):
        """
        更新每个段落的标签状态
        
        状态判断优先级：
        1. 首先检查错误管理器中的状态（最权威）
        2. 然后检查结果中的 status 字段
        
        这样确保在并发环境下，错误状态不会被覆盖。
        """
        from utils.tag_error_manager import is_tag_failed
        
        paragraphs = result.get("generated_content", {}).get("paragraphs", [])
        for para in paragraphs:
            pid = para.get("paragraph_id")
            if not pid:
                continue
            
            # 优先检查错误管理器中的状态
            if is_tag_failed(pid):
                tag_status = "生成失败"
                logger.warning(f"✗ 段落生成失败（错误管理器标记）: {pid} -> {tag_status}")
            else:
                # 再检查结果中的状态
                status_str = para.get("status", "success")
                tag_status = "生成完毕" if status_str == "success" else "生成失败"
                logger.info(f"✓ 段落生成完成: {pid} -> {tag_status}")
            
            callback.update_single_tag_status(pid, tag_status)
    
    def _send_compose_callback(
        self, 
        callback_url: Optional[str], 
        auth_token: Optional[str], 
        data: dict
    ):
        """
        发送完整流程的完成回调
        
        回调接口: POST http://192.168.3.32:8088/ky/sys/projectCreateManage/getReportAIResult
        参数格式: form-data
        参数:
            - dataJson: 生成结果JSON字符串
            - project_id: 项目ID
        """
        if not callback_url:
            logger.info(f"[Compose回调] 未配置回调URL，跳过回调")
            return
        
        try:
            import httpx
            
            # 提取 project_id
            project_id = data.get("project_id", "")
            
            # 将完整结果转为JSON字符串
            data_json_str = json.dumps(data, ensure_ascii=False)
            
            # 构建 form-data 请求参数
            form_data = {
                "dataJson": data_json_str,
                "project_id": project_id
            }
            
            # 请求头（form-data 格式）
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            if auth_token:
                headers["X-Access-Token"] = auth_token
            
            logger.info(f"[Compose回调] 发送回调到: {callback_url}")
            logger.info(f"[Compose回调] project_id: {project_id}")
            logger.info(f"[Compose回调] 数据: {data_json_str[:200]}")
            logger.info(f"[Compose回调] 插入信息: {data.get('insertion_config','')}")  # 方便插入流程出错后测试
            logger.info(f"[Compose回调] 数据: status={data.get('status')}, success={data.get('success')}")
            
            with httpx.Client(timeout=30.0) as client:
                response = client.post(callback_url, data=form_data, headers=headers)
                
                if response.status_code == 200:
                    logger.info(f"[Compose回调] 回调成功: {response.text[:200]}")
                else:
                    logger.warning(f"[Compose回调] 回调返回非200: {response.status_code} - {response.text[:200]}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"[Compose回调] 回调发送失败: {e}")


# ============================================================
# 全局单例（线程安全）
# ============================================================

_generation_service: Optional[GenerationService] = None
_generation_service_lock = threading.Lock()


def get_generation_service() -> GenerationService:
    """获取生成服务单例（线程安全）"""
    global _generation_service
    if _generation_service is None:
        with _generation_service_lock:
            if _generation_service is None:
                _generation_service = GenerationService()
    return _generation_service
