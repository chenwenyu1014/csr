"""
模板处理服务

功能说明：
- 统一封装模板处理的业务逻辑
- 提供异步处理能力（使用后台线程）
- 处理回调通知

主要类：
- TemplateProcessingService: 模板处理服务类
"""

# ========== 标准库导入 ==========
import asyncio
import json
import logging
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

# ========== 本地导入 ==========
from config import get_settings
from utils.output_manager import save_json

from .template_marker import get_template_marker
from .table_converter import get_table_converter
from .template_comment_parser import parse_and_convert

# ========== 模块配置 ==========
logger = logging.getLogger(__name__)
settings = get_settings()


class TemplateProcessingService:
    """
    模板处理服务类

    封装了模板处理的核心业务逻辑，使用后台线程实现异步处理。
    """

    def __init__(self):
        """初始化模板处理服务"""
        self.settings = settings
        self.marker = get_template_marker()
        self.converter = get_table_converter()

    # ============================================================
    # 公开方法 - 启动后台线程
    # ============================================================

    def start_async_task(
            self,
            task_id: str,
            template_file: str,
            template_id: Optional[str] = None,
            output_dir: Optional[str] = None,
            callback_base_url: Optional[str] = None,
            file_id: Optional[str] = None,
            auth_token: Optional[str] = None
    ) -> threading.Thread:
        """
        启动后台线程执行模板处理任务

        Args:
            task_id: 任务ID
            template_file: 模板文件路径（相对于AAA/）
            template_id: 模板ID
            output_dir: 输出目录
            callback_base_url: 回调基础URL（用于拼接状态回调、结果回调）
            file_id: 文件ID
            auth_token: 认证Token

        Returns:
            启动的线程对象
        """
        thread = threading.Thread(
            target=self._run_async,
            args=(task_id, template_file, template_id, output_dir, callback_base_url, file_id, auth_token),
            daemon=True
        )
        thread.start()
        return thread

    # ============================================================
    # 私有方法 - 后台线程执行
    # ============================================================

    def _run_async(
            self,
            task_id: str,
            template_file: str,
            template_id: Optional[str] = None,
            output_dir: Optional[str] = None,
            callback_base_url: Optional[str] = None,
            file_id: Optional[str] = None,
            auth_token: Optional[str] = None
    ):
        """
        在后台线程中执行模板处理任务

        Args:
            task_id: 任务ID
            template_file: 模板文件路径（相对于AAA/）
            template_id: 模板ID
            output_dir: 输出目录
            callback_base_url: 回调基础URL
            file_id: 文件ID
            auth_token: 认证Token
        """
        # 验证参数
        if not template_file or not template_file.strip():
            error_msg = "template_file 参数不能为空"
            logger.error(error_msg)
            self._send_callbacks(callback_base_url, file_id, template_id, task_id, None, error_msg, auth_token)
            return
        logger.info(f"[模板处理] 开始异步处理: {task_id}, 文件: {template_file}")
        start_time = time.time()

        run_dir = None
        current_step = "未知步骤"  # 用于异常定位

        try:
            # ========== 步骤1: 准备路径 ==========
            current_step = "准备路径"
            logger.info(f"{current_step}")
            # 源文件路径（相对于AAA/）
            # ✅ 清理模板路径：确保是相对路径
            # 先去除首尾的空白和引号
            clean_template = template_file.strip().strip('"').strip("'")

            # 处理各种路径前缀
            if clean_template.startswith('//'):
                # 处理 //project_data/... 格式，转换为 project_data/...
                clean_template = clean_template[2:]
            else:
                # 只取 AAA 段之后的内容，与项目 AAA 根拼接
                # 兼容 AAA/...、/AAA/...、/测试/AAA/... 等各种前缀
                marker = "AAA/"
                idx = clean_template.find(marker)
                clean_template = clean_template[idx + len(marker):] if idx != -1 else clean_template.lstrip("/")
            base_path = Path("AAA")
            source_file = base_path / clean_template

            if not source_file.exists():
                error_msg = f"模板文件不存在: {source_file}"
                logger.error(error_msg)
                # 发送失败回调
                self._send_callbacks(callback_base_url, file_id, template_id, task_id, None, error_msg, auth_token)
                return  # 直接返回，不抛异常

            # 输出目录
            # 默认: AAA/Preprocessing/Template/{原文件名}/
            template_name = Path(template_file).stem
            if output_dir:
                output_base = Path(output_dir)
            else:
                output_base = Path("AAA") / "Preprocessing" / "Template" / template_name
            # 创建运行目录
            run_dir = output_base
            run_dir.mkdir(parents=True, exist_ok=True)
            # 创建img子目录
            img_dir = run_dir / "img"
            img_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"模板处理任务开始, template_file={template_file}")

            # ========== 步骤2: 格式规范化（确保 python-docx 可读）==========
            current_step = "格式规范化"
            if source_file.suffix.lower() == '.doc':
                logger.info(f"检测到 .doc 格式文件，正在转换为 .docx: {source_file}")
                source_file = self._convert_doc_to_docx(source_file)
                logger.info(f"转换完成，新文件: {source_file}")
            source_file = self._ensure_transitional_docx(source_file, run_dir)

            # ========== 步骤3: 标记表格 ==========
            current_step = "标记表格"
            logger.info(f"{current_step}")
            marked_file, table_titles = self.marker.mark_tables_with_titles(
                source_file,
                run_dir
            )
            logger.info(f"标记完成: {len(table_titles)} 个表格")

            if not table_titles:
                # 没有表格，仍需要解析批注（纯文本标签场景）
                comment_result = parse_and_convert(
                    str(marked_file), str(marked_file),
                )
                paragraph_ids = comment_result.get("paragraph_ids", [])
                file_group = self._generate_file_group(paragraph_ids, task_id)
                # 发送回调
                result = self._build_empty_result(
                    template_file, marked_file, file_id,
                    paragraph_ids=paragraph_ids,
                    file_group=file_group,
                )
                self._send_callbacks(callback_base_url, file_id, template_id, task_id, result, None, auth_token)
                logger.info("模板中没有表格，处理完成")
                return

            # ========== 步骤4: 导出表格区域 ==========
            current_step = "导出表格区域"
            logger.info(f"{current_step}")
            export_dir = run_dir / "table_exports"  # 导出目录（存放Word和PDF中间文件）
            table_docx_files = self._export_table_regions(marked_file, export_dir, table_titles)
            logger.info(f"导出完成: {len(table_docx_files)} 个文件")

            # ========== 步骤5: 转换表格 ==========
            current_step = "转换表格"
            logger.info(f"{current_step}")
            resources = self._convert_all_tables(table_docx_files, export_dir, run_dir)
            logger.info(f"转换完成: {len(resources)} 个表格")

            # ========== 步骤6: 清理中间文件 ==========
            current_step = "清理中间文件"
            logger.info(f"{current_step}")
            self._cleanup_intermediate_files(export_dir)

            # ========== 步骤7: 批注解析与内容控件创建 ==========
            current_step = "批注解析"
            logger.info(f"{current_step}")
            try:
                comment_result = parse_and_convert(
                    str(marked_file), str(marked_file),
                )
                paragraph_ids = comment_result.get("paragraph_ids", [])
                logger.info(f"批注解析完成: {len(paragraph_ids)} 个段落标签")
            except Exception as e:
                logger.warning(f"批注解析失败（模板处理不受影响）: {e}", exc_info=True)
                paragraph_ids = []

            # ========== 步骤8: 构建结果 ==========
            current_step = "构建结果"
            logger.info(f"{current_step}")
            file_group = self._generate_file_group(paragraph_ids, task_id)
            result = self._build_result(template_file, marked_file, resources, file_id,
                                        paragraph_ids=paragraph_ids,
                                        file_group=file_group)
            logger.info("结果构建完成")

            # 保存结果到 JSON 文件
            result_file = run_dir / "result.json"
            save_json(result_file, result)
            logger.info(f"结果已保存到: {result_file}")

            # ========== 步骤9: 回调通知 ==========
            current_step = "回调通知"
            self._send_callbacks(callback_base_url, file_id, template_id, task_id, result, None, auth_token)
            logger.info(f"[模板处理] 完成: {task_id},用时: {time.time() - start_time:.2f}秒")

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(
                f"[模板处理] 失败: task_id={task_id}, 步骤={current_step}, 错误={e}",
                exc_info=True
            )

            # 发送失败回调
            error_msg = f"{current_step}失败: {e}"
            self._send_callbacks(callback_base_url, file_id, template_id, task_id, None, error_msg, auth_token)

    # ============================================================
    # 私有方法 - 导出表格区域
    # ============================================================

    def _export_table_regions(
            self,
            marked_file: Path,
            export_dir: Path,
            table_titles: List[Dict]
    ) -> List[Dict]:
        """
        导出标记的表格区域为独立Word文件

        复用现有的 WordRegionExtractor

        Args:
            marked_file: 标记后的Word文件
            export_dir: 导出目录（存放Word和PDF中间文件）
            table_titles: 表格标题列表

        Returns:
            导出的表格文件信息列表 [{"index": N, "title": "...", "path": "..."}, ...]
        """
        try:
            from service.windows.preprocessing.preprocessing_function.word.word_pipeline import WordRegionExtractor

            extractor = WordRegionExtractor()

            # 创建导出目录
            export_dir.mkdir(parents=True, exist_ok=True)

            # 导出区域
            exported_regions = extractor.extract_regions(
                str(marked_file),
                str(export_dir)
            )

            # 过滤只保留TemplateTable区域，并匹配标题
            table_docx_files = []
            for region in exported_regions:
                name = region.get("name", "")
                path = Path(region.get("path", ""))

                # 只处理TemplateTable区域
                if name.startswith("TemplateTable_"):
                    # 提取表格编号
                    import re
                    match = re.match(r"TemplateTable_(\d+)_Start", name)
                    if match:
                        table_idx = int(match.group(1))
                        # 匹配标题
                        title_info = self._find_title_for_table(table_titles, table_idx)
                        table_docx_files.append({
                            "index": table_idx,
                            "title": title_info.get("title", "无标题表格"),
                            "path": path
                        })

            logger.info(f"导出 {len(table_docx_files)} 个表格区域")
            return table_docx_files

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"导出表格区域失败: {e}", exc_info=True)
            raise

    def _convert_doc_to_docx(self, doc_file: Path, max_retries: int = 3) -> Path:
        """
        将 .doc 文件转换为 .docx 格式

        优先通过 Windows Bridge 调用 Word COM 转换（无损保真，正确保留批注
        锚定 run 的中文字体与标题样式继承）；bridge 未配置或失败时回退 soffice。

        Args:
            doc_file: .doc 文件路径
            max_retries: 最大重试次数（仅 soffice 兜底使用）

        Returns:
            转换后的 .docx 文件路径
        """
        # 输出目录（与源文件同目录）
        output_dir = doc_file.parent

        # 目标 .docx 文件路径
        docx_file = output_dir / f"{doc_file.stem}.docx"

        # 如果已存在转换后的文件，直接返回
        if docx_file.exists():
            logger.info(f".docx 文件已存在，跳过转换: {docx_file}")
            return docx_file

        # 优先：Windows Bridge + Word COM（无损保真），失败回退 soffice
        try:
            from service.linux.bridge.windows_bridge_client import WindowsBridgeClient
            bridge_client = WindowsBridgeClient(self.settings.windows_bridge_url)
            if bridge_client.is_configured():
                # 转换为相对 AAA 的路径传给 bridge
                doc_path_rel = self._to_aaa_relative(doc_file)
                logger.info(f"尝试通过 Windows Bridge (Word COM) 转换 .doc -> .docx: {doc_path_rel}")
                bridge_result = bridge_client.convert_doc_to_docx(doc_path_rel)
                if bridge_result and bridge_result.get("success") and docx_file.exists():
                    logger.info(f"Windows Bridge 转换 .doc -> .docx 成功: {docx_file}")
                    return docx_file
                bridge_err = bridge_result.get("error") if bridge_result else "无响应"
                logger.warning(f"Windows Bridge 转换失败，回退 soffice: {bridge_err}")
        except Exception as e:
            logger.warning(f"Windows Bridge 转换异常，回退 soffice: {e}")

        # 兜底：soffice 转换
        # 为每个soffice调用创建独立的用户配置目录，避免多线程/多进程间锁冲突
        user_install = tempfile.mkdtemp(prefix="soffice_profile_")

        try:
            cmd = [
                "soffice",
                "--headless",
                "-env:UserInstallation=" + Path(user_install).as_uri(),  # Path.as_uri() 确保跨平台兼容
                "--convert-to", "docx",
                "--outdir", str(output_dir.resolve()),
                str(doc_file.resolve())
            ]

            for attempt in range(max_retries):
                try:
                    logger.debug(f"执行 soffice 转换 .doc -> .docx (尝试 {attempt + 1}/{max_retries}): {cmd}")

                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=120
                    )

                    if result.returncode != 0:
                        logger.warning(
                            f"soffice 返回非零 (尝试 {attempt + 1}/{max_retries}): "
                            f"returncode={result.returncode}, stderr={result.stderr}"
                        )

                    if docx_file.exists():
                        logger.info(f"soffice 转换 .doc -> .docx 成功: {docx_file}")
                        return docx_file

                    logger.warning(
                        f"soffice返回码={result.returncode}但docx未生成 (尝试 {attempt + 1}/{max_retries})\n"
                        f"  stdout: {result.stdout}\n"
                        f"  stderr: {result.stderr}\n"
                        f"  输出目录内容: {list(output_dir.iterdir()) if output_dir.exists() else '目录不存在'}"
                    )

                    if attempt < max_retries - 1:
                        logger.warning(f"等待5秒后重试...")
                        time.sleep(5)

                except subprocess.TimeoutExpired:
                    if attempt < max_retries - 1:
                        logger.warning(f"soffice 转换超时，等待10秒后重试 (第{attempt + 1}/{max_retries}次)...")
                        time.sleep(10)
                        continue
                    logger.error("soffice 转换 .doc -> .docx 超时（重试后仍失败）")
                    raise RuntimeError("soffice 转换 .doc -> .docx 超时")

                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"soffice 异常: {e}，等待3秒后重试 (第{attempt + 1}/{max_retries}次)...")
                        time.sleep(3)
                        continue
                    logger.error(f"soffice 转换 .doc -> .docx 失败: {e}")
                    raise

            raise RuntimeError(f".doc 转 .docx 失败（重试{max_retries}次后文件未生成）: {docx_file}")

        finally:
            shutil.rmtree(user_install, ignore_errors=True)

    def _ensure_transitional_docx(self, docx_path: Path, work_dir: Path) -> Path:
        """
        确保 docx 为 python-docx 可读的过渡格式(Transitional)。

        Args:
            docx_path: 源 docx 路径
            work_dir: 工作目录

        Returns:
            过渡格式 docx 路径；若本身已是过渡格式，原样返回。
        """
        # 判定是否为严格格式(Strict)
        if not self._is_strict_ooxml(docx_path):
            return docx_path

        logger.warning(f"检测到严格格式 OOXML，规范化中: {docx_path}")

        # 复制到工作目录，避免触碰原模板
        local_copy = work_dir / f"{docx_path.stem}_transitional{docx_path.suffix}"
        shutil.copy2(docx_path, local_copy)

        try:
            from service.linux.bridge.windows_bridge_client import WindowsBridgeClient
            bridge_client = WindowsBridgeClient(self.settings.windows_bridge_url)
            if not bridge_client.is_configured():
                raise RuntimeError("严格格式 OOXML 文件需 Word COM 规范化，但 Windows Bridge 未配置")

            result = bridge_client.convert_doc_to_docx(self._to_aaa_relative(local_copy))
            if not (result and result.get("success") and local_copy.exists()):
                err = result.get("error") if result else "无响应"
                raise RuntimeError(f"Windows Bridge 规范化失败: {err}")

            logger.info("严格格式 OOXML 规范化完成")
            return local_copy
        except Exception:
            # 失败时清理副本，避免残留半成品干扰后续流程
            try:
                local_copy.unlink()
            except Exception:
                pass
            raise

    @staticmethod
    def _is_strict_ooxml(docx_path: Path) -> bool:
        """嗅探 docx 是否为严格格式(Strict)OOXML（包级 _rels/.rels 含 purl.oclc.org）。"""
        try:
            import zipfile
            with zipfile.ZipFile(docx_path) as z:
                rels = z.read("_rels/.rels").decode("utf-8", "ignore")
                return "purl.oclc.org" in rels
        except Exception:
            return False

    @staticmethod
    def _to_aaa_relative(path: Path) -> str:
        """将路径转为相对 AAA 的 posix 路径，供 Windows Bridge 使用。"""
        posix = Path(path).as_posix()
        if posix.startswith("AAA/"):
            return posix[4:]
        if posix.startswith("/AAA/"):
            return posix[5:]
        if posix.startswith("/"):
            return posix[1:]
        return posix

    def _find_title_for_table(self, table_titles: List[Dict], table_idx: int) -> Dict:
        """根据表格编号查找标题信息"""
        for title_info in table_titles:
            if title_info.get("index") == table_idx:
                return title_info
        return {"index": table_idx, "title": "无标题表格"}

    # ============================================================
    # 私有方法 - 转换表格
    # ============================================================

    def _convert_all_tables(
            self,
            table_docx_files: List[Dict],
            export_dir: Path,
            run_dir: Path
    ) -> List[Dict]:
        """
        转换所有表格为HTML和图片

        Args:
            table_docx_files: 表格Word文件列表
            export_dir: 导出目录（存放PDF等中间文件，后续会被清理）
            run_dir: 运行目录（存放PNG图片等最终保留的文件）

        Returns:
            资源列表 [{"title": "...", "start_tag": "...", "html": "...", "pic": "..."}, ...]
        """
        resources = []

        for table_info in table_docx_files:
            table_idx = table_info.get("index", 0)
            table_title = table_info.get("title", "无标题表格")
            table_path = Path(table_info.get("path", ""))

            if not table_path.exists():
                logger.warning(f"表格文件不存在: {table_path}")
                continue

            try:
                # 转换表格
                convert_result = self.converter.convert_table(
                    table_path,
                    export_dir,
                    run_dir,
                    table_idx
                )

                # 构建资源项
                resource = {
                    "title": table_title,
                    "start_tag": f"TemplateTable_{table_idx}",
                    "html": convert_result.get("html", ""),
                    "pic": convert_result.get("pic", "")
                }
                resources.append(resource)

                logger.info(f"表格 {table_idx} 转换成功: title='{table_title}'")

            except Exception as e:
                import traceback
                traceback.print_exc()
                logger.error(f"表格 {table_idx} 转换失败: {e}")

                # 记录失败的资源
                resource = {
                    "title": table_title,
                    "start_tag": f"TemplateTable_{table_idx}",
                    "html": "",
                    "pic": "",
                    "error": str(e)
                }
                resources.append(resource)

        return resources

    # ============================================================
    # 私有方法 - 清理中间文件
    # ============================================================

    def _cleanup_intermediate_files(self, export_dir: Path):
        """
        清理中间文件

        删除整个 export_dir 目录（包含Word导出文件和PDF文件）

        保留：
        - 标记后的Word文件（_marked.docx）
        - PNG图片（img目录）
        - result.json

        注释掉此函数的调用可以保留中间文件用于调试
        """
        try:
            if export_dir.exists():
                shutil.rmtree(export_dir)
                logger.info(f"删除中间目录: {export_dir}")
        except Exception as e:
            logger.warning(f"清理中间文件失败: {e}")

    # ============================================================
    # 私有方法 - 构建结果
    # ============================================================

    def _inject_doc_source_ids(self, doc_source_list: List[Dict]) -> List[Dict]:
        """为 docSourceInfo 的每个元素注入 12 位纯数字 UUID，确保全局唯一"""
        for item in doc_source_list:
            if "id" not in item:
                item["id"] = str(uuid.uuid4().int)[:12]
        return doc_source_list

    def _transform_paragraph_ids(
            self,
            paragraph_ids: List[Dict],
            resources: List[Dict]
    ) -> List[Dict]:
        """
        将批注解析器输出的 paragraph_ids 转换为目标格式。

        转换规则：
        - paragraphId ← paragraph_id
        - generateLogic ← llm_info.generateLogic
        - docSourceInfo ← llm_info.docSourceInfo（注入 12 位数字 UUID）
        - tagType ← type
        - table 类型：从 resources 中按 start_tag 匹配 tableTitle/tableHtml
        - text 类型：tableTag/tableTitle/tableHtml 均为空字符串
        """
        resource_map = {}
        for r in resources:
            tag = r.get("start_tag", "")
            if tag:
                resource_map[tag] = {
                    "title": r.get("title", ""),
                    "html": r.get("html", ""),
                }

        default_doc_source = [{
            "id": str(uuid.uuid4().int)[:12],
            "number": "1",
            "first_match_logic": "",
            "second_match_logic": "",
            "is_match_original": False,
            "insert_original": False,
        }]

        result = []
        for item in (paragraph_ids or []):
            llm_info = item.get("llm_info") or {}
            tag_type = item.get("type", "text")

            doc_source = llm_info.get("docSourceInfo", default_doc_source)
            self._inject_doc_source_ids(doc_source)

            new_item = {
                "paragraphId": item.get("paragraph_id", ""),
                "generateLogic": llm_info.get("generateLogic", ""),
                "docSourceInfo": doc_source,
                "tagType": tag_type,
                "tableTag": "",
                "tableTitle": "",
                "tableHtml": "",
            }

            if tag_type == "table":
                table_tag = item.get("table_tag", "")
                new_item["tableTag"] = table_tag
                if table_tag and table_tag in resource_map:
                    new_item["tableTitle"] = resource_map[table_tag]["title"]
                    new_item["tableHtml"] = resource_map[table_tag]["html"]

            result.append(new_item)

        return result

    def _collect_first_match_logics(self, paragraph_ids: List[Dict]) -> List[Dict[str, str]]:
        """从 paragraph_ids 中收集所有唯一的 first_match_logic，格式与清单接口一致"""
        seen = set()
        result = []
        for item in (paragraph_ids or []):
            llm_info = item.get("llm_info") or {}
            for src in (llm_info.get("docSourceInfo") or []):
                logic = (src.get("first_match_logic") or "").strip()
                if logic and logic not in seen:
                    seen.add(logic)
                    result.append({"first_match_logic": logic})
        return result

    def _generate_file_group(self, paragraph_ids: List[Dict], task_name: str) -> List[Dict]:
        """根据 paragraph_ids 中的 first_match_logic 生成 file_group"""
        first_match = self._collect_first_match_logics(paragraph_ids)
        if not first_match:
            return []
        try:
            from service.linux.file_service.data_source_service import DataSourceService
            data_svc = DataSourceService()
            spec = {"first_match": first_match}
            fg_result = asyncio.run(data_svc.generate_manifest(spec, task_name=task_name))
            if fg_result.get("success"):
                return fg_result.get("data", {}).get("file_group", [])
        except Exception as e:
            logger.warning(f"file_group 生成失败（模板处理不受影响）: {e}", exc_info=True)
        return []

    def _build_result(
            self,
            template_file: str,
            marked_file: Path,
            resources: List[Dict],
            file_id: Optional[str] = None,
            paragraph_ids: Optional[List[Dict]] = None,
            file_group: Optional[List[Dict]] = None
    ) -> Dict:
        """
        构建返回结果

        Args:
            template_file: 原模板文件路径
            marked_file: 标记后的文件路径
            resources: 资源列表
            file_id: 文件ID
            paragraph_ids: 段落标签列表（批注解析结果）
            file_group: 文件分组清单

        Returns:
            结果字典
        """
        result = {
            "file": template_file,
            "processed_file": str(marked_file),
            "resources": resources,
            "paragraph_ids": self._transform_paragraph_ids(paragraph_ids or [], resources),
            "file_group": file_group or [],
        }
        if file_id:
            result["file_id"] = file_id
        return result

    def _build_empty_result(
            self,
            template_file: str,
            marked_file: Path,
            file_id: Optional[str] = None,
            paragraph_ids: Optional[List[Dict]] = None,
            file_group: Optional[List[Dict]] = None
    ) -> Dict:
        """
        构建空结果（模板中没有表格）

        Args:
            template_file: 原模板文件路径
            marked_file: 标记后的文件路径
            file_id: 文件ID
            paragraph_ids: 段落标签列表（批注解析结果）
            file_group: 文件分组清单

        Returns:
            结果字典
        """
        result = {
            "file_id": file_id,
            "file": template_file,
            "processed_file": str(marked_file),
            "resources": [],
            "paragraph_ids": self._transform_paragraph_ids(paragraph_ids or [], []),
            "file_group": file_group or [],
        }
        return result

    # ============================================================
    # 私有方法 - 回调通知
    # ============================================================

    def _send_callbacks(
            self,
            callback_base_url: Optional[str],
            file_id: Optional[str],
            template_id: Optional[str],
            task_id: str,
            result: Optional[Dict],
            error: Optional[str],
            auth_token: Optional[str] = None
    ):
        """
        发送回调通知（状态回调 + 结果回调）

        Args:
            callback_base_url: 回调基础URL
            file_id: 文件ID
            template_id: 模板ID
            task_id: 任务ID
            result: 处理结果
            error: 错误信息
            auth_token: 认证Token
        """
        success = error is None

        # 1. 先发送状态回调（通知调用方任务结束）
        status = "success" if success else "fail"
        self._send_status_callback(callback_base_url, file_id, template_id, status, auth_token)

        # 2. 再发送结果回调（传递详细数据或错误原因）
        self._send_result_callback(callback_base_url, file_id, template_id, task_id, result, status,error, auth_token)

        logger.info(f"[回调通知] task_id={task_id}, success={success}")

    def _send_status_callback(
            self,
            callback_base_url: Optional[str],
            file_id: Optional[str],
            template_id: Optional[str],
            status: str,
            auth_token: Optional[str] = None
    ):
        """
        发送状态回调

        接口: /ky/KM/kmFile/updateFileStatus
        参数: id, status

        Args:
            callback_base_url: 回调基础URL
            file_id: 文件ID
            status: 状态（"处理完毕" 或 "处理失败"）
            auth_token: 认证Token
        """
        if not callback_base_url:
            logger.debug("未配置回调基础URL，跳过状态回调")
            return

        if not file_id:
            logger.debug("未配置文件ID，跳过状态回调")
            return

        # 拼接状态回调URL
        status_url = f"{callback_base_url.rstrip('/')}/ky/KM/kmFile/updateFileStatus"

        try:
            import httpx

            data = {
                "file_id": file_id,
                "template_id": template_id,
                "status": status
            }

            # 请求头（form-data 格式，带认证Token）
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            if auth_token:
                headers["X-Access-Token"] = auth_token

            logger.info(f"[状态回调] 发送: file_id={file_id},template_id={template_id}, status={status}")
            logger.debug(f"[状态回调] URL: {status_url}")

            response = httpx.post(
                status_url,
                data=data,
                headers=headers,
                timeout=30.0
            )

            if response.status_code == 200:
                logger.info(f"[状态回调] 发送成功")
            else:
                logger.warning(f"[状态回调] 发送失败: status={response.status_code}")

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.warning(f"[状态回调] 发送失败: {e}")

    def _send_result_callback(
            self,
            callback_base_url: Optional[str],
            file_id: Optional[str],
            template_id: Optional[str],
            task_id: str,
            result: Optional[Dict],
            status: str,
            error: Optional[str],
            auth_token: Optional[str] = None
    ):
        """
        发送结果回调

        接口: /ky/KM/kmFile/callBackTemplateFile
        参数: dataJson

        Args:
            callback_base_url: 回调基础URL
            file_id: 文件ID
            template_id: 模板ID
            task_id: 任务ID
            result: 处理结果
            error: 错误信息
            auth_token: 认证Token
        """
        if not callback_base_url:
            logger.debug("未配置回调基础URL，跳过结果回调")
            return

        # 拼接结果回调URL
        result_url = f"{callback_base_url.rstrip('/')}/ky/KM/kmFile/callBackTemplateFile"

        # 构建回调数据
        callback_data = {
            "status": status,
            "file_id": file_id,
            "template_id": template_id,
            "task_id": task_id,
            "success": error is None,
        }

        if result:
            callback_data.update(result)

        if error:
            callback_data["error"] = error

        try:
            import httpx

            # 序列化为JSON字符串
            data_json = json.dumps(callback_data, ensure_ascii=False)

            # 构建 form-data 请求参数
            form_data = {"dataJson": data_json}

            # 请求头（form-data 格式，带认证Token）
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            if auth_token:
                headers["X-Access-Token"] = auth_token

            logger.info(f"[结果回调] 发送: task_id={task_id},file_id={file_id},template_id={template_id}, success={error is None}")
            logger.info(f"[结果回调] URL: {result_url}")
            logger.info(f"[结果回调] dataJson长度: {len(data_json)} 字符")

            response = httpx.post(
                result_url,
                data=form_data,
                headers=headers,
                timeout=60.0
            )

            if response.status_code == 200:
                logger.info(f"[结果回调] 发送成功")
            else:
                logger.warning(f"[结果回调] 发送失败: status={response.status_code}")

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.warning(f"[结果回调] 发送失败: {e}")


# ============================================================
# 全局单例（线程安全）
# ============================================================

_template_service: Optional[TemplateProcessingService] = None
_template_service_lock = threading.Lock()


def get_template_service() -> TemplateProcessingService:
    """获取模板处理服务单例（线程安全）"""
    global _template_service
    if _template_service is None:
        with _template_service_lock:
            if _template_service is None:
                _template_service = TemplateProcessingService()
    return _template_service
