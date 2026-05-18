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
import json
import logging
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# ========== 本地导入 ==========
from config import get_settings

from .template_marker import TemplateMarker, get_template_marker
from .table_converter import TableConverter, get_table_converter

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
            output_dir: 输出目录
            callback_base_url: 回调基础URL（用于拼接状态回调、结果回调）
            file_id: 文件ID
            auth_token: 认证Token

        Returns:
            启动的线程对象
        """
        thread = threading.Thread(
            target=self._run_async,
            args=(task_id, template_file, output_dir, callback_base_url, file_id, auth_token),
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
            output_dir: 输出目录
            callback_base_url: 回调基础URL
            file_id: 文件ID
            auth_token: 认证Token
        """
        # 验证参数
        if not template_file or not template_file.strip():
            error_msg = "template_file 参数不能为空"
            logger.error(error_msg)
            self._send_callbacks(callback_base_url, file_id, task_id, None, error_msg, auth_token)
            return
        logger.info(f"[模板处理] 开始异步处理: {task_id}, 文件: {template_file}")
        start_time = time.time()

        run_dir = None
        current_step = "未知步骤"  # 用于异常定位

        try:
            # ========== 1. 准备路径 ==========
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
            elif clean_template.startswith('/AAA/'):
                clean_template = clean_template[5:]
            elif clean_template.startswith('AAA/'):
                clean_template = clean_template[4:]
            elif clean_template.startswith('/'):
                clean_template = clean_template[1:]
            base_path = Path("AAA")
            source_file = base_path / clean_template

            if not source_file.exists():
                error_msg = f"模板文件不存在: {source_file}"
                logger.error(error_msg)
                # 发送失败回调
                self._send_callbacks(callback_base_url, file_id, task_id, None, error_msg, auth_token)
                return  # 直接返回，不抛异常

            # ========== 1.5 .doc 转 .docx（如果需要）==========
            current_step = "格式转换"
            if source_file.suffix.lower() == '.doc':
                logger.info(f"{current_step}")
                logger.info(f"检测到 .doc 格式文件，正在转换为 .docx: {source_file}")
                source_file = self._convert_doc_to_docx(source_file)
                logger.info(f"转换完成，新文件: {source_file}")

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

            # ========== 2. 标记表格 ==========
            current_step = "标记表格"
            logger.info(f"{current_step}")
            marked_file, table_titles = self.marker.mark_tables_with_titles(
                source_file,
                run_dir
            )
            logger.info(f"标记完成: {len(table_titles)} 个表格")

            if not table_titles:
                # 没有表格，直接返回空结果
                result = self._build_empty_result(template_file, marked_file, file_id)
                # 发送回调
                self._send_callbacks(callback_base_url, file_id, task_id, result, None, auth_token)
                logger.info("模板中没有表格，处理完成")
                return

            # ========== 3. 导出表格区域 ==========
            current_step = "导出表格区域"
            logger.info(f"{current_step}")
            export_dir = run_dir / "table_exports"  # 导出目录（存放Word和PDF中间文件）
            table_docx_files = self._export_table_regions(marked_file, export_dir, table_titles)
            logger.info(f"导出完成: {len(table_docx_files)} 个文件")

            # ========== 4. 转换表格 ==========
            current_step = "转换表格"
            logger.info(f"{current_step}")
            resources = self._convert_all_tables(table_docx_files, export_dir, run_dir)
            logger.info(f"转换完成: {len(resources)} 个表格")

            # ========== 5. 清理中间文件 ==========
            current_step = "清理中间文件"
            logger.info(f"{current_step}")
            self._cleanup_intermediate_files(export_dir)

            # ========== 6. 构建结果 ==========
            current_step = "构建结果"
            logger.info(f"{current_step}")
            result = self._build_result(template_file, marked_file, resources, file_id)
            logger.info("结果构建完成")

            # 保存结果到 JSON 文件
            result_file = run_dir / "result.json"
            result_file.write_text(
                json.dumps(result, ensure_ascii=False, indent=2),
                encoding='utf-8'
            )
            logger.info(f"结果已保存到: {result_file}")

            # ========== 7. 回调通知 ==========
            current_step = "回调通知"
            self._send_callbacks(callback_base_url, file_id, task_id, result, None, auth_token)
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
            self._send_callbacks(callback_base_url, file_id, task_id, None, error_msg, auth_token)

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
        使用 soffice 将 .doc 文件转换为 .docx 格式

        Args:
            doc_file: .doc 文件路径
            max_retries: 最大重试次数

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

        # 使用 soffice 转换
        # soffice --headless --convert-to docx --outdir output_dir input.doc
        cmd = [
            "soffice",
            "--headless",
            "--convert-to", "docx",
            "--outdir", str(output_dir),
            str(doc_file)
        ]

        for attempt in range(max_retries):
            try:
                logger.debug(f"执行 soffice 转换 .doc -> .docx (尝试 {attempt + 1}/{max_retries}): {cmd}")

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=120  # 120秒超时（.doc 转 .docx 可能较慢）
                )

                if result.returncode != 0:
                    logger.warning(f"soffice 返回非零: returncode={result.returncode}, stderr={result.stderr}")

                # 检查 .docx 是否生成
                if docx_file.exists():
                    logger.info(f"soffice 转换 .doc -> .docx 成功: {docx_file}")
                    return docx_file

                # 文件未生成，可能需要重试
                if attempt < max_retries - 1:
                    logger.warning(f".docx 未生成，等待5秒后重试 (第{attempt + 1}/{max_retries}次)...")
                    time.sleep(5)
                    continue
                else:
                    raise RuntimeError(f".doc 转 .docx 失败（重试{max_retries}次后文件未生成）: {docx_file}")

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
                raise RuntimeError(f"soffice 转换 .doc -> .docx 失败: {e}")

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
                    "start_tag": f"Table_{table_idx}",
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

    def _build_result(
            self,
            template_file: str,
            marked_file: Path,
            resources: List[Dict],
            file_id: Optional[str] = None
    ) -> Dict:
        """
        构建返回结果

        Args:
            template_file: 原模板文件路径
            marked_file: 标记后的文件路径
            resources: 资源列表
            file_id: 文件ID

        Returns:
            结果字典
        """
        result = {
            "file": template_file,
            "processed_file": str(marked_file),
            "resources": resources
        }
        if file_id:
            result["file_id"] = file_id
        return result

    def _build_empty_result(
            self,
            template_file: str,
            marked_file: Path,
            file_id: Optional[str] = None
    ) -> Dict:
        """
        构建空结果（模板中没有表格）

        Args:
            template_file: 原模板文件路径
            marked_file: 标记后的文件路径
            file_id: 文件ID

        Returns:
            结果字典
        """
        result = {
            "file_id": file_id,
            "file": template_file,
            "processed_file": str(marked_file),
            "resources": []
        }
        return result

    # ============================================================
    # 私有方法 - 回调通知
    # ============================================================

    def _send_callbacks(
            self,
            callback_base_url: Optional[str],
            file_id: Optional[str],
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
            task_id: 任务ID
            result: 处理结果
            error: 错误信息
            auth_token: 认证Token
        """
        success = error is None

        # 1. 先发送状态回调（通知调用方任务结束）
        status = "处理完毕" if success else "处理失败"
        self._send_status_callback(callback_base_url, file_id, status, auth_token)

        # 2. 再发送结果回调（传递详细数据或错误原因）
        self._send_result_callback(callback_base_url, file_id, task_id, result, error, auth_token)

        logger.info(f"[回调通知] task_id={task_id}, success={success}")

    def _send_status_callback(
            self,
            callback_base_url: Optional[str],
            file_id: Optional[str],
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
                "id": file_id,
                "status": status
            }

            # 请求头（form-data 格式，带认证Token）
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            if auth_token:
                headers["X-Access-Token"] = auth_token

            logger.info(f"[状态回调] 发送: id={file_id}, status={status}")
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
            task_id: str,
            result: Optional[Dict],
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
            "id": file_id,
            "task_id": task_id,
            "success": error is None,
        }

        if result:
            callback_data["file"] = result.get("file", "")
            callback_data["processed_file"] = result.get("processed_file", "")
            callback_data["resources"] = result.get("resources", [])

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

            logger.info(f"[结果回调] 发送: task_id={task_id}, success={error is None}")
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
