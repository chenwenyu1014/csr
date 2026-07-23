"""
表格转换器

功能说明：
- 将单个表格Word文件转换为HTML和PNG图片
- 使用mammoth转HTML
- 使用soffice转PDF
- 使用PyMuPDF转PNG并裁剪白边

主要类：
- TableConverter: 表格转换器类
"""

# ========== 标准库导入 ==========
import logging
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Dict, Optional

# ========== 第三方库导入 ==========
import fitz  # PyMuPDF
import mammoth
from PIL import Image, ImageOps

# ========== 模块配置 ==========
logger = logging.getLogger(__name__)


class TableConverter:
    """
    表格转换器

    将单个表格Word文件转换为HTML和PNG图片。
    """

    def convert_table(
        self,
        table_docx: Path,
        export_dir: Path,
        run_dir: Path,
        table_index: int
    ) -> Dict:
        """
        转换单个表格文件

        Args:
            table_docx: 表格Word文件路径
            export_dir: 导出目录（存放PDF等中间文件，后续会被清理）
            run_dir: 运行目录（存放PNG图片等最终保留的文件）
            table_index: 表格编号

        Returns:
            {
                "html": "<table>...</table>",
                "pic": "img/Table_1.png"
            }
        """
        result = {
            "html": "",
            "pic": ""
        }

        try:
            # ========== 1. mammoth 转 HTML ==========
            html = self._convert_to_html(table_docx)
            result["html"] = html
            logger.info(f"表格 {table_index}: HTML转换完成，长度={len(html)}")

            # ========== 2. soffice 转 PDF（保存到export_dir）==========
            pdf_file = self._convert_to_pdf(table_docx, export_dir)
            logger.info(f"表格 {table_index}: PDF转换完成")

            # ========== 3. PyMuPDF 转 PNG（保存到run_dir/img）==========
            png_file = self._convert_to_png(pdf_file, run_dir, table_index)
            result["pic"] = str(png_file) if png_file else ""
            logger.info(f"表格 {table_index}: PNG转换完成 -> {png_file}")

            return result

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"表格 {table_index} 转换失败: {e}", exc_info=True)
            raise

    def _convert_to_html(self, table_docx: Path) -> str:
        """
        使用mammoth将Word转换为HTML

        Args:
            table_docx: Word文件路径

        Returns:
            HTML字符串
        """
        try:
            with open(table_docx, "rb") as f:
                result = mammoth.convert_to_html(f)
                html = result.value

                # 记录警告信息
                messages = result.messages
                if messages:
                    for msg in messages:
                        logger.debug(f"mammoth警告: {msg}")

                return html

        except Exception as e:
            logger.error(f"mammoth转换HTML失败: {e}")
            raise

    def _convert_to_pdf(self, table_docx: Path, output_dir: Path, max_retries: int = 3) -> Path:
        """
        使用soffice将Word转换为PDF（带重试机制）

        Args:
            table_docx: Word文件路径
            output_dir: 输出目录
            max_retries: 最大重试次数（默认3次）

        Returns:
            PDF文件路径
        """
        # PDF文件路径
        pdf_file = output_dir / f"{table_docx.stem}.pdf"

        # 为每个soffice调用创建独立的用户配置目录，避免多线程/多进程间锁冲突
        user_install = tempfile.mkdtemp(prefix="soffice_profile_")

        try:
            cmd = [
                "soffice",
                "--headless",
                "-env:UserInstallation=" + Path(user_install).as_uri(),     # Path.as_uri() 确保跨平台兼容
                "--convert-to", "pdf",
                "--outdir", str(output_dir.resolve()),
                str(table_docx.resolve())
            ]

            for attempt in range(max_retries):
                try:
                    logger.debug(f"执行soffice命令 (尝试 {attempt + 1}/{max_retries}): {cmd}")

                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=60
                    )

                    if result.returncode != 0:
                        logger.warning(
                            f"soffice返回非零 (尝试 {attempt + 1}/{max_retries}): "
                            f"returncode={result.returncode}, stderr={result.stderr}"
                        )

                    if pdf_file.exists():
                        logger.info(f"soffice转换成功 (尝试 {attempt + 1} 次)")
                        return pdf_file

                    # PDF未生成，记录详细信息用于诊断
                    logger.warning(
                        f"soffice返回码={result.returncode}但PDF未生成 (尝试 {attempt + 1}/{max_retries})\n"
                        f"  stdout: {result.stdout}\n"
                        f"  stderr: {result.stderr}\n"
                        f"  输出目录内容: {list(output_dir.iterdir()) if output_dir.exists() else '目录不存在'}"
                    )

                    if attempt < max_retries - 1:
                        logger.warning(f"等待5秒后重试...")
                        time.sleep(5)

                except subprocess.TimeoutExpired:
                    if attempt < max_retries - 1:
                        logger.warning(f"soffice超时，等待10秒后重试 (第{attempt + 1}/{max_retries}次)...")
                        time.sleep(10)
                        continue
                    logger.error("soffice转换超时（重试后仍失败）")
                    raise RuntimeError("soffice转换超时（重试3次后仍失败）")

                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"soffice异常: {e}，等待3秒后重试 (第{attempt + 1}/{max_retries}次)...")
                        time.sleep(3)
                        continue
                    logger.error(f"soffice转换PDF失败（重试后仍失败）: {e}")
                    raise

            raise RuntimeError(f"PDF文件未生成（重试{max_retries}次后失败）: {pdf_file}")

        finally:
            # 清理临时用户配置目录
            shutil.rmtree(user_install, ignore_errors=True)

    def _convert_to_png(
        self,
        pdf_file: Path,
        output_dir: Path,
        table_index: int,
        dpi: int = 200
    ) -> Optional[Path]:
        """
        使用PyMuPDF将PDF渲染为PNG，并裁剪白边

        Args:
            pdf_file: PDF文件路径
            output_dir: 输出目录
            table_index: 表格编号
            dpi: 图片分辨率

        Returns:
            PNG文件路径
        """
        doc = None
        try:
            # 图片输出目录
            img_dir = output_dir / "img"
            img_dir.mkdir(parents=True, exist_ok=True)

            # PNG文件路径
            png_file = img_dir / f"TemplateTable_{table_index}.png"

            # 使用PyMuPDF渲染PDF第一页（表格通常只有一页）
            doc = fitz.open(str(pdf_file))
            if doc.page_count == 0:
                raise RuntimeError("PyMuPDF未能生成图片：PDF无页面")

            page = doc[0]
            pix = page.get_pixmap(dpi=dpi)
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)

            # 转灰度，找内容边界
            gray = img.convert("L")
            # 反色（白底变黑底），让getbbox能识别内容区域
            inverted = ImageOps.invert(gray)
            bbox = inverted.getbbox()  # (left, top, right, bottom)

            if bbox:
                # 加一点padding
                padding = 20
                bbox_with_padding = (
                    max(0, bbox[0] - padding),
                    max(0, bbox[1] - padding),
                    min(img.width, bbox[2] + padding),
                    min(img.height, bbox[3] + padding),
                )
                cropped = img.crop(bbox_with_padding)
            else:
                # 没有检测到内容边界，使用原图
                cropped = img

            # 保存PNG
            cropped.save(png_file, "PNG")

            return png_file

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"PyMuPDF转换PNG失败: {e}")
            raise
        finally:
            if doc is not None:
                doc.close()


# ========== 全局单例 ==========
_table_converter: Optional[TableConverter] = None


def get_table_converter() -> TableConverter:
    """获取表格转换器单例"""
    global _table_converter
    if _table_converter is None:
        _table_converter = TableConverter()
    return _table_converter