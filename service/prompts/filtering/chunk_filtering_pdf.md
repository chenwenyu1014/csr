【任务目标】
根据用户的提取需求，从PDF文档的多个分块中筛选出最相关的分块。

【用户提取需求】
{{extraction_query}}

【项目背景（可选）】
{{project_desc}}

【PDF文档分块列表】
以下是PDF文档的所有分块，每个分块包含ID、标题和摘要：

{{chunks_list}}

【筛选要求】
1. **相关性判断**：
   - 根据用户的提取需求，判断每个分块是否包含相关信息
   - 分块的标题和摘要是判断的主要依据
   - 考虑OCR可能的误差，语义相近即可认为相关

2. **筛选原则**：
   - 宽松筛选：如果分块可能包含相关信息，就应该保留
   - 避免遗漏：宁可多选，不可漏选关键信息

3. **输出格式**（严格JSON）：
必须只输出一个 JSON 对象，不要使用 Markdown 代码块或任何额外文字。

空输入/无相关分块时，输出：
{
  "relevant_sections": [],
  "total_selected": 0,
  "selection_summary": ""
}

标准格式示例：
{
  "relevant_sections": [
    {
      "section_id": "h1_1",
      "title": "试验概述",
      "relevance_score": 0.9,
      "reason": "该分块包含xxx信息，与提取需求相关"
    },
    {
      "section_id": "h1_2",
      "title": "试验人群",
      "relevance_score": 0.85,
      "reason": "该分块描述了xxx，可能包含相关数据"
    }
  ],
  "total_selected": 2,
  "selection_summary": "筛选出2个相关分块，主要涵盖xxx内容"
}

【字段说明】
- `section_id`: 分块ID（必须与"【分块 <ID>】"中的ID完全一致，如 h1_1, h1_2）
- `title`: 分块标题
- `relevance_score`: 相关性评分（0-1），越高越相关
- `reason`: 筛选理由
- `total_selected`: 筛选出的分块总数
- `selection_summary`: 筛选摘要

【注意事项】
- 只输出JSON，不要任何额外文字
- 如果没有相关分块，relevant_sections为空数组
- section_id必须从提供的分块列表中复制，不要修改
