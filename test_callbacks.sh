#!/bin/bash
# CSR 回调接口测试脚本

BASE_URL="http://192.168.3.32:8088"
PROJECT_ID="test_proj_$(date +%s)"
TAG_ID="test_tag_001"

echo "==================================="
echo "CSR 回调接口测试"
echo "==================================="
echo ""
echo "项目ID: $PROJECT_ID"
echo "标签ID: $TAG_ID"
echo ""

# 测试1: 标签状态更新接口
echo "1️⃣ 测试标签状态更新接口"
echo "-----------------------------------"
echo "URL: $BASE_URL/ky/sys/projectTagsSourceInfo/updateStatus"
echo ""

curl -X POST "$BASE_URL/ky/sys/projectTagsSourceInfo/updateStatus" \
  -F "id=$TAG_ID" \
  -F "project_id=$PROJECT_ID" \
  -F "status=生成内容中" \
  -w "\n\nHTTP状态码: %{http_code}\n" \
  -s

echo ""
echo ""

# 测试2: 标签结果推送接口
echo "2️⃣ 测试标签结果推送接口"
echo "-----------------------------------"
echo "URL: $BASE_URL/ky/sys/projectTagsSourceInfo/getTagAIResult"
echo ""

# 构建测试数据
DATA_JSON=$(cat <<EOF
{
  "paragraph_id": "$TAG_ID",
  "generated_content": "这是一个测试生成的内容。本研究采用随机、双盲、安慰剂对照的试验设计，旨在评估药物的有效性和安全性。",
  "status": "success",
  "provenance": {
    "extracted_items": [
      {
        "source_file": "test_protocol.docx",
        "matched_content": "研究设计采用随机双盲方法",
        "chunk_id": "test_chunk_1"
      }
    ]
  },
  "resource_mappings": {
    "tables": [],
    "figures": []
  }
}
EOF
)

curl -X POST "$BASE_URL/ky/sys/projectTagsSourceInfo/getTagAIResult" \
  -F "id=$TAG_ID" \
  -F "project_id=$PROJECT_ID" \
  -F "dataJson=$DATA_JSON" \
  -w "\n\nHTTP状态码: %{http_code}\n" \
  -s

echo ""
echo ""

# 测试3: 完整流程模拟
echo "3️⃣ 模拟完整生成流程"
echo "-----------------------------------"

STATUSES=("等待处理" "预处理中" "提取数据中" "生成内容中" "生成完成")

for status in "${STATUSES[@]}"; do
    echo "更新状态: $status"
    curl -X POST "$BASE_URL/ky/sys/projectTagsSourceInfo/updateStatus" \
      -F "id=$TAG_ID" \
      -F "project_id=$PROJECT_ID" \
      -F "status=$status" \
      -w " (HTTP: %{http_code})\n" \
      -s -o /dev/null
    sleep 1
done

echo ""
echo "最终推送结果..."
curl -X POST "$BASE_URL/ky/sys/projectTagsSourceInfo/getTagAIResult" \
  -F "id=$TAG_ID" \
  -F "project_id=$PROJECT_ID" \
  -F "dataJson=$DATA_JSON" \
  -w " (HTTP: %{http_code})\n" \
  -s -o /dev/null

echo ""
echo "==================================="
echo "测试完成！"
echo "==================================="

