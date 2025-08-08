#!/bin/bash

# 创建代码清理前的备份标签脚本
# 使用方法: ./create_backup_tag.sh

# 获取当前日期
DATE=$(date +%Y%m%d)

# 创建标签名
TAG_NAME="backup/pre-cleanup-$DATE"

# 创建标签 (指向主分支的当前状态)
git tag -a "$TAG_NAME" main -m "代码清理前的备份 - $DATE

这个标签标记了在执行冗余代码清理之前的项目状态。
包含的清理内容:
- 移除未使用的 buffer_pool.py
- 优化 engine.py 中的冗余代码
- 改进错误处理和测试覆盖

如需回滚，可以使用: git checkout $TAG_NAME"

echo "✅ 已创建备份标签: $TAG_NAME"
echo "📝 标签信息:"
git tag -n3 "$TAG_NAME"

echo ""
echo "🚀 推送标签到远程仓库 (可选):"
echo "   git push origin $TAG_NAME"
echo ""
echo "🔄 如需删除标签:"
echo "   本地: git tag -d $TAG_NAME"
echo "   远程: git push origin --delete $TAG_NAME"
