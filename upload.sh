#!/bin/bash
# Git 上传脚本

echo "=================================="
echo "上传代码到 GitHub"
echo "=================================="
echo

# 检查 git
if ! command -v git &> /dev/null; then
    echo "❌ Git 未安装"
    echo "请下载安装: https://git-scm.com/download/win"
    exit 1
fi

echo "✅ Git 已安装:"
git --version
echo

# 初始化
echo "1️⃣ 初始化 Git 仓库..."
git init
echo "   ✅ 完成"
echo

# 配置用户
echo "2️⃣ 配置 Git 用户..."
git config user.name "forg1ve1125"
git config user.email "no-reply@github.com"
echo "   ✅ 完成"
echo

# 添加文件
echo "3️⃣ 添加所有文件..."
git add .
echo "   ✅ 完成"
echo

# 显示将要提交的文件
echo "📁 将要提交的文件数量:"
git status --short | wc -l
echo

# 提交
echo "4️⃣ 提交代码..."
git commit -m "Initial Prefect deployment"
echo "   ✅ 完成"
echo

# 添加远程仓库
echo "5️⃣ 配置远程仓库..."
git remote add origin https://github.com/forg1ve1125/Prefect_Project.git
echo "   ✅ 完成"
echo

# 重命名分支
echo "6️⃣ 重命名分支..."
git branch -M main
echo "   ✅ 完成"
echo

# 推送
echo "7️⃣ 推送代码到 GitHub..."
echo "   ⚠️  输入 GitHub 凭证..."
git push -u origin main
echo "   ✅ 完成"
echo

echo "=================================="
echo "✅ 成功!"
echo "=================================="
echo
echo "代码已上传到:"
echo "https://github.com/forg1ve1125/Prefect_Project"
echo
echo "🎉 下一步:"
echo "   1. 去 Prefect Cloud 添加计划 (schedules)"
echo "   2. 流会自动从 GitHub 拉取代码"
echo "   3. 每月 17 日自动运行"
