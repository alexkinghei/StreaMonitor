#!/bin/bash

# Git 提交脚本
# 用法: ./git-commit.sh "提交信息" [push]
# 例如: ./git-commit.sh "修复下载问题" 
#       ./git-commit.sh "更新配置" push

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查是否在 git 仓库中
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo -e "${RED}错误: 当前目录不是 git 仓库${NC}"
    exit 1
fi

# 检查是否有提交信息
if [ -z "$1" ]; then
    echo -e "${YELLOW}用法: $0 \"提交信息\" [push]${NC}"
    echo -e "${YELLOW}例如: $0 \"修复下载问题\"${NC}"
    echo -e "${YELLOW}      $0 \"更新配置\" push${NC}"
    exit 1
fi

COMMIT_MSG="$1"
SHOULD_PUSH="${2:-}"

# 显示当前状态
echo -e "${GREEN}=== Git 状态 ===${NC}"
git status --short

# 检查是否有更改
if [ -z "$(git status --porcelain)" ]; then
    echo -e "${YELLOW}没有需要提交的更改${NC}"
    exit 0
fi

# 添加所有更改的文件（排除 .DS_Store）
echo -e "${GREEN}=== 添加文件 ===${NC}"
git add -A

# 移除 .DS_Store（如果被添加了）
if git diff --cached --name-only | grep -q "\.DS_Store"; then
    echo -e "${YELLOW}移除 .DS_Store 文件${NC}"
    git reset HEAD .DS_Store 2>/dev/null || true
fi

# 显示将要提交的文件
echo -e "${GREEN}=== 将要提交的文件 ===${NC}"
git diff --cached --name-status

# 确认提交
echo -e "${YELLOW}提交信息: ${COMMIT_MSG}${NC}"
read -p "确认提交? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${RED}已取消${NC}"
    exit 1
fi

# 提交
echo -e "${GREEN}=== 提交代码 ===${NC}"
git commit -m "$COMMIT_MSG"

# 如果需要推送
if [ "$SHOULD_PUSH" = "push" ]; then
    echo -e "${GREEN}=== 推送到远程仓库 ===${NC}"
    CURRENT_BRANCH=$(git branch --show-current)
    echo -e "${YELLOW}当前分支: ${CURRENT_BRANCH}${NC}"
    read -p "确认推送到 origin/${CURRENT_BRANCH}? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git push origin "$CURRENT_BRANCH"
        echo -e "${GREEN}✓ 推送成功${NC}"
    else
        echo -e "${YELLOW}已跳过推送${NC}"
    fi
fi

echo -e "${GREEN}✓ 完成${NC}"
