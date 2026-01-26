#!/bin/bash

# Telegram Auto Sender - Git Update Script
# 用于快速提交和推送代码到GitHub

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Telegram Auto Sender - Git Update Script ===${NC}\n"

# 检查是否在git仓库中
if [ ! -d .git ]; then
    echo -e "${RED}错误: 当前目录不是Git仓库${NC}"
    echo -e "${YELLOW}提示: 请先运行 git init 初始化仓库${NC}"
    exit 1
fi

# 获取提交信息
if [ -z "$1" ]; then
    echo -e "${YELLOW}请输入提交信息（或按Enter使用默认信息）:${NC}"
    read -r commit_message
    if [ -z "$commit_message" ]; then
        commit_message="Update: $(date '+%Y-%m-%d %H:%M:%S')"
    fi
else
    commit_message="$1"
fi

echo -e "\n${GREEN}1. 检查Git状态...${NC}"
git status

echo -e "\n${GREEN}2. 添加所有更改...${NC}"
git add .

echo -e "\n${GREEN}3. 提交更改...${NC}"
git commit -m "$commit_message"

echo -e "\n${GREEN}4. 检查远程仓库...${NC}"
if ! git remote | grep -q origin; then
    echo -e "${YELLOW}未找到远程仓库 'origin'${NC}"
    echo -e "${YELLOW}请先添加远程仓库:${NC}"
    echo -e "  git remote add origin <your-repo-url>"
    exit 1
fi

echo -e "\n${GREEN}5. 推送到远程仓库...${NC}"
current_branch=$(git branch --show-current)
echo -e "${YELLOW}当前分支: $current_branch${NC}"

read -p "是否推送到远程仓库? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    git push origin "$current_branch"
    echo -e "\n${GREEN}✓ 更新完成！${NC}"
else
    echo -e "\n${YELLOW}已取消推送${NC}"
fi
