#!/bin/bash
echo "=== 系统环境信息收集 ==="
echo "收集时间: $(date)"
echo

echo "------- 1. CPU 信息 -------"
lscpu
echo

echo "------- 2. 内存信息 -------"
free -h
echo

echo "------- 3. 操作系统信息 -------"
echo "内核: $(uname -r)"
echo "主机名: $(hostname)"
if [ -f /etc/os-release ]; then
    echo "发行版信息:"
    cat /etc/os-release
fi
