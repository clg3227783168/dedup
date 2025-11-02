#!/bin/bash
# 自动转换功能测试脚本

set -e

DEDUP_ROOT="${DEDUP_ROOT:-/var/lib/containerd/io.containerd.snapshotter.v1.dedup}"
TEST_IMAGE="${TEST_IMAGE:-docker.io/library/alpine:latest}"
SNAPSHOTTER_SOCK="/run/containerd/dedup-snapshotter.sock"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "此脚本需要 root 权限"
        exit 1
    fi
}

check_requirements() {
    log_info "检查系统要求..."

    # containerd
    if ! command -v ctr &> /dev/null; then
        log_error "未找到 ctr 命令,请安装 containerd"
        exit 1
    fi

    # mkfs.erofs
    if ! command -v mkfs.erofs &> /dev/null; then
        log_error "未找到 mkfs.erofs,请安装 erofs-utils"
        exit 1
    fi

    # erofs 内核支持
    if ! grep -q erofs /proc/filesystems; then
        log_warn "内核未加载 erofs 模块,尝试加载..."
        modprobe erofs || log_warn "无法加载 erofs 模块"
    fi

    log_info "✓ 系统要求检查通过"
}

check_snapshotter() {
    log_info "检查 dedup-snapshotter 状态..."

    if [[ ! -S "$SNAPSHOTTER_SOCK" ]]; then
        log_error "Snapshotter socket 不存在: $SNAPSHOTTER_SOCK"
        log_error "请先启动 dedup-snapshotter 服务"
        exit 1
    fi

    log_info "✓ Snapshotter 正在运行"
}

cleanup_old_image() {
    log_info "清理旧镜像(如果存在)..."

    # 删除旧容器
    ctr container rm test-auto-convert 2>/dev/null || true

    # 删除旧镜像
    ctr image rm "$TEST_IMAGE" 2>/dev/null || true

    log_info "✓ 清理完成"
}

test_auto_conversion() {
    log_info "=================================="
    log_info "测试自动转换功能"
    log_info "=================================="
    log_info "测试镜像: $TEST_IMAGE"
    log_info ""

    # 启动日志监控(后台)
    log_info "启动日志监控..."
    journalctl -u dedup-snapshotter -f > /tmp/dedup-test.log 2>&1 &
    LOG_PID=$!
    sleep 1

    # 拉取镜像
    log_info "拉取镜像(自动转换)..."
    log_info "命令: ctr image pull --snapshotter=dedup $TEST_IMAGE"

    if ! ctr image pull --snapshotter=dedup "$TEST_IMAGE"; then
        log_error "镜像拉取失败"
        kill $LOG_PID 2>/dev/null || true
        exit 1
    fi

    log_info "✓ 镜像拉取完成"
    sleep 2

    # 停止日志监控
    kill $LOG_PID 2>/dev/null || true

    # 检查日志中是否有自动转换记录
    log_info ""
    log_info "检查自动转换日志..."

    if grep -q "auto-converting to EROFS" /tmp/dedup-test.log; then
        log_info "✓ 发现自动转换日志"
        echo ""
        log_info "转换日志摘要:"
        grep -E "auto-converting|successfully auto-converted" /tmp/dedup-test.log | head -5
    else
        log_warn "未发现自动转换日志"
        log_warn "可能原因:"
        log_warn "  1. 层已经存在(之前转换过)"
        log_warn "  2. 转换失败"
        log_warn "  3. 日志级别过滤"
    fi

    # 检查 EROFS 镜像文件
    log_info ""
    log_info "检查 EROFS 镜像文件..."

    if ls "$DEDUP_ROOT"/images/*.erofs >/dev/null 2>&1; then
        log_info "✓ 找到 EROFS 镜像文件:"
        ls -lh "$DEDUP_ROOT"/images/*.erofs | tail -5
    else
        log_error "未找到 EROFS 镜像文件"
        exit 1
    fi

    # 检查元数据
    log_info ""
    log_info "检查元数据文件..."

    if ls "$DEDUP_ROOT"/metadata/*.json >/dev/null 2>&1; then
        log_info "✓ 找到元数据文件:"
        ls -lh "$DEDUP_ROOT"/metadata/*.json | tail -3
    else
        log_warn "未找到元数据文件"
    fi
}

test_container_run() {
    log_info ""
    log_info "=================================="
    log_info "测试容器运行"
    log_info "=================================="

    log_info "运行测试容器..."

    if ctr run --snapshotter=dedup --rm "$TEST_IMAGE" test-auto-convert echo "Hello from dedup snapshotter!"; then
        log_info "✓ 容器运行成功"
    else
        log_error "容器运行失败"
        exit 1
    fi
}

verify_fscache() {
    log_info ""
    log_info "=================================="
    log_info "验证 Fscache 支持"
    log_info "=================================="

    if grep -q fscache /proc/filesystems 2>/dev/null; then
        log_info "✓ Fscache 已启用"

        if [[ -f /proc/fs/fscache/stats ]]; then
            log_info "Fscache 统计:"
            head -10 /proc/fs/fscache/stats
        fi
    else
        log_warn "Fscache 未启用 (需要内核 >= 5.19)"
        log_warn "将使用 loop mount 模式"
    fi
}

show_statistics() {
    log_info ""
    log_info "=================================="
    log_info "统计信息"
    log_info "=================================="

    log_info "EROFS 镜像数量: $(ls "$DEDUP_ROOT"/images/*.erofs 2>/dev/null | wc -l)"
    log_info "EROFS 镜像总大小: $(du -sh "$DEDUP_ROOT"/images 2>/dev/null | cut -f1)"
    log_info "快照数量: $(ls -d "$DEDUP_ROOT"/snapshots/* 2>/dev/null | wc -l)"
    log_info "缓存大小: $(du -sh "$DEDUP_ROOT"/cache 2>/dev/null | cut -f1)"
}

main() {
    log_info "=================================="
    log_info "Dedup Snapshotter 自动转换测试"
    log_info "=================================="
    echo ""

    check_root
    check_requirements
    check_snapshotter
    cleanup_old_image

    echo ""
    test_auto_conversion
    test_container_run
    verify_fscache
    show_statistics

    echo ""
    log_info "=================================="
    log_info "✓ 所有测试通过!"
    log_info "=================================="
    echo ""
    log_info "自动转换功能正常工作"
    log_info "现在可以正常使用: ctr image pull --snapshotter=dedup <image>"
    echo ""
}

main "$@"
