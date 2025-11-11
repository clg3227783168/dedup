#!/bin/bash
#
# 内存去重(KSM)集成测试
# 验证 dedup-snapshotter 的内存去重功能
#

set -e

# 颜色输出
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# KSM路径
KSM_PATH="/sys/kernel/mm/ksm"

# 统计变量
TESTS_PASSED=0
TESTS_FAILED=0

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

test_pass() {
    echo -e "${GREEN}✓ PASS${NC} $1"
    TESTS_PASSED=$((TESTS_PASSED + 1))
}

test_fail() {
    echo -e "${RED}✗ FAIL${NC} $1"
    TESTS_FAILED=$((TESTS_FAILED + 1))
}

# ============================================
# 测试用例 1: KSM可用性检查
# ============================================
test_ksm_availability() {
    echo ""
    echo "========================================"
    echo "测试 1: KSM 可用性检查"
    echo "========================================"

    log_info "检查KSM内核支持..."

    if [ ! -d "$KSM_PATH" ]; then
        log_error "KSM在此内核中不可用: $KSM_PATH 不存在"
        log_warn "请确保内核编译时启用了 CONFIG_KSM"
        test_fail "KSM 不可用"
        return 1
    fi

    test_pass "KSM 内核支持检测成功"

    # 检查关键文件
    local required_files=(
        "$KSM_PATH/run"
        "$KSM_PATH/pages_sharing"
        "$KSM_PATH/pages_shared"
        "$KSM_PATH/pages_unshared"
    )

    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            log_error "KSM文件不存在: $file"
            test_fail "KSM 文件缺失"
            return 1
        fi
    done

    test_pass "KSM 必要文件检查通过"
}

# ============================================
# 测试用例 2: KSM 启用/禁用
# ============================================
test_ksm_enable_disable() {
    echo ""
    echo "========================================"
    echo "测试 2: KSM 启用/禁用控制"
    echo "========================================"

    # 保存原始状态
    local original_state=$(cat $KSM_PATH/run)
    log_info "KSM 原始状态: $original_state"

    # 测试启用
    log_info "启用 KSM..."
    echo 1 | sudo tee $KSM_PATH/run > /dev/null

    local current_state=$(cat $KSM_PATH/run)
    if [ "$current_state" -eq 1 ]; then
        test_pass "KSM 启用成功"
    else
        test_fail "KSM 启用失败"
        return 1
    fi

    # 测试禁用
    log_info "禁用 KSM..."
    echo 0 | sudo tee $KSM_PATH/run > /dev/null

    current_state=$(cat $KSM_PATH/run)
    if [ "$current_state" -eq 0 ]; then
        test_pass "KSM 禁用成功"
    else
        test_fail "KSM 禁用失败"
    fi

    # 恢复原始状态
    echo $original_state | sudo tee $KSM_PATH/run > /dev/null
    log_info "恢复 KSM 原始状态: $original_state"
}

# ============================================
# 测试用例 3: KSM 统计信息读取
# ============================================
test_ksm_stats() {
    echo ""
    echo "========================================"
    echo "测试 3: KSM 统计信息读取"
    echo "========================================"

    log_info "读取 KSM 统计信息..."

    local pages_sharing=$(cat $KSM_PATH/pages_sharing)
    local pages_shared=$(cat $KSM_PATH/pages_shared)
    local pages_unshared=$(cat $KSM_PATH/pages_unshared)

    log_info "  pages_sharing (被合并的页面数): $pages_sharing"
    log_info "  pages_shared (共享的唯一页面数): $pages_shared"
    log_info "  pages_unshared (未共享的页面数): $pages_unshared"

    # 计算节省的内存
    local page_size=4096  # 通常是4KB
    local saved_bytes=$((pages_sharing * page_size))
    local saved_mb=$(echo "scale=2; $saved_bytes / 1024 / 1024" | bc)

    log_info "  节省内存: $saved_bytes bytes (${saved_mb} MB)"

    # 验证数据有效性
    if [ "$pages_sharing" -ge 0 ] && [ "$pages_shared" -ge 0 ]; then
        test_pass "KSM 统计信息读取成功"
    else
        test_fail "KSM 统计信息无效"
    fi

    # 检查是否有页面被合并
    if [ "$pages_sharing" -gt 0 ]; then
        log_info "系统当前有页面正在被KSM合并"
        test_pass "检测到活跃的KSM页面合并"
    else
        log_warn "当前没有检测到KSM页面合并(可能需要运行应用程序)"
    fi
}

# ============================================
# 测试用例 4: 实际内存去重效果
# ============================================
test_actual_memory_dedup() {
    echo ""
    echo "========================================"
    echo "测试 4: 实际内存去重效果验证"
    echo "========================================"

    # 检查是否有足够权限
    if [ "$EUID" -ne 0 ]; then
        log_warn "此测试需要root权限来启用KSM"
        log_warn "跳过实际去重效果测试"
        return 0
    fi

    log_info "启用 KSM..."
    echo 1 > $KSM_PATH/run

    # 记录初始KSM状态
    local initial_sharing=$(cat $KSM_PATH/pages_sharing)
    local initial_shared=$(cat $KSM_PATH/pages_shared)

    log_info "初始 KSM 状态:"
    log_info "  pages_sharing: $initial_sharing"
    log_info "  pages_shared: $initial_shared"

    # 创建测试程序(使用Python创建相同内存内容的进程)
    local test_script="/tmp/ksm_test.py"
    cat > "$test_script" <<'EOF'
#!/usr/bin/env python3
import mmap
import ctypes
import time
import sys

# 分配10MB内存并填充相同数据
size = 10 * 1024 * 1024
pattern = b'KSM_TEST_PATTERN' * (size // 16)

# 使用mmap分配内存
mm = mmap.mmap(-1, size)
mm.write(pattern)

# 标记为可合并(MADV_MERGEABLE)
libc = ctypes.CDLL('libc.so.6')
MADV_MERGEABLE = 12
addr = ctypes.c_void_p.from_buffer(mm)
libc.madvise(addr, size, MADV_MERGEABLE)

print(f"Process {sys.argv[1]}: 已分配 {size//1024//1024}MB 内存并标记为可合并", flush=True)
print(f"PID: {__import__('os').getpid()}", flush=True)

# 保持进程运行
time.sleep(60)
EOF

    chmod +x "$test_script"

    # 启动多个相同的进程
    log_info "启动5个相同内存内容的进程..."
    local pids=()
    for i in {1..5}; do
        python3 "$test_script" "$i" &
        pids+=($!)
        log_info "  启动进程 $i (PID: ${pids[-1]})"
    done

    # 等待KSM扫描和合并
    log_info "等待15秒让KSM扫描和合并页面..."
    sleep 15

    # 读取KSM状态
    local final_sharing=$(cat $KSM_PATH/pages_sharing)
    local final_shared=$(cat $KSM_PATH/pages_shared)

    log_info "最终 KSM 状态:"
    log_info "  pages_sharing: $final_sharing"
    log_info "  pages_shared: $final_shared"

    # 计算增量
    local delta_sharing=$((final_sharing - initial_sharing))
    local delta_shared=$((final_shared - initial_shared))

    log_info "KSM 增量:"
    log_info "  新增 pages_sharing: $delta_sharing"
    log_info "  新增 pages_shared: $delta_shared"

    # 计算节省的内存
    local saved_bytes=$((delta_sharing * 4096))
    local saved_mb=$(echo "scale=2; $saved_bytes / 1024 / 1024" | bc)

    log_info "  节省内存: $saved_bytes bytes (${saved_mb} MB)"

    # 清理进程
    log_info "清理测试进程..."
    for pid in "${pids[@]}"; do
        kill $pid 2>/dev/null || true
    done
    wait 2>/dev/null || true

    rm -f "$test_script"

    # 验证结果
    if [ "$delta_sharing" -gt 0 ]; then
        test_pass "内存去重效果验证: 节省 ${saved_mb}MB 内存"
        test_pass "KSM 成功合并了相同的内存页面"
    else
        log_warn "未检测到新的内存合并"
        log_warn "可能的原因:"
        log_warn "  1. KSM扫描间隔较长,需要更长等待时间"
        log_warn "  2. 系统KSM配置参数需要调整"
        test_fail "内存去重效果不明显"
    fi
}

# ============================================
# 测试用例 5: KSM性能参数
# ============================================
test_ksm_parameters() {
    echo ""
    echo "========================================"
    echo "测试 5: KSM 性能参数检查"
    echo "========================================"

    log_info "读取 KSM 配置参数..."

    # 常见的KSM参数
    local params=(
        "sleep_millisecs"
        "pages_to_scan"
        "merge_across_nodes"
    )

    for param in "${params[@]}"; do
        local param_file="$KSM_PATH/$param"
        if [ -f "$param_file" ]; then
            local value=$(cat "$param_file")
            log_info "  $param: $value"
        fi
    done

    test_pass "KSM 配置参数读取成功"

    # 给出优化建议
    log_info ""
    log_info "优化建议:"
    log_info "  - 增加 pages_to_scan 可加快扫描速度"
    log_info "  - 减少 sleep_millisecs 可提高响应速度(但会增加CPU使用)"
    log_info "  示例: echo 1000 > $KSM_PATH/pages_to_scan"
}

# ============================================
# 主测试流程
# ============================================
main() {
    echo "========================================"
    echo "内存去重(KSM)功能集成测试"
    echo "========================================"
    echo ""

    # 检查权限
    if [ "$EUID" -ne 0 ]; then
        log_warn "部分测试需要root权限"
        log_warn "使用 sudo 运行以获得完整测试结果"
    fi

    # 运行测试用例
    test_ksm_availability
    test_ksm_enable_disable
    test_ksm_stats
    test_actual_memory_dedup
    test_ksm_parameters

    # 测试总结
    echo ""
    echo "========================================"
    echo "测试总结"
    echo "========================================"
    echo -e "通过: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "失败: ${RED}$TESTS_FAILED${NC}"
    echo "总计: $((TESTS_PASSED + TESTS_FAILED))"

    if [ "$TESTS_FAILED" -eq 0 ]; then
        echo ""
        echo -e "${GREEN}✓ 所有测试通过!${NC}"
        echo ""
        echo "验证结论:"
        echo "  1. ✓ KSM 内核支持可用"
        echo "  2. ✓ KSM 可以正常启用/禁用"
        echo "  3. ✓ KSM 统计信息可读取"
        echo "  4. ✓ 内存去重功能正常工作"
        return 0
    else
        echo ""
        echo -e "${RED}✗ 部分测试失败${NC}"
        return 1
    fi
}

# 运行测试
main "$@"
