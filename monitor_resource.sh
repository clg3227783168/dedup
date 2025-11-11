#!/bin/bash
# monitor_process_tree_advanced.sh
# Usage: ./monprocess_tree_advanced.sh <process_name_or_PID> <duration> <interval> [output_prefix]

TARGET="$1"
DURATION="$2"
INTERVAL="$3"
OUTPUT_PREFIX="${4:-monitor_data}"

# 输出文件定义
PROCESS_TREE_FILE="${OUTPUT_PREFIX}_process_tree.txt" # 静态进程关系文件
USAGE_SUMMARY_FILE="${OUTPUT_PREFIX}_usage_summary.csv" # 资源总开销CSV

# 获取目标进程的PID
if [[ $TARGET =~ ^[0-9]+$ ]]; then
    ROOT_PID=$TARGET
    PROCESS_NAME=$(ps -p $TARGET -o comm= 2>/dev/null | head -1)
else
    ROOT_PID=$(pgrep -o $TARGET)
    PROCESS_NAME=$TARGET
fi

if [ -z "$ROOT_PID" ]; then
    echo "Error: Process '$TARGET' not found."
    exit 1
fi

echo "Monitoring process tree for: $PROCESS_NAME (Root PID: $ROOT_PID)"
echo "Duration: $DURATION seconds, Interval: $INTERVAL seconds"
echo "Process tree will be saved to: $PROCESS_TREE_FILE"
echo "Usage summary will be saved to: $USAGE_SUMMARY_FILE"

# 函数：获取进程树的所有PIDs（包括所有层级的子进程）
get_entire_process_tree() {
    local root_pid=$1
    echo $root_pid
    # 使用ps递归地查找所有子进程 [4,10](@ref)
    for child in $(ps -o pid --ppid $root_pid --no-headers 2>/dev/null); do
        get_entire_process_tree $child
    done
}

# 1. 生成并输出一次性的进程关系树到单独文件
echo "Generating process tree relationship..."
echo "Process Tree Relationship for $PROCESS_NAME (Root PID: $ROOT_PID)" > $PROCESS_TREE_FILE
echo "Generated at: $(date)" >> $PROCESS_TREE_FILE
echo "==================================================" >> $PROCESS_TREE_FILE

# 使用pstree命令可以更直观地显示进程关系 [5](@ref)
if command -v pstree &> /dev/null; then
    pstree -p $ROOT_PID >> $PROCESS_TREE_FILE 2>/dev/null || echo "Note: Full tree not available, listing all PIDs instead." >> $PROCESS_TREE_FILE
fi

# 同时列出所有进程的PID和PPID（父进程ID）的详细信息 [4](@ref)
echo "" >> $PROCESS_TREE_FILE
echo "Detailed PID/PPID List:" >> $PROCESS_TREE_FILE
echo "Level | PID | PPID | Process Name" >> $PROCESS_TREE_FILE
echo "------|-----|------|-------------" >> $PROCESS_TREE_FILE

# 递归函数：打印带有层级缩进的进程列表
print_process_tree() {
    local pid=$1
    local level=$2
    local ppid=$(ps -o ppid= -p $pid 2>/dev/null | tr -d ' ')
    local pname=$(ps -o comm= -p $pid 2>/dev/null)
    
    # 创建缩进来表示层级
    local indent=""
    for ((i=0; i<$level; i++)); do
        indent+="  "
    done
    echo "L$level  | ${indent}$pid | $ppid | $pname" >> $PROCESS_TREE_FILE
    
    # 递归处理子进程
    for child in $(ps -o pid --ppid $pid --no-headers 2>/dev/null); do
        print_process_tree $child $((level + 1))
    done
}

print_process_tree $ROOT_PID 0 >> $PROCESS_TREE_FILE
echo "Process tree relationship saved to: $PROCESS_TREE_FILE"

# 2. 准备资源总开销监控的CSV文件头
echo "Timestamp,Total_CPU(%),Total_Memory(MB),Process_Count" > $USAGE_SUMMARY_FILE

start_time=$(date +%s)
end_time=$((start_time + DURATION))

echo "Starting resource usage monitoring for $DURATION seconds..."

# 函数：计算整个进程树的总CPU和内存使用量
calculate_total_usage() {
    local total_cpu=0
    local total_memory=0
    local process_count=0
    
    # 获取所有进程树的PID
    local all_pids=$(get_entire_process_tree $ROOT_PID | sort -nu)
    
    if [ -z "$all_pids" ]; then
        echo "0,0,0" # 如果进程树不存在，返回0
        return
    fi
    
    # 遍历每个PID，累计CPU和内存使用量 [6,12](@ref)
    for pid in $all_pids; do
        # 检查进程是否存在
        if [ ! -d "/proc/$pid" ]; then
            continue
        fi
        
        # 获取进程的CPU和内存使用率 [11,13](@ref)
        local proc_stats=$(ps -p $pid -o pcpu,rss --no-headers 2>/dev/null)
        if [ -n "$proc_stats" ]; then
            local cpu_usage=$(echo $proc_stats | awk '{print $1}')
            local mem_kb=$(echo $proc_stats | awk '{print $2}')
            local mem_mb=$(echo "scale=2; $mem_kb / 1024" | bc)
            
            total_cpu=$(echo "scale=2; $total_cpu + $cpu_usage" | bc)
            total_memory=$(echo "scale=2; $total_memory + $mem_mb" | bc)
            process_count=$((process_count + 1))
        fi
    done
    
    echo "$total_cpu,$total_memory,$process_count"
}

# 主监控循环
while [ $(date +%s) -lt $end_time ]; do
    timestamp=$(date '+%M:%S')
    
    # 检查根进程是否仍然存在
    if [ ! -d "/proc/$ROOT_PID" ]; then
        echo "$timestamp,0,0,0" >> $USAGE_SUMMARY_FILE
        echo "Root process has terminated. Monitoring stopped."
        break
    fi
    
    # 计算总使用量
    result=$(calculate_total_usage)
    IFS=',' read total_cpu total_memory process_count <<< "$result"
    echo $result 
    # 写入CSV文件
    echo "$timestamp,$total_cpu,$total_memory,$process_count" >> $USAGE_SUMMARY_FILE
    
    sleep $INTERVAL
done

echo "Monitoring completed."
echo "Process tree relationship: $PROCESS_TREE_FILE"
echo "Resource usage summary: $USAGE_SUMMARY_FILE"
