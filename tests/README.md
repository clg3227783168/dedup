# 块级去重测试套件

本目录包含用于验证 dedup-snapshotter 块级去重功能的完整测试套件。

## 测试目标

根据评测指标中的"先进性(20分)"要求,验证:

1. **块粒度数据去重 (3分)**: 证明实现了4MB固定块级别的去重,而非文件级去重
2. **不破坏文件元数据 (3分)**: 验证去重不影响文件权限、时间戳等元数据
3. **支持内存去重 (4分)**: 验证KSM内存页面合并功能

## 测试结构

```
tests/
├── README.md                          # 本文件
├── quick_demo.sh                      # 快速演示脚本(5分钟)
├── integration/
│   ├── block_dedup_test.sh           # 块级去重集成测试
│   └── memory_dedup_test.sh          # 内存去重集成测试
└── unit/
    └── (通过 go test 运行)
```

## 快速开始

### 1. 快速演示(推荐用于评审展示)

快速展示去重效果,适合向评委演示:

```bash
cd /home/clg/dedup/tests
chmod +x quick_demo.sh
./quick_demo.sh
```

**预期输出**:
- 创建3个镜像层,模拟真实场景
- 展示块级去重效果(20MB → 12MB)
- 可视化块引用关系
- 计算并显示去重率(~40%)

**运行时间**: ~30秒

---

### 2. 单元测试

测试核心去重逻辑:

```bash
cd /home/clg/dedup/dedup-snapshotter

# 测试块级去重
go test -v ./pkg/storage -run TestChunkLevelDeduplication

# 测试固定块大小
go test -v ./pkg/storage -run TestBlockBoundaryFixedSize

# 测试元数据保留
go test -v ./pkg/storage -run TestMetadataPreservation

# 测试去重率
go test -v ./pkg/storage -run TestDeduplicationRatio

# 运行所有存储测试
go test -v ./pkg/storage

# 测试内存去重
go test -v ./pkg/memory
```

**关键测试用例**:

| 测试用例 | 验证内容 | 对应评分点 |
|---------|---------|-----------|
| `TestChunkLevelDeduplication` | 跨文件块去重 | 块粒度去重(3分) |
| `TestBlockBoundaryFixedSize` | 固定4MB切分 | 块粒度去重(3分) |
| `TestMetadataPreservation` | 元数据独立性 | 不破坏元数据(3分) |
| `TestMemoryDeduplication` | KSM页面合并 | 内存去重(4分) |

---

### 3. 集成测试

#### 3.1 块级去重集成测试

```bash
cd /home/clg/dedup/tests/integration
chmod +x block_dedup_test.sh
./block_dedup_test.sh
```

**测试内容**:
1. 跨文件块去重验证
2. 固定块大小验证
3. 元数据完整性验证
4. 去重率统计

**预期结果**:
```
✓ PASS 块级去重: 3个文件(24MB)存储为3个唯一块
✓ PASS 固定块大小验证通过
✓ PASS 权限独立: fileA=755, fileB=644
✓ PASS 去重率符合预期: 42.86%
```

#### 3.2 内存去重集成测试

```bash
cd /home/clg/dedup/tests/integration
chmod +x memory_dedup_test.sh

# 需要root权限来控制KSM
sudo ./memory_dedup_test.sh
```

**测试内容**:
1. KSM内核支持检测
2. KSM启用/禁用控制
3. KSM统计信息读取
4. 实际内存去重效果

**预期结果**:
```
✓ PASS KSM 内核支持检测成功
✓ PASS KSM 启用成功
✓ PASS KSM 统计信息读取成功
✓ PASS 内存去重效果验证: 节省 XX MB 内存
```

---

## 测试用例详解

### 用例1: 跨文件块去重

**原理**: 不同文件包含相同的块时,应该共享存储

```
文件A: [块1:AAAA][块2:BBBB]  8MB
文件B: [块1:AAAA][块3:CCCC]  8MB
文件C: [块2:BBBB][块3:CCCC]  8MB
────────────────────────────
原始: 24MB
去重: 12MB (3个唯一块)
```

**验证方法**:
```bash
# 检查chunks目录块数量
ls -1 /var/lib/dedup-snapshotter/chunks | wc -l
# 应该是 3,而不是 6

# 计算去重率
du -sh /var/lib/dedup-snapshotter/chunks
# 应该约 12MB,而非 24MB
```

---

### 用例2: 固定块大小 vs 内容感知

**原理**: 证明使用固定4MB切分,而非CDC(Content-Defined Chunking)

```
文件A: [4MB 全零][4MB 全零]
文件B: [1MB 随机 + 3MB 零][4MB 全零]
```

**预期**:
- 固定4MB切分: 3个唯一块 ✓
  - fileA块1(4MB零)
  - fileA块2(4MB零) = fileB块2 (共享)
  - fileB块1(1MB随机+3MB零,与fileA块1不同)

- 如果是CDC: 可能识别到7MB连续的零 (错误)

**验证**: 块数量必须是3个,证明固定切分

---

### 用例3: 元数据独立性

**原理**: 即使块被共享,文件元数据应该独立

```bash
# 创建相同内容的文件
echo "SAME" > fileA
echo "SAME" > fileB

# 设置不同权限和时间
chmod 755 fileA
chmod 644 fileB
touch -t 202301010000 fileA
touch -t 202401010000 fileB

# 验证
stat fileA  # 应该显示 755, 2023-01-01
stat fileB  # 应该显示 644, 2024-01-01
sha256sum fileA fileB  # 内容哈希应该相同
```

---

### 用例4: 内存去重(KSM)

**原理**: 多个容器映射相同文件时,内存页面应该被合并

**验证方法**:
```bash
# 检查KSM启用
cat /sys/kernel/mm/ksm/run  # 应该是 1

# 查看共享页面数
cat /sys/kernel/mm/ksm/pages_sharing  # 应该 > 0

# 计算节省内存
pages_sharing=$(cat /sys/kernel/mm/ksm/pages_sharing)
saved_mb=$((pages_sharing * 4 / 1024))
echo "节省内存: ${saved_mb}MB"
```

---

## 性能基准

运行性能测试:

```bash
# 去重性能
go test -bench=. ./pkg/storage -benchtime=10s

# 内存去重性能
go test -bench=. ./pkg/memory -benchtime=10s
```

**预期结果**:
```
BenchmarkChunkDedup-8    1000    1234567 ns/op
BenchmarkMemoryDedup-8    500    2345678 ns/op
```

---

## 评审展示建议

### 演示流程(10分钟)

1. **快速演示** (3分钟)
   ```bash
   ./tests/quick_demo.sh
   ```
   - 展示块级去重效果可视化
   - 强调去重率数据

2. **运行单元测试** (3分钟)
   ```bash
   go test -v ./pkg/storage -run TestChunkLevelDeduplication
   ```
   - 展示测试通过
   - 强调"3个文件 → 3个唯一块"

3. **查看实际chunks目录** (2分钟)
   ```bash
   ls -lh /var/lib/dedup-snapshotter/chunks
   tree /var/lib/dedup-snapshotter
   ```
   - 展示块存储结构
   - 展示块哈希文件

4. **内存去重验证** (2分钟)
   ```bash
   cat /sys/kernel/mm/ksm/pages_sharing
   # 展示KSM统计
   ```

### 关键展示点

| 展示内容 | 证明点 | 评分项 |
|---------|-------|--------|
| chunks目录文件数 = 唯一块数 | 块级去重 | 3分 |
| 相同内容不同元数据 | 元数据独立 | 3分 |
| KSM pages_sharing > 0 | 内存去重 | 4分 |
| 去重率统计 | 量化效果 | 加分项 |

---

## 故障排查

### 单元测试失败

```bash
# 检查Go环境
go version

# 清理缓存
go clean -testcache

# 详细输出
go test -v -x ./pkg/storage
```

### KSM不可用

```bash
# 检查内核配置
grep CONFIG_KSM /boot/config-$(uname -r)

# 应该显示: CONFIG_KSM=y

# 如果KSM未启用,重新编译内核或使用支持KSM的内核
```

### 权限问题

```bash
# KSM控制需要root
sudo -i

# 或使用sudo运行测试
sudo ./tests/integration/memory_dedup_test.sh
```

---

## 测试覆盖率

生成测试覆盖率报告:

```bash
# 生成覆盖率数据
go test ./pkg/storage -coverprofile=coverage.out

# 查看覆盖率
go tool cover -func=coverage.out

# 生成HTML报告
go tool cover -html=coverage.out -o coverage.html
```

**目标覆盖率**: > 80%

---

## CI/CD集成

### GitHub Actions示例

```yaml
name: Dedup Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Go
        uses: actions/setup-go@v4
        with:
          go-version: '1.21'

      - name: Run Unit Tests
        run: |
          cd dedup-snapshotter
          go test -v ./pkg/storage
          go test -v ./pkg/memory

      - name: Run Integration Tests
        run: |
          cd tests/integration
          ./block_dedup_test.sh

      - name: Upload Coverage
        uses: codecov/codecov-action@v3
```

---

## 参考文档

- [评测指标](../软件评测指标.md)
- [竞赛题目](../竞赛题目.md)
- [KSM文档](https://www.kernel.org/doc/html/latest/admin-guide/mm/ksm.html)
- [EROFS](https://docs.kernel.org/filesystems/erofs.html)

---

## 联系方式

如有问题,请查看:
- 项目README: `/home/clg/dedup/dedup-snapshotter/README.md`
- 测试方案: `/home/clg/dedup/测试方案-块级去重验证.md`
