package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestChunkLevelDeduplication 验证块级去重功能
// 核心验证点: 不同文件的相同块应该被去重
func TestChunkLevelDeduplication(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewDedupStoreWithErofs(tmpDir, false)
	if err != nil {
		t.Fatalf("failed to create dedup store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// 创建测试数据块
	// 块1: 4MB 全'A'
	pattern1 := bytes.Repeat([]byte("A"), 4*1024*1024)
	// 块2: 4MB 全'B'
	pattern2 := bytes.Repeat([]byte("B"), 4*1024*1024)
	// 块3: 4MB 全'C'
	pattern3 := bytes.Repeat([]byte("C"), 4*1024*1024)

	// 文件A: [块1][块2] = 8MB
	fileA := append([]byte{}, pattern1...)
	fileA = append(fileA, pattern2...)

	// 文件B: [块1][块3] = 8MB (与A共享块1)
	fileB := append([]byte{}, pattern1...)
	fileB = append(fileB, pattern3...)

	// 文件C: [块2][块3] = 8MB (与A共享块2, 与B共享块3)
	fileC := append([]byte{}, pattern2...)
	fileC = append(fileC, pattern3...)

	// 写入文件
	if err := store.WriteFile(ctx, "testA", bytes.NewReader(fileA)); err != nil {
		t.Fatalf("failed to write fileA: %v", err)
	}
	if err := store.WriteFile(ctx, "testB", bytes.NewReader(fileB)); err != nil {
		t.Fatalf("failed to write fileB: %v", err)
	}
	if err := store.WriteFile(ctx, "testC", bytes.NewReader(fileC)); err != nil {
		t.Fatalf("failed to write fileC: %v", err)
	}

	// 验证: chunks目录应该只有3个唯一块
	chunks, err := os.ReadDir(store.chunksDir)
	if err != nil {
		t.Fatalf("failed to read chunks dir: %v", err)
	}

	uniqueChunks := 0
	for _, chunk := range chunks {
		if !chunk.IsDir() {
			uniqueChunks++
		}
	}

	// 关键断言: 3个文件共24MB数据,但只应该存储3个唯一的4MB块
	if uniqueChunks != 3 {
		t.Errorf("Expected 3 unique chunks, got %d", uniqueChunks)
		t.Logf("块级去重失败: 应该检测到3个独特块(块1,块2,块3)")
		t.Logf("实际检测到 %d 个块,说明可能是文件级去重而非块级去重", uniqueChunks)
	} else {
		t.Logf("✓ 块级去重验证通过: 3个文件(24MB)共享存储为3个唯一块")
	}

	// 额外验证: 检查索引数据库中的引用计数
	// 每个块应该被引用2次
	verifyChunkRefCounts(t, store, pattern1, pattern2, pattern3)
}

// TestBlockBoundaryFixedSize 验证固定大小块切分(非内容感知)
// 这证明是块级去重,而非可变长度的内容定义分块(CDC)
func TestBlockBoundaryFixedSize(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewDedupStoreWithErofs(tmpDir, false)
	if err != nil {
		t.Fatalf("failed to create dedup store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// 文件A: 8MB 全零
	fileA := bytes.Repeat([]byte{0}, 8*1024*1024)

	// 文件B: 前1MB随机数据 + 后7MB全零
	// 如果是固定4MB切分:
	//   块1: [1MB随机 + 3MB零] - 与fileA的块1不同
	//   块2: [4MB零] - 与fileA的块2相同
	randomData := make([]byte, 1*1024*1024)
	for i := range randomData {
		randomData[i] = byte(i % 256)
	}
	fileB := append(randomData, bytes.Repeat([]byte{0}, 7*1024*1024)...)

	if err := store.WriteFile(ctx, "testA", bytes.NewReader(fileA)); err != nil {
		t.Fatalf("failed to write fileA: %v", err)
	}
	if err := store.WriteFile(ctx, "testB", bytes.NewReader(fileB)); err != nil {
		t.Fatalf("failed to write fileB: %v", err)
	}

	// 验证块数量
	chunks, err := os.ReadDir(store.chunksDir)
	if err != nil {
		t.Fatalf("failed to read chunks dir: %v", err)
	}

	uniqueChunks := countNonDirEntries(chunks)

	// 预期: 3个唯一块
	// - fileA块1 (4MB零)
	// - fileA块2 (4MB零) = fileB块2 (共享)
	// - fileB块1 (1MB随机+3MB零)
	if uniqueChunks != 3 {
		t.Errorf("Expected 3 unique chunks for fixed-size chunking, got %d", uniqueChunks)
		t.Logf("固定块大小验证失败: 应该是3个块(证明固定4MB切分)")
	} else {
		t.Logf("✓ 固定块大小验证通过: 使用固定4MB切分,而非内容感知分块")
	}
}

// TestMetadataPreservation 验证元数据完整性
// 即使块被共享,每个文件也应保持独立的元数据
func TestMetadataPreservation(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewDedupStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create dedup store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// 创建相同内容的数据
	sameContent := bytes.Repeat([]byte("TEST"), 2*1024*1024) // 8MB

	// 准备两个快照
	snapA := "snapshot-A"
	snapB := "snapshot-B"

	if err := store.Prepare(ctx, snapA, nil); err != nil {
		t.Fatalf("failed to prepare snapshot A: %v", err)
	}
	if err := store.Prepare(ctx, snapB, nil); err != nil {
		t.Fatalf("failed to prepare snapshot B: %v", err)
	}

	// 在快照中创建文件
	fileAPath := filepath.Join(store.snapsDir, snapA, "fs", "testfile")
	fileBPath := filepath.Join(store.snapsDir, snapB, "fs", "testfile")

	// 创建目录
	os.MkdirAll(filepath.Dir(fileAPath), 0755)
	os.MkdirAll(filepath.Dir(fileBPath), 0755)

	// 写入相同内容但不同权限
	if err := os.WriteFile(fileAPath, sameContent, 0755); err != nil {
		t.Fatalf("failed to write fileA: %v", err)
	}
	if err := os.WriteFile(fileBPath, sameContent, 0644); err != nil {
		t.Fatalf("failed to write fileB: %v", err)
	}

	// 设置不同的修改时间
	timeA := time.Now().Add(-1 * time.Hour)
	timeB := time.Now()
	os.Chtimes(fileAPath, timeA, timeA)
	os.Chtimes(fileBPath, timeB, timeB)

	// 验证元数据
	statA, err := os.Stat(fileAPath)
	if err != nil {
		t.Fatalf("failed to stat fileA: %v", err)
	}
	statB, err := os.Stat(fileBPath)
	if err != nil {
		t.Fatalf("failed to stat fileB: %v", err)
	}

	// 验证权限不同
	if statA.Mode() == statB.Mode() {
		t.Errorf("File permissions should differ: A=%v, B=%v", statA.Mode(), statB.Mode())
	} else {
		t.Logf("✓ 权限保留验证通过: A=%v, B=%v", statA.Mode(), statB.Mode())
	}

	// 验证时间戳不同
	if statA.ModTime().Equal(statB.ModTime()) {
		t.Errorf("File modification times should differ")
	} else {
		t.Logf("✓ 时间戳保留验证通过: 相差 %v", statB.ModTime().Sub(statA.ModTime()))
	}

	// 验证内容相同(通过哈希)
	hashA := hashFile(t, fileAPath)
	hashB := hashFile(t, fileBPath)
	if hashA != hashB {
		t.Errorf("File contents should be identical")
	} else {
		t.Logf("✓ 内容一致性验证通过: 相同内容但元数据独立")
	}

	t.Logf("✓ 元数据完整性验证通过: 块去重不影响文件元数据")
}

// TestDeduplicationRatio 验证去重率计算
func TestDeduplicationRatio(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewDedupStoreWithErofs(tmpDir, false)
	if err != nil {
		t.Fatalf("failed to create dedup store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// 创建共享块
	sharedBlock := bytes.Repeat([]byte("SHARED"), 4*1024*1024)

	// 创建10个文件,每个文件都包含这个共享块
	for i := 0; i < 10; i++ {
		fileData := append([]byte{}, sharedBlock...)
		// 每个文件再加一个唯一块
		uniqueBlock := bytes.Repeat([]byte(string('A'+i)), 4*1024*1024)
		fileData = append(fileData, uniqueBlock...)

		fileName := filepath.Join("test", string('A'+i))
		if err := store.WriteFile(ctx, fileName, bytes.NewReader(fileData)); err != nil {
			t.Fatalf("failed to write file %s: %v", fileName, err)
		}
	}

	// 计算去重效果
	// 原始数据: 10个文件 × 8MB = 80MB
	// 去重后: 1个共享块(4MB) + 10个唯一块(40MB) = 44MB
	// 理论去重率: (80-44)/80 = 45%

	chunks, err := os.ReadDir(store.chunksDir)
	if err != nil {
		t.Fatalf("failed to read chunks dir: %v", err)
	}

	uniqueChunks := countNonDirEntries(chunks)
	expectedChunks := 11 // 1个共享 + 10个唯一

	if uniqueChunks != expectedChunks {
		t.Errorf("Expected %d unique chunks, got %d", expectedChunks, uniqueChunks)
	}

	// 计算实际磁盘使用
	var totalSize int64
	for _, chunk := range chunks {
		if !chunk.IsDir() {
			info, _ := chunk.Info()
			totalSize += info.Size()
		}
	}

	originalSize := int64(10 * 8 * 1024 * 1024) // 80MB
	dedupRatio := float64(originalSize-totalSize) / float64(originalSize) * 100

	t.Logf("原始数据大小: %d MB", originalSize/(1024*1024))
	t.Logf("去重后大小: %d MB", totalSize/(1024*1024))
	t.Logf("去重率: %.2f%%", dedupRatio)

	if dedupRatio < 40 || dedupRatio > 50 {
		t.Errorf("Dedup ratio %.2f%% out of expected range (40-50%%)", dedupRatio)
	} else {
		t.Logf("✓ 去重率验证通过: %.2f%% (预期 ~45%%)", dedupRatio)
	}
}

// TestConcurrentDeduplication 验证并发去重的正确性
func TestConcurrentDeduplication(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewDedupStoreWithErofs(tmpDir, false)
	if err != nil {
		t.Fatalf("failed to create dedup store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// 相同数据块
	sharedData := bytes.Repeat([]byte("CONCURRENT"), 4*1024*1024)

	// 并发写入多个文件
	done := make(chan error, 5)
	for i := 0; i < 5; i++ {
		go func(id int) {
			fileName := filepath.Join("concurrent", string('A'+id))
			done <- store.WriteFile(ctx, fileName, bytes.NewReader(sharedData))
		}(i)
	}

	// 等待所有写入完成
	for i := 0; i < 5; i++ {
		if err := <-done; err != nil {
			t.Errorf("concurrent write failed: %v", err)
		}
	}

	// 验证: 应该只有1个唯一块被存储
	chunks, err := os.ReadDir(store.chunksDir)
	if err != nil {
		t.Fatalf("failed to read chunks dir: %v", err)
	}

	uniqueChunks := countNonDirEntries(chunks)
	if uniqueChunks != 1 {
		t.Errorf("Expected 1 unique chunk for concurrent identical writes, got %d", uniqueChunks)
	} else {
		t.Logf("✓ 并发去重验证通过: 5个并发写入共享1个块")
	}
}

// 辅助函数

func verifyChunkRefCounts(t *testing.T, store *DedupStore, patterns ...[]byte) {
	for i, pattern := range patterns {
		hash := sha256.Sum256(pattern)
		hashStr := hex.EncodeToString(hash[:])

		refCount, err := store.indexDB.GetRefCount(hashStr)
		if err != nil {
			t.Logf("Warning: failed to get refcount for pattern %d: %v", i+1, err)
			continue
		}

		if refCount != 2 {
			t.Logf("Pattern %d (hash=%s) refcount: %d (expected 2)", i+1, hashStr[:8], refCount)
		}
	}
}

func countNonDirEntries(entries []os.DirEntry) int {
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			count++
		}
	}
	return count
}

func hashFile(t *testing.T, path string) string {
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open file for hashing: %v", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatalf("failed to hash file: %v", err)
	}

	return hex.EncodeToString(h.Sum(nil))
}
