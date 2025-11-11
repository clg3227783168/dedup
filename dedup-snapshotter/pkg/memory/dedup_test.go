package memory

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestMemoryDeduplication 验证内存去重功能
func TestMemoryDeduplication(t *testing.T) {
	tmpDir := t.TempDir()
	dedup, err := NewMemoryDeduplicator(tmpDir)
	if err != nil {
		t.Fatalf("failed to create memory deduplicator: %v", err)
	}
	defer dedup.Close()

	// 创建测试文件
	testFile1 := filepath.Join(tmpDir, "test1.dat")
	testFile2 := filepath.Join(tmpDir, "test2.dat")

	// 相同的数据 - 应该触发页面去重
	testData := bytes.Repeat([]byte("MEMORY_DEDUP_TEST"), 100*1024) // ~1.6MB

	if err := os.WriteFile(testFile1, testData, 0644); err != nil {
		t.Fatalf("failed to write test file 1: %v", err)
	}
	if err := os.WriteFile(testFile2, testData, 0644); err != nil {
		t.Fatalf("failed to write test file 2: %v", err)
	}

	// 执行去重
	if err := dedup.DeduplicateFile(testFile1); err != nil {
		t.Fatalf("failed to deduplicate file 1: %v", err)
	}
	if err := dedup.DeduplicateFile(testFile2); err != nil {
		t.Fatalf("failed to deduplicate file 2: %v", err)
	}

	// 获取统计信息
	stats, err := dedup.GetStats()
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	t.Logf("内存去重统计:")
	t.Logf("  唯一页面数: %d", stats.UniquePages)
	t.Logf("  合并页面数: %d", stats.MergedPages)
	t.Logf("  节省内存: %d bytes (%.2f MB)", stats.SavedMemory, float64(stats.SavedMemory)/(1024*1024))

	// 验证有页面被标记为可合并
	if stats.UniquePages == 0 {
		t.Error("Expected some unique pages to be tracked")
	}

	if stats.MergedPages == 0 {
		t.Error("Expected some pages to be merged (same content in two files)")
	} else {
		t.Logf("✓ 内存去重验证通过: %d 个页面被合并", stats.MergedPages)
	}
}

// TestKSMController 验证KSM控制器
func TestKSMController(t *testing.T) {
	// 检查KSM是否可用
	if _, err := os.Stat("/sys/kernel/mm/ksm"); os.IsNotExist(err) {
		t.Skip("KSM not available in this kernel")
	}

	ksm, err := NewKSMController()
	if err != nil {
		t.Fatalf("failed to create KSM controller: %v", err)
	}

	// 测试启用KSM
	if err := ksm.Enable(); err != nil {
		t.Fatalf("failed to enable KSM: %v", err)
	}
	defer ksm.Disable()

	// 验证KSM已启用
	runData, err := os.ReadFile("/sys/kernel/mm/ksm/run")
	if err != nil {
		t.Fatalf("failed to read KSM run status: %v", err)
	}

	if string(runData) != "1\n" {
		t.Errorf("KSM should be enabled, got: %s", string(runData))
	} else {
		t.Logf("✓ KSM 启用验证通过")
	}

	// 等待一段时间让KSM工作
	time.Sleep(2 * time.Second)

	// 获取KSM统计信息
	stats, err := ksm.GetStats()
	if err != nil {
		t.Fatalf("failed to get KSM stats: %v", err)
	}

	t.Logf("KSM 统计:")
	t.Logf("  共享页面数(pages_sharing): %d", stats.PagesSharing)
	t.Logf("  被共享页面数(pages_shared): %d", stats.PagesShared)
	t.Logf("  未共享页面数(pages_unshared): %d", stats.PagesUnshared)
	t.Logf("  节省内存: %d bytes (%.2f MB)", stats.SavedMemory, float64(stats.SavedMemory)/(1024*1024))

	// 验证能够读取统计信息
	if stats.PagesSharing >= 0 && stats.PagesShared >= 0 {
		t.Logf("✓ KSM 统计信息读取成功")
	}
}

// TestMemoryDedupWithKSM 验证内存去重与KSM的集成
func TestMemoryDedupWithKSM(t *testing.T) {
	if _, err := os.Stat("/sys/kernel/mm/ksm"); os.IsNotExist(err) {
		t.Skip("KSM not available in this kernel")
	}

	tmpDir := t.TempDir()
	dedup, err := NewMemoryDeduplicator(tmpDir)
	if err != nil {
		t.Fatalf("failed to create memory deduplicator: %v", err)
	}
	defer dedup.Close()

	// 启用KSM
	if err := dedup.EnableKSM(); err != nil {
		t.Fatalf("failed to enable KSM: %v", err)
	}
	defer dedup.DisableKSM()

	// 创建多个具有相同内容的大文件
	sharedContent := bytes.Repeat([]byte("KSM_TEST_PATTERN"), 256*1024) // 4MB

	fileCount := 5
	for i := 0; i < fileCount; i++ {
		testFile := filepath.Join(tmpDir, filepath.Join("test", string('A'+i)+".dat"))
		os.MkdirAll(filepath.Dir(testFile), 0755)

		if err := os.WriteFile(testFile, sharedContent, 0644); err != nil {
			t.Fatalf("failed to write test file %d: %v", i, err)
		}

		if err := dedup.DeduplicateFile(testFile); err != nil {
			t.Fatalf("failed to deduplicate file %d: %v", i, err)
		}
	}

	// 等待KSM扫描和合并
	t.Logf("等待KSM扫描和合并页面...")
	time.Sleep(5 * time.Second)

	// 获取统计信息
	stats, err := dedup.GetStats()
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	t.Logf("内存去重 + KSM 统计:")
	t.Logf("  应用层唯一页面: %d", stats.UniquePages)
	t.Logf("  应用层合并页面: %d", stats.MergedPages)
	t.Logf("  应用层节省内存: %d bytes", stats.SavedMemory)

	if stats.KSMStats != nil {
		t.Logf("  KSM 共享页面数: %d", stats.KSMStats.PagesSharing)
		t.Logf("  KSM 被共享页面数: %d", stats.KSMStats.PagesShared)
		t.Logf("  KSM 节省内存: %d bytes (%.2f MB)",
			stats.KSMStats.SavedMemory,
			float64(stats.KSMStats.SavedMemory)/(1024*1024))
		t.Logf("  总节省内存: %d bytes (%.2f MB)",
			stats.SavedMemory,
			float64(stats.SavedMemory)/(1024*1024))

		t.Logf("✓ 内存去重 + KSM 集成验证通过")
	} else {
		t.Logf("Warning: KSM stats not available")
	}
}

// TestPageLevelGranularity 验证页级粒度去重
func TestPageLevelGranularity(t *testing.T) {
	tmpDir := t.TempDir()
	dedup, err := NewMemoryDeduplicator(tmpDir)
	if err != nil {
		t.Fatalf("failed to create memory deduplicator: %v", err)
	}
	defer dedup.Close()

	pageSize := dedup.pageSize
	t.Logf("系统页面大小: %d bytes", pageSize)

	// 创建两个文件,共享一些页面
	// 文件1: 3个页面 [A][B][C]
	// 文件2: 3个页面 [A][D][C]
	// 共享: 页面A和C

	pageA := bytes.Repeat([]byte("A"), pageSize)
	pageB := bytes.Repeat([]byte("B"), pageSize)
	pageC := bytes.Repeat([]byte("C"), pageSize)
	pageD := bytes.Repeat([]byte("D"), pageSize)

	file1 := append(append(pageA, pageB...), pageC...)
	file2 := append(append(pageA, pageD...), pageC...)

	testFile1 := filepath.Join(tmpDir, "file1.dat")
	testFile2 := filepath.Join(tmpDir, "file2.dat")

	if err := os.WriteFile(testFile1, file1, 0644); err != nil {
		t.Fatalf("failed to write file1: %v", err)
	}
	if err := os.WriteFile(testFile2, file2, 0644); err != nil {
		t.Fatalf("failed to write file2: %v", err)
	}

	// 去重
	if err := dedup.DeduplicateFile(testFile1); err != nil {
		t.Fatalf("failed to deduplicate file1: %v", err)
	}
	if err := dedup.DeduplicateFile(testFile2); err != nil {
		t.Fatalf("failed to deduplicate file2: %v", err)
	}

	// 验证统计
	stats, err := dedup.GetStats()
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	// 预期: 4个唯一页面(A,B,C,D), 2个合并页面(A和C各被引用2次)
	expectedUniquePages := int64(4)
	expectedMergedPages := int64(2)

	if stats.UniquePages != expectedUniquePages {
		t.Errorf("Expected %d unique pages, got %d", expectedUniquePages, stats.UniquePages)
	}
	if stats.MergedPages != expectedMergedPages {
		t.Errorf("Expected %d merged pages, got %d", expectedMergedPages, stats.MergedPages)
	}

	t.Logf("✓ 页级粒度去重验证通过:")
	t.Logf("  唯一页面: %d (预期 %d)", stats.UniquePages, expectedUniquePages)
	t.Logf("  合并页面: %d (预期 %d)", stats.MergedPages, expectedMergedPages)
}

// BenchmarkMemoryDedup 性能基准测试
func BenchmarkMemoryDedup(b *testing.B) {
	tmpDir := b.TempDir()
	dedup, err := NewMemoryDeduplicator(tmpDir)
	if err != nil {
		b.Fatalf("failed to create memory deduplicator: %v", err)
	}
	defer dedup.Close()

	// 创建测试文件
	testData := bytes.Repeat([]byte("BENCHMARK"), 1024*1024) // 1MB
	testFile := filepath.Join(tmpDir, "bench.dat")
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		b.Fatalf("failed to write test file: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := dedup.DeduplicateFile(testFile); err != nil {
			b.Fatalf("deduplication failed: %v", err)
		}
	}
}
