package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/containerd/containerd/archive"
	"github.com/containerd/containerd/archive/compression"
	"github.com/containerd/log"
)

// LayerProcessor 处理 OCI 镜像层
type LayerProcessor struct {
	store *DedupStore
}

// NewLayerProcessor 创建层处理器
func NewLayerProcessor(store *DedupStore) *LayerProcessor {
	return &LayerProcessor{
		store: store,
	}
}

// ProcessLayer 处理一个镜像层:解压 → 去重 → 转 EROFS → 注册 fscache
func (lp *LayerProcessor) ProcessLayer(ctx context.Context, layerID string, layerData io.Reader, parent string) error {
	log.L.Infof("processing layer %s (parent: %s)", layerID, parent)

	// 1. 计算层的哈希作为唯一标识
	digest, tempFile, err := lp.saveLayerToTemp(layerID, layerData)
	if err != nil {
		return fmt.Errorf("failed to save layer: %w", err)
	}
	defer os.Remove(tempFile)

	// 2. 检查是否已处理过此层(根据内容哈希)
	if lp.isLayerProcessed(digest) {
		log.L.Infof("layer %s already processed (digest: %s)", layerID, digest[:12])
		return nil
	}

	// 3. 解压层到临时目录
	extractDir := filepath.Join(lp.store.root, "extract", layerID)
	if err := os.MkdirAll(extractDir, 0755); err != nil {
		return err
	}
	defer os.RemoveAll(extractDir)

	file, err := os.Open(tempFile)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := extractLayer(file, extractDir); err != nil {
		return fmt.Errorf("failed to extract layer: %w", err)
	}

	// 4. 如果有父层,合并文件系统
	if parent != "" {
		if err := lp.mergeWithParent(ctx, extractDir, parent); err != nil {
			log.L.WithError(err).Warnf("failed to merge with parent %s", parent)
		}
	}

	// 5. 转换为 EROFS 格式
	if err := lp.store.BuildErofsImage(ctx, extractDir, layerID); err != nil {
		return fmt.Errorf("failed to build erofs: %w", err)
	}

	// 6. 生成并保存层元数据
	metadata := &LayerMetadata{
		LayerID:      layerID,
		Digest:       digest,
		Parent:       parent,
		ErofsImage:   filepath.Join(lp.store.imagesDir, layerID+".erofs"),
		Size:         getDirSize(extractDir),
		FileCount:    countFiles(extractDir),
	}

	if err := lp.saveLayerMetadata(layerID, metadata); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	// 7. 注册到 fscache (如果启用)
	if lp.store.useFscache && lp.store.dedupDaemon != nil {
		manifestPath := lp.generateManifestPath(layerID)
		if err := lp.generateLayerManifest(extractDir, manifestPath); err != nil {
			log.L.WithError(err).Warnf("failed to generate manifest for %s", layerID)
		} else {
			if err := lp.store.RegisterImageForFscache(ctx, layerID, manifestPath); err != nil {
				log.L.WithError(err).Warnf("failed to register layer %s to fscache", layerID)
			}
		}
	}

	log.L.Infof("successfully processed layer %s", layerID)
	return nil
}

// saveLayerToTemp 保存层数据到临时文件并计算哈希
func (lp *LayerProcessor) saveLayerToTemp(layerID string, data io.Reader) (string, string, error) {
	tempFile := filepath.Join(lp.store.root, "temp", layerID+".tar.gz")
	if err := os.MkdirAll(filepath.Dir(tempFile), 0755); err != nil {
		return "", "", err
	}

	file, err := os.Create(tempFile)
	if err != nil {
		return "", "", err
	}
	defer file.Close()

	hasher := sha256.New()
	writer := io.MultiWriter(file, hasher)

	if _, err := io.Copy(writer, data); err != nil {
		return "", "", err
	}

	digest := hex.EncodeToString(hasher.Sum(nil))
	return digest, tempFile, nil
}

// isLayerProcessed 检查层是否已处理
func (lp *LayerProcessor) isLayerProcessed(digest string) bool {
	digestPath := filepath.Join(lp.store.root, "digests", digest[:2], digest)
	_, err := os.Stat(digestPath)
	return err == nil
}

// mergeWithParent 合并父层的文件系统
func (lp *LayerProcessor) mergeWithParent(ctx context.Context, currentDir, parentID string) error {
	// 简化版本:实际应该挂载父层的 EROFS 并复制文件
	log.L.Debugf("merging layer with parent %s", parentID)
	return nil
}

// generateLayerManifest 生成层的元数据清单用于 fscache
func (lp *LayerProcessor) generateLayerManifest(sourceDir, manifestPath string) error {
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0755); err != nil {
		return err
	}

	file, err := os.Create(manifestPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 遍历目录生成文件清单
	return filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Mode().IsRegular() {
			relPath, _ := filepath.Rel(sourceDir, path)
			// 格式: 相对路径 \t 文件大小 \t SHA256
			hash, _ := hashFile(path)
			fmt.Fprintf(file, "%s\t%d\t%s\n", relPath, info.Size(), hash)
		}
		return nil
	})
}

// generateManifestPath 生成清单文件路径
func (lp *LayerProcessor) generateManifestPath(layerID string) string {
	return filepath.Join(lp.store.root, "manifests", layerID+".manifest")
}

// saveLayerMetadata 保存层元数据
func (lp *LayerProcessor) saveLayerMetadata(layerID string, metadata *LayerMetadata) error {
	metadataPath := filepath.Join(lp.store.root, "metadata", layerID+".json")
	if err := os.MkdirAll(filepath.Dir(metadataPath), 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(metadataPath, data, 0644)
}

// LayerMetadata 层元数据
type LayerMetadata struct {
	LayerID    string `json:"layer_id"`
	Digest     string `json:"digest"`
	Parent     string `json:"parent"`
	ErofsImage string `json:"erofs_image"`
	Size       int64  `json:"size"`
	FileCount  int    `json:"file_count"`
}

// extractLayer 解压层归档(支持 tar, tar.gz, tar.zst 等)
// 使用 containerd 的 archive 包,完整支持 OCI 层特性:
// - 自动检测和解压缩 (gzip, zstd, etc.)
// - whiteout 文件处理 (删除标记)
// - 扩展属性和权限保留
func extractLayer(reader io.Reader, targetDir string) error {
	ctx := context.Background()
	log.L.Debugf("extracting layer to %s using containerd archive", targetDir)

	// 使用 containerd 的 compression.DecompressStream 自动检测压缩格式
	decompressed, err := compression.DecompressStream(reader)
	if err != nil {
		return fmt.Errorf("failed to decompress layer: %w", err)
	}
	defer decompressed.Close()

	// 使用 archive.Apply 应用层,支持所有 OCI 特性
	// 包括: whiteout 文件、特殊权限、扩展属性等
	if _, err := archive.Apply(ctx, targetDir, decompressed); err != nil {
		return fmt.Errorf("failed to apply layer archive: %w", err)
	}

	log.L.Debugf("successfully extracted layer to %s", targetDir)
	return nil
}

// getDirSize 获取目录大小
func getDirSize(path string) int64 {
	var size int64
	filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}

// countFiles 统计文件数量
func countFiles(path string) int {
	count := 0
	filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			count++
		}
		return nil
	})
	return count
}

// hashFile 计算文件哈希
func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}
