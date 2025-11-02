package erofs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/containerd/log"
)

const (
	BlockSize     = 4096
	ChunkSize     = 4 * 1024 * 1024
	ErofsImageExt = ".erofs"
)

type Builder struct {
	root      string
	chunksDir string
	indexer   *ChunkIndexer
}

type ChunkInfo struct {
	Hash   string
	Offset int64
	Size   int64
}

type FileMetadata struct {
	Path   string
	Mode   os.FileMode
	Size   int64
	Chunks []ChunkInfo
}

func NewBuilder(root string) (*Builder, error) {
	chunksDir := filepath.Join(root, "erofs-chunks")
	if err := os.MkdirAll(chunksDir, 0700); err != nil {
		return nil, err
	}

	indexer, err := NewChunkIndexer(filepath.Join(root, "chunk-index.db"))
	if err != nil {
		return nil, err
	}

	return &Builder{
		root:      root,
		chunksDir: chunksDir,
		indexer:   indexer,
	}, nil
}

func (b *Builder) BuildImage(ctx context.Context, sourceDir, imageID string) (string, error) {
	imagePath := filepath.Join(b.root, "images", imageID+ErofsImageExt)
	if err := os.MkdirAll(filepath.Dir(imagePath), 0755); err != nil {
		return "", err
	}

	stagingDir := filepath.Join(b.root, "staging", imageID)
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		return "", err
	}
	defer os.RemoveAll(stagingDir)

	if err := b.processDirectory(ctx, sourceDir, stagingDir, imageID); err != nil {
		return "", err
	}

	if err := b.buildErofsImage(stagingDir, imagePath); err != nil {
		return "", err
	}

	log.G(ctx).Infof("built erofs image: %s", imagePath)
	return imagePath, nil
}

func (b *Builder) processDirectory(ctx context.Context, sourceDir, targetDir, imageID string) error {
	return filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}

		targetPath := filepath.Join(targetDir, relPath)

		if info.IsDir() {
			return os.MkdirAll(targetPath, info.Mode())
		}

		if info.Mode().IsRegular() {
			return b.processFile(ctx, path, targetPath, imageID, info)
		}

		if info.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(link, targetPath)
		}

		return nil
	})
}

func (b *Builder) processFile(ctx context.Context, sourcePath, targetPath, imageID string, info os.FileInfo) error {
	if info.Size() < ChunkSize {
		return b.copySmallFile(sourcePath, targetPath)
	}

	return b.deduplicateFile(ctx, sourcePath, targetPath, imageID, info)
}

func (b *Builder) copySmallFile(source, target string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()

	output, err := os.Create(target)
	if err != nil {
		return err
	}
	defer output.Close()

	_, err = io.Copy(output, input)
	return err
}

func (b *Builder) deduplicateFile(ctx context.Context, sourcePath, targetPath, imageID string, info os.FileInfo) error {
	file, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer file.Close()

	chunks, err := b.chunkFile(file)
	if err != nil {
		return err
	}

	_ = &FileMetadata{
		Path:   targetPath,
		Mode:   info.Mode(),
		Size:   info.Size(),
		Chunks: chunks,
	}

	for _, chunk := range chunks {
		if err := b.indexer.RecordChunk(imageID, chunk.Hash, chunk.Size); err != nil {
			return err
		}
	}

	return b.reconstructFile(targetPath, chunks)
}

func (b *Builder) chunkFile(file *os.File) ([]ChunkInfo, error) {
	var chunks []ChunkInfo
	buffer := make([]byte, ChunkSize)
	offset := int64(0)

	for {
		n, err := io.ReadFull(file, buffer)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, err
		}
		if n == 0 {
			break
		}

		hash := sha256.Sum256(buffer[:n])
		hashStr := hex.EncodeToString(hash[:])

		chunkPath := filepath.Join(b.chunksDir, hashStr)
		if _, statErr := os.Stat(chunkPath); os.IsNotExist(statErr) {
			if err := os.WriteFile(chunkPath, buffer[:n], 0644); err != nil {
				return nil, err
			}
		}

		chunks = append(chunks, ChunkInfo{
			Hash:   hashStr,
			Offset: offset,
			Size:   int64(n),
		})

		offset += int64(n)

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}

	return chunks, nil
}

func (b *Builder) reconstructFile(targetPath string, chunks []ChunkInfo) error {
	output, err := os.Create(targetPath)
	if err != nil {
		return err
	}
	defer output.Close()

	for _, chunk := range chunks {
		chunkPath := filepath.Join(b.chunksDir, chunk.Hash)
		data, err := os.ReadFile(chunkPath)
		if err != nil {
			return err
		}
		if _, err := output.Write(data); err != nil {
			return err
		}
	}

	return nil
}

func (b *Builder) buildErofsImage(sourceDir, imagePath string) error {
	cmd := exec.Command("mkfs.erofs",
		"-zlz4hc",
		"-T", "0",
		"--all-root",
		imagePath,
		sourceDir,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mkfs.erofs failed: %w, output: %s", err, string(output))
	}

	return nil
}

func (b *Builder) GetChunkStats(imageID string) (*ChunkStats, error) {
	return b.indexer.GetImageStats(imageID)
}

func (b *Builder) Close() error {
	return b.indexer.Close()
}
