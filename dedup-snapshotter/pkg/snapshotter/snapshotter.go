package snapshotter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/log"
	"github.com/opencloudos/dedup-snapshotter/pkg/audit"
	dedupStorage "github.com/opencloudos/dedup-snapshotter/pkg/storage"
)

type Snapshotter struct {
	ms             *storage.MetaStore
	storage        *dedupStorage.DedupStore
	root           string
	activeMounts   map[string]bool
	activeMountsMu sync.RWMutex
	auditLogger    *audit.AuditLogger
}

func NewSnapshotter(root string) (snapshots.Snapshotter, error) {
	return NewSnapshotterWithAudit(root, nil)
}

func NewSnapshotterWithAudit(root string, auditLogger *audit.AuditLogger) (snapshots.Snapshotter, error) {
	ms, err := storage.NewMetaStore(root)
	if err != nil {
		return nil, err
	}

	dedupStore, err := dedupStorage.NewDedupStore(root)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	if err := dedupStore.RecoverSnapshots(ctx); err != nil {
		log.L.WithError(err).Warn("snapshot recovery failed")
	}

	if err := dedupStore.VerifyChunks(ctx); err != nil {
		log.L.WithError(err).Warn("chunk verification failed")
	}

	return &Snapshotter{
		ms:           ms,
		storage:      dedupStore,
		root:         root,
		activeMounts: make(map[string]bool),
		auditLogger:  auditLogger,
	}, nil
}

func (s *Snapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	ctx, t, err := s.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Info{}, err
	}
	defer t.Rollback()

	_, info, _, err := storage.GetInfo(ctx, key)
	return info, err
}

func (s *Snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	ctx, t, err := s.ms.TransactionContext(ctx, true)
	if err != nil {
		return snapshots.Info{}, err
	}

	info, err = storage.UpdateInfo(ctx, info, fieldpaths...)
	if err != nil {
		t.Rollback()
		return snapshots.Info{}, err
	}

	if err := t.Commit(); err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

func (s *Snapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	ctx, t, err := s.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Usage{}, err
	}
	defer t.Rollback()

	id, info, usage, err := storage.GetInfo(ctx, key)
	if err != nil {
		return snapshots.Usage{}, err
	}

	if info.Kind == snapshots.KindActive {
		du, err := s.storage.DiskUsage(ctx, id)
		if err != nil {
			return snapshots.Usage{}, err
		}
		usage = snapshots.Usage(du)
	}

	return usage, nil
}

func (s *Snapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	s.activeMountsMu.Lock()
	s.activeMounts[key] = true
	s.activeMountsMu.Unlock()

	ctx, t, err := s.ms.TransactionContext(ctx, false)
	if err != nil {
		return nil, err
	}
	defer t.Rollback()

	snap, err := storage.GetSnapshot(ctx, key)
	if err != nil {
		return nil, err
	}

	return s.mounts(snap)
}

func (s *Snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) (mounts []mount.Mount, err error) {
	if s.auditLogger != nil {
		ctx = audit.StartAudit(ctx, "prepare_snapshot", key, "containerd", os.Getpid(), map[string]interface{}{
			"parent": parent,
			"key":    key,
		})
		defer func() {
			result := "success"
			if err != nil {
				result = "failure"
			}
			audit.FinishAudit(ctx, s.auditLogger, result, err)
		}()
	}
	return s.createSnapshot(ctx, snapshots.KindActive, key, parent, opts...)
}

func (s *Snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return s.createSnapshot(ctx, snapshots.KindView, key, parent, opts...)
}

func (s *Snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) (err error) {
	if s.auditLogger != nil {
		ctx = audit.StartAudit(ctx, "commit_snapshot", key, "containerd", os.Getpid(), map[string]interface{}{
			"name": name,
			"key":  key,
		})
		defer func() {
			result := "success"
			if err != nil {
				result = "failure"
			}
			audit.FinishAudit(ctx, s.auditLogger, result, err)
		}()
	}

	ctx, t, err := s.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			t.Rollback()
		}
	}()

	id, _, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return err
	}

	usage, err := s.storage.DiskUsage(ctx, id)
	if err != nil {
		return err
	}

	if _, err := storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
		return err
	}

	return t.Commit()
}

func (s *Snapshotter) Remove(ctx context.Context, key string) (err error) {
	if s.auditLogger != nil {
		ctx = audit.StartAudit(ctx, "remove_snapshot", key, "containerd", os.Getpid(), map[string]interface{}{
			"key": key,
		})
		defer func() {
			result := "success"
			if err != nil {
				result = "failure"
			}
			audit.FinishAudit(ctx, s.auditLogger, result, err)
		}()
	}

	s.activeMountsMu.Lock()
	if s.activeMounts[key] {
		s.activeMountsMu.Unlock()
		log.L.Infof("snapshot %s is actively mounted, deferring removal", key)
		return nil
	}
	delete(s.activeMounts, key)
	s.activeMountsMu.Unlock()

	ctx, t, err := s.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			t.Rollback()
		}
	}()

	id, _, err := storage.Remove(ctx, key)
	if err != nil {
		return err
	}

	if err := s.storage.Remove(ctx, id); err != nil {
		return err
	}

	return t.Commit()
}

func (s *Snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	ctx, t, err := s.ms.TransactionContext(ctx, false)
	if err != nil {
		return err
	}
	defer t.Rollback()

	return storage.WalkInfo(ctx, fn, fs...)
}

func (s *Snapshotter) Close() error {
	return s.ms.Close()
}

func (s *Snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	ctx, t, err := s.ms.TransactionContext(ctx, true)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			t.Rollback()
		}
	}()

	snap, err := storage.CreateSnapshot(ctx, kind, key, parent, opts...)
	if err != nil {
		return nil, err
	}

	// 准备快照存储
	if err := s.storage.Prepare(ctx, snap.ID, snap.ParentIDs); err != nil {
		return nil, err
	}

	// 检查并自动转换层(如果需要)
	// 当 containerd 拉取镜像时,会为每一层调用 Prepare
	// 我们在这里检测是否是新层,如果是则自动转换为 EROFS
	if err := s.autoConvertLayer(ctx, snap.ID, snap.ParentIDs); err != nil {
		log.L.WithError(err).Warnf("auto-convert layer %s failed, will use fallback", snap.ID)
	}

	if err := t.Commit(); err != nil {
		return nil, err
	}

	return s.mounts(snap)
}

// autoConvertLayer 自动检测并转换新层为 EROFS 格式
func (s *Snapshotter) autoConvertLayer(ctx context.Context, snapID string, parentIDs []string) error {
	// 检查是否已经有 EROFS 镜像
	if s.storage.HasErofsImage(snapID) {
		log.L.Debugf("layer %s already has erofs image, skip conversion", snapID)
		return nil
	}

	// 检查快照目录是否有内容(说明是新拉取的层)
	snapPath := s.storage.GetSnapshotPath(snapID)
	fsPath := filepath.Join(snapPath, "fs")

	isEmpty, err := isDirEmpty(fsPath)
	if err != nil || isEmpty {
		// 目录不存在或为空,跳过转换
		return nil
	}

	// 有内容,说明是新层,自动转换为 EROFS
	log.L.Infof("detected new layer %s, auto-converting to EROFS", snapID)

	if err := s.storage.BuildErofsImage(ctx, fsPath, snapID); err != nil {
		return fmt.Errorf("failed to build erofs for layer %s: %w", snapID, err)
	}

	// 注册到 fscache
	if err := s.registerLayerToFscache(ctx, snapID, fsPath); err != nil {
		log.L.WithError(err).Warnf("failed to register layer %s to fscache", snapID)
	}

	log.L.Infof("successfully auto-converted layer %s to EROFS", snapID)
	return nil
}

// registerLayerToFscache 注册层到 fscache
func (s *Snapshotter) registerLayerToFscache(ctx context.Context, layerID string, sourceDir string) error {
	manifestPath := filepath.Join(s.root, "manifests", layerID+".manifest")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0755); err != nil {
		return err
	}

	// 生成简单的文件清单
	file, err := os.Create(manifestPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 遍历生成清单
	filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			relPath, _ := filepath.Rel(sourceDir, path)
			fmt.Fprintf(file, "%s\t%d\n", relPath, info.Size())
		}
		return nil
	})

	return s.storage.RegisterImageForFscache(ctx, layerID, manifestPath)
}

// isDirEmpty 检查目录是否为空
func isDirEmpty(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	return len(entries) == 0, nil
}

func (s *Snapshotter) mounts(snap storage.Snapshot) ([]mount.Mount, error) {
	mounts, err := s.storage.Mounts(snap.ID, snap.ParentIDs)
	if err != nil {
		return nil, err
	}

	log.L.Debugf("mounts for snapshot %s: %+v", snap.ID, mounts)
	return mounts, nil
}
