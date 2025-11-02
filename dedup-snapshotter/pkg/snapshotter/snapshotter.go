package snapshotter

import (
	"context"
	"os"
	"sync"
	"time"

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

func (s *Snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	startTime := time.Now()
	if s.auditLogger != nil {
		ctx = audit.StartAudit(ctx, "prepare_snapshot", key, "containerd", os.Getpid(), map[string]interface{}{
			"parent": parent,
			"key":    key,
		})
		defer func() {
			duration := time.Since(startTime)
			result := "success"
			var err error
			audit.FinishAudit(ctx, s.auditLogger, result, err)
		}()
	}
	return s.createSnapshot(ctx, snapshots.KindActive, key, parent, opts...)
}

func (s *Snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return s.createSnapshot(ctx, snapshots.KindView, key, parent, opts...)
}

func (s *Snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	if s.auditLogger != nil {
		ctx = audit.StartAudit(ctx, "commit_snapshot", key, "containerd", os.Getpid(), map[string]interface{}{
			"name": name,
			"key":  key,
		})
		defer func(err *error) {
			result := "success"
			if *err != nil {
				result = "failure"
			}
			audit.FinishAudit(ctx, s.auditLogger, result, *err)
		}(&err)
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

func (s *Snapshotter) Remove(ctx context.Context, key string) error {
	if s.auditLogger != nil {
		ctx = audit.StartAudit(ctx, "remove_snapshot", key, "containerd", os.Getpid(), map[string]interface{}{
			"key": key,
		})
		defer func(err *error) {
			result := "success"
			if *err != nil {
				result = "failure"
			}
			audit.FinishAudit(ctx, s.auditLogger, result, *err)
		}(&err)
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

	if err := s.storage.Prepare(ctx, snap.ID, snap.ParentIDs); err != nil {
		return nil, err
	}

	if err := t.Commit(); err != nil {
		return nil, err
	}

	return s.mounts(snap)
}

func (s *Snapshotter) mounts(snap storage.Snapshot) ([]mount.Mount, error) {
	mounts, err := s.storage.Mounts(snap.ID, snap.ParentIDs)
	if err != nil {
		return nil, err
	}

	log.L.Debugf("mounts for snapshot %s: %+v", snap.ID, mounts)
	return mounts, nil
}
