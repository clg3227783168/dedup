package erofs

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/containerd/containerd/mount"
	"github.com/containerd/log"
)

type MountManager struct {
	root        string
	mountsDir   string
	mountsMu    sync.RWMutex
	activeMounts map[string]*MountPoint
}

type MountPoint struct {
	ID         string
	ImagePath  string
	MountPath  string
	LoopDevice string
	RefCount   int
}

func NewMountManager(root string) (*MountManager, error) {
	mountsDir := filepath.Join(root, "mounts")
	if err := os.MkdirAll(mountsDir, 0755); err != nil {
		return nil, err
	}

	return &MountManager{
		root:         root,
		mountsDir:    mountsDir,
		activeMounts: make(map[string]*MountPoint),
	}, nil
}

func (m *MountManager) MountErofs(imageID, imagePath string) (string, error) {
	m.mountsMu.Lock()
	defer m.mountsMu.Unlock()

	if mp, ok := m.activeMounts[imageID]; ok {
		mp.RefCount++
		log.L.Debugf("reusing existing mount for %s, refcount=%d", imageID, mp.RefCount)
		return mp.MountPath, nil
	}

	mountPath := filepath.Join(m.mountsDir, imageID)
	if err := os.MkdirAll(mountPath, 0755); err != nil {
		return "", err
	}

	loopDev, err := m.setupLoopDevice(imagePath)
	if err != nil {
		return "", fmt.Errorf("failed to setup loop device: %w", err)
	}

	if err := m.mountErofsImage(loopDev, mountPath); err != nil {
		m.detachLoopDevice(loopDev)
		return "", err
	}

	m.activeMounts[imageID] = &MountPoint{
		ID:         imageID,
		ImagePath:  imagePath,
		MountPath:  mountPath,
		LoopDevice: loopDev,
		RefCount:   1,
	}

	log.L.Infof("mounted erofs image %s at %s (loop=%s)", imageID, mountPath, loopDev)
	return mountPath, nil
}

func (m *MountManager) setupLoopDevice(imagePath string) (string, error) {
	cmd := exec.Command("losetup", "-f", "--show", imagePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("losetup failed: %w, output: %s", err, string(output))
	}

	loopDev := strings.TrimSpace(string(output))
	return loopDev, nil
}

func (m *MountManager) mountErofsImage(loopDev, mountPath string) error {
	cmd := exec.Command("mount", "-t", "erofs", "-o", "ro", loopDev, mountPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount failed: %w, output: %s", err, string(output))
	}

	return nil
}

func (m *MountManager) MountErofsWithFscache(imageID, fsid, domain string) (string, error) {
	m.mountsMu.Lock()
	defer m.mountsMu.Unlock()

	if mp, ok := m.activeMounts[imageID]; ok {
		mp.RefCount++
		log.L.Debugf("reusing existing fscache mount for %s, refcount=%d", imageID, mp.RefCount)
		return mp.MountPath, nil
	}

	mountPath := filepath.Join(m.mountsDir, imageID)
	if err := os.MkdirAll(mountPath, 0755); err != nil {
		return "", err
	}

	mountOpts := fmt.Sprintf("ro,fsid=%s,domain=%s", fsid, domain)
	cmd := exec.Command("mount", "-t", "erofs", "-o", mountOpts, "none", mountPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("fscache mount failed: %w, output: %s", err, string(output))
	}

	m.activeMounts[imageID] = &MountPoint{
		ID:         imageID,
		ImagePath:  fmt.Sprintf("fscache://%s/%s", domain, fsid),
		MountPath:  mountPath,
		LoopDevice: "",
		RefCount:   1,
	}

	log.L.Infof("mounted erofs with fscache: %s at %s (fsid=%s, domain=%s)", imageID, mountPath, fsid, domain)
	return mountPath, nil
}

func (m *MountManager) Unmount(imageID string) error {
	m.mountsMu.Lock()
	defer m.mountsMu.Unlock()

	mp, ok := m.activeMounts[imageID]
	if !ok {
		return fmt.Errorf("mount point not found for %s", imageID)
	}

	mp.RefCount--
	if mp.RefCount > 0 {
		log.L.Debugf("decremented refcount for %s, refcount=%d", imageID, mp.RefCount)
		return nil
	}

	if err := m.unmountPath(mp.MountPath); err != nil {
		return err
	}

	if err := m.detachLoopDevice(mp.LoopDevice); err != nil {
		log.L.Warnf("failed to detach loop device %s: %v", mp.LoopDevice, err)
	}

	delete(m.activeMounts, imageID)
	os.RemoveAll(mp.MountPath)

	log.L.Infof("unmounted erofs image %s", imageID)
	return nil
}

func (m *MountManager) unmountPath(mountPath string) error {
	cmd := exec.Command("umount", mountPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("umount failed: %w, output: %s", err, string(output))
	}
	return nil
}

func (m *MountManager) detachLoopDevice(loopDev string) error {
	cmd := exec.Command("losetup", "-d", loopDev)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("losetup detach failed: %w, output: %s", err, string(output))
	}
	return nil
}

func (m *MountManager) GetMountPath(imageID string) (string, bool) {
	m.mountsMu.RLock()
	defer m.mountsMu.RUnlock()

	if mp, ok := m.activeMounts[imageID]; ok {
		return mp.MountPath, true
	}
	return "", false
}

func (m *MountManager) CreateOverlayMounts(snapshotID string, lowerDirs []string, upperDir, workDir string) ([]mount.Mount, error) {
	if err := os.MkdirAll(upperDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(workDir, 0700); err != nil {
		return nil, err
	}

	options := []string{
		fmt.Sprintf("upperdir=%s", upperDir),
		fmt.Sprintf("workdir=%s", workDir),
	}

	if len(lowerDirs) > 0 {
		lowerDir := strings.Join(lowerDirs, ":")
		options = append(options, fmt.Sprintf("lowerdir=%s", lowerDir))
	}

	return []mount.Mount{
		{
			Type:    "overlay",
			Source:  "overlay",
			Options: options,
		},
	}, nil
}

func (m *MountManager) UnmountAll() error {
	m.mountsMu.Lock()
	defer m.mountsMu.Unlock()

	var errs []error
	for id, mp := range m.activeMounts {
		if err := m.unmountPath(mp.MountPath); err != nil {
			errs = append(errs, fmt.Errorf("failed to unmount %s: %w", id, err))
			continue
		}

		if err := m.detachLoopDevice(mp.LoopDevice); err != nil {
			errs = append(errs, fmt.Errorf("failed to detach loop %s: %w", mp.LoopDevice, err))
		}

		os.RemoveAll(mp.MountPath)
	}

	m.activeMounts = make(map[string]*MountPoint)

	if len(errs) > 0 {
		return fmt.Errorf("unmount errors: %v", errs)
	}

	return nil
}

func (m *MountManager) GetStats() map[string]*MountPoint {
	m.mountsMu.RLock()
	defer m.mountsMu.RUnlock()

	stats := make(map[string]*MountPoint)
	for id, mp := range m.activeMounts {
		stats[id] = &MountPoint{
			ID:         mp.ID,
			ImagePath:  mp.ImagePath,
			MountPath:  mp.MountPath,
			LoopDevice: mp.LoopDevice,
			RefCount:   mp.RefCount,
		}
	}
	return stats
}
