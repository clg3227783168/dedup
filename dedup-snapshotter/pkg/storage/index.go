package storage

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/containerd/log"
	_ "github.com/mattn/go-sqlite3"
)

type IndexDB struct {
	db       *sql.DB
	mu       sync.RWMutex
	path     string
	lockFile string
}

func NewIndexDB(path string) (*IndexDB, error) {
	lockFile := path + ".lock"

	if err := checkAndRecover(path, lockFile); err != nil {
		log.L.WithError(err).Warn("crash recovery check failed, attempting recovery")
		if err := recoverDatabase(path); err != nil {
			return nil, fmt.Errorf("database recovery failed: %w", err)
		}
	}

	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_synchronous=FULL")
	if err != nil {
		return nil, err
	}

	idx := &IndexDB{
		db:       db,
		path:     path,
		lockFile: lockFile,
	}

	if err := idx.init(); err != nil {
		return nil, err
	}

	if err := idx.createLockFile(); err != nil {
		return nil, err
	}

	if err := idx.verifyIntegrity(); err != nil {
		log.L.WithError(err).Warn("database integrity check failed, attempting rebuild")
		if err := idx.rebuild(); err != nil {
			return nil, fmt.Errorf("database rebuild failed: %w", err)
		}
	}

	return idx, nil
}

func (i *IndexDB) init() error {
	schema := `
	CREATE TABLE IF NOT EXISTS chunks (
		hash TEXT PRIMARY KEY,
		size INTEGER,
		ref_count INTEGER DEFAULT 1
	);

	CREATE TABLE IF NOT EXISTS files (
		path TEXT PRIMARY KEY,
		chunks TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_chunks_hash ON chunks(hash);
	CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
	`

	_, err := i.db.Exec(schema)
	return err
}

func (i *IndexDB) IndexFile(path string, chunks []ChunkInfo) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	tx, err := i.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var chunkHashes string
	for idx, chunk := range chunks {
		if idx > 0 {
			chunkHashes += ","
		}
		chunkHashes += chunk.Hash

		_, err := tx.Exec("INSERT OR IGNORE INTO chunks (hash, size) VALUES (?, ?)", chunk.Hash, chunk.Size)
		if err != nil {
			return err
		}

		_, err = tx.Exec("UPDATE chunks SET ref_count = ref_count + 1 WHERE hash = ?", chunk.Hash)
		if err != nil {
			return err
		}
	}

	_, err = tx.Exec("INSERT OR REPLACE INTO files (path, chunks) VALUES (?, ?)", path, chunkHashes)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (i *IndexDB) IncrementRefCount(hash string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	_, err := i.db.Exec("UPDATE chunks SET ref_count = ref_count + 1 WHERE hash = ?", hash)
	return err
}

func (i *IndexDB) DecrementRefCount(hash string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	_, err := i.db.Exec("UPDATE chunks SET ref_count = ref_count - 1 WHERE hash = ?", hash)
	return err
}

func (i *IndexDB) GetChunkRefCount(hash string) (int64, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var count int64
	err := i.db.QueryRow("SELECT ref_count FROM chunks WHERE hash = ?", hash).Scan(&count)
	return count, err
}

func (i *IndexDB) Close() error {
	if i.lockFile != "" {
		os.Remove(i.lockFile)
	}
	return i.db.Close()
}

func (i *IndexDB) createLockFile() error {
	f, err := os.Create(i.lockFile)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(fmt.Sprintf("%d", time.Now().Unix()))
	return err
}

func checkAndRecover(dbPath, lockFile string) error {
	if _, err := os.Stat(lockFile); err == nil {
		log.L.Warn("detected unclean shutdown, lock file exists")
		return fmt.Errorf("unclean shutdown detected")
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil
	}

	if _, err := os.Stat(dbPath + "-wal"); err == nil {
		log.L.Warn("detected WAL file from previous run")
		return fmt.Errorf("WAL file exists")
	}

	return nil
}

func recoverDatabase(dbPath string) error {
	log.L.Info("attempting database recovery")

	backupPath := dbPath + ".backup." + fmt.Sprintf("%d", time.Now().Unix())
	if err := copyFile(dbPath, backupPath); err != nil {
		log.L.WithError(err).Warn("failed to create backup")
	} else {
		log.L.Infof("created database backup at %s", backupPath)
	}

	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL")
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}

	var integrityResult string
	err = db.QueryRow("PRAGMA integrity_check").Scan(&integrityResult)
	if err != nil {
		return fmt.Errorf("integrity check failed: %w", err)
	}

	if integrityResult != "ok" {
		log.L.Warnf("database integrity check result: %s", integrityResult)
		return fmt.Errorf("database corrupted: %s", integrityResult)
	}

	_, err = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	if err != nil {
		return fmt.Errorf("WAL checkpoint failed: %w", err)
	}

	log.L.Info("database recovery completed successfully")
	return nil
}

func (i *IndexDB) verifyIntegrity() error {
	var result string
	err := i.db.QueryRow("PRAGMA integrity_check").Scan(&result)
	if err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("integrity check failed: %s", result)
	}

	var chunkCount, fileCount int64
	err = i.db.QueryRow("SELECT COUNT(*) FROM chunks").Scan(&chunkCount)
	if err != nil {
		return fmt.Errorf("failed to count chunks: %w", err)
	}

	err = i.db.QueryRow("SELECT COUNT(*) FROM files").Scan(&fileCount)
	if err != nil {
		return fmt.Errorf("failed to count files: %w", err)
	}

	log.L.Infof("database integrity verified: %d chunks, %d files", chunkCount, fileCount)
	return nil
}

func (i *IndexDB) rebuild() error {
	log.L.Info("starting database rebuild")

	backupPath := i.path + ".rebuild_backup." + fmt.Sprintf("%d", time.Now().Unix())
	if err := copyFile(i.path, backupPath); err == nil {
		log.L.Infof("created rebuild backup at %s", backupPath)
	}

	tx, err := i.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec("DELETE FROM chunks WHERE ref_count <= 0")
	if err != nil {
		return fmt.Errorf("failed to clean invalid chunks: %w", err)
	}

	rows, err := tx.Query("SELECT path, chunks FROM files")
	if err != nil {
		return fmt.Errorf("failed to query files: %w", err)
	}
	defer rows.Close()

	refCounts := make(map[string]int64)
	for rows.Next() {
		var path, chunks string
		if err := rows.Scan(&path, &chunks); err != nil {
			log.L.WithError(err).Warnf("failed to scan file row")
			continue
		}

		if chunks == "" {
			continue
		}

		chunkHashes := parseChunkHashes(chunks)
		for _, hash := range chunkHashes {
			refCounts[hash]++
		}
	}

	for hash, count := range refCounts {
		_, err = tx.Exec("UPDATE chunks SET ref_count = ? WHERE hash = ?", count, hash)
		if err != nil {
			log.L.WithError(err).Warnf("failed to update ref count for chunk %s", hash)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit rebuild: %w", err)
	}

	_, err = i.db.Exec("VACUUM")
	if err != nil {
		log.L.WithError(err).Warn("VACUUM failed")
	}

	log.L.Info("database rebuild completed successfully")
	return nil
}

func parseChunkHashes(chunks string) []string {
	if chunks == "" {
		return nil
	}
	var result []string
	start := 0
	for i := 0; i < len(chunks); i++ {
		if chunks[i] == ',' {
			if i > start {
				result = append(result, chunks[start:i])
			}
			start = i + 1
		}
	}
	if start < len(chunks) {
		result = append(result, chunks[start:])
	}
	return result
}

func copyFile(src, dst string) error {
	input, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, input, 0644)
}
