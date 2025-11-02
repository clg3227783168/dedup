package erofs

import (
	"database/sql"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type ChunkIndexer struct {
	db *sql.DB
	mu sync.RWMutex
}

type ChunkStats struct {
	TotalChunks  int64
	UniqueChunks int64
	TotalSize    int64
	DedupeSize   int64
	DedupRatio   float64
}

func NewChunkIndexer(dbPath string) (*ChunkIndexer, error) {
	db, err := sql.Open("sqlite3", dbPath+"?cache=shared&mode=rwc")
	if err != nil {
		return nil, err
	}

	indexer := &ChunkIndexer{db: db}
	if err := indexer.init(); err != nil {
		db.Close()
		return nil, err
	}

	return indexer, nil
}

func (c *ChunkIndexer) init() error {
	schema := `
	CREATE TABLE IF NOT EXISTS chunks (
		hash TEXT PRIMARY KEY,
		size INTEGER NOT NULL,
		ref_count INTEGER DEFAULT 0,
		first_seen INTEGER DEFAULT (strftime('%s', 'now'))
	);

	CREATE TABLE IF NOT EXISTS image_chunks (
		image_id TEXT NOT NULL,
		chunk_hash TEXT NOT NULL,
		chunk_order INTEGER NOT NULL,
		PRIMARY KEY (image_id, chunk_hash, chunk_order),
		FOREIGN KEY (chunk_hash) REFERENCES chunks(hash)
	);

	CREATE TABLE IF NOT EXISTS images (
		image_id TEXT PRIMARY KEY,
		created_at INTEGER DEFAULT (strftime('%s', 'now')),
		total_size INTEGER DEFAULT 0,
		chunk_count INTEGER DEFAULT 0
	);

	CREATE INDEX IF NOT EXISTS idx_chunks_hash ON chunks(hash);
	CREATE INDEX IF NOT EXISTS idx_chunks_refcount ON chunks(ref_count);
	CREATE INDEX IF NOT EXISTS idx_image_chunks_image ON image_chunks(image_id);
	CREATE INDEX IF NOT EXISTS idx_images_created ON images(created_at);
	`

	_, err := c.db.Exec(schema)
	return err
}

func (c *ChunkIndexer) RecordChunk(imageID, chunkHash string, size int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO chunks (hash, size, ref_count)
		VALUES (?, ?, 1)
		ON CONFLICT(hash) DO UPDATE SET ref_count = ref_count + 1
	`, chunkHash, size)
	if err != nil {
		return err
	}

	var order int
	err = tx.QueryRow(`
		SELECT COALESCE(MAX(chunk_order), -1) + 1
		FROM image_chunks
		WHERE image_id = ?
	`, imageID).Scan(&order)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		INSERT OR IGNORE INTO image_chunks (image_id, chunk_hash, chunk_order)
		VALUES (?, ?, ?)
	`, imageID, chunkHash, order)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		INSERT INTO images (image_id, total_size, chunk_count)
		VALUES (?, ?, 1)
		ON CONFLICT(image_id) DO UPDATE SET
			total_size = total_size + ?,
			chunk_count = chunk_count + 1
	`, imageID, size, size)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (c *ChunkIndexer) GetImageStats(imageID string) (*ChunkStats, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var stats ChunkStats

	err := c.db.QueryRow(`
		SELECT
			COUNT(*) as total_chunks,
			COUNT(DISTINCT chunk_hash) as unique_chunks,
			SUM(c.size) as total_size
		FROM image_chunks ic
		JOIN chunks c ON ic.chunk_hash = c.hash
		WHERE ic.image_id = ?
	`, imageID).Scan(&stats.TotalChunks, &stats.UniqueChunks, &stats.TotalSize)

	if err != nil {
		return nil, err
	}

	err = c.db.QueryRow(`
		SELECT SUM(c.size)
		FROM (
			SELECT DISTINCT chunk_hash
			FROM image_chunks
			WHERE image_id = ?
		) ic
		JOIN chunks c ON ic.chunk_hash = c.hash
	`, imageID).Scan(&stats.DedupeSize)

	if err != nil {
		return nil, err
	}

	if stats.TotalSize > 0 {
		stats.DedupRatio = float64(stats.TotalSize-stats.DedupeSize) / float64(stats.TotalSize) * 100
	}

	return &stats, nil
}

func (c *ChunkIndexer) GetChunk(chunkHash string) (*ChunkInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var chunk ChunkInfo
	var refCount int64
	err := c.db.QueryRow(`
		SELECT hash, size, ref_count
		FROM chunks
		WHERE hash = ?
	`, chunkHash).Scan(&chunk.Hash, &chunk.Size, &refCount)

	if err != nil {
		return nil, err
	}

	return &chunk, nil
}

func (c *ChunkIndexer) GetImageChunks(imageID string) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	rows, err := c.db.Query(`
		SELECT chunk_hash
		FROM image_chunks
		WHERE image_id = ?
		ORDER BY chunk_order
	`, imageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunks []string
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		chunks = append(chunks, hash)
	}

	return chunks, rows.Err()
}

func (c *ChunkIndexer) RemoveImage(imageID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		UPDATE chunks
		SET ref_count = ref_count - 1
		WHERE hash IN (
			SELECT chunk_hash FROM image_chunks WHERE image_id = ?
		)
	`, imageID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DELETE FROM chunks WHERE ref_count <= 0`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DELETE FROM image_chunks WHERE image_id = ?`, imageID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DELETE FROM images WHERE image_id = ?`, imageID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (c *ChunkIndexer) GetGlobalStats() (*GlobalStats, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var stats GlobalStats
	err := c.db.QueryRow(`
		SELECT
			COUNT(*) as total_chunks,
			SUM(size) as total_size,
			SUM(size * ref_count) as logical_size
		FROM chunks
	`).Scan(&stats.TotalChunks, &stats.TotalSize, &stats.LogicalSize)

	if err != nil {
		return nil, err
	}

	if stats.LogicalSize > 0 {
		stats.DedupRatio = float64(stats.LogicalSize-stats.TotalSize) / float64(stats.LogicalSize) * 100
	}

	err = c.db.QueryRow(`SELECT COUNT(*) FROM images`).Scan(&stats.ImageCount)
	if err != nil {
		return nil, err
	}

	return &stats, nil
}

func (c *ChunkIndexer) Close() error {
	return c.db.Close()
}

type GlobalStats struct {
	TotalChunks int64
	TotalSize   int64
	LogicalSize int64
	DedupRatio  float64
	ImageCount  int64
}
