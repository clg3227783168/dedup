package audit

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/containerd/log"
	_ "github.com/mattn/go-sqlite3"
)

type AuditLogger struct {
	db   *sql.DB
	mu   sync.RWMutex
	path string
}

type AuditEntry struct {
	ID        int64     `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Operation string    `json:"operation"`
	Target    string    `json:"target"`
	User      string    `json:"user"`
	PID       int       `json:"pid"`
	Details   string    `json:"details"`
	Result    string    `json:"result"`
	Error     string    `json:"error,omitempty"`
	Duration  int64     `json:"duration_ms"`
}

type QueryFilter struct {
	StartTime *time.Time
	EndTime   *time.Time
	Operation string
	Target    string
	User      string
	Result    string
	Limit     int
	Offset    int
}

func NewAuditLogger(dbPath string) (*AuditLogger, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_synchronous=FULL")
	if err != nil {
		return nil, fmt.Errorf("failed to open audit database: %w", err)
	}

	logger := &AuditLogger{
		db:   db,
		path: dbPath,
	}

	if err := logger.init(); err != nil {
		return nil, fmt.Errorf("failed to initialize audit database: %w", err)
	}

	return logger, nil
}

func (a *AuditLogger) init() error {
	schema := `
	CREATE TABLE IF NOT EXISTS audit_log (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp DATETIME NOT NULL,
		operation TEXT NOT NULL,
		target TEXT NOT NULL,
		user TEXT NOT NULL,
		pid INTEGER NOT NULL,
		details TEXT,
		result TEXT NOT NULL,
		error TEXT,
		duration_ms INTEGER NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
	CREATE INDEX IF NOT EXISTS idx_audit_operation ON audit_log(operation);
	CREATE INDEX IF NOT EXISTS idx_audit_target ON audit_log(target);
	CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user);
	CREATE INDEX IF NOT EXISTS idx_audit_result ON audit_log(result);
	`

	_, err := a.db.Exec(schema)
	return err
}

func (a *AuditLogger) LogOperation(ctx context.Context, operation, target, user string, pid int, details interface{}, result string, err error, duration time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()

	detailsJSON := ""
	if details != nil {
		if data, jsonErr := json.Marshal(details); jsonErr == nil {
			detailsJSON = string(data)
		}
	}

	errorStr := ""
	if err != nil {
		errorStr = err.Error()
	}

	entry := &AuditEntry{
		Timestamp: time.Now(),
		Operation: operation,
		Target:    target,
		User:      user,
		PID:       pid,
		Details:   detailsJSON,
		Result:    result,
		Error:     errorStr,
		Duration:  duration.Milliseconds(),
	}

	_, dbErr := a.db.Exec(`
		INSERT INTO audit_log (timestamp, operation, target, user, pid, details, result, error, duration_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, entry.Timestamp, entry.Operation, entry.Target, entry.User, entry.PID,
		entry.Details, entry.Result, entry.Error, entry.Duration)

	if dbErr != nil {
		log.L.WithError(dbErr).Error("failed to write audit log")
	}
}

func (a *AuditLogger) QueryLogs(ctx context.Context, filter *QueryFilter) ([]AuditEntry, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	query := `SELECT id, timestamp, operation, target, user, pid, details, result, error, duration_ms FROM audit_log WHERE 1=1`
	var args []interface{}

	if filter.StartTime != nil {
		query += " AND timestamp >= ?"
		args = append(args, filter.StartTime)
	}

	if filter.EndTime != nil {
		query += " AND timestamp <= ?"
		args = append(args, filter.EndTime)
	}

	if filter.Operation != "" {
		query += " AND operation = ?"
		args = append(args, filter.Operation)
	}

	if filter.Target != "" {
		query += " AND target LIKE ?"
		args = append(args, "%"+filter.Target+"%")
	}

	if filter.User != "" {
		query += " AND user = ?"
		args = append(args, filter.User)
	}

	if filter.Result != "" {
		query += " AND result = ?"
		args = append(args, filter.Result)
	}

	query += " ORDER BY timestamp DESC"

	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
	}

	if filter.Offset > 0 {
		query += " OFFSET ?"
		args = append(args, filter.Offset)
	}

	rows, err := a.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query audit logs: %w", err)
	}
	defer rows.Close()

	var entries []AuditEntry
	for rows.Next() {
		var entry AuditEntry
		var errorStr sql.NullString

		err := rows.Scan(
			&entry.ID,
			&entry.Timestamp,
			&entry.Operation,
			&entry.Target,
			&entry.User,
			&entry.PID,
			&entry.Details,
			&entry.Result,
			&errorStr,
			&entry.Duration,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan audit entry: %w", err)
		}

		if errorStr.Valid {
			entry.Error = errorStr.String
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func (a *AuditLogger) GetStats(ctx context.Context) (map[string]interface{}, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	stats := make(map[string]interface{})

	var totalEntries int64
	err := a.db.QueryRow("SELECT COUNT(*) FROM audit_log").Scan(&totalEntries)
	if err != nil {
		return nil, fmt.Errorf("failed to get total entries: %w", err)
	}
	stats["total_entries"] = totalEntries

	rows, err := a.db.Query(`
		SELECT operation, COUNT(*) as count
		FROM audit_log
		WHERE timestamp >= datetime('now', '-24 hours')
		GROUP BY operation
		ORDER BY count DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get operation stats: %w", err)
	}
	defer rows.Close()

	operationStats := make(map[string]int64)
	for rows.Next() {
		var operation string
		var count int64
		if err := rows.Scan(&operation, &count); err != nil {
			continue
		}
		operationStats[operation] = count
	}
	stats["operations_24h"] = operationStats

	rows2, err := a.db.Query(`
		SELECT result, COUNT(*) as count
		FROM audit_log
		WHERE timestamp >= datetime('now', '-24 hours')
		GROUP BY result
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get result stats: %w", err)
	}
	defer rows2.Close()

	resultStats := make(map[string]int64)
	for rows2.Next() {
		var result string
		var count int64
		if err := rows2.Scan(&result, &count); err != nil {
			continue
		}
		resultStats[result] = count
	}
	stats["results_24h"] = resultStats

	return stats, nil
}

func (a *AuditLogger) Cleanup(ctx context.Context, retentionDays int) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	cutoffTime := time.Now().AddDate(0, 0, -retentionDays)

	result, err := a.db.Exec("DELETE FROM audit_log WHERE timestamp < ?", cutoffTime)
	if err != nil {
		return fmt.Errorf("failed to cleanup audit logs: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	log.L.Infof("cleaned up %d audit log entries older than %d days", rowsAffected, retentionDays)

	_, err = a.db.Exec("VACUUM")
	if err != nil {
		log.L.WithError(err).Warn("failed to vacuum audit database")
	}

	return nil
}

func (a *AuditLogger) Close() error {
	return a.db.Close()
}

type AuditContext struct {
	Operation string
	Target    string
	User      string
	PID       int
	StartTime time.Time
	Details   interface{}
}

func StartAudit(ctx context.Context, operation, target, user string, pid int, details interface{}) context.Context {
	auditCtx := &AuditContext{
		Operation: operation,
		Target:    target,
		User:      user,
		PID:       pid,
		StartTime: time.Now(),
		Details:   details,
	}
	return context.WithValue(ctx, "audit", auditCtx)
}

func FinishAudit(ctx context.Context, logger *AuditLogger, result string, err error) {
	if auditCtx, ok := ctx.Value("audit").(*AuditContext); ok {
		duration := time.Since(auditCtx.StartTime)
		logger.LogOperation(ctx, auditCtx.Operation, auditCtx.Target, auditCtx.User,
			auditCtx.PID, auditCtx.Details, result, err, duration)
	}
}