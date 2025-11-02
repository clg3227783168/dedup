package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/containerd/log"
	"github.com/opencloudos/dedup-snapshotter/pkg/audit"
	"github.com/opencloudos/dedup-snapshotter/pkg/config"
)

type APIServer struct {
	auditLogger *audit.AuditLogger
	config      *config.Config
	configPath  string
	server      *http.Server
}

type Response struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func NewAPIServer(addr string, auditLogger *audit.AuditLogger, cfg *config.Config, configPath string) *APIServer {
	api := &APIServer{
		auditLogger: auditLogger,
		config:      cfg,
		configPath:  configPath,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/audit/logs", api.handleAuditLogs)
	mux.HandleFunc("/api/v1/audit/stats", api.handleAuditStats)
	mux.HandleFunc("/api/v1/config", api.handleConfig)
	mux.HandleFunc("/api/v1/config/reload", api.handleConfigReload)
	mux.HandleFunc("/api/v1/health", api.handleHealth)

	api.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return api
}

func (a *APIServer) Start() error {
	log.L.Infof("starting API server on %s", a.server.Addr)
	return a.server.ListenAndServe()
}

func (a *APIServer) Stop(ctx context.Context) error {
	log.L.Info("stopping API server")
	return a.server.Shutdown(ctx)
}

func (a *APIServer) handleAuditLogs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		a.getAuditLogs(w, r)
	default:
		a.respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (a *APIServer) getAuditLogs(w http.ResponseWriter, r *http.Request) {
	filter := &audit.QueryFilter{}

	if startTimeStr := r.URL.Query().Get("start_time"); startTimeStr != "" {
		if startTime, err := time.Parse(time.RFC3339, startTimeStr); err == nil {
			filter.StartTime = &startTime
		}
	}

	if endTimeStr := r.URL.Query().Get("end_time"); endTimeStr != "" {
		if endTime, err := time.Parse(time.RFC3339, endTimeStr); err == nil {
			filter.EndTime = &endTime
		}
	}

	filter.Operation = r.URL.Query().Get("operation")
	filter.Target = r.URL.Query().Get("target")
	filter.User = r.URL.Query().Get("user")
	filter.Result = r.URL.Query().Get("result")

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil && limit > 0 {
			filter.Limit = limit
		}
	}
	if filter.Limit == 0 {
		filter.Limit = 100
	}

	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		if offset, err := strconv.Atoi(offsetStr); err == nil && offset >= 0 {
			filter.Offset = offset
		}
	}

	logs, err := a.auditLogger.QueryLogs(r.Context(), filter)
	if err != nil {
		a.respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to query logs: %v", err))
		return
	}

	a.respond(w, http.StatusOK, logs)
}

func (a *APIServer) handleAuditStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		stats, err := a.auditLogger.GetStats(r.Context())
		if err != nil {
			a.respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get stats: %v", err))
			return
		}
		a.respond(w, http.StatusOK, stats)
	default:
		a.respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (a *APIServer) handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		a.respond(w, http.StatusOK, a.config)
	case http.MethodPut:
		a.updateConfig(w, r)
	default:
		a.respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (a *APIServer) updateConfig(w http.ResponseWriter, r *http.Request) {
	var newConfig config.Config
	if err := json.NewDecoder(r.Body).Decode(&newConfig); err != nil {
		a.respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	if err := newConfig.Validate(); err != nil {
		a.respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid config: %v", err))
		return
	}

	if err := newConfig.Save(a.configPath); err != nil {
		a.respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to save config: %v", err))
		return
	}

	a.config = &newConfig

	log.L.Info("configuration updated via API")

	ctx := audit.StartAudit(r.Context(), "config_update", "config", "api", os.Getpid(), newConfig)
	audit.FinishAudit(ctx, a.auditLogger, "success", nil)

	a.respond(w, http.StatusOK, map[string]interface{}{
		"message": "configuration updated successfully",
		"config":  a.config,
	})
}

func (a *APIServer) handleConfigReload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodPost:
		newConfig, err := config.LoadConfig(a.configPath)
		if err != nil {
			a.respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to reload config: %v", err))
			return
		}

		a.config = newConfig
		log.L.Info("configuration reloaded from file")

		ctx := audit.StartAudit(r.Context(), "config_reload", "config", "api", os.Getpid(), nil)
		audit.FinishAudit(ctx, a.auditLogger, "success", nil)

		a.respond(w, http.StatusOK, map[string]interface{}{
			"message": "configuration reloaded successfully",
			"config":  a.config,
		})
	default:
		a.respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (a *APIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		a.respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"version":   "1.0.0",
	}

	a.respond(w, http.StatusOK, health)
}

func (a *APIServer) respond(w http.ResponseWriter, status int, data interface{}) {
	response := Response{
		Success: status < 400,
		Data:    data,
	}

	w.WriteHeader(status)
	json.NewEncoder(w).Encode(response)
}

func (a *APIServer) respondError(w http.ResponseWriter, status int, message string) {
	response := Response{
		Success: false,
		Error:   message,
	}

	w.WriteHeader(status)
	json.NewEncoder(w).Encode(response)
}

func (a *APIServer) GetConfig() *config.Config {
	return a.config
}