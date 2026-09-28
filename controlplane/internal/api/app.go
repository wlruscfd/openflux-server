package api

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"openflux-control/internal/auth"
	"openflux-control/internal/config"
	"openflux-control/internal/store"
)

type App struct {
	Store   *store.Store
	Hasher  auth.Hasher
	Cipher  auth.TokenCipher
	Config  config.Config
	limiter *ipRateLimiter
}

func NewApp(st *store.Store, hasher auth.Hasher, cipher auth.TokenCipher, cfg config.Config) *App {
	return &App{
		Store:   st,
		Hasher:  hasher,
		Cipher:  cipher,
		Config:  cfg,
		limiter: newIPRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst),
	}
}

func (a *App) Router() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /admin/", handleAdminUI)
	mux.HandleFunc("GET /admin/index.html", handleAdminUI)

	mux.HandleFunc("POST /v1/admin/nodes", a.withAdmin(a.handleCreateNode))
	mux.HandleFunc("GET /v1/admin/nodes", a.withAdmin(a.handleListNodes))
	mux.HandleFunc("PATCH /v1/admin/nodes/{id}", a.withAdmin(a.handlePatchNode))
	mux.HandleFunc("POST /v1/admin/nodes/{id}/rotate-token", a.withAdmin(a.handleRotateNodeToken))
	mux.HandleFunc("GET /v1/admin/system", a.withAdmin(a.handleSystem))
	mux.HandleFunc("GET /v1/admin/stats/summary", a.withAdmin(a.handleStatsSummary))
	mux.HandleFunc("GET /v1/admin/stats/usage", a.withAdmin(a.handleStatsUsage))
	mux.HandleFunc("GET /v1/admin/ingest-tokens", a.withAdmin(a.handleListIngestTokens))
	mux.HandleFunc("POST /v1/admin/ingest-tokens", a.withAdmin(a.handleCreateIngestToken))
	mux.HandleFunc("POST /v1/admin/ingest-tokens/{id}/enable", a.withAdmin(a.handleSetIngestTokenEnabled(true)))
	mux.HandleFunc("POST /v1/admin/ingest-tokens/{id}/disable", a.withAdmin(a.handleSetIngestTokenEnabled(false)))
	mux.HandleFunc("GET /v1/admin/keys", a.withAdmin(a.handleListKeys))
	mux.HandleFunc("POST /v1/admin/keys", a.withAdmin(a.handleCreateKey))
	mux.HandleFunc("GET /v1/admin/keys/{id}", a.withAdmin(a.handleGetKey))
	mux.HandleFunc("GET /v1/admin/keys/{id}/usage", a.withAdmin(a.handleKeyUsage))
	mux.HandleFunc("PATCH /v1/admin/keys/{id}", a.withAdmin(a.handlePatchKey))
	mux.HandleFunc("DELETE /v1/admin/keys/{id}", a.withAdmin(a.handleDeleteKey))
	mux.HandleFunc("POST /v1/admin/keys/{id}/rotate-token", a.withAdmin(a.handleRotateKeyToken))
	mux.HandleFunc("POST /v1/admin/keys/{id}/enable", a.withAdmin(a.handleSetKeyEnabled(true)))
	mux.HandleFunc("POST /v1/admin/keys/{id}/disable", a.withAdmin(a.handleSetKeyEnabled(false)))
	mux.HandleFunc("PATCH /v1/admin/keys/{id}/final-exit", a.withAdmin(a.handlePatchKeyFinalExit))

	mux.HandleFunc("POST /v1/ingest/keys", a.withIngestToken(a.handleIngestKeys))

	mux.HandleFunc("GET /v1/nodes/keys", a.withNodeToken(a.handleNodeListKeys))
	mux.HandleFunc("GET /v1/nodes/relay-keys", a.withNodeToken(a.handleNodeListRelayKeys))
	mux.HandleFunc("POST /v1/nodes/{id}/usage", a.withNodeToken(a.handleNodeUsage))
	mux.HandleFunc("POST /v1/nodes/{id}/heartbeat", a.withNodeToken(a.handleNodeHeartbeat))

	mux.HandleFunc("POST /v1/resolve", a.withRateLimit(a.handleResolve))
	mux.HandleFunc("POST /v1/keys/cookies", a.handlePostKeyCookies)
	mux.HandleFunc("DELETE /v1/keys/cookies", a.handlePostKeyCookies)
	mux.HandleFunc("GET /v1/nodes/cookies", a.withNodeToken(a.handleNodeListCookies))
	mux.HandleFunc("GET /v1/admin/keys/{id}/cookies/status", a.withAdmin(a.handleKeyCookieStatus))

	return withLogging(mux)
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		log.Printf("%s %s -> %d (%s)", r.Method, r.URL.Path, rec.status, time.Since(start))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// writeInternalError responds 500 with a client-facing message (never err's raw text, which could leak internals) while logging the actual error server-side.
func writeInternalError(w http.ResponseWriter, r *http.Request, message string, err error) {
	log.Printf("%s %s: %s: %v", r.Method, r.URL.Path, message, err)
	writeError(w, http.StatusInternalServerError, message)
}

func readJSON(r *http.Request, dst interface{}) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}
