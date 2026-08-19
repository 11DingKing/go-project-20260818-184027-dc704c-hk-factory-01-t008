package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"regdispatch/internal/clock"
	"regdispatch/internal/config"
	"regdispatch/internal/orchestrator"
	"regdispatch/internal/scheduler"
	"regdispatch/internal/store"
	"regdispatch/internal/upstream"
)

func testServer(t *testing.T) (*httptest.Server, *store.Repositories) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	st, err := store.Open(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	_, err = st.EnsureSchema(context.Background())
	require.NoError(t, err)
	repos := st.AllRepositories()
	clk := clock.RealClock{}
	log := zerolog.Nop()

	selector := upstream.NewSelector(nil)
	upClient := upstream.NewClient(selector, repos.Traces, clk, 5*time.Second)
	orch := orchestrator.New(repos, upClient, clk, log, 3, 1*time.Second, 10*time.Second)
	sched := scheduler.New(clk, log, st)

	cfg := &config.Config{
		Server:     config.ServerConfig{Port: 0, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, ShutdownTimeout: 5 * time.Second},
		Storage:    config.StorageConfig{DataDir: dir, DBPath: dbPath},
		Dispatch:   config.DispatchConfig{MaxRetries: 3, RetryBaseDelay: 1 * time.Second, RetryMaxDelay: 10 * time.Second},
		Compaction: config.CompactionConfig{Interval: 5 * time.Minute, RetainEvents: 10000},
		Upstream:   config.UpstreamConfig{Timeout: 5 * time.Second, BreakerThreshold: 5, BreakerTimeout: 30 * time.Second, BreakerHalfOpenMax: 3},
		Logging:    config.LoggingConfig{Level: "info", Format: "console"},
	}

	server := New(cfg, orch, st, sched, selector, upClient, log)
	ts := httptest.NewServer(server.buildRouter())
	t.Cleanup(func() { ts.Close() })
	return ts, repos
}

func mustRegisterEnterpriseHTTP(t *testing.T, ts *httptest.Server, suffix string) string {
	body := EnterpriseRequest{
		Name:                "测试科技有限公司" + suffix,
		LegalRepresentative: "原法定代表人",
		UnifiedCreditCode:   "91110000MA0" + suffix + "000012",
		RegisteredCapital:   "500万元",
		BusinessScope:       "软件开发",
		IndustryCode:        "I6510",
	}
	b, _ := json.Marshal(body)
	resp, err := http.Post(ts.URL+"/api/v1/enterprises", "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "enterprise creation should return 201")
	var result EnterpriseResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	resp.Body.Close()
	return result.ID
}

func mustSubmitChangeHTTP(t *testing.T, ts *httptest.Server, enterpriseID string) string {
	body := ChangeRequest{
		EnterpriseID: enterpriseID,
		ChangeType:   "legal_representative",
		NewValue:     "新法定代表人",
		SubmittedBy:  "clerk1",
	}
	b, _ := json.Marshal(body)
	resp, err := http.Post(ts.URL+"/api/v1/changes", "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var result ChangeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	resp.Body.Close()
	return result.ID
}

func TestHTTPHealthz(t *testing.T) {
	ts, _ := testServer(t)
	resp, err := http.Get(ts.URL + "/healthz")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, "alive", result["status"])
	resp.Body.Close()
}

func TestHTTPReadyzReturnsServiceUnavailable(t *testing.T) {
	ts, _ := testServer(t)
	resp, err := http.Get(ts.URL + "/readyz")
	require.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	var result map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, "not_ready", result["status"])
	resp.Body.Close()
}

func TestHTTPCreateEnterprise(t *testing.T) {
	ts, _ := testServer(t)
	id := mustRegisterEnterpriseHTTP(t, ts, "A")
	assert.NotEmpty(t, id)
}

func TestHTTPListEnterprisesPagination(t *testing.T) {
	ts, _ := testServer(t)
	for i := 0; i < 3; i++ {
		mustRegisterEnterpriseHTTP(t, ts, string(rune('A'+i)))
	}

	resp, err := http.Get(ts.URL + "/api/v1/enterprises?offset=0&limit=2")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var result struct {
		Items []EnterpriseResponse `json:"items"`
		Total int                  `json:"total"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, 3, result.Total)
	assert.Len(t, result.Items, 2)
	resp.Body.Close()
}

func TestHTTPSubmitChange(t *testing.T) {
	ts, _ := testServer(t)
	entID := mustRegisterEnterpriseHTTP(t, ts, "B")
	chgID := mustSubmitChangeHTTP(t, ts, entID)
	assert.NotEmpty(t, chgID)
}

func TestHTTPSubmitChangeValidationError(t *testing.T) {
	ts, _ := testServer(t)
	body := ChangeRequest{ChangeType: "legal_representative"}
	b, _ := json.Marshal(body)
	resp, err := http.Post(ts.URL+"/api/v1/changes", "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp.Body.Close()
}

func TestHTTPDispatchChange(t *testing.T) {
	ts, _ := testServer(t)
	entID := mustRegisterEnterpriseHTTP(t, ts, "C")
	chgID := mustSubmitChangeHTTP(t, ts, entID)

	dispBody := DispatchRequest{Operator: "op1"}
	b2, _ := json.Marshal(dispBody)
	resp, err := http.Post(ts.URL+"/api/v1/changes/"+chgID+"/dispatch", "application/json", bytes.NewReader(b2))
	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
	resp.Body.Close()
}

func TestHTTPDispatchDuplicateRejected(t *testing.T) {
	ts, _ := testServer(t)
	entID := mustRegisterEnterpriseHTTP(t, ts, "D")
	chgID := mustSubmitChangeHTTP(t, ts, entID)

	dispBody := DispatchRequest{Operator: "op1"}
	b2, _ := json.Marshal(dispBody)
	resp, _ := http.Post(ts.URL+"/api/v1/changes/"+chgID+"/dispatch", "application/json", bytes.NewReader(b2))
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	resp.Body.Close()

	resp2, _ := http.Post(ts.URL+"/api/v1/changes/"+chgID+"/dispatch", "application/json", bytes.NewReader(b2))
	assert.Equal(t, http.StatusConflict, resp2.StatusCode)
	resp2.Body.Close()
}

func TestHTTPRevokeChange(t *testing.T) {
	ts, _ := testServer(t)
	entID := mustRegisterEnterpriseHTTP(t, ts, "E")
	chgID := mustSubmitChangeHTTP(t, ts, entID)

	revBody := RevokeRequest{Operator: "admin", Reason: "误操作"}
	b, _ := json.Marshal(revBody)
	resp, _ := http.Post(ts.URL+"/api/v1/changes/"+chgID+"/revoke", "application/json", bytes.NewReader(b))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestHTTPListDepartmentDispatches(t *testing.T) {
	ts, _ := testServer(t)
	entID := mustRegisterEnterpriseHTTP(t, ts, "F")
	chgID := mustSubmitChangeHTTP(t, ts, entID)

	dispBody := DispatchRequest{Operator: "op1"}
	b2, _ := json.Marshal(dispBody)
	resp, _ := http.Post(ts.URL+"/api/v1/changes/"+chgID+"/dispatch", "application/json", bytes.NewReader(b2))
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	resp.Body.Close()

	resp2, _ := http.Get(ts.URL + "/api/v1/departments/tax/dispatches")
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	var result map[string]any
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&result))
	assert.Greater(t, result["total"], float64(0))
	resp2.Body.Close()
}

func TestHTTPAuditRecords(t *testing.T) {
	ts, _ := testServer(t)
	mustRegisterEnterpriseHTTP(t, ts, "G")

	resp, _ := http.Get(ts.URL + "/api/v1/audit")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Greater(t, result["total"], float64(0))
	resp.Body.Close()
}

func TestHTTPExportReconciliation(t *testing.T) {
	ts, _ := testServer(t)
	entID := mustRegisterEnterpriseHTTP(t, ts, "H")
	chgID := mustSubmitChangeHTTP(t, ts, entID)

	dispBody := DispatchRequest{Operator: "op1"}
	b2, _ := json.Marshal(dispBody)
	resp, _ := http.Post(ts.URL+"/api/v1/changes/"+chgID+"/dispatch", "application/json", bytes.NewReader(b2))
	resp.Body.Close()

	resp2, _ := http.Get(ts.URL + "/api/v1/export/reconciliation?department_code=tax")
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	resp2.Body.Close()
}

func TestHTTPBacklog(t *testing.T) {
	ts, _ := testServer(t)
	entID := mustRegisterEnterpriseHTTP(t, ts, "I")
	chgID := mustSubmitChangeHTTP(t, ts, entID)

	dispBody := DispatchRequest{Operator: "op1"}
	b2, _ := json.Marshal(dispBody)
	resp, _ := http.Post(ts.URL+"/api/v1/changes/"+chgID+"/dispatch", "application/json", bytes.NewReader(b2))
	resp.Body.Close()

	resp2, _ := http.Get(ts.URL + "/api/v1/backlog")
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	resp2.Body.Close()
}

func TestHTTPListUpstreams(t *testing.T) {
	ts, _ := testServer(t)
	resp, _ := http.Get(ts.URL + "/api/v1/admin/upstreams")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestHTTPEnterpriseNotFound(t *testing.T) {
	ts, _ := testServer(t)
	resp, _ := http.Get(ts.URL + "/api/v1/enterprises/nonexistent")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	resp.Body.Close()
}

func TestHTTPResolveOrder(t *testing.T) {
	ts, _ := testServer(t)
	entID := mustRegisterEnterpriseHTTP(t, ts, "J")
	chgID := mustSubmitChangeHTTP(t, ts, entID)
	require.NotEmpty(t, chgID)

	resp, _ := http.Post(ts.URL+"/api/v1/changes/"+entID+"/resolve", "application/json", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestHTTPListDeadLetters(t *testing.T) {
	ts, _ := testServer(t)
	resp, _ := http.Get(ts.URL + "/api/v1/dead-letters")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	resp.Body.Close()
}

func TestHTTPConcurrentSubmitRace(t *testing.T) {
	ts, _ := testServer(t)
	entID := mustRegisterEnterpriseHTTP(t, ts, "K")

	var wg sync.WaitGroup
	errs := make([]int, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := ChangeRequest{
				EnterpriseID: entID,
				ChangeType:   "legal_representative",
				NewValue:     "法人",
				SubmittedBy:  "clerk1",
			}
			b, _ := json.Marshal(body)
			resp, err := http.Post(ts.URL+"/api/v1/changes", "application/json", bytes.NewReader(b))
			if err != nil {
				errs[idx] = 1
				return
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				errs[idx] = 1
			}
		}(i)
	}
	wg.Wait()

	failed := 0
	for _, e := range errs {
		failed += e
	}
	assert.Equal(t, 0, failed, "all concurrent change submissions should succeed")
}
