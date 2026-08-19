package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"regdispatch/internal/clock"
	"regdispatch/internal/errorsx"
	"regdispatch/internal/store"
)

// DispatchRequest is the payload sent to an upstream department backend.
type DispatchRequest struct {
	ChangeID       string `json:"change_id"`
	EnterpriseID   string `json:"enterprise_id"`
	ChangeType     string `json:"change_type"`
	DepartmentCode string `json:"department_code"`
	BeforeValue    string `json:"before_value"`
	AfterValue     string `json:"after_value"`
	Attempt        int    `json:"attempt"`
}

// DispatchResponse is the reply from an upstream department backend.
type DispatchResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Detail  string `json:"detail,omitempty"`
}

// Client forwards dispatch requests to department upstream backends with
// circuit breaker protection, timeout, and request/response tracing.
type Client struct {
	selector  *Selector
	traceRepo store.TraceRepository
	httpCli   *http.Client
	clk       clock.Clock
}

// NewClient creates an upstream client wired to the given selector and trace repo.
func NewClient(selector *Selector, traceRepo store.TraceRepository, clk clock.Clock, timeout time.Duration) *Client {
	return &Client{
		selector:  selector,
		traceRepo: traceRepo,
		httpCli:   &http.Client{Timeout: timeout},
		clk:       clk,
	}
}

// Forward sends a dispatch request to an available upstream, with circuit
// breaker protection and automatic fallback to a backup upstream.
func (c *Client) Forward(ctx context.Context, req DispatchRequest) (*DispatchResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal dispatch request: %w", err)
	}

	attempts := c.selector.UpstreamCount()
	if attempts == 0 {
		return nil, errorsx.Wrap("upstream", "no upstreams configured", errorsx.ErrAllUpstreamsDown)
	}

	var lastErr error
	for i := 0; i < attempts; i++ {
		up, err := c.selector.Next()
		if err != nil {
			break
		}
		result, err := c.forwardOne(ctx, up, body, req)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !errorsx.IsCircuitOpen(err) {
			break // non-circuit errors don't trigger fallback
		}
	}
	if lastErr == nil {
		lastErr = errorsx.ErrAllUpstreamsDown
	}
	return nil, fmt.Errorf("all upstreams failed: %w", lastErr)
}

func (c *Client) forwardOne(ctx context.Context, up Upstream, body []byte, req DispatchRequest) (*DispatchResponse, error) {
	url := up.URL + "/api/departments/" + req.DepartmentCode + "/process"
	start := c.clk.Now()

	result, statusCode, err := c.breakerExecute(up, ctx, url, body)

	duration := c.clk.Now().Sub(start)
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	respBody := ""
	if result != nil {
		if b, mErr := json.Marshal(result); mErr == nil {
			respBody = string(b)
		}
	}
	if c.traceRepo != nil {
		_ = c.traceRepo.Record(ctx, up.Name, http.MethodPost, url, string(body), respBody, statusCode, duration, errStr)
	}
	return result, err
}

func (c *Client) breakerExecute(up Upstream, ctx context.Context, url string, body []byte) (*DispatchResponse, int, error) {
	result, err := up.Breaker.Execute(func() (any, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create request to %s: %w", url, err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		resp, err := c.httpCli.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("call upstream %s: %w", up.Name, err)
		}
		defer resp.Body.Close()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read upstream response: %w", err)
		}
		if resp.StatusCode >= 500 {
			return nil, fmt.Errorf("upstream %s returned %d: %s", up.Name, resp.StatusCode, string(respBody))
		}
		var dr DispatchResponse
		if err := json.Unmarshal(respBody, &dr); err != nil {
			return nil, fmt.Errorf("decode upstream response: %w", err)
		}
		return &dr, nil
	})
	if err != nil {
		return nil, 0, err
	}
	dr, ok := result.(*DispatchResponse)
	if !ok {
		return nil, 0, fmt.Errorf("unexpected response type from upstream %s", up.Name)
	}
	return dr, 200, nil
}

// BreakerState reports the current state of a named upstream's circuit breaker.
func (c *Client) BreakerState(name string) string {
	up, err := c.selector.ByName(name)
	if err != nil {
		return "unknown"
	}
	return up.Breaker.State().String()
}

// UpstreamInfo describes an upstream for the management API.
type UpstreamInfo struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	State     string `json:"breaker_state"`
	Requests  uint64 `json:"requests"`
	Failures  uint64 `json:"failures"`
	Successes uint64 `json:"successes"`
}
