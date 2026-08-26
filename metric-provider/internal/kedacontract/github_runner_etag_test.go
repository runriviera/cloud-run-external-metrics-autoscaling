package kedacontract

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/kedacore/keda/v2/pkg/scalers"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	"github.com/stretchr/testify/require"
)

func TestGitHubRunnerETagCacheIsolatedByWorkflowRun(t *testing.T) {
	const (
		owner       = "runriviera"
		repo        = "os"
		runnerLabel = "gcp-ci-unit-cloud-run"
	)

	var mu sync.Mutex
	requestCounts := map[string]int{}
	conditionalRequests := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCounts[r.URL.RequestURI()]++
		requestCount := requestCounts[r.URL.RequestURI()]
		mu.Unlock()

		if requestCount > 1 {
			if r.Header.Get("If-None-Match") == "" {
				http.Error(w, "missing If-None-Match", http.StatusPreconditionRequired)
				return
			}
			mu.Lock()
			conditionalRequests++
			mu.Unlock()
			w.WriteHeader(http.StatusNotModified)
			return
		}

		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", r.URL.RequestURI()))
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.URL.Path == fmt.Sprintf("/repos/%s/%s/actions/runs", owner, repo) && r.URL.Query().Get("status") == "queued":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"total_count": 2,
				"workflow_runs": []map[string]any{
					{"id": 100, "status": "queued", "repository": map[string]any{"name": repo}},
					{"id": 200, "status": "queued", "repository": map[string]any{"name": repo}},
				},
			})
		case r.URL.Path == fmt.Sprintf("/repos/%s/%s/actions/runs", owner, repo) && r.URL.Query().Get("status") == "in_progress":
			_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 0, "workflow_runs": []any{}})
		case r.URL.Path == fmt.Sprintf("/repos/%s/%s/actions/runs/100/jobs", owner, repo):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"total_count": 1,
				"jobs": []map[string]any{
					{"id": 1, "run_id": 100, "status": "queued", "labels": []string{runnerLabel}},
				},
			})
		case r.URL.Path == fmt.Sprintf("/repos/%s/%s/actions/runs/200/jobs", owner, repo):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"total_count": 3,
				"jobs": []map[string]any{
					{"id": 2, "run_id": 200, "status": "queued", "labels": []string{runnerLabel}},
					{"id": 3, "run_id": 200, "status": "queued", "labels": []string{runnerLabel}},
					{"id": 4, "run_id": 200, "status": "queued", "labels": []string{runnerLabel}},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	scaler, err := scalers.NewGitHubRunnerScaler(&scalersconfig.ScalerConfig{
		TriggerMetadata: map[string]string{
			"githubApiURL":              server.URL,
			"runnerScope":               "repo",
			"owner":                     owner,
			"repos":                     repo,
			"labels":                    runnerLabel,
			"enableEtags":               "true",
			"targetWorkflowQueueLength": "1",
		},
		AuthParams:        map[string]string{"personalAccessToken": "test-token"},
		GlobalHTTPTimeout: time.Second,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, scaler.Close(context.Background())) }()

	readQueueLength := func() float64 {
		metrics, _, metricsErr := scaler.GetMetricsAndActivity(context.Background(), "github-runner-queue")
		require.NoError(t, metricsErr)
		require.Len(t, metrics, 1)
		return metrics[0].Value.AsApproximateFloat64()
	}

	require.Equal(t, 4.0, readQueueLength())
	require.Equal(t, 4.0, readQueueLength())
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 4, conditionalRequests)
}
