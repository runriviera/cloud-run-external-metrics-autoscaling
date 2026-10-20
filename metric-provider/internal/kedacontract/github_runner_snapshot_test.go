package kedacontract

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kedacore/keda/v2/pkg/scalers"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	"github.com/stretchr/testify/require"
)

const snapshotRunnerLabel = "gcp-ci-shared-cloud-run"

// Removing the whole-run restart would lose demand when GitHub adds a matrix
// job between the first and second pages, as observed with totals 95 and 96.
func TestGitHubRunnerRetriesGrowingJobSnapshot(t *testing.T) {
	var attempts atomic.Int64
	scaler := newSnapshotScaler(t, func(w http.ResponseWriter, r *http.Request) {
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page == 1 {
			attempts.Add(1)
		}
		total := 96
		if attempts.Load() == 1 && page == 1 {
			total = 95
		}
		writeSnapshotPage(w, page, total, fmt.Sprintf("\"attempt-%d-page-%d\"", attempts.Load(), page), false)
	})

	metrics, active, err := scaler.GetMetricsAndActivity(context.Background(), "github-runner-queue")
	require.NoError(t, err)
	require.True(t, active)
	require.Len(t, metrics, 1)
	require.Equal(t, 96.0, metrics[0].Value.AsApproximateFloat64(), "the restarted scan must include all 96 queued jobs")
	require.Equal(t, int64(2), attempts.Load(), "restart once after the changing snapshot")
}

// Accepting a partial scan, returning cached demand, or publishing failed
// attempt validators would break this no-metric/recovery contract.
func TestGitHubRunnerRejectsPersistentChurnAndRecoversNextPoll(t *testing.T) {
	var phase atomic.Int64
	var churnAttempts atomic.Int64
	var recoveredValidators atomic.Int64
	scaler := newSnapshotScaler(t, func(w http.ResponseWriter, r *http.Request) {
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		total := 95
		etag := fmt.Sprintf("\"baseline-%d\"", page)
		switch phase.Load() {
		case 1:
			if page == 1 {
				churnAttempts.Add(1)
			} else {
				total = 96
			}
			etag = fmt.Sprintf("\"failed-attempt-%d-page-%d\"", churnAttempts.Load(), page)
		case 2:
			total = 96
			if r.Header.Get("If-None-Match") == etag {
				recoveredValidators.Add(1)
			}
			etag = fmt.Sprintf("\"recovered-%d\"", page)
		}
		writeSnapshotPage(w, page, total, etag, false)
	})

	metrics, _, err := scaler.GetMetricsAndActivity(context.Background(), "github-runner-queue")
	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Equal(t, 95.0, metrics[0].Value.AsApproximateFloat64())

	phase.Store(1)
	metrics, active, err := scaler.GetMetricsAndActivity(context.Background(), "github-runner-queue")
	require.ErrorContains(t, err, "GitHub workflow job snapshot changed during pagination:")
	require.Empty(t, metrics, "unstable demand must not become zero, partial, or cached metrics")
	require.False(t, active)
	require.Equal(t, int64(2), churnAttempts.Load(), "churn must not create an unbounded retry loop")

	phase.Store(2)
	metrics, active, err = scaler.GetMetricsAndActivity(context.Background(), "github-runner-queue")
	require.NoError(t, err)
	require.True(t, active)
	require.Len(t, metrics, 1)
	require.Equal(t, 96.0, metrics[0].Value.AsApproximateFloat64())
	require.Equal(t, int64(4), recoveredValidators.Load(), "every page validator must still come from the last complete snapshot")
}

// Classifying count changes before validating the page would turn malformed
// API responses into a transient dependency warning instead of an internal error.
func TestGitHubRunnerDoesNotRetryMalformedChangingPage(t *testing.T) {
	for _, test := range []struct {
		name       string
		total      int
		invalidJob bool
	}{
		{name: "negative count", total: -1},
		{name: "count exceeds scan bound", total: 30001},
		{name: "invalid job with changed count", total: 96, invalidJob: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var attempts atomic.Int64
			scaler := newSnapshotScaler(t, func(w http.ResponseWriter, r *http.Request) {
				page, _ := strconv.Atoi(r.URL.Query().Get("page"))
				if page == 1 {
					attempts.Add(1)
					writeSnapshotPage(w, page, 95, "\"first\"", false)
					return
				}
				writeSnapshotPage(w, page, test.total, "\"malformed\"", test.invalidJob)
			})

			metrics, active, err := scaler.GetMetricsAndActivity(context.Background(), "github-runner-queue")
			require.Error(t, err)
			require.NotContains(t, err.Error(), "GitHub workflow job snapshot changed during pagination:")
			require.Empty(t, metrics)
			require.False(t, active)
			require.Equal(t, int64(1), attempts.Load(), "malformed payloads must fail immediately")
		})
	}
}

func newSnapshotScaler(t *testing.T, jobsHandler http.HandlerFunc) scalers.Scaler {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/runriviera/os/actions/runs":
			runs := []map[string]any{}
			if r.URL.Query().Get("status") == "queued" {
				runs = append(runs, map[string]any{"id": 400, "status": "queued", "repository": map[string]any{"name": "os"}})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"total_count": len(runs), "workflow_runs": runs})
		case "/repos/runriviera/os/actions/runs/400/jobs":
			jobsHandler(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	scaler, err := scalers.NewGitHubRunnerScaler(&scalersconfig.ScalerConfig{
		TriggerMetadata: map[string]string{
			"githubApiURL": server.URL, "runnerScope": "repo", "owner": "runriviera", "repos": "os",
			"labels": snapshotRunnerLabel, "enableEtags": "true", "targetWorkflowQueueLength": "1",
		},
		AuthParams: map[string]string{"personalAccessToken": "test-token"}, GlobalHTTPTimeout: time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, scaler.Close(context.Background())) })
	return scaler
}

func writeSnapshotPage(w http.ResponseWriter, page, total int, etag string, invalidJob bool) {
	w.Header().Set("ETag", etag)
	jobs := []map[string]any{}
	for id := (page-1)*30 + 1; id <= min(page*30, total); id++ {
		jobID := id
		if invalidJob && len(jobs) == 0 {
			jobID = 0
		}
		jobs = append(jobs, map[string]any{"id": jobID, "run_id": 400, "status": "queued", "labels": []string{snapshotRunnerLabel}})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"total_count": total, "jobs": jobs})
}
