package scaling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/go-logr/logr/funcr"
	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	"github.com/kedacore/keda/v2/pkg/scaling/cache"
	"github.com/stretchr/testify/require"
)

func TestStateProviderLogsOnlyCallerCancellationAsInfo(t *testing.T) {
	for _, test := range []struct {
		name        string
		cancel      bool
		scalerErr   error
		information bool
	}{
		{name: "canceled caller", cancel: true, scalerErr: context.Canceled, information: true},
		{name: "wrapped canceled caller", cancel: true, scalerErr: fmt.Errorf("read jobs: %w", context.Canceled), information: true},
		{name: "cancellation error from active caller", scalerErr: context.Canceled},
		{name: "genuine fault while caller canceled", cancel: true, scalerErr: errors.New("invalid GitHub jobs payload")},
		{name: "deadline error while caller canceled", cancel: true, scalerErr: context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if test.cancel {
				cancel()
			}
			var mu sync.Mutex
			var entries []map[string]any
			var decodeErrors []error
			logger := funcr.NewJSON(func(line string) {
				var entry map[string]any
				err := json.Unmarshal([]byte(line), &entry)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					decodeErrors = append(decodeErrors, err)
					return
				}
				entries = append(entries, entry)
			}, funcr.Options{})
			provider := NewStateProvider(&logger)
			scaledObject := &kedav1alpha1.ScaledObject{
				Spec: kedav1alpha1.ScaledObjectSpec{
					ScaleTargetRef: &kedav1alpha1.ScaleTarget{Name: "test-runners"},
					Triggers:       []kedav1alpha1.ScaleTriggers{{Type: "github-runner"}},
				},
			}

			state, err := provider.GetScaledObjectState(ctx, scaledObject, []cache.ScalerBuilder{{
				Scaler:       &mockScaler{err: test.scalerErr},
				ScalerConfig: scalersconfig.ScalerConfig{TriggerIndex: 0},
			}})

			require.ErrorContains(t, err, "failed to retrieve any metrics for scaling")
			require.Empty(t, state.MetricAndTargetValues, "cancellation must not publish a zero or partial metric")
			require.False(t, state.IsActive)
			mu.Lock()
			defer mu.Unlock()
			require.Empty(t, decodeErrors)
			require.Len(t, entries, 1)
			_, information := entries[0]["level"] // funcr includes the level only on Info records.
			require.Equal(t, test.information, information, "only matching caller cancellation may lower log severity")
		})
	}
}
