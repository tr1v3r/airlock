package airlock

import (
	"fmt"
	"os"
	"testing"
)

// The default matrix stays small. Opt in to deployment-limit dimensions with
// AIRLOCK_BENCH_LARGE=1 and select individual R/M/Q subbenchmarks as needed.
// Fixtures and their validation are excluded from timing; each operation uses
// the identical snapshot/backend time, so history cannot grow with b.N.
func benchmarkTransitionScale(b *testing.B, optimistic bool) {
	recordCounts, mappingCounts := []int{8, 128}, []int{0, 64}
	if os.Getenv("AIRLOCK_BENCH_LARGE") == "1" {
		recordCounts = append(recordCounts, 1000, 10000)
		mappingCounts = append(mappingCounts, 1000, 10000)
	}
	for _, records := range recordCounts {
		for _, mappings := range mappingCounts {
			for _, queued := range []int{0, records - 1} {
				b.Run(fmt.Sprintf("R=%d/M=%d/Q=%d", records, mappings, queued), func(b *testing.B) {
					data, now, cmd := scaleBytes(b, records, mappings, queued)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var next []byte
						var result Result
						var err error
						if optimistic {
							next, result, _, err = transitionOptimistic(data, now, cmd)
						} else {
							next, result, err = transitionSnapshot(data, now, cmd)
						}
						if err != nil || len(next) == 0 || result.Revision != 2 {
							b.Fatalf("invalid transition: %+v %v", result, err)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(len(data)), "snapshot_bytes")
				})
			}
		}
	}
}

func BenchmarkTransitionScale(b *testing.B) {
	benchmarkTransitionScale(b, false)
}

func BenchmarkTransitionOptimisticScale(b *testing.B) {
	benchmarkTransitionScale(b, true)
}
