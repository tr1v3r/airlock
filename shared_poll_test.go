package airlock

import (
	"testing"
	"time"
)

func TestPollDelayBounds(t *testing.T) {
	for _, interval := range []time.Duration{time.Nanosecond, 4 * time.Nanosecond, 5 * time.Nanosecond, 11 * time.Nanosecond, time.Millisecond, 50 * time.Millisecond, time.Minute} {
		t.Run(interval.String(), func(t *testing.T) {
			jitter := interval / 5
			for range 1000 {
				delay := pollDelay(interval)
				if delay <= 0 || delay < interval-jitter || delay > interval+jitter {
					t.Fatalf("delay %s outside [%s, %s]", delay, interval-jitter, interval+jitter)
				}
			}
		})
	}
}
