package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

type fakeLease struct {
	done    chan struct{}
	invalid bool
	renew   func(context.Context) error
	release func(context.Context) error
}

func (l *fakeLease) Valid() bool { return !l.invalid }

func (l *fakeLease) Done() <-chan struct{} { return l.done }

func (l *fakeLease) Renew(ctx context.Context) error { return l.renew(ctx) }

func (l *fakeLease) Release(ctx context.Context) error { return l.release(ctx) }

func testLease(t *testing.T) *fakeLease {
	t.Helper()
	return &fakeLease{
		done:  make(chan struct{}),
		renew: func(context.Context) error { return nil },
		release: func(ctx context.Context) error {
			if ctx.Err() != nil {
				t.Errorf("cleanup inherited cancellation: %v", ctx.Err())
			}

			if _, ok := ctx.Deadline(); !ok {
				t.Error("cleanup has no deadline")
			}
			return nil
		},
	}
}

func TestJobRenewsAndReleasesAfterWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	lease := testLease(t)
	renewed := make(chan struct{}, 1)
	lease.renew = func(context.Context) error {
		select {
		case renewed <- struct{}{}:
		default:
		}

		return nil
	}
	stopped, releases := false, 0
	lease.release = func(context.Context) error {
		if !stopped {
			t.Error("released before work stopped")
		}

		releases++
		return nil
	}
	err := runJob(ctx, lease, time.Millisecond, time.Second, func(ctx context.Context) error {
		defer func() { stopped = true }()

		select {
		case <-renewed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil || releases != 1 {
		t.Fatalf("err=%v releases=%d", err, releases)
	}
}

func TestJobLeaseLossCancelsBlockedRenewalBeforeRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	lease := testLease(t)
	started := make(chan struct{})
	var renewalStopped atomic.Bool
	lease.renew = func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		renewalStopped.Store(true)
		return ctx.Err()
	}
	workStopped := false
	lease.release = func(ctx context.Context) error {
		if !workStopped || !renewalStopped.Load() {
			t.Error("release raced work or renewal")
		}

		if ctx.Err() != nil {
			t.Error("cancelled cleanup")
		}
		return nil
	}
	err := runJob(ctx, lease, time.Millisecond, time.Second, func(ctx context.Context) error {
		defer func() { workStopped = true }()

		select {
		case <-started:
			close(lease.done)
		case <-ctx.Done():
			return ctx.Err()
		}

		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, airlock.ErrLeaseLost) {
		t.Fatal(err)
	}
}

func TestJobRenewalErrorStopsWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	lease := testLease(t)
	lostReply := errors.New("renew response lost")
	lease.renew = func(context.Context) error { return lostReply }
	err := runJob(ctx, lease, time.Millisecond, time.Second, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, lostReply) {
		t.Fatal(err)
	}
}

func TestJobPreservesRenewErrorWhenDoneClosesFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	lease := testLease(t)
	lostReply := errors.New("renew committed but response was lost")
	lease.renew = func(ctx context.Context) error {
		close(lease.done)
		<-ctx.Done() // Force the Done watcher to win before returning the error.
		return lostReply
	}
	err := runJob(ctx, lease, time.Millisecond, time.Second, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, lostReply) || !errors.Is(err, airlock.ErrLeaseLost) {
		t.Fatalf("lost renewal failure: %v", err)
	}
}

func TestJobWorkFinishedPreservesLateRenewalFailure(t *testing.T) {
	lostReply := errors.New("renew committed but response was lost")

	for _, tc := range []struct {
		name     string
		renewErr error
		failed   bool
	}{
		{"cancelled cleanup", context.Canceled, false},
		{"wrapped cancelled cleanup", fmt.Errorf("rpc: %w", context.Canceled), false},
		{"joined cancelled cleanup", errors.Join(context.Canceled, fmt.Errorf("rpc: %w", context.Canceled)), false},
		{"lost backend reply", lostReply, true},
		{"joined cancellation and backend failure", errors.Join(context.Canceled, lostReply), true},
		{"wrapped mixed failure", fmt.Errorf("rpc: %w", errors.Join(fmt.Errorf("cancel: %w", context.Canceled), lostReply)), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			lease := testLease(t)
			started := make(chan struct{})
			var joined atomic.Bool
			lease.renew = func(ctx context.Context) error {
				close(started)
				<-ctx.Done() // Work returns first; defer cancels with errWorkFinished.
				joined.Store(true)
				return tc.renewErr
			}
			releases := 0
			lease.release = func(ctx context.Context) error {
				if !joined.Load() || ctx.Err() != nil {
					t.Error("release did not join renewal with an independent context")
				}

				releases++
				return nil
			}
			err := runJob(ctx, lease, time.Millisecond, time.Second, func(ctx context.Context) error {
				select {
				case <-started:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			if errors.Is(err, lostReply) != tc.failed || (!tc.failed && err != nil) || releases != 1 {
				t.Fatalf("failed=%v error=%v releases=%d", tc.failed, err, releases)
			}
		})
	}
}

type testErrorGroup []error

func (e testErrorGroup) Error() string { return "test error group" }

func (e testErrorGroup) Unwrap() []error { return e }

func TestOnlyContextError(t *testing.T) {
	backend := errors.New("backend unavailable")

	for _, tc := range []struct {
		name        string
		err, target error
		want        bool
	}{
		{"nil error", nil, context.Canceled, false},
		{"nil target", context.Canceled, nil, false},
		{"empty group", testErrorGroup{}, context.Canceled, false},
		{"nil leaf", testErrorGroup{nil}, context.Canceled, false},
		{"bare cancellation", context.Canceled, context.Canceled, true},
		{"single wrapper", fmt.Errorf("rpc: %w", context.Canceled), context.Canceled, true},
		{"pure tree", fmt.Errorf("rpc: %w", errors.Join(context.Canceled, fmt.Errorf("inner: %w", context.Canceled))), context.Canceled, true},
		{"mixed tree", errors.Join(context.Canceled, backend), context.Canceled, false},
		{"deadline not cancellation", context.DeadlineExceeded, context.Canceled, false},
		{"parent deadline", fmt.Errorf("rpc: %w", context.DeadlineExceeded), context.DeadlineExceeded, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := onlyContextError(tc.err, tc.target); got != tc.want {
				t.Fatalf("onlyContextError(%v, %v)=%v want=%v", tc.err, tc.target, got, tc.want)
			}
		})
	}
}

func TestJobParentCancellationAndInvalidLease(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		lease := testLease(t)
		lease.invalid = invalid
		called := false
		err := runJob(ctx, lease, time.Hour, time.Second, func(ctx context.Context) error {
			called = true
			cancel()
			<-ctx.Done()
			return ctx.Err()
		})
		cancel()
		want := context.Canceled
		if invalid {
			want = airlock.ErrLeaseLost
		}

		if !errors.Is(err, want) || called == invalid {
			t.Fatalf("invalid=%v called=%v err=%v", invalid, called, err)
		}
	}
}

func TestJobReleaseRetryKeepsIdentityAndDoesNotRetryWork(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		lease := testLease(t)
		lostReply, workError := errors.New("release response lost"), errors.New("work failed")
		attempts, workCalls := 0, 0
		var previous context.Context
		lease.release = func(ctx context.Context) error {
			attempts++
			if ctx.Err() != nil || ctx == previous {
				t.Error("cleanup did not get a fresh context")
			}

			previous = ctx
			if persistent || attempts == 1 {
				return lostReply
			}
			return nil
		}
		err := runJob(ctx, lease, time.Hour, time.Second, func(context.Context) error {
			workCalls++
			cancel()
			return workError
		})
		cancel()
		if !errors.Is(err, workError) || errors.Is(err, lostReply) != persistent || attempts != 2 || workCalls != 1 {
			t.Fatalf("persistent=%v err=%v attempts=%d work calls=%d", persistent, err, attempts, workCalls)
		}
	}
}
