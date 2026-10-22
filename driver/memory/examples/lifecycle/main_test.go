package main

import (
	"context"
	"errors"
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

func (l *fakeLease) Valid() bool                       { return !l.invalid }
func (l *fakeLease) Done() <-chan struct{}             { return l.done }
func (l *fakeLease) Renew(ctx context.Context) error   { return l.renew(ctx) }
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
