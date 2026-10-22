// This application-owned example demonstrates lease lifecycle management without
// an external backend. Ordinary in-process admission should use airlock.New.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/driver/memory"
)

func main() {
	if err := demo(); err != nil {
		log.Fatal(err)
	}
}

func demo() error {
	client, err := airlock.NewShared(memory.New(), airlock.SharedOptions{
		Resource: "lifecycle-example",
		Config: airlock.ResourceConfig{
			Admission: airlock.Config{Capacity: 1},
			LeaseTTL:  time.Second, // Shortened only to demonstrate renewals quickly.
		},
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lease, err := client.Acquire(ctx)
	if err != nil {
		return err
	}
	// In a remote deployment, choose renewal and I/O bounds with ample margin
	// before LeaseTTL. The ordinary default TTL is 30s, not this demo's 1s.
	err = runJob(ctx, lease, 250*time.Millisecond, 100*time.Millisecond, func(ctx context.Context) error {
		fmt.Println("work started")
		timer := time.NewTimer(1200 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			fmt.Println("work stopped")
			return ctx.Err()
		case <-timer.C:
			fmt.Println("work finished")
			return nil
		}
	})
	if err == nil {
		fmt.Println("lease released")
	}
	return err
}

// A small interface lets the example test failure ordering without a server.
// *airlock.Lease implements it; this helper is application code, not an SDK API.
type jobLease interface {
	Valid() bool
	Done() <-chan struct{}
	Renew(context.Context) error
	Release(context.Context) error
}

var errWorkFinished = errors.New("example: work finished")

// runJob executes work exactly once. Work must honor its context and return only
// after ALL its child tasks have stopped. Context cancellation cannot kill code
// that ignores it. The library itself does not start these application workers.
func runJob(ctx context.Context, lease jobLease, renewEvery, operationTimeout time.Duration, work func(context.Context) error) (err error) {
	workCtx, cancel := context.WithCancelCause(ctx)
	watchDone, renewDone := make(chan struct{}), make(chan struct{})
	var renewalErr error // Read only after joining renewDone.

	// Keep observing expiry even while a renewal is blocked on network I/O.
	go func() {
		defer close(watchDone)
		select {
		case <-lease.Done():
			cancel(airlock.ErrLeaseLost)
		case <-workCtx.Done():
		}
	}()
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(renewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
				if workCtx.Err() != nil {
					return
				}
				op, stop := context.WithTimeout(workCtx, operationTimeout)
				err := lease.Renew(op)
				stop()
				if err != nil {
					renewalErr = fmt.Errorf("renew lease: %w", err)
					cancel(renewalErr)
					return
				}
				fmt.Println("lease renewed")
			}
		}
	}()

	defer func() {
		// Work has returned before this cleanup runs. Stop and JOIN both
		// supervisors before releasing; never release underneath live work.
		cancel(errWorkFinished)
		<-watchDone
		<-renewDone
		if cause := context.Cause(workCtx); !errors.Is(cause, errWorkFinished) {
			if !errors.Is(err, cause) {
				err = errors.Join(err, cause)
			}
			// Renew can close Done before returning its error. Preserve the
			// backend failure even if the watcher won the cancellation race.
			if renewalErr != nil && !errors.Is(renewalErr, workCtx.Err()) && !errors.Is(err, renewalErr) {
				err = errors.Join(err, renewalErr)
			}
		}
		err = errors.Join(err, releaseStoppedJob(ctx, lease, operationTimeout))
	}()
	if !lease.Valid() {
		cancel(airlock.ErrLeaseLost)
	}
	if workCtx.Err() != nil {
		return context.Cause(workCtx)
	}
	return work(workCtx)
}

func releaseStoppedJob(ctx context.Context, lease jobLease, timeout time.Duration) error {
	var failures []error
	// A failed release may already have committed. Retry cleanup once using the
	// SAME lease identity; never acquire a new lease or rerun the business task.
	for attempt := 0; attempt < 2; attempt++ {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		err := lease.Release(cleanup)
		cancel()
		if err == nil {
			return nil
		}
		failures = append(failures, err)
	}
	return fmt.Errorf("release outcome unknown after two attempts: %w", errors.Join(failures...))
}
