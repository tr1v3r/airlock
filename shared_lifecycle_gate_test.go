package airlock

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSharedReleaseCancelsGateWaitAndInvalidatesInFlightRenew(t *testing.T) {
	entered, finish := make(chan struct{}), make(chan struct{})
	d := &clientStateDriver{after: func(ctx context.Context, cmd Command, result Result, err error) (Result, error) {
		if cmd.Kind == Renew {
			close(entered)
			select {
			case <-finish:
			case <-ctx.Done():
				return Result{}, ctx.Err()
			}
		}
		return result, err
	}}
	o := clientOptions()
	o.OperationTimeout = 3 * time.Second
	o.Config.LeaseTTL = 5 * time.Second
	l := clientAcquire(t, clientNew(t, d, o))
	renewed := make(chan error, 1)
	go func() { renewed <- l.Renew(context.Background()) }()
	clientAwait(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	released := make(chan error, 1)
	go func() { released <- l.Release(ctx) }()
	// Release closes validity before acquiring the occupied operation gate.
	clientAwait(t, l.Done())
	clientClosed(t, l)
	cancel()
	select {
	case err := <-released:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("release ignored cancellation while waiting for renewal")
	}
	for _, cmd := range d.commands() {
		if cmd.Kind == Release {
			t.Fatal("cancelled release reached backend before renewal finished")
		}
	}
	close(finish)
	if err := <-renewed; !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("late renewal resurrected lease: %v", err)
	}
	clientClosed(t, l)
	// A fresh cleanup attempt can still return backend capacity.
	if err := l.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSharedLeaseGateWaitingIsBounded(t *testing.T) {
	for _, op := range []string{"renew", "release"} {
		for _, bound := range []string{"caller", "operation"} {
			t.Run(op+"/"+bound, func(t *testing.T) {
				o := clientOptions()
				o.OperationTimeout = 20 * time.Millisecond
				d := &clientStateDriver{}
				l := clientAcquire(t, clientNew(t, d, o))
				l.operations <- struct{}{} // An independently controlled in-flight operation.
				defer func() { <-l.operations }()
				ctx := context.Background()
				if bound == "caller" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				returned := make(chan error, 1)
				go func() {
					if op == "renew" {
						returned <- l.Renew(ctx)
					} else {
						returned <- l.Release(ctx)
					}
				}()
				want := context.DeadlineExceeded
				if bound == "caller" {
					want = context.Canceled
				}
				select {
				case err := <-returned:
					if !errors.Is(err, want) {
						t.Fatalf("got %v want %v", err, want)
					}
				case <-time.After(time.Second):
					t.Fatal("lease operation blocked outside its time budget")
				}
				clientClosed(t, l)
				if len(d.commands()) != 1 {
					t.Fatal("failed gate wait called driver")
				}
			})
		}
	}
}
