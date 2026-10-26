package airlock

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tr1v3r/airlock/protocol"
)

// clockStateDriver uses the real state machine with a stable backend clock
// offset, not a backward jump between operations on an existing resource.
// after can lose or delay a response after its transition has committed.
type clockStateDriver struct {
	clientStateDriver
	offset time.Duration
}

func (d *clockStateDriver) executeClock(ctx context.Context, cmd protocol.Command) (protocol.Result, error) {
	d.mu.Lock()
	next, result, err := protocol.ApplySnapshot(d.state, time.Now().Add(d.offset), cmd)
	if next != nil {
		d.state = next
	}
	d.calls = append(d.calls, cmd)
	d.mu.Unlock()
	if d.after != nil {
		return d.after(ctx, cmd, result, err)
	}
	return result, err
}
func (d *clockStateDriver) Submit(ctx context.Context, r protocol.Request) (protocol.Result, error) {
	return d.executeClock(ctx, protocol.Command{Kind: protocol.Submit, Request: r})
}
func (d *clockStateDriver) Observe(ctx context.Context, r protocol.Request) (protocol.Result, error) {
	return d.executeClock(ctx, protocol.Command{Kind: protocol.Observe, Request: r})
}
func (d *clockStateDriver) Cancel(ctx context.Context, r protocol.Request) (protocol.Result, error) {
	return d.executeClock(ctx, protocol.Command{Kind: protocol.Cancel, Request: r})
}
func (d *clockStateDriver) Renew(ctx context.Context, r protocol.LeaseRef) (protocol.Result, error) {
	return d.executeClock(ctx, protocol.Command{Kind: protocol.Renew, Request: r.Request, LeaseID: r.ID})
}
func (d *clockStateDriver) Release(ctx context.Context, r protocol.LeaseRef) (protocol.Result, error) {
	return d.executeClock(ctx, protocol.Command{Kind: protocol.Release, Request: r.Request, LeaseID: r.ID})
}

func clockOptions() SharedOptions {
	o := clientOptions()
	o.Config.SubmitWindow = 10 * time.Second
	o.Config.MaxWait = 10 * time.Second
	o.Config.LeaseTTL = 5 * time.Second
	return o
}

func TestSharedClockAllowanceOffsets(t *testing.T) {
	for _, tc := range []struct {
		name      string
		offset    time.Duration
		allowance time.Duration
		want      error
	}{
		{"zero unchanged", 0, 0, nil},
		{"zero does not calibrate", -time.Second, 0, ErrInvalidRequest},
		{"client ahead", -time.Second, 1500 * time.Millisecond, nil},
		{"client behind", time.Second, 1500 * time.Millisecond, nil},
		{"allowance with aligned clocks", 0, 1500 * time.Millisecond, nil},
		{"insufficient allowance fails closed", -2 * time.Second, 1500 * time.Millisecond, ErrInvalidRequest},
		{"backend beyond submission window", 11 * time.Second, 1500 * time.Millisecond, ErrSubmitExpired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &clockStateDriver{offset: tc.offset}
			o := clockOptions()
			o.ClockSkewAllowance = tc.allowance
			c := clientNew(t, d, o)
			for attempt := 0; attempt < 3; attempt++ {
				lease, err := c.Acquire(context.Background())
				if !errors.Is(err, tc.want) {
					t.Fatalf("attempt %d: lease=%v error=%v want=%v", attempt, lease, err, tc.want)
				}
				if tc.want != nil {
					if lease != nil {
						t.Fatal("failed acquisition authorized work")
					}
					continue
				}
				if lease == nil || !lease.Valid() {
					t.Fatal("live grant missing")
				}
				if err := lease.Renew(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := lease.Release(context.Background()); err != nil {
					t.Fatal(err)
				}
				clientClosed(t, lease)
			}
		})
	}
}

func TestSharedClockAllowanceDeadlineIdentity(t *testing.T) {
	for _, early := range []bool{false, true} {
		t.Run(fmt.Sprint(early), func(t *testing.T) {
			d := &clockStateDriver{offset: -time.Second}
			o := clockOptions()
			o.ClockSkewAllowance = 1500 * time.Millisecond
			c := clientNew(t, d, o)
			ctx := context.Background()
			var callerDeadline time.Time
			if early {
				callerDeadline = time.Now().Add(4 * time.Second)
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, callerDeadline)
				defer cancel()
			}
			before := time.Now().UTC().Truncate(time.Millisecond)
			lease, err := c.Acquire(ctx)
			after := time.Now().UTC().Truncate(time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lease.Release(context.Background()) }()
			r := d.commands()[0].Request
			window := o.Config.SubmitWindow - o.ClockSkewAllowance
			if r.SubmitUntil.Before(before.Add(window)) || r.SubmitUntil.After(after.Add(window)) {
				t.Fatalf("submission budget not reserved: %v", r.SubmitUntil)
			}
			if !strings.HasPrefix(r.ID, strconv.FormatInt(r.SubmitUntil.UnixMilli(), 10)+"-") {
				t.Fatal("request identity lost its immutable deadline binding")
			}
			if early {
				if !r.WaitUntil.Equal(callerDeadline.UTC().Truncate(time.Millisecond).Add(-o.ClockSkewAllowance)) {
					t.Fatalf("caller deadline not shortened: %v", r.WaitUntil)
				}
			} else {
				wait := o.Config.MaxWait - o.ClockSkewAllowance
				if r.WaitUntil.Before(before.Add(wait)) || r.WaitUntil.After(after.Add(wait)) {
					t.Fatalf("waiting budget not reserved: %v", r.WaitUntil)
				}
			}
			if r.Config != o.Config { // This fixture has no pointer-valued priority policy.
				t.Fatal("client allowance changed the immutable resource policy")
			}
		})
	}
}

func TestSharedClockAllowanceShortContextDoesNotSubmit(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		d := &clockStateDriver{}
		o := clockOptions()
		o.ClockSkewAllowance = 1500 * time.Millisecond
		c := clientNew(t, d, o)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if cancelled {
			cancel()
		}
		lease, err := c.Acquire(ctx)
		cancel()
		want := ErrLeaseLost
		if cancelled {
			want = context.Canceled
		}
		if lease != nil || !errors.Is(err, want) || len(d.commands()) != 0 {
			t.Fatalf("cancelled=%v lease=%v error=%v commands=%d", cancelled, lease, err, len(d.commands()))
		}
	}
}

// deadlineCancellingContext exposes cancellation between Acquire's initial
// context check and its short-budget preflight, without a timer/scheduler race.
type deadlineCancellingContext struct {
	context.Context
	cancel   context.CancelFunc
	deadline time.Time
}

func (c deadlineCancellingContext) Deadline() (time.Time, bool) {
	c.cancel()
	return c.deadline, true
}

func TestSharedClockAllowanceCancellationDuringBudgetCheck(t *testing.T) {
	d := &clockStateDriver{}
	o := clockOptions()
	o.ClockSkewAllowance = 1500 * time.Millisecond
	client := clientNew(t, d, o)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := deadlineCancellingContext{Context: parent, cancel: cancel, deadline: time.Now().Add(time.Second)}
	if ctx.Err() != nil {
		t.Fatal("context cancelled before acquisition preflight")
	}
	lease, err := client.Acquire(ctx)
	if lease != nil || !errors.Is(err, context.Canceled) || errors.Is(err, ErrLeaseLost) || len(d.commands()) != 0 {
		t.Fatalf("cancellation precedence: lease=%v error=%v commands=%d", lease, err, len(d.commands()))
	}
}

// delayedDeadlineContext models a caller timer notification becoming visible
// during cleanup, after the backend has already observed the same deadline.
// Its logical clock is advanced by the fixture, so no wall-clock sleep is needed.
type delayedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c delayedDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }
func (c delayedDeadlineContext) Err() error {
	if errors.Is(context.Cause(c.Context), context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return c.Context.Err()
}

type deadlineOutcomeDriver struct {
	clockStateDriver
	expire     func()
	observeErr error
	cleanupErr error
	observed   protocol.Result
}

func (d *deadlineOutcomeDriver) Observe(ctx context.Context, r protocol.Request) (protocol.Result, error) {
	// Advance backend time to the immutable queue deadline. The real protocol.Transition
	// must expire the queued request before the caller's timer is signalled.
	d.offset = time.Until(r.WaitUntil)
	result, err := d.clockStateDriver.Observe(ctx, r)
	d.observed = result
	return result, errors.Join(err, d.observeErr)
}

func (d *deadlineOutcomeDriver) Cancel(ctx context.Context, r protocol.Request) (protocol.Result, error) {
	result, err := d.clockStateDriver.Cancel(ctx, r)
	if d.expire != nil {
		d.expire()
	}
	return result, errors.Join(err, d.cleanupErr)
}

func TestSharedExpiredResultPreservesCallerDeadline(t *testing.T) {
	lostObserve := errors.New("observe transport response lost")
	lostCleanup := errors.New("cleanup transport response lost")
	for _, tc := range []struct {
		name                   string
		expire                 bool
		observeErr, cleanupErr error
	}{
		{"caller deadline during cleanup", true, nil, nil},
		{"caller deadline and cleanup failure", true, nil, lostCleanup},
		{"backend expiry before caller timer", false, nil, nil},
		{"transport error remains original", true, lostObserve, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			deadline := time.Now().Add(20 * time.Second).UTC().Truncate(time.Millisecond)
			ctx := delayedDeadlineContext{Context: base, deadline: deadline}
			d := &deadlineOutcomeDriver{observeErr: tc.observeErr, cleanupErr: tc.cleanupErr}
			if tc.expire {
				d.expire = func() { cancel(context.DeadlineExceeded) }
			}
			o := sharedConcurrentOptions(16)
			o.Config.Admission.Capacity = 1
			o.Config.LeaseTTL = time.Minute // Holder survives the queued request's deadline.
			c := clientNew(t, d, o)
			holder := clientAcquire(t, c)
			defer func() { _ = holder.Release(context.Background()) }()
			lease, err := c.Acquire(ctx)
			if d.observed.Status != protocol.Expired {
				t.Fatalf("fixture did not observe backend expiry: %+v", d.observed)
			}
			wantLeaseLost := tc.observeErr == nil
			wantDeadline := wantLeaseLost
			if lease != nil || errors.Is(err, ErrLeaseLost) != wantLeaseLost || errors.Is(err, context.DeadlineExceeded) != wantDeadline {
				t.Fatalf("lease=%v error=%v want leaseLost=%v deadline=%v", lease, err, wantLeaseLost, wantDeadline)
			}
			if tc.observeErr != nil && !errors.Is(err, tc.observeErr) || tc.cleanupErr != nil && !errors.Is(err, tc.cleanupErr) {
				t.Fatalf("original operation/cleanup error lost: %v", err)
			}
			calls := d.commands()
			if len(calls) != 4 || calls[1].Kind != protocol.Submit || calls[2].Kind != protocol.Observe || calls[3].Kind != protocol.Cancel {
				t.Fatalf("unexpected cleanup sequence: %+v", calls)
			}
			if !calls[1].Request.WaitUntil.Equal(deadline) || calls[3].Request.ID != calls[1].Request.ID {
				t.Fatal("queue deadline or cleanup identity changed")
			}
		})
	}
}

// A waiting deadline folded from the caller's context is the caller's own
// deadline. When the backend observes that expiry first — before the caller's
// timer is even visible — the error must still report the deadline, not a bare
// ErrLeaseLost, because whichever side notices first must not change semantics.
func TestSharedFoldedDeadlineExpiryReportsCallerDeadline(t *testing.T) {
	for _, tc := range []struct {
		name         string
		withDeadline bool
	}{
		{"backend expiry before caller timer", true},
		{"unfolded window expiry keeps lease lost", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.withDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
			}
			d := &deadlineOutcomeDriver{}
			o := sharedConcurrentOptions(16)
			o.Config.Admission.Capacity = 1
			o.Config.LeaseTTL = time.Minute
			c := clientNew(t, d, o)
			holder := clientAcquire(t, c)
			defer func() { _ = holder.Release(context.Background()) }()
			lease, err := c.Acquire(ctx)
			if d.observed.Status != protocol.Expired {
				t.Fatalf("fixture did not observe backend expiry: %+v", d.observed)
			}
			wantDeadline := tc.withDeadline
			if lease != nil || !errors.Is(err, ErrLeaseLost) || errors.Is(err, context.DeadlineExceeded) != wantDeadline {
				t.Fatalf("lease=%v error=%v want leaseLost=true deadline=%v", lease, err, wantDeadline)
			}
		})
	}
}

func TestSharedClockAllowanceIsClientLocal(t *testing.T) {
	d := &clockStateDriver{}
	o := clockOptions()
	without := clientNew(t, d, o)
	o.ClockSkewAllowance = time.Second
	with := clientNew(t, d, o)
	first := clientAcquire(t, without)
	second := clientAcquire(t, with)
	if !first.Valid() || !second.Valid() {
		t.Fatal("client-local allowance prevented sharing a resource")
	}
}

func TestSharedClockAllowanceCancelledGrantCannotRevive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &clockStateDriver{offset: -time.Second}
	d.after = func(_ context.Context, cmd protocol.Command, result protocol.Result, err error) (protocol.Result, error) {
		if cmd.Kind == protocol.Submit {
			cancel() // The backend granted, but the caller no longer accepts it.
		}
		return result, err
	}
	o := clockOptions()
	o.ClockSkewAllowance = 1500 * time.Millisecond
	c := clientNew(t, d, o)
	lease, err := c.Acquire(ctx)
	if lease != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled grant: %v %v", lease, err)
	}
	calls := d.commands()
	if len(calls) != 2 || calls[1].Kind != protocol.Cancel || calls[0].Request.ID != calls[1].Request.ID {
		t.Fatal("cleanup changed the submission identity")
	}
	result, err := d.Submit(context.Background(), calls[0].Request)
	if err != nil || result.Status != protocol.Cancelled {
		t.Fatalf("late submission revived cancelled work: %+v %v", result, err)
	}
}
