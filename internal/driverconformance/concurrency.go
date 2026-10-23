package driverconformance

import (
	"context"
	"errors"
	"os"

	"github.com/tr1v3r/airlock"
)

type concurrentOutcome struct {
	result airlock.Result
	err    error
}

// raceCommands releases a bounded group together and joins every worker before
// assertions. Workers do not call Fatal, and results retain the command's index.
func raceCommands(h *harness, commands []airlock.Command) []concurrentOutcome {
	outcomes := make([]concurrentOutcome, len(commands))
	type indexedOutcome struct {
		index int
		concurrentOutcome
	}
	completed := make(chan indexedOutcome, len(commands))
	start := make(chan struct{})
	for i, command := range commands {
		go func(i int, command airlock.Command) {
			<-start
			result, err := h.execute(command.Kind, command.Request, command.LeaseID)
			completed <- indexedOutcome{i, concurrentOutcome{result, err}}
		}(i, command)
	}
	close(start)
	for range commands {
		outcome := <-completed
		outcomes[outcome.index] = outcome.concurrentOutcome
	}
	return outcomes
}

func raceFanout() int {
	if os.Getenv("AIRLOCK_STRESS") == "1" {
		return 16
	}
	return 8
}

func repeatedCommand(command airlock.Command, count int) []airlock.Command {
	commands := make([]airlock.Command, count)
	for i := range commands {
		commands[i] = command
	}
	return commands
}

func concurrentIdentity(h *harness) {
	r := h.request("holder", h.cfg.Admission.Capacity)
	outcomes := raceCommands(h, repeatedCommand(airlock.Command{Kind: airlock.Submit, Request: r}, raceFanout()))
	lease := outcomes[0].result.LeaseID
	revisions := make(map[uint64]bool)
	for i, outcome := range outcomes {
		got := outcome.result
		if outcome.err != nil || got.Status != airlock.Granted || lease == "" || got.LeaseID != lease || got.Weight != r.Weight || got.Remaining <= 0 || got.Remaining > h.cfg.LeaseTTL || got.Revision == 0 || revisions[got.Revision] {
			h.t.Fatalf("concurrent Submit[%d]: %+v err=%v; original lease=%q", i, got, outcome.err, lease)
		}
		revisions[got.Revision] = true
	}
	// A duplicate Submit must neither consume capacity again nor mint another
	// lease. Duplicate Release must not free the successor's capacity.
	next, last := h.request("next", r.Weight), h.request("last", r.Weight)
	h.must(airlock.Submit, next, "", airlock.Queued)
	h.must(airlock.Submit, last, "", airlock.Queued)
	for i, outcome := range raceCommands(h, repeatedCommand(airlock.Command{Kind: airlock.Release, Request: r, LeaseID: lease}, raceFanout())) {
		if outcome.err != nil || outcome.result.Status != airlock.Released || outcome.result.LeaseID != lease || outcome.result.Weight != r.Weight {
			h.t.Fatalf("concurrent Release[%d]: %+v err=%v", i, outcome.result, outcome.err)
		}
	}
	g := h.must(airlock.Observe, next, "", airlock.Granted)
	h.must(airlock.Observe, last, "", airlock.Queued)
	h.must(airlock.Release, next, g.LeaseID, airlock.Released)
	g = h.must(airlock.Observe, last, "", airlock.Granted)
	h.must(airlock.Release, last, g.LeaseID, airlock.Released)
}

func concurrentOwnerConflict(h *harness) {
	r := h.request("holder", h.cfg.Admission.Capacity)
	grant := h.must(airlock.Submit, r, "", airlock.Granted)
	wrong := r
	wrong.Owner = h.request("identity-only", 1).Owner
	operations := []airlock.Operation{airlock.Submit, airlock.Observe, airlock.Cancel, airlock.Renew, airlock.Release}
	commands := make([]airlock.Command, 0, 8)
	for _, kind := range operations {
		commands = append(commands, airlock.Command{Kind: kind, Request: wrong, LeaseID: grant.LeaseID})
	}
	for range 3 {
		commands = append(commands, airlock.Command{Kind: airlock.Submit, Request: r})
	}
	for i, outcome := range raceCommands(h, commands) {
		if i < len(operations) {
			if !errors.Is(outcome.err, airlock.ErrRequestConflict) || outcome.result != (airlock.Result{}) {
				h.t.Fatalf("wrong owner %s: %+v err=%v", operations[i], outcome.result, outcome.err)
			}
		} else if outcome.err != nil || outcome.result.Status != airlock.Granted || outcome.result.LeaseID != grant.LeaseID {
			h.t.Fatalf("valid replay: %+v err=%v", outcome.result, outcome.err)
		}
	}
	observed := h.must(airlock.Observe, r, "", airlock.Granted)
	if observed.LeaseID != grant.LeaseID || observed.Remaining > grant.Remaining {
		h.t.Fatalf("conflicting commands changed lease: before=%+v after=%+v", grant, observed)
	}
	h.must(airlock.Submit, h.request("blocked", r.Weight), "", airlock.Queued)
	h.must(airlock.Release, r, grant.LeaseID, airlock.Released)
}

func concurrentCancellation(h *harness) {
	for _, before := range []bool{true, false} {
		r := h.request("cancelled", h.cfg.Admission.Capacity)
		if before {
			h.must(airlock.Cancel, r, "", airlock.Cancelled)
		}
		commands := repeatedCommand(airlock.Command{Kind: airlock.Submit, Request: r}, raceFanout())
		for i := 0; i < len(commands); i += 2 {
			commands[i].Kind = airlock.Cancel
		}
		for i, outcome := range raceCommands(h, commands) {
			wantCancelled := before || commands[i].Kind == airlock.Cancel
			if outcome.err != nil || outcome.result.Weight != r.Weight || (wantCancelled && outcome.result.Status != airlock.Cancelled) || (!wantCancelled && outcome.result.Status != airlock.Cancelled && outcome.result.Status != airlock.Granted) {
				h.t.Fatalf("cancel-before=%t command=%s: %+v err=%v", before, commands[i].Kind, outcome.result, outcome.err)
			}
		}
		h.must(airlock.Submit, r, "", airlock.Cancelled)
		h.must(airlock.Observe, r, "", airlock.Cancelled)
		fresh := h.request("replacement", r.Weight)
		grant := h.must(airlock.Submit, fresh, "", airlock.Granted)
		h.must(airlock.Release, fresh, grant.LeaseID, airlock.Released)
	}
}

func concurrentRenewRelease(h *harness) {
	r := h.request("holder", h.cfg.Admission.Capacity)
	grant := h.must(airlock.Submit, r, "", airlock.Granted)
	next, last := h.request("next", r.Weight), h.request("last", r.Weight)
	h.must(airlock.Submit, next, "", airlock.Queued)
	h.must(airlock.Submit, last, "", airlock.Queued)
	commands := repeatedCommand(airlock.Command{Kind: airlock.Renew, Request: r, LeaseID: grant.LeaseID}, raceFanout())
	for i := 0; i < len(commands); i += 2 {
		commands[i].Kind = airlock.Release
	}
	for i, outcome := range raceCommands(h, commands) {
		got := outcome.result
		if commands[i].Kind == airlock.Release {
			if outcome.err != nil || got.Status != airlock.Released {
				h.t.Fatalf("Release racing Renew: %+v err=%v", got, outcome.err)
			}
		} else {
			renewSucceeded := outcome.err == nil && got.Status == airlock.Granted && got.Remaining > 0 && got.Remaining <= h.cfg.LeaseTTL
			releasedWon := errors.Is(outcome.err, airlock.ErrLeaseLost) && got.Status == airlock.Released
			if !renewSucceeded && !releasedWon {
				h.t.Fatalf("Renew racing Release: %+v err=%v", got, outcome.err)
			}
		}
		if got.LeaseID != grant.LeaseID || got.Weight != r.Weight {
			h.t.Fatalf("race changed identity/accounting: %+v", got)
		}
	}
	h.must(airlock.Observe, r, "", airlock.Released)
	h.wantError(airlock.Renew, r, grant.LeaseID, airlock.ErrLeaseLost)
	g := h.must(airlock.Observe, next, "", airlock.Granted)
	h.must(airlock.Observe, last, "", airlock.Queued)
	h.must(airlock.Release, next, g.LeaseID, airlock.Released)
	g = h.must(airlock.Observe, last, "", airlock.Granted)
	h.must(airlock.Release, last, g.LeaseID, airlock.Released)
}

func cancelledOperations(h *harness) {
	r := h.request("holder", h.cfg.Admission.Capacity)
	grant := h.must(airlock.Submit, r, "", airlock.Granted)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, kind := range []airlock.Operation{airlock.Submit, airlock.Observe, airlock.Cancel, airlock.Renew, airlock.Release} {
		got, err := Invoke(ctx, h.driver, airlock.Command{Kind: kind, Request: r, LeaseID: grant.LeaseID})
		if !errors.Is(err, context.Canceled) || got != (airlock.Result{}) {
			h.t.Fatalf("pre-cancelled %s: %+v err=%v", kind, got, err)
		}
	}
	got := h.must(airlock.Observe, r, "", airlock.Granted)
	if got.LeaseID != grant.LeaseID || got.Remaining > grant.Remaining || got.Revision != grant.Revision+1 {
		h.t.Fatalf("pre-cancelled commands mutated state: before=%+v after=%+v", grant, got)
	}
	h.must(airlock.Release, r, grant.LeaseID, airlock.Released)
}
