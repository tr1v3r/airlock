package driverconformance

import (
	"context"
	"errors"
	"os"

	"github.com/tr1v3r/airlock/protocol"
)

// concurrentOutcome keeps a command result paired with its error for race assertions.
type concurrentOutcome struct {
	// result is the protocol reply from one concurrent invocation.
	result protocol.Result
	// err retains that invocation's backend or business error.
	err error
}

// raceCommands releases a bounded group together and joins every worker before
// assertions. Workers do not call Fatal, and results retain the command's index.
func raceCommands(h *harness, commands []protocol.Command) []concurrentOutcome {
	outcomes := make([]concurrentOutcome, len(commands))
	type indexedOutcome struct {
		index int
		concurrentOutcome
	}
	completed := make(chan indexedOutcome, len(commands))
	start := make(chan struct{})
	for i, command := range commands {
		go func(i int, command protocol.Command) {
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

// raceFanout chooses the bounded contender count for ordinary or stress checks.
func raceFanout() int {
	if os.Getenv("AIRLOCK_STRESS") == "1" {
		return 16
	}
	return 8
}

// repeatedCommand copies an immutable command for simultaneous replay tests.
func repeatedCommand(command protocol.Command, count int) []protocol.Command {
	commands := make([]protocol.Command, count)
	for i := range commands {
		commands[i] = command
	}
	return commands
}

// concurrentIdentity checks duplicate submissions/releases preserve identity and token accounting.
func concurrentIdentity(h *harness) {
	r := h.request("holder", h.cfg.Admission.Capacity)
	outcomes := raceCommands(h, repeatedCommand(protocol.Command{Kind: protocol.Submit, Request: r}, raceFanout()))
	lease := outcomes[0].result.LeaseID
	revisions := make(map[uint64]bool)
	for i, outcome := range outcomes {
		got := outcome.result
		if outcome.err != nil || got.Status != protocol.Granted || lease == "" || got.LeaseID != lease || got.Weight != r.Weight || got.Remaining <= 0 || got.Remaining > h.cfg.LeaseTTL || got.Revision == 0 || revisions[got.Revision] {
			h.t.Fatalf("concurrent Submit[%d]: %+v err=%v; original lease=%q", i, got, outcome.err, lease)
		}
		revisions[got.Revision] = true
	}
	// A duplicate Submit must neither consume capacity again nor mint another
	// lease. Duplicate Release must not free the successor's capacity.
	next, last := h.request("next", r.Weight), h.request("last", r.Weight)
	h.must(protocol.Submit, next, "", protocol.Queued)
	h.must(protocol.Submit, last, "", protocol.Queued)
	for i, outcome := range raceCommands(h, repeatedCommand(protocol.Command{Kind: protocol.Release, Request: r, LeaseID: lease}, raceFanout())) {
		if outcome.err != nil || outcome.result.Status != protocol.Released || outcome.result.LeaseID != lease || outcome.result.Weight != r.Weight {
			h.t.Fatalf("concurrent Release[%d]: %+v err=%v", i, outcome.result, outcome.err)
		}
	}
	g := h.must(protocol.Observe, next, "", protocol.Granted)
	h.must(protocol.Observe, last, "", protocol.Queued)
	h.must(protocol.Release, next, g.LeaseID, protocol.Released)
	g = h.must(protocol.Observe, last, "", protocol.Granted)
	h.must(protocol.Release, last, g.LeaseID, protocol.Released)
}

// concurrentOwnerConflict verifies conflicting owners cannot mutate an existing grant.
func concurrentOwnerConflict(h *harness) {
	r := h.request("holder", h.cfg.Admission.Capacity)
	grant := h.must(protocol.Submit, r, "", protocol.Granted)
	wrong := r
	wrong.Owner = h.request("identity-only", 1).Owner
	operations := []protocol.Operation{protocol.Submit, protocol.Observe, protocol.Cancel, protocol.Renew, protocol.Release}
	commands := make([]protocol.Command, 0, 8)
	for _, kind := range operations {
		commands = append(commands, protocol.Command{Kind: kind, Request: wrong, LeaseID: grant.LeaseID})
	}
	for range 3 {
		commands = append(commands, protocol.Command{Kind: protocol.Submit, Request: r})
	}
	for i, outcome := range raceCommands(h, commands) {
		if i < len(operations) {
			if !errors.Is(outcome.err, protocol.ErrRequestConflict) || outcome.result != (protocol.Result{}) {
				h.t.Fatalf("wrong owner %s: %+v err=%v", operations[i], outcome.result, outcome.err)
			}
		} else if outcome.err != nil || outcome.result.Status != protocol.Granted || outcome.result.LeaseID != grant.LeaseID {
			h.t.Fatalf("valid replay: %+v err=%v", outcome.result, outcome.err)
		}
	}
	observed := h.must(protocol.Observe, r, "", protocol.Granted)
	if observed.LeaseID != grant.LeaseID || observed.Remaining > grant.Remaining {
		h.t.Fatalf("conflicting commands changed lease: before=%+v after=%+v", grant, observed)
	}
	h.must(protocol.Submit, h.request("blocked", r.Weight), "", protocol.Queued)
	h.must(protocol.Release, r, grant.LeaseID, protocol.Released)
}

// concurrentCancellation checks submission/cancellation races converge on a terminal tombstone.
func concurrentCancellation(h *harness) {
	for _, before := range []bool{true, false} {
		r := h.request("cancelled", h.cfg.Admission.Capacity)
		if before {
			h.must(protocol.Cancel, r, "", protocol.Cancelled)
		}
		commands := repeatedCommand(protocol.Command{Kind: protocol.Submit, Request: r}, raceFanout())
		for i := 0; i < len(commands); i += 2 {
			commands[i].Kind = protocol.Cancel
		}
		for i, outcome := range raceCommands(h, commands) {
			wantCancelled := before || commands[i].Kind == protocol.Cancel
			if outcome.err != nil || outcome.result.Weight != r.Weight || (wantCancelled && outcome.result.Status != protocol.Cancelled) || (!wantCancelled && outcome.result.Status != protocol.Cancelled && outcome.result.Status != protocol.Granted) {
				h.t.Fatalf("cancel-before=%t command=%s: %+v err=%v", before, commands[i].Kind, outcome.result, outcome.err)
			}
		}
		h.must(protocol.Submit, r, "", protocol.Cancelled)
		h.must(protocol.Observe, r, "", protocol.Cancelled)
		fresh := h.request("replacement", r.Weight)
		grant := h.must(protocol.Submit, fresh, "", protocol.Granted)
		h.must(protocol.Release, fresh, grant.LeaseID, protocol.Released)
	}
}

// concurrentRenewRelease verifies release cannot be undone by a racing renewal.
func concurrentRenewRelease(h *harness) {
	r := h.request("holder", h.cfg.Admission.Capacity)
	grant := h.must(protocol.Submit, r, "", protocol.Granted)
	next, last := h.request("next", r.Weight), h.request("last", r.Weight)
	h.must(protocol.Submit, next, "", protocol.Queued)
	h.must(protocol.Submit, last, "", protocol.Queued)
	commands := repeatedCommand(protocol.Command{Kind: protocol.Renew, Request: r, LeaseID: grant.LeaseID}, raceFanout())
	for i := 0; i < len(commands); i += 2 {
		commands[i].Kind = protocol.Release
	}
	for i, outcome := range raceCommands(h, commands) {
		got := outcome.result
		if commands[i].Kind == protocol.Release {
			if outcome.err != nil || got.Status != protocol.Released {
				h.t.Fatalf("Release racing Renew: %+v err=%v", got, outcome.err)
			}
		} else {
			renewSucceeded := outcome.err == nil && got.Status == protocol.Granted && got.Remaining > 0 && got.Remaining <= h.cfg.LeaseTTL
			releasedWon := errors.Is(outcome.err, protocol.ErrLeaseLost) && got.Status == protocol.Released
			if !renewSucceeded && !releasedWon {
				h.t.Fatalf("Renew racing Release: %+v err=%v", got, outcome.err)
			}
		}
		if got.LeaseID != grant.LeaseID || got.Weight != r.Weight {
			h.t.Fatalf("race changed identity/accounting: %+v", got)
		}
	}
	h.must(protocol.Observe, r, "", protocol.Released)
	h.wantError(protocol.Renew, r, grant.LeaseID, protocol.ErrLeaseLost)
	g := h.must(protocol.Observe, next, "", protocol.Granted)
	h.must(protocol.Observe, last, "", protocol.Queued)
	h.must(protocol.Release, next, g.LeaseID, protocol.Released)
	g = h.must(protocol.Observe, last, "", protocol.Granted)
	h.must(protocol.Release, last, g.LeaseID, protocol.Released)
}

// cancelledOperations verifies already-canceled contexts do not advance backend state.
func cancelledOperations(h *harness) {
	r := h.request("holder", h.cfg.Admission.Capacity)
	grant := h.must(protocol.Submit, r, "", protocol.Granted)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, kind := range []protocol.Operation{protocol.Submit, protocol.Observe, protocol.Cancel, protocol.Renew, protocol.Release} {
		got, err := Invoke(ctx, h.driver, protocol.Command{Kind: kind, Request: r, LeaseID: grant.LeaseID})
		if !errors.Is(err, context.Canceled) || got != (protocol.Result{}) {
			h.t.Fatalf("pre-cancelled %s: %+v err=%v", kind, got, err)
		}
	}
	got := h.must(protocol.Observe, r, "", protocol.Granted)
	if got.LeaseID != grant.LeaseID || got.Remaining > grant.Remaining || got.Revision != grant.Revision+1 {
		h.t.Fatalf("pre-cancelled commands mutated state: before=%+v after=%+v", grant, got)
	}
	h.must(protocol.Release, r, grant.LeaseID, protocol.Released)
}
