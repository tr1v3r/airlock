package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tr1v3r/airlock/protocol"
	"github.com/tr1v3r/airlock/internal/driverconformance"
)

// A structurally valid stored grant must not be overwritten by an identity or
// configuration conflict. Unlike business rejections that perform maintenance,
// these transitions return no next snapshot and the transaction must roll back.
func TestConflictingCommandsRollbackWithoutPersist(t *testing.T) {
	for _, conflict := range []string{"owner", "configuration"} {
		for _, operation := range []protocol.Operation{protocol.Submit, protocol.Observe, protocol.Cancel, protocol.Renew, protocol.Release} {
			t.Run(conflict+"/"+string(operation), func(t *testing.T) {
				d, mock := mockDriver(t)
				now := time.Now().UTC().Truncate(time.Millisecond)
				r := request(now)
				snapshot, grant, err := protocol.ApplySnapshot(nil, now, protocol.Command{Kind: protocol.Submit, Request: r})
				if err != nil || grant.Status != protocol.Granted {
					t.Fatalf("seed: %+v err=%v", grant, err)
				}
				wrong := r
				want := protocol.ErrRequestConflict
				if conflict == "owner" {
					wrong.Owner = protocol.NewRequest(now).Owner
				} else {
					wrong.Config.Admission.Capacity++
					want = protocol.ErrConfigConflict
				}
				beforeTransition(mock, snapshot, now.Add(time.Millisecond))
				// No UPDATE or COMMIT expectation: either would fail this test.
				mock.ExpectRollback()
				got, err := driverconformance.Invoke(context.Background(), d, protocol.Command{Kind: operation, Request: wrong, LeaseID: grant.LeaseID})
				if !errors.Is(err, want) || got != (protocol.Result{}) {
					t.Fatalf("conflicting %s exposed result: %+v err=%v want=%v", operation, got, err, want)
				}
			})
		}
	}
}

func TestBackwardBackendClockRollsBackValidSnapshot(t *testing.T) {
	d, mock := mockDriver(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := request(now)
	snapshot, grant, err := protocol.ApplySnapshot(nil, now, protocol.Command{Kind: protocol.Submit, Request: r})
	if err != nil || grant.Status != protocol.Granted {
		t.Fatalf("seed: %+v err=%v", grant, err)
	}
	beforeTransition(mock, snapshot, now.Add(-time.Millisecond))
	mock.ExpectRollback()
	got, err := d.Renew(context.Background(), protocol.LeaseRef{Request: r, ID: grant.LeaseID})
	if !errors.Is(err, protocol.ErrCorruptState) || got != (protocol.Result{}) {
		t.Fatalf("backward clock returned usable renewal: %+v err=%v", got, err)
	}
}
