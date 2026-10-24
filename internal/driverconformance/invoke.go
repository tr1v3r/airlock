package driverconformance

import (
	"context"

	"github.com/tr1v3r/airlock"
)

// Invoke runs a table-driven protocol test through the named public Driver
// methods. It is a test helper, not an alternate production driver interface.
func Invoke(ctx context.Context, driver airlock.Driver, cmd airlock.Command) (airlock.Result, error) {
	switch cmd.Kind {
	case airlock.Submit:
		return driver.Submit(ctx, cmd.Request)
	case airlock.Observe:
		return driver.Observe(ctx, cmd.Request)
	case airlock.Cancel:
		return driver.Cancel(ctx, cmd.Request)
	case airlock.Renew:
		return driver.Renew(ctx, airlock.LeaseRef{Request: cmd.Request, ID: cmd.LeaseID})
	case airlock.Release:
		return driver.Release(ctx, airlock.LeaseRef{Request: cmd.Request, ID: cmd.LeaseID})
	default:
		return airlock.Result{}, airlock.ErrInvalidRequest
	}
}
