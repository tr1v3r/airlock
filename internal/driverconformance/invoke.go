package driverconformance

import (
	"context"

	"github.com/tr1v3r/airlock/protocol"
)

// Invoke runs a table-driven protocol test through the named public Driver
// methods. It is a test helper, not an alternate production driver interface.
func Invoke(ctx context.Context, driver protocol.Driver, cmd protocol.Command) (protocol.Result, error) {
	switch cmd.Kind {
	case protocol.Submit:
		return driver.Submit(ctx, cmd.Request)
	case protocol.Observe:
		return driver.Observe(ctx, cmd.Request)
	case protocol.Cancel:
		return driver.Cancel(ctx, cmd.Request)
	case protocol.Renew:
		return driver.Renew(ctx, protocol.LeaseRef{Request: cmd.Request, ID: cmd.LeaseID})
	case protocol.Release:
		return driver.Release(ctx, protocol.LeaseRef{Request: cmd.Request, ID: cmd.LeaseID})
	default:
		return protocol.Result{}, protocol.ErrInvalidRequest
	}
}
