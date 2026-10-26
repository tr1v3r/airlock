package airlock

import "github.com/tr1v3r/airlock/protocol"

// DistributedOptions is the former name for SharedOptions.
//
// Deprecated: Use SharedOptions instead.
type DistributedOptions = SharedOptions

// Distributed is the former name for Shared.
//
// Deprecated: Use Shared instead.
type Distributed = Shared

// NewDistributed constructs shared admission using the former API name.
//
// Deprecated: Use NewShared instead.
func NewDistributed(driver protocol.Driver, options DistributedOptions) (*Distributed, error) {
	return NewShared(driver, options)
}
