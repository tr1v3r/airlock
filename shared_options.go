package airlock

import "time"

// withDefaults normalizes client configuration before validation and snapshotting.
// Direct Driver/Transition callers still supply a complete ResourceConfig.
// Only zero selects a default; invalid nonzero values remain validation errors.
func (o SharedOptions) withDefaults() SharedOptions {
	if o.Config.LeaseTTL == 0 {
		o.Config.LeaseTTL = 30 * time.Second
	}

	if o.Config.MaxWait == 0 {
		o.Config.MaxWait = 30 * time.Second
	}

	if o.Config.SubmitWindow == 0 {
		o.Config.SubmitWindow = 5 * time.Second
	}

	if o.Config.Retention == 0 {
		o.Config.Retention = time.Minute
	}

	if o.Config.MaxRecords == 0 {
		o.Config.MaxRecords = 10000
	}

	if o.OperationTimeout == 0 {
		o.OperationTimeout = 5 * time.Second
	}

	if o.PollInterval == 0 {
		o.PollInterval = 50 * time.Millisecond
	}
	return o
}
