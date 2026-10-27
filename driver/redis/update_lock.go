package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// WithUpdateLockTTL sets the per-resource update lease (default one second).
// Positive values round up to whole milliseconds. There is no background renewal:
// if computation outlasts the lease, its commit is rejected and retried from a
// fresh snapshot. This lease is separate from admitted application leases.
func WithUpdateLockTTL(ttl time.Duration) Option {
	return func(d *Driver) error {
		if ttl <= 0 {
			return errors.New("airlock/redis: update lock TTL must be positive")
		}
		d.updateLockTTL = ttl
		return nil
	}
}

// updateLockKey shares the state's Redis Cluster hash tag and isolation domain.
func updateLockKey(stateKey string) string {
	return strings.TrimSuffix(stateKey, ":state") + ":update-lock"
}

// tryUpdateLock returns true only on a confirmed fresh SET NX PX success.
// Even the same owner cannot reacquire/renew an existing lock.
func (d *Driver) tryUpdateLock(ctx context.Context, key, owner string) (bool, error) {
	milliseconds := int64(d.updateLockTTL / time.Millisecond)
	if d.updateLockTTL%time.Millisecond != 0 {
		milliseconds++
	}
	raw, err := d.eval(ctx, acquireUpdateLockScript, updateLockKey(key), owner, milliseconds)
	if err != nil {
		return false, fmt.Errorf("airlock/redis: acquire update lock: %w", err)
	}
	value, ok := raw.(int64)
	if !ok || (value != 0 && value != 1) {
		return false, errors.New("airlock/redis: malformed update lock response")
	}
	return value == 1, nil
}

// releaseUpdateLock cannot remove a successor. Cleanup also runs after caller
// cancellation with an independent, bounded context. A cleanup failure cannot
// undo a committed command or turn an uncertain one into success; TTL recovers it.
func (d *Driver) releaseUpdateLock(ctx context.Context, key, owner string) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), min(d.timeout, 100*time.Millisecond))
	defer cancel()
	_, _ = d.eval(cleanup, releaseUpdateLockScript, updateLockKey(key), owner)
}
