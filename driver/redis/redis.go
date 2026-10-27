// Package redis stores Airlock admission state in one persistent Redis key per
// resource, plus a short-lived per-resource update lock. It uses backend time and
// owner-checked commits within a time window. It never executes work or owns the
// injected client. Durability depends on the Redis deployment.
// Methods require non-nil contexts and follow airlock.Driver recovery rules.
// Confirmed ownership/window misses may retry; ambiguous writes never authorize work.
package redis

import (
	"context"
	"crypto/sha1" // Redis script cache identifier, not a security primitive.
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	goredis "github.com/redis/go-redis/v9"

	"github.com/tr1v3r/airlock"
)

// Driver is a concurrency-safe Redis adapter. Construct it with New.
// Its zero value is invalid. Resource keys have no expiration.
type Driver struct {
	// client is the caller-owned primary-routed Redis transport.
	client goredis.UniversalClient
	// namespace isolates resource hashes without exposing logical names in keys.
	namespace string
	// timeout bounds reads, update-lock contention and guarded commit retries for one operation.
	timeout time.Duration
	// updateLockTTL bounds abandoned ownership; expired owners cannot commit.
	updateLockTTL time.Duration
}

// Option configures the adapter before use.
type Option func(*Driver) error

// WithOperationTimeout bounds the operation, including update-lock contention.
// Best-effort lock cleanup may take up to an additional 100 milliseconds.
// The timeout must be positive. The default is five seconds; an earlier caller
// deadline takes precedence.
func WithOperationTimeout(timeout time.Duration) Option {
	return func(d *Driver) error {
		if timeout <= 0 {
			return errors.New("airlock/redis: timeout must be positive")
		}

		d.timeout = timeout
		return nil
	}
}

// New constructs an adapter without I/O. Client must be a nonnil *redis.Client
// or *redis.ClusterClient with ContextTimeoutEnabled and socket deadline writes
// enabled. Replica routing is rejected. Raw ReadTimeout/WriteTimeout=-1 is
// accepted (context deadlines remain active); -2 and other disabling values are
// rejected after go-redis normalization.
// Namespace must be UTF-8, contain no NUL, and be at most 256 bytes. The caller owns
// the client and must not change options or install hooks retrying ambiguous
// writes. Network retries are disabled per command, preserving client options.
func New(client goredis.UniversalClient, namespace string, options ...Option) (*Driver, error) {
	switch c := client.(type) {
	case *goredis.Client:
		if c == nil || !c.Options().ContextTimeoutEnabled {
			return nil, errors.New("airlock/redis: client requires ContextTimeoutEnabled")
		}
		// Options() is already normalized: raw -1 becomes zero, preserving
		// context deadlines; raw -2 becomes negative and skips Set*Deadline.
		if o := c.Options(); o.ReadTimeout < 0 || o.WriteTimeout < 0 {
			return nil, errors.New("airlock/redis: socket deadlines must be enabled")
		}

	case *goredis.ClusterClient:
		if c == nil {
			return nil, errors.New("airlock/redis: nil cluster client")
		}

		o := c.Options()
		// Cluster normalization leaves raw -2 negative; the node clients
		// would normalize it again to a disabled socket deadline.
		if o.ReadTimeout < 0 || o.WriteTimeout < 0 {
			return nil, errors.New("airlock/redis: cluster socket deadlines must be enabled")
		}
		if !o.ContextTimeoutEnabled || o.ReadOnly || o.RouteByLatency || o.RouteRandomly {
			return nil, errors.New("airlock/redis: cluster requires context deadlines and primary routing")
		}

	default:
		return nil, errors.New("airlock/redis: unsupported or nil client")
	}

	if len(namespace) > 256 || !utf8.ValidString(namespace) || strings.ContainsRune(namespace, 0) {
		return nil, errors.New("airlock/redis: invalid namespace")
	}

	d := &Driver{client: client, namespace: namespace, timeout: 5 * time.Second, updateLockTTL: time.Second}

	for _, option := range options {
		if option == nil {
			return nil, errors.New("airlock/redis: nil option")
		}
		if err := option(d); err != nil {
			return nil, err
		}
	}

	return d, nil
}

// key hashes namespace and resource into one persistent Redis Cluster key.
func (d *Driver) key(resource string) string {
	return fmt.Sprintf("airlock:{%x}:state", sha256.Sum256([]byte(d.namespace+"\x00"+resource)))
}

// noRetryCmd prevents transport replay of a possibly committed protocol operation.
type noRetryCmd struct {
	// Cmd retains the original routing, arguments and response storage.
	*goredis.Cmd
}

// NoRetry marks this command as unsafe to replay after an ambiguous response.
func (*noRetryCmd) NoRetry() bool { return true }

// eval tries EVALSHA once, then EVAL only when NOSCRIPT proves no script ran.
func (d *Driver) eval(ctx context.Context, script, key string, args ...any) (any, error) {
	return d.evalKeys(ctx, script, []string{key}, args...)
}

// evalKeys declares every script key explicitly for Redis Cluster routing.
func (d *Driver) evalKeys(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	hash := fmt.Sprintf("%x", sha1.Sum([]byte(script)))

	for _, head := range []struct{ name, body string }{{"evalsha", hash}, {"eval", script}} {
		argv := []any{head.name, head.body, len(keys)}
		for _, key := range keys {
			argv = append(argv, key)
		}
		argv = append(argv, args...)
		cmd := &noRetryCmd{goredis.NewCmd(ctx, argv...)}
		cmd.SetFirstKeyPos(3)
		err := d.client.Process(ctx, cmd)
		if err == nil {
			return cmd.Result()
		}
		if head.name == "evalsha" && goredis.HasErrorPrefix(err, "NOSCRIPT") {
			continue
		}
		return nil, err
	}

	return nil, errors.New("airlock/redis: script unavailable")
}

// maxExactMicro is the largest integer exactly representable by Redis Lua numbers (2^53 - 1).
const maxExactMicro int64 = 9007199254740991

// micro encodes exact positive Unix microseconds; zero time denotes an unbounded window.
func micro(t time.Time) (int64, error) {
	if t.IsZero() {
		return 0, nil
	}

	n := t.UnixMicro()
	if n <= 0 || n > maxExactMicro || t.Nanosecond()%1000 != 0 {
		return 0, errors.New("airlock/redis: clock outside exact microsecond range")
	}
	return n, nil
}

// decodeRead validates the atomic snapshot/existence/time reply before computing a transition.
func decodeRead(raw any) ([]byte, time.Time, error) {
	values, ok := raw.([]any)
	if !ok || len(values) != 4 {
		return nil, time.Time{}, errors.New("airlock/redis: malformed read response")
	}

	exists, ok := values[0].(int64)
	state, okState := values[1].(string)
	sec, okSec := values[2].(string)
	us, okUs := values[3].(string)
	if !ok || (exists != 0 && exists != 1) || !okState || !okSec || !okUs || (exists == 0 && state != "") {
		return nil, time.Time{}, errors.New("airlock/redis: malformed read fields")
	}

	seconds, e1 := strconv.ParseInt(sec, 10, 64)
	micros, e2 := strconv.ParseInt(us, 10, 64)
	if e1 != nil || e2 != nil || seconds <= 0 || seconds > maxExactMicro/1000000 || micros < 0 || micros >= 1000000 {
		return nil, time.Time{}, errors.New("airlock/redis: invalid server time")
	}

	now := time.Unix(seconds, micros*1000).UTC()
	if _, err := micro(now); err != nil {
		return nil, time.Time{}, err
	}
	if exists == 1 && state == "" {
		return nil, time.Time{}, airlock.ErrCorruptState
	}
	return []byte(state), now, nil
}

// execute atomically evaluates a protocol command. Business rejections commit
// maintenance changes. Transport/script errors return a zero Result and are not
// retried: a write may have succeeded. Recover with the original request identity.
// Lock contention and confirmed ownership or clock-window misses retry.
func (d *Driver) execute(ctx context.Context, command airlock.Command) (airlock.Result, error) {
	if err := ctx.Err(); err != nil {
		return airlock.Result{}, err
	}
	if d == nil || d.client == nil {
		return airlock.Result{}, errors.New("airlock/redis: uninitialized driver")
	}
	if err := airlock.ValidateCommand(command); err != nil {
		return airlock.Result{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	key := d.key(command.Request.Resource)

	for attempt := 0; ; attempt++ {
		if command.Kind == airlock.Observe {
			if result, ready, err := d.observe(ctx, command.Request); ready || err != nil {
				return result, err
			}
		}

		result, retry, err := d.executeUpdate(ctx, command, key)
		if !retry {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return airlock.Result{}, ctxErr
			}
			return result, err
		}

		timer := time.NewTimer(time.Duration(1+min(attempt, 9)) * time.Millisecond)

		select {
		case <-ctx.Done():
			timer.Stop()
			return airlock.Result{}, ctx.Err()
		case <-timer.C:
		}
	}
}

// executeUpdate takes ownership before loading or computing a complete state.
// Known lock/ownership/window misses retry; ambiguous writes never do.
func (d *Driver) executeUpdate(ctx context.Context, command airlock.Command, key string) (airlock.Result, bool, error) {
	owner := airlock.NewRequest(time.Now()).Owner
	acquired, err := d.tryUpdateLock(ctx, key, owner)
	if err != nil {
		// Acquisition may have succeeded. Its TTL bounds abandoned ownership.
		return airlock.Result{}, false, err
	}
	if !acquired {
		return airlock.Result{}, true, nil
	}
	defer d.releaseUpdateLock(ctx, key, owner)

	// Another owner may have completed maintenance while we waited. Recheck
	// under the lock before transferring a snapshot or restoring the scheduler.
	if command.Kind == airlock.Observe {
		if result, ready, err := d.observe(ctx, command.Request); ready || err != nil {
			return result, false, err
		}
	}

	raw, err := d.eval(ctx, readScript, key)
	if err != nil {
		return airlock.Result{}, false, fmt.Errorf("airlock/redis: read: %w", err)
	}

	snapshot, now, err := decodeRead(raw)
	if err != nil {
		return airlock.Result{}, false, err
	}

	state, decodeErr := airlock.DecodeState(snapshot)
	if decodeErr != nil {
		return airlock.Result{}, false, decodeErr
	}

	nextState, result, window, transitionErr := airlock.Transition(state, now, command)
	var next []byte
	if nextState != nil {
		var encodeErr error
		next, encodeErr = airlock.EncodeState(nextState)
		if encodeErr != nil {
			return airlock.Result{}, false, encodeErr
		}
	}

	if next == nil {
		return result, false, transitionErr
	}

	lower, err := micro(window.NotBefore)
	if err != nil {
		return airlock.Result{}, false, err
	}

	upper, err := micro(window.Before)
	if err != nil {
		return airlock.Result{}, false, err
	}

	projection, err := index(nextState)
	if err != nil {
		return airlock.Result{}, false, err
	}

	raw, err = d.evalKeys(
		ctx, commitScript, []string{key, updateLockKey(key)},
		owner, string(next), lower, upper, string(projection),
	)
	if err != nil {
		return airlock.Result{}, false, fmt.Errorf("airlock/redis: commit: %w", err)
	}

	committed, ok := raw.(int64)
	if !ok || (committed != 0 && committed != 1) {
		return airlock.Result{}, false, errors.New("airlock/redis: malformed commit response")
	}

	if committed == 1 {
		if err := ctx.Err(); err != nil {
			return airlock.Result{}, false, err
		}
		return result, false, transitionErr
	}
	return airlock.Result{}, true, nil
}

// Verify the Redis backend implements the public Driver contract.
var _ airlock.Driver = (*Driver)(nil)

// Submit atomically submits the whole request without refreshing an existing lease.
func (d *Driver) Submit(ctx context.Context, request airlock.Request) (airlock.Result, error) {
	return d.execute(ctx, airlock.Command{Kind: airlock.Submit, Request: request})
}

// Observe reads the committed outcome, advancing expiry and scheduling when due.
func (d *Driver) Observe(ctx context.Context, request airlock.Request) (airlock.Result, error) {
	return d.execute(ctx, airlock.Command{Kind: airlock.Observe, Request: request})
}

// Cancel cancels the request, including a grant or a cancellation before submission.
func (d *Driver) Cancel(ctx context.Context, request airlock.Request) (airlock.Result, error) {
	return d.execute(ctx, airlock.Command{Kind: airlock.Cancel, Request: request})
}

// Renew extends a live lease matching the original request and lease ID.
func (d *Driver) Renew(ctx context.Context, lease airlock.LeaseRef) (airlock.Result, error) {
	return d.execute(ctx, airlock.Command{Kind: airlock.Renew, Request: lease.Request, LeaseID: lease.ID})
}

// Release ends a matching lease and returns its whole weight at most once.
func (d *Driver) Release(ctx context.Context, lease airlock.LeaseRef) (airlock.Result, error) {
	return d.execute(ctx, airlock.Command{Kind: airlock.Release, Request: lease.Request, LeaseID: lease.ID})
}
