package protocol

import "github.com/tr1v3r/airlock/internal/scheduler"

// Config defines fixed limits for an Airlock. Queue limits count pending
// requests, not granted permits. Zero queue limits disable waiting.
type Config = scheduler.Config

// PriorityGroup reserves waiting space and selection opportunities for a group
// of tenants. It does not reserve execution capacity or preempt running work.
type PriorityGroup = scheduler.PriorityGroup

// PriorityPolicy maps trusted tenant IDs to fixed priority groups. It is part of
// the immutable resource configuration and must not change between commands.
type PriorityPolicy = scheduler.PriorityPolicy

// ErrInvalidConfig reports invalid limits or a priority policy.
var ErrInvalidConfig = scheduler.ErrInvalidConfig

// ErrInvalidTenant reports an empty explicitly supplied tenant ID.
var ErrInvalidTenant = scheduler.ErrInvalidTenant

// ErrInvalidWeight reports a nonpositive requested token count.
var ErrInvalidWeight = scheduler.ErrInvalidWeight

// ErrWeightExceedsCapacity reports a request larger than the fixed capacity.
var ErrWeightExceedsCapacity = scheduler.ErrWeightExceedsCapacity

// ErrQueueFull reports that the global waiting queue is full.
var ErrQueueFull = scheduler.ErrQueueFull

// ErrGroupQueueFull reports that the tenant's priority group waiting queue is full.
var ErrGroupQueueFull = scheduler.ErrGroupQueueFull

// ErrTenantQueueFull reports that the tenant's waiting queue is full.
var ErrTenantQueueFull = scheduler.ErrTenantQueueFull

// ErrWouldBlock reports that TryAcquire cannot grant capacity immediately
// without bypassing a queued request. The attempt does not join the queue.
var ErrWouldBlock = scheduler.ErrWouldBlock

func newScheduler(c Config) (*scheduler.Airlock, error) { return scheduler.New(c) }
