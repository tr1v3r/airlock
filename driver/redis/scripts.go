package redis

import _ "embed"

// observeScript returns only the resource header, one request and backend time.
// Candidate publication tags are checked in Go; missing/incomplete indexes fall back.
//
//go:embed observe.lua
var observeScript string

// acquireUpdateLockScript installs a bounded owner lease without refreshing an
// existing owner's TTL. The integer response distinguishes contention from I/O errors.
//
//go:embed acquire_update_lock.lua
var acquireUpdateLockScript string

// releaseUpdateLockScript never deletes a lock belonging to a newer updater.
//
//go:embed release_update_lock.lua
var releaseUpdateLockScript string

// partitionRead reads all active state, the target and expired history only.
// The expiry index and hash are authoritative together; incomplete publications
// remain unavailable rather than silently recreating a resource.
//
//go:embed partition_read.lua
var partitionRead string

// partitionCommit deliberately publishes an unavailable marker before any
// multi-command mutation. Redis scripts do not roll back after runtime errors.
// A failed publication can never expose partial capacity/history as valid.
//
//go:embed partition_commit.lua
var partitionCommit string
