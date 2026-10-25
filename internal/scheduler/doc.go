// Package scheduler implements the common weighted tenant scheduling policy.
// Local admission uses its synchronized methods. The protocol state machine
// restores an exclusively owned instance and accesses its scheduling fields
// directly; those accesses must never race with synchronized local operations.
package scheduler
