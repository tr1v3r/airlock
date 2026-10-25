// Package protocol defines shared admission commands, outcomes, and the reference
// state machine. Transition owns scheduling and lifecycle decisions; drivers own
// backend time, exclusive access or CAS, persistence, and ambiguous I/O outcomes.
// It performs no backend I/O and does not depend on the root airlock package.
package protocol
