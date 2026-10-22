// Package airlock provides context-aware concurrency limits without worker
// management or task completion tracking. Each acquisition holds one token;
// weighted acquisition is not yet implemented.
package airlock
