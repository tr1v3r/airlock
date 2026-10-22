// Package airlock provides context-aware concurrency limits without worker
// management or task completion tracking. Each acquisition holds one token;
// weighted acquisition is not yet implemented.
// Pool offers a simple concurrency cap; Gate adds tenant-aware round-robin
// admission and bounded waiting queues. Neither executes user work.
package airlock
