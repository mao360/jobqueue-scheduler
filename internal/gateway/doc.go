// Package gateway owns the worker registry and job dispatching: which workers
// are connected, what they're doing, how to assign the next job, and what
// happens when a worker disconnects mid-job. Stateful, transport-agnostic.
package gateway