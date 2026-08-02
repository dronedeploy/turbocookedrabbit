package tcr

import (
	"runtime"
	"testing"
	"time"
)

// Test_forwardError_doesNotLeakGoroutines reproduces the second EAI-1172 leak:
// forwardError used to spawn "go func() { cp.errors <- err }()" - a goroutine
// that blocks forever if nobody reads Errors() (cp.errors was unbuffered, and
// none of stormie's code reads ConnectionPool.Errors()). Every pool-level
// error (connection dial failures, cache-channel recreation failures, etc.)
// leaked one goroutine, permanently.
func Test_forwardError_doesNotLeakGoroutines(t *testing.T) {
	cp := &ConnectionPool{errors: make(chan error, 100)}

	runtime.GC()
	before := runtime.NumGoroutine()

	for i := 0; i < 500; i++ {
		cp.forwardError(errTest)
	}

	// Give any (buggy) leaked goroutines a moment to have been scheduled and
	// parked on the blocking send, then confirm none are still alive.
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	after := runtime.NumGoroutine()

	if after > before {
		t.Fatalf("forwardError leaked goroutines: before=%d after=%d (500 calls, nobody reading Errors())", before, after)
	}
}

var errTest = &testError{}

type testError struct{}

func (e *testError) Error() string { return "test error" }
