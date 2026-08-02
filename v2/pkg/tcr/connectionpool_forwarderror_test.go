package tcr

import (
	"runtime"
	"testing"
	"time"
)

// forwardError must not leak a goroutine per call when nobody reads
// Errors() (EAI-1172) - it used to block forever on an unbuffered send.
func Test_forwardError_doesNotLeakGoroutines(t *testing.T) {
	cp := &ConnectionPool{errors: make(chan error, 100)}

	runtime.GC()
	before := runtime.NumGoroutine()

	for i := 0; i < 500; i++ {
		cp.forwardError(errTest)
	}

	// let any leaked goroutines get scheduled before checking
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
