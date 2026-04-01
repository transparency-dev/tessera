package witness

import (
	"io"
	"testing"
)

// globalReader is used to store the result of benchmark runs to prevent
// the Go compiler from optimizing away the function call (dead-code elimination).
var globalReader io.Reader

func BenchmarkBuildRequestBody(b *testing.B) {
	w := &witness{size: 0}
	proof := make([][]byte, 20)
	for i := range proof {
		proof[i] = []byte("representative-proof-data-block")
	}
	cp := []byte("checkpoint-data-is-usually-longer-and-has-multiple-lines\n")

	b.ResetTimer()
	for b.Loop() {
		globalReader = w.buildRequestBody(proof, cp)
	}
}
