package witness

import (
	"testing"
)

func BenchmarkBuildRequestBody(b *testing.B) {
	w := &witness{size: 0}
	proof := make([][]byte, 20)
	for i := range proof {
		proof[i] = []byte("representative-proof-data-block")
	}
	cp := []byte("checkpoint-data-is-usually-longer-and-has-multiple-lines\n")

	b.ResetTimer()
	for b.Loop() {
		_ = w.buildRequestBody(proof, cp)
	}
}
