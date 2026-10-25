package scheduler

import "testing"

func FuzzLocalDynamicModel(f *testing.F) {
	// Byte zero selects priority; remaining triples are action/identity/weight.
	// The replay always starts with full capacity and finishes by releasing all
	// grants. Truncation bounds goroutines, queue memory and runtime per input.
	f.Add([]byte{0})
	f.Add([]byte{1, 0, 2, 7, 0, 2, 2, 0, 3, 1, 6, 1, 0, 4, 2, 0, 3, 4, 1, 6, 0, 0})
	f.Add([]byte{0, 0, 0, 7, 0, 1, 7, 3, 2, 1, 4, 2, 0, 6, 0, 0, 7, 0, 0})
	f.Add([]byte{1, 0, 2, 0, 0, 2, 8, 3, 3, 8, 0, 6, 3, 0, 5, 4, 5, 2, 0, 7, 1, 0})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) == 0 {
			return
		}
		const maxBytes = 1 + 3*96
		if len(input) > maxBytes {
			input = input[:maxBytes]
		}
		runLocalEvents(t, input[0]&1 != 0, input[1:])
	})
}
